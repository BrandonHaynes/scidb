/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2014 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

// header groups:
// std C++
#include <iostream>
#include <utility>
// std C
// de-facto standards
#include <boost/foreach.hpp>
#include <boost/fusion/include/std_pair.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/spirit/include/phoenix.hpp>
#include <boost/spirit/include/phoenix_object.hpp>
#include <boost/spirit/include/phoenix_stl.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/variant.hpp>
#include <boost/variant/apply_visitor.hpp>
// SciDB
#include <log4cxx/logger.h>
#include <system/Exceptions.h>
#include <util/Platform.h>
// MPI/ScaLAPACK
#include <DLAErrors.h>
// local
#include "GEMMOptions.hpp"

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.libdense_linear_algebra.ops.gemm"));

namespace scidb
{
namespace qi      = boost::spirit::qi;
namespace phoenix = boost::phoenix;

// for a quick start on boost::spirit sufficient to example this use of it:
// 1) turning an EBNF grammar into a spirit::qi parser for a tiny syntax like this one:
//     http://boost-spirit.com/home/articles/qi-example/parsing-a-list-of-key-value-pairs-using-spirit-qi
// 2) adding error handling to a qi parsing:
//     http://www.boost.org/doc/libs/1_41_0/libs/spirit/doc/html/spirit/qi/tutorials/mini_xml___error_handling.html
// for a broad introduction to qi, the tutorials are listed here:
// 3) http://www.boost.org/doc/libs/1_41_0/libs/spirit/doc/html/spirit/qi/tutorials
//
// 4) http://www.boost.org/doc/libs/1_49_0/libs/spirit/doc/html/spirit/qi/reference/operator/expect.htm

typedef std::string                                     ParsedKey_t;
typedef double                                          ParsedValue_t;
typedef std::pair<ParsedKey_t, ParsedValue_t>           ParsedOption_t;
typedef std::vector<ParsedOption_t>                     ParsedOptions_t;

template <typename Iterator_tt>
struct GEMMOptionParser : qi::grammar<Iterator_tt, ParsedOptions_t()>
{
    //
    // a clean tiny parser for the *exact* language of a legal GEMM option string
    //
    GEMMOptionParser()
      : GEMMOptionParser::base_type(_options, "GEMMOptions")
    {
        // list of options separated by colons:
        _options   = _option > *(';' >> _option);                // after ; another _option is mandatory
        // PROBLEM: here we need _option to be the type of the alternation of
        //          a std::string (the noValkey) and the _keyVal (pair<std::string,double>)
        _option    = _key > '=' > _value;
        _key       = qi::string("ALPHA") 
                   | qi::string("BETA") 
                   | qi::string("TRANSA") 
                   | qi::string("TRANSB");
        _value     = qi::double_ ;

        // names are supposed to support error handling in some way I don't recognize yet
        _options.name("optionsList");
        _option.name ("option");
        _key.name    ("valueKey");
        _value.name  ("value");

        // only syntactically limiting aspect of spirit::qi so far:
        //
        // would like to have a function, errorHandle(_1, _2, _3, _4), however
        // I can't find the expression that gives the types of the arguments
        // within each context.
        // Others complain of this at:
        //     http://boost-spirit.com/home/2010/03/03/the-anatomy-of-semantic-actions-in-qi
        // and here for other issues with creating an on_error<>:
        //     http://comments.gmane.org/gmane.comp.lib.boost.user/67089
        // Workaround is to use an expression that converts the arguments to a known
        // datatype by shifiting them onto an ostream.

        using namespace qi::labels;     // provides _1, _2, _3, _4

        qi::on_error<qi::fail>(_options,
                               _errorMsg << " expecting <option> (;<option>)* "
                                         << _4
                                         << phoenix::val(" here: \"")
                                         << phoenix::construct<std::string>(_3, _2)
                                         << phoenix::val("\"")
                                         << std::endl);
        qi::on_error<qi::fail>(_option,
                               _errorMsg << " expecting a KEY=<value> "
                                         << _4
                                         << phoenix::val(" here: \"")
                                         << phoenix::construct<std::string>(_3, _2)
                                         << phoenix::val("\"")
                                         << std::endl);

        qi::on_error<qi::fail>(_key,
                               _errorMsg << " expecting KEY=<value> "
                                         << _4
                                         << phoenix::val(" here: \"")
                                         << phoenix::construct<std::string>(_3, _2)
                                         << phoenix::val("\"")
                                         << std::endl);

        qi::on_error<qi::fail>(_value,
                               _errorMsg << " expecting <value> "
                                         << _4
                                         << phoenix::val(" here: \"")
                                         << phoenix::construct<std::string>(_3, _2)
                                         << phoenix::val("\"")
                                         << std::endl);
    }

    qi::rule<Iterator_tt, ParsedOptions_t()>    _options;
    qi::rule<Iterator_tt, ParsedOption_t()>     _option;
    qi::rule<Iterator_tt, ParsedKey_t()>        _key;
    qi::rule<Iterator_tt, ParsedValue_t()>      _value;

    std::string                                 errorStr() const { return _errorMsg.str(); }

private:
    std::ostringstream                          _errorMsg;
};

GEMMOptions::GEMMOptions(const std::string& input)
           : transposeA(false), transposeB(false), alpha(1.0), beta(1.0)
{
    if (input.empty()) {
        // there aren't any options to parse, the grammer would give an error
        return;  // all defaults
    }

    GEMMOptionParser<std::string::const_iterator> parser;
    ParsedOptions_t parseResult;

    std::string::const_iterator current = input.begin();

    bool const success    = qi::parse(current, input.end(), parser, parseResult);
    bool const completely = (current == input.end());

    if(!success || !completely) {
        std::ostringstream ss;
        ss << "Error parsing the 4th argument to gemm(), the option string. ";
        if (!success) {
            ss << parser.errorStr();
        } else {
            ss << " successfully parsed '" << std::string(input.begin(), current) << "'" ;
            ss << " but the remainder '"   << std::string(current, input.end()) << "' is not legal syntax";
        }
        throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR46) << ss.str();
    }

    // NOTE: the following loop is the most simple method at this point; however,
    //       there is a way to parse into a struct (better if, e.g. if more keywords added)
    // http://www.boost.org/doc/libs/1_41_0/libs/spirit/doc/html/spirit/qi/tutorials/employee___parsing_into_structs.html

    BOOST_FOREACH (const ParsedOption_t& kv,parseResult)
    {
        LOG4CXX_TRACE(logger, "GEMMOptions: found key: " << kv.first << " value " << kv.second);

        if      (kv.first == "ALPHA")    alpha      = kv.second;
        else if (kv.first == "BETA")     beta       = kv.second;
        else if (kv.first == "TRANSA")   transposeA = bool(kv.second);
        else if (kv.first == "TRANSB")   transposeB = bool(kv.second);
        else                             SCIDB_UNREACHABLE();

        LOG4CXX_TRACE(logger, "GEMMOptions: alpha: "      << alpha);
        LOG4CXX_TRACE(logger, "GEMMOptions: beta: "       << beta);
        LOG4CXX_TRACE(logger, "GEMMOptions: transposeA: " << transposeA);
        LOG4CXX_TRACE(logger, "GEMMOptions: transposeB: " << transposeB);
    }
}

} // namespace scidb
