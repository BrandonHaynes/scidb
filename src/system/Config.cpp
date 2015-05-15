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

/**
 * @file Config.cpp
 *
 * @brief Wrapper around boost::program_options and config parser which
 * consolidate command-line arguments, enviroment variables and config options.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include <string>
#include <stdlib.h>
#include <fstream>
#include <errno.h>

#include <boost/program_options.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/format.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>

#include "lib_json/json.h"

#include "system/Config.h"
#include "system/Exceptions.h"
#include "util/forced_lexical_cast.h"
#include "util/Platform.h"

using namespace std;
using namespace boost;
namespace po = boost::program_options;
namespace alg = boost::algorithm;

namespace scidb
{

/** @see scidb::validate */
struct checked_size {
    checked_size(size_t sz) : sz(sz) {}
    size_t sz;
};

/**
 * @brief Ensure that @c Config::SIZE parameters are indeed unsigned.
 *
 * @description Sadly, @c boost::lexical_cast<size_t>("-1") returns
 * 18446744073709551615 rather than throw @c boost::bad_lexical_cast.
 * As a workaround you can use @c forced_lexical_cast<>() (and we do
 * in several places), but getting the @c boost::program_options
 * library to reject negative numbers for @c Config::SIZE options
 * requires additional machinery: this "custom validator" function.
 *
 * @see forced_lexical_cast.h
 * @see http://www.boost.org/doc/libs/1_55_0/doc/html/program_options/howto.html#idp163429032
 * @see https://svn.boost.org/trac/boost/ticket/5494
 */
void validate(boost::any& v,
              const vector<string>& values,
              checked_size* target_type, size_t)
{
    // Make sure no previous assignment to 'v' was made.
    po::validators::check_first_occurrence(v);

    // Extract first string from 'values'.  If there is more than one
    // string, it's an error, and an exception will be thrown.
    const string& s = po::validators::get_single_string(values);

    // Digits only, no minus signs allowed!
    if (s.find_first_not_of("0123456789") == string::npos) {
        v = boost::any(checked_size(boost::lexical_cast<size_t>(s)));
    } else {
        throw po::validation_error(po::invalid_option_value(s));
    }
}


const char* toString(RepartAlgorithm value)
{
    switch(value) {
    case RepartAuto:
        return "auto";
    case RepartDense:
        return "dense";
    case RepartSparse:
        return "sparse";
    default:
        SCIDB_ASSERT(false);
        return NULL; // make gcc happy
    }
}

static po::value_semantic* optTypeToValSem(Config::ConfigOptionType optionType);

static void stringToVector(const string& str, vector<string>& strs);

// For iterating through map::pair<int32_t, Config::ConfigOption*> in BOOST_FOREACH
typedef pair<int32_t, Config::ConfigOption*> opt_pair;

Config::ConfigAddOption::ConfigAddOption(Config *owner) :
	_owner(owner)
{
}

Config::ConfigAddOption& Config::ConfigAddOption::operator()(
		int32_t option,
		char shortCmdLineArg,
		const std::string &longCmdLineArg,
		const std::string &configOption,
		const std::string &envVariable,
		ConfigOptionType type,
		const std::string &description,
		const boost::any &value,
		bool required)
{
	_owner->addOption(option, shortCmdLineArg, longCmdLineArg, configOption,
			envVariable, type, description, value, required);
	return *this;
}

Config::ConfigAddOption& Config::ConfigAddOption::operator()(
        int32_t option,
        char shortCmdLineArg,
        const std::string &longCmdLineArg,
        const std::string &configOption,
        const std::string &envVariable,
        const std::vector< std::string > &envDefinition,
        const std::string &description,
        const boost::any &value,
        bool required)
{
    _owner->addOption(option, shortCmdLineArg, longCmdLineArg, configOption,
            envVariable, envDefinition, description, value, required);
    return *this;
}

void Config::ConfigOption::init(const boost::any &value)
{
    if (!value.empty())
    {
        setValue(value);
    }
    else
    {
        // If we not have default value but option not required, so
        // throw exception here to avoid getting unsetted option in future.
        //TODO: exception here?
        if (!_required)
            assert(0);
    }
}

Config::ConfigOption::ConfigOption(
		char shortCmdLineArg,
		const std::string &longCmdLineArg,
		const std::string &configOption,
		const std::string &envVariable,
		ConfigOptionType type,
		const std::string &description,
		const boost::any &value,
		bool required) :
	_short(shortCmdLineArg),
	_long(longCmdLineArg),
	_config(configOption),
	_env(envVariable),
	_type(type),
	_required(required),
	_activated(false),
	_description(description)
{
    init(value);
}

Config::ConfigOption::ConfigOption(
        char shortCmdLineArg,
        const std::string &longCmdLineArg,
        const std::string &configOption,
        const std::string &envVariable,
        const std::vector< std::string > &envDefinition,
        const std::string &description,
        const boost::any &value,
        bool required) :
    _short(shortCmdLineArg),
    _long(longCmdLineArg),
    _config(configOption),
    _env(envVariable),
    _type(Config::SET),
    _set(envDefinition),
    _required(required),
    _activated(false),
    _description(description)
{
    SCIDB_ASSERT(_set.empty() == false);
    _description += " Possible values: [";
    for (size_t i = 0, count = _set.size(); i < count; ++i) {
        if (i != 0) {
            _description += ",";
        }
        _description += _set[i];
    }
    _description += " ]";
    init(value);
}

void Config::ConfigOption::setValue(const std::string& value)
{
    switch(_type) {
    case STRING: {
        _value = boost::any(value);
        break;
    }
    case SET: {
        int result = -1;
        int position = 0;
        BOOST_FOREACH(const std::string& element, _set) {
            if (element == value) {
                result = position;
                break;
            } else {
                ++position;
            }
        }
        if (result == -1) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION,
                                   SCIDB_LE_ERROR_NEAR_CONFIG_OPTION)
                    << (std::string("invalid value \"") + value + "\"")
                    << getConfigName();
        } else {
            _value = boost::any(result);
        }
        break;
    }
    case INTEGER:
    case SIZE:
    case REAL:
    case BOOLEAN:
    case STRING_LIST:
    default:
        SCIDB_ASSERT(false);
    }
}

void Config::ConfigOption::setValue(int value)
{
    SCIDB_ASSERT(_type == INTEGER);
    _value = boost::any(value);
}

void Config::ConfigOption::setValue(size_t value)
{
    SCIDB_ASSERT(_type == SIZE);
    _value = boost::any(value);
}

void Config::ConfigOption::setValue(double value)
{
    SCIDB_ASSERT(_type == REAL);
    _value = boost::any(value);
}

void Config::ConfigOption::setValue(bool value)
{
    SCIDB_ASSERT(_type == BOOLEAN);
    _value = boost::any(value);
}

void Config::ConfigOption::setValue(const std::vector< std::string >& value)
{
    SCIDB_ASSERT(_type == STRING_LIST);
    _value = boost::any(value);
}

void Config::ConfigOption::setValue(const boost::any &value)
{
    // Just runtime check of values types which will be stored in boost::any.
    // Exception will be thrown if value type in any not match specified.
    switch(_type)
    {
    case SET:
    case STRING:
        setValue(boost::any_cast<std::string>(value));
        break;
    case INTEGER:
        setValue(boost::any_cast<int>(value));
        break;
    case SIZE:
        setValue(boost::any_cast<size_t>(value));
        break;
    case REAL:
        setValue(boost::any_cast<double>(value));
        break;
    case BOOLEAN:
        setValue(boost::any_cast<bool>(value));
        break;
    case STRING_LIST:
        setValue(boost::any_cast< std::vector< std::string > >(value));
        break;
    default:
        //TODO: Throw scidb's exceptions here?
        assert(false);
    }
}

std::string Config::ConfigOption::getValueAsString() const
{
    switch (_type)
    {
    case Config::BOOLEAN:
        return boost::lexical_cast<std::string>(boost::any_cast<bool>(_value));
        break;
    case Config::STRING:
        return boost::any_cast<std::string>(_value);
    case Config::SET:
        return _set[boost::any_cast<int>(_value)];
    case Config::INTEGER:
        return boost::lexical_cast<std::string>(boost::any_cast<int>(_value));
    case Config::SIZE:
        return boost::lexical_cast<std::string>(boost::any_cast<size_t>(_value));
    case Config::REAL:
        return boost::lexical_cast<std::string>(boost::any_cast<double>(_value));
    default:
        SCIDB_UNREACHABLE();
    }
    return "";
}

Config::ConfigAddOption Config::addOption(
		int32_t option,
		char shortCmdLineArg,
		const std::string &longCmdLineArg,
		const std::string &configOption,
		const std::string &envVariable,
		ConfigOptionType type,
		const std::string &description,
		const boost::any &value,
		bool required)
{
	// For accessing command line arguments, long argument always must be defined
	assert (!(shortCmdLineArg != 0 && longCmdLineArg == ""));

	_longArgToOption[longCmdLineArg] = option;

    _values[option] = new Config::ConfigOption(
                shortCmdLineArg,
                longCmdLineArg,
                configOption,
                envVariable,
                type,
                description,
                value,
                required);
	return ConfigAddOption(this);
}

Config::ConfigAddOption Config::addOption(
        int32_t option,
        char shortCmdLineArg,
        const std::string &longCmdLineArg,
        const std::string &configOption,
        const std::string &envVariable,
        const std::vector< std::string > &envDefinition,
        const std::string &description,
        const boost::any &value,
        bool required)
{
    // For accessing command line arguments, long argument always must be defined
    assert (!(shortCmdLineArg != 0 && longCmdLineArg == ""));

    _longArgToOption[longCmdLineArg] = option;

    _values[option] = new Config::ConfigOption(
                shortCmdLineArg,
                longCmdLineArg,
                configOption,
                envVariable,
                envDefinition,
                description,
                envDefinition[boost::any_cast<int>(value)],
                required);

    return ConfigAddOption(this);
}

std::string Config::toString()
{
    stringstream ss;
    BOOST_FOREACH(opt_pair p, _values)
    {
        ConfigOption *opt = p.second;
        assert(opt);
        ss << opt->getLongName() << " : " << opt->getValueAsString() << endl;
    }
    return ss.str();
}

void Config::parse(int argc, char **argv, const char* configFileName)
{
    _configFileName = configFileName;

    /*
     * Loading environment variables
     */
    BOOST_FOREACH(opt_pair p, _values)
    {
        ConfigOption *opt = p.second;
        if (opt->getEnvName() == "")
            continue;
        char *env = getenv(opt->getEnvName().c_str());
        if (env == NULL)
            continue;

        switch (opt->getType())
        {
        case Config::BOOLEAN:
            opt->setValue(lexical_cast<bool>(env));
            break;
        case Config::STRING:
        case Config::SET:
            opt->setValue(lexical_cast<string>(env));
            break;
        case Config::INTEGER:
            opt->setValue(lexical_cast<int>(env));
            break;
        case Config::SIZE:
            opt->setValue(forced_lexical_cast<size_t>(env));
            break;
        case Config::REAL:
            opt->setValue(lexical_cast<double>(env));
            break;
        case Config::STRING_LIST:
            {
                vector<string> strs;
                stringToVector(env, strs);
                opt->setValue(strs);
            }
            break;
        }
        opt->setActivated();

        BOOST_FOREACH(void (*hook)(int32_t), _hooks)
        {
            hook(p.first);
        }
    }

    /*
     * Parsing command line arguments
     */
    po::options_description argsDesc;
    po::options_description helpDesc;

    BOOST_FOREACH(opt_pair p, _values)
    {
        ConfigOption *opt = p.second;
        if (opt->getLongName() == "")
            continue;

        string arg;
        if (opt->getShortName()) {
            arg = str(format("%s,%c") % opt->getLongName() % opt->getShortName());
        } else {
            arg = opt->getLongName();
        }
	
        switch(opt->getType())
        {
        case Config::BOOLEAN:
            helpDesc.add_options()(arg.c_str(),
                                   opt->getDescription().c_str());
            if (!boost::any_cast<bool>(opt->getValue())) {
                argsDesc.add_options()(arg.c_str(),
                                       opt->getDescription().c_str());
            } else { 
                argsDesc.add_options()(arg.c_str(),
                                       optTypeToValSem(opt->getType()),
                                       opt->getDescription().c_str());
            }
            break;
        default:
            helpDesc.add_options()
                (arg.c_str(),
                 optTypeToValSem(opt->getType()),
                 opt->getDescription().c_str());
            argsDesc.add_options()
                (arg.c_str(),
                 optTypeToValSem(opt->getType()),
                 opt->getDescription().c_str());
        }
    }

    stringstream desrcStream;
    desrcStream << helpDesc;
    _description = desrcStream.str();

    po::variables_map cmdLineArgs;
    try
    {
        po::store(po::parse_command_line(argc, argv, argsDesc), cmdLineArgs);
    }
    catch (const po::error &e)
    {
        cerr << "Error during options parsing: " << e.what()
             << ". Use --help option for details." << endl;
        ::exit(1);
    }
    catch (const std::exception &e)
    {
        cerr << "Unknown exception during options parsing: " << e.what()
             << " [" << typeid(e).name() << ']' << endl;
        ::exit(1);
    }
    catch (...)
    {
        cerr << "Unknown non-exception during options parsing" << endl;
        ::exit(1);
    }

    notify(cmdLineArgs);

	BOOST_FOREACH(opt_pair p, _values)
	{
		ConfigOption *opt = p.second;

        if (cmdLineArgs.count(opt->getLongName()))
		{
            switch (opt->getType())
            {
            case Config::BOOLEAN:
                // If the default value is false, the presence of the option in the command line indicates a true.
                // For example, to enable watchdog, the command line has "--no-watchdog".
                opt->setValue(!boost::any_cast<bool>(opt->getValue()) ?
                              true :
                              cmdLineArgs[opt->getLongName()].empty() || cmdLineArgs[opt->getLongName()].as<bool>()
                    );
                break;
            case Config::SET:
            case Config::STRING:
                opt->setValue(cmdLineArgs[opt->getLongName()].as<string>());
                break;
            case Config::INTEGER:
                opt->setValue(cmdLineArgs[opt->getLongName()].as<int>());
                break;
            case Config::SIZE:
                {
                    checked_size cs = cmdLineArgs[opt->getLongName()].as<checked_size>();
                    opt->setValue(cs.sz);
                }
                break;
            case Config::REAL:
                opt->setValue(cmdLineArgs[opt->getLongName()].as<double>());
                break;
            case Config::STRING_LIST:
                opt->setValue(cmdLineArgs[opt->getLongName()].as<std::vector< std::string> >());
            }

            opt->setActivated();
			
			BOOST_FOREACH(void (*hook)(int32_t), _hooks)
			{
				hook(p.first);
			}
		}
	}

    /*
     * Parsing config file. Though config file parsed after getting environment
     * variables and command line arguments it not overwrite already setted values
     * because config has lowest priority.
     */
    if (_configFileName != "")
    {
        Json::Value root;
        Json::Reader reader;
        ifstream ifile(_configFileName.c_str());

        if (!ifile.is_open())
        {
            throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_CANT_OPEN_FILE)
                << _configFileName << ::strerror(errno) << errno;
        }

        string str((istreambuf_iterator<char>(ifile)), istreambuf_iterator<char>());
        alg::trim(str);
        if (!("" == str))
        {
            ifile.seekg(0, ios::beg);
            const bool parsed = reader.parse(ifile, root);
            ifile.close();
            if (parsed)
            {
                //Dumb nested loops search items from config file in defined Config options
                //if some not exists in Config we fail with error.
                BOOST_FOREACH(const string &member, root.getMemberNames())
                {
                    bool found = false;
                    BOOST_FOREACH(const opt_pair &p, _values)
                    {
                        if (p.second->getConfigName() == member)
                        {
                            found = true;
                            break;
                        }
                    }

                    if (!found)
                    {
                        throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_UNKNOWN_CONFIG_OPTION) << member;
                    }
                }


                BOOST_FOREACH(opt_pair p, _values)
                {
                    ConfigOption *opt = p.second;

                    if (!opt->getActivated())
                    {
                        try
                        {
                            switch (opt->getType())
                            {
                                case Config::BOOLEAN:
                                {
                                    if (root.isMember(opt->getConfigName())) {
                                        opt->setValue(bool(root[opt->getConfigName()].asBool()));
                                        opt->setActivated();
                                    }
                                    break;
                                }
                                case Config::STRING:
                                case Config::SET:
                                {
                                    if (root.isMember(opt->getConfigName())) {
                                        std::string value = root[opt->getConfigName()].asString();
                                        opt->setValue(value);
                                        opt->setActivated();
                                    }
                                    break;
                                }
                                case Config::INTEGER:
                                {
                                    if (root.isMember(opt->getConfigName())) {
                                        int value = root[opt->getConfigName()].asInt();
                                        opt->setValue(value);
                                        opt->setActivated();
                                    }
                                    break;
                                }
                                case Config::SIZE:
                                {
                                    if (root.isMember(opt->getConfigName())) {
                                        size_t value = root[opt->getConfigName()].asUInt();
                                        opt->setValue(value);
                                        opt->setActivated();
                                    }
                                    break;
                                }
                                case Config::REAL:
                                {
                                    if (root.isMember(opt->getConfigName())) {
                                        double value = root[opt->getConfigName()].asDouble();
                                        opt->setValue(value);
                                        opt->setActivated();
                                    }
                                    break;
                                }
                                case Config::STRING_LIST:
                                {
                                    if (root.isMember(opt->getConfigName())) {
                                        vector<string> strs;
                                        const Json::Value lst = root[opt->getConfigName()];
                                        for (unsigned int i = 0; i < lst.size(); i++) {
                                            strs.push_back(lst[i].asString());
                                        }
                                        opt->setValue(strs);
                                        opt->setActivated();
                                    }
                                    break;
                                }
                            }
                        }
                        catch(const std::exception &e)
                        {
                            throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_ERROR_NEAR_CONFIG_OPTION) << e.what() << opt->getConfigName();
                        }

                        if (opt->getActivated()) {
                            BOOST_FOREACH(void (*hook)(int32_t), _hooks)
                            {
                                hook(p.first);
                            }
                        }
                    }
                }
            }
            else
            {
                throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_ERROR_IN_CONFIGURATION_FILE) << reader.getFormatedErrorMessages();
            }
        }
        else
        {
            ifile.close();
        }
    }
	
	BOOST_FOREACH(opt_pair p, _values)
	{
		ConfigOption *opt = p.second;
        if ((opt->getRequired() && !opt->getActivated())
            || (!opt->getRequired() && opt->getValue().empty()))
		{
			//FIXME: Replace with scidb exception
			stringstream ss;
			ss << "One of program options required but value not set or value is empty. "
				  "You can set this option with next way(s):\n";
            if (opt->getShortName() != 0 || opt->getLongName() != "")
			{
				ss << "* Through command line argument ";
                if (opt->getShortName())
                    ss << "-" << opt->getShortName();
				
                if (opt->getLongName() != "")
				{
                    if (opt->getShortName() != 0)
						ss << " (";
                    ss << "--" << opt->getLongName();
                    if (opt->getShortName() != 0)
						ss << ")";
				}
				ss << endl;
			}
			
            if (opt->getEnvName() != "")
			{
                ss << "* Through environment variable '" << opt->getEnvName() << "'" << endl;
			}

            if (opt->getConfigName() != "")
			{
                ss << "* Through config variable '" << opt->getConfigName() << "'" << endl;
			}

			cerr << ss.str();
            ::exit(1);
		}
	}
}

Config::~Config()
{
	for(std::map<int32_t, ConfigOption*>::iterator it = _values.begin();
			it != _values.end(); ++it)
	{
		delete it->second;
	}
}


void Config::addHook(void (*hook)(int32_t))
{
	_hooks.push_back(hook);
}

//TODO: Will be good to support more then one config file for loading
//e.g system configs from /etc and user config from ~/.config/ for different
//utilites
void Config::setConfigFileName(const std::string& configFileName)
{
    _configFileName = configFileName;
}

const std::string& Config::getDescription() const
{
    return _description;
}

const std::string& Config::getConfigFileName() const
{
    return _configFileName;
}

bool Config::optionActivated(int32_t option)
{
    assert(_values[option]);
    return _values[option]->getActivated();
}

void Config::setOption(int32_t option, const boost::any &value)
{
    assert(_values[option]);
    _values[option]->setValue(value);
}


std::string Config::setOptionValue(std::string const& name, std::string const& newValue)
{
    std::map<std::string, int32_t>::const_iterator i = _longArgToOption.find(name);
    if (i == _longArgToOption.end())
        throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_UNKNOWN_CONFIG_OPTION) << name;
    ConfigOption *opt = _values[i->second];
    std::string oldValue = getOptionValue(name);
    switch (opt->getType())
    {
      case Config::BOOLEAN:
        opt->setValue(boost::lexical_cast<bool>(newValue));
        break;
      case Config::STRING:
      case Config::SET:
        opt->setValue(newValue);
        break;
      case Config::INTEGER:
        opt->setValue(boost::lexical_cast<int>(newValue));
        break;
      case Config::SIZE:
        opt->setValue(forced_lexical_cast<size_t>(newValue));
        break;
      case Config::REAL:
        opt->setValue(boost::lexical_cast<double>(newValue));
        break;
      default:
        SCIDB_UNREACHABLE();
    }
    return oldValue;
}

std::string Config::getOptionValue(std::string const& name)
{
    std::map<std::string, int32_t>::const_iterator i = _longArgToOption.find(name);
    if (i == _longArgToOption.end())
        throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_UNKNOWN_CONFIG_OPTION) << name;
    ConfigOption *opt = _values[i->second];
    return opt->getValueAsString();
}

static po::value_semantic* optTypeToValSem(Config::ConfigOptionType optionType)
{
    switch (optionType)
    {
    case Config::BOOLEAN:
        return po::value<bool>()->implicit_value(true)->default_value(true);
    case Config::STRING:
    case Config::SET:
        return po::value<string>();
    case Config::INTEGER:
        return po::value<int>();
    case Config::SIZE:
        return po::value<checked_size>();
    case Config::REAL:
        return po::value<double>();
    case Config::STRING_LIST:
        return po::value<vector<string> >()->multitoken();
    default:
        SCIDB_UNREACHABLE();
    }

    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "optTypetoValSem";
}

//TODO: Maybe replace with something more complicated? (e.g. little boost::spirit parser)
static void stringToVector(const string& str, vector<string>& strs)
{
    alg::split(strs, str, alg::is_any_of(":"));
}


} // namespace common
