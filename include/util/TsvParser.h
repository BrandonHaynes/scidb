/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2014-2014 SciDB, Inc.
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

#ifndef TSV_PARSER_H_
#define TSV_PARSER_H_

namespace scidb {

/**
 *  @brief      Behold the One True TSV Parser.
 *
 *  @details    This class parses lines of 'tab separated values' text.  It
 *              modifies the input line in-place to unescape TSV escape chars,
 *              returning one field at a time.
 *
 *              The only possible error is caused by a <Backslash><FieldDelim>
 *              sequence (e.g. "\\\t" or "\\\n"), which is illegal.
 *
 *  @note       If you choose a field delimiter other than TAB (ascii 0x09) be
 *              certain that the data columns do not contain that character or
 *              you will get unexpected results.  Use of non-TAB delimiters is
 *              discouraged for this reason.
 *
 *  @see        http://dataprotocols.org/linear-tsv/
 *  @see        https://www.iana.org/assignments/media-types/text/tab-separated-values
 *
 *  @author     mjl@paradigm4.com
 */
class TsvParser
{
public:

    TsvParser();
    explicit TsvParser(char *line);

    /// Prepare to parse a new line buffer in-place.
    void reset(char *line);

    /// Set the field delimiter.  Not recommended, you should be using tabs!
    void setDelim(char delim);

    /// Return values for #getField .
    enum {
        OK = 0,                 ///< Everything is beautiful (in its own way)
        EOL = 1,                ///< Reached end-of-line
        ERR = 2                 ///< Parse error on returned field
    };

    /**
     * Parse next field from the line buffer.
     *
     * @description Return the next field in the line buffer, or EOL
     * (the end-of-line indicator).  EOL is always returned by itself,
     * so that parsing an empty line results in two calls: one
     * returning @c OK and an empty field, and one returning @c EOL
     * (and the field value is not meaningful).
     *
     * If there is a parsing error, the parser will do its best to
     * assemble the output @c field, and @c ERR will be returned.
     *
     * @note The returned @c field pointer will never be NULL, so it
     * can safely be passed to the @c std::string constructor.
     *
     * @param field [OUT] non-NULL pointer to const parsed field or to empty string
     * @returns OK, ERR, or EOL
     */
    int getField(char const*& field);

private:
    TsvParser(TsvParser const&);
    TsvParser& operator=(TsvParser const&);

    char*       _cursor;
    bool        _eol;
    char        _delim;
};

} // namespace

#endif  /* ! TSV_PARSER_H_ */
