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
#ifndef TEXT_SCANNER_H
#define TEXT_SCANNER_H

#include <util/BufferedFileInput.h>

namespace scidb {

    enum Token
    {
        TKN_TUPLE_BEGIN,
        TKN_TUPLE_END,
        TKN_ARRAY_BEGIN,
        TKN_ARRAY_END,
        TKN_COORD_BEGIN,
        TKN_COORD_END,
        TKN_COMMA,
        TKN_SEMICOLON,
        TKN_LITERAL,
        TKN_MULTIPLY,
        TKN_EOF
    };

    enum InputType
    {
        AS_EMPTY,
        AS_TEXT_FILE,
        AS_BINARY_FILE,
        AS_STRING
    };

    class Scanner
    {
        FILE* filePtr;          // not owned, do not close
        string filePath;
        int missingReason;
        int lineNo;
        int columnNo;
        int64_t pos;
        boost::shared_ptr<BufferedFileInput> doubleBuffer;

        // A temporary string which is used to hold the current token.
        // Normally, bytes are copied to stringBuf.  When stringBuf is
        // filled up, or when getValue() is called, content in
        // stringBuf is appended to the end of tmpValue.  This
        // optimization is used to avoid expensive calls to
        // string::operation+=() for individual bytes.
        //
        string tmpValue;

        // Max size of the temporary stringBuf.
        static const size_t MAX_TEMP_BUF_SIZE = 100;

        // When constructing the string content, individual bytes are appended to the end of char[100].
        char stringBuf[MAX_TEMP_BUF_SIZE];

        // how many chars are in use in stringBuf
        size_t nStringBuf;

        // Use BufferedFileInput rather than stdio.  Configurable, see ticket #4459.
        bool useDoubleBuffering;

      public:
        bool isNull() const
        {
            return missingReason >= 0;
        }

        int getMissingReason() const
        {
            return missingReason;
        }

        string const& getValue()
        {
            tmpValue.append(stringBuf, nStringBuf);
            nStringBuf = 0;
            return tmpValue;
        }

        int64_t getPosition() const
        {
            return pos;
        }

        void setPosition(int64_t p) 
        {
            pos = p;
        }

        int getLine() const
        {
            return lineNo;
        }

        int getColumn() const
        {
            return columnNo;
        }

        bool isDoubleBuffering() const
        {
            return useDoubleBuffering;
        }

        void open(FILE* fp, boost::shared_ptr<Query> query)
        {
            assert(doubleBuffer.get() == 0);
            if (useDoubleBuffering) {
                // Warning: the BufferedFileInput constructor is going
                // to start worker threads that will begin reading the
                // file immediately!
                doubleBuffer =
                    shared_ptr<BufferedFileInput>(new BufferedFileInput(fp, query));
            } else {
                filePtr = fp;
            }
        }

        Scanner() : filePtr(0), missingReason(0), lineNo(0), columnNo(0), pos(0), nStringBuf(0),
                    useDoubleBuffering(Config::getInstance()->getOption<bool>(CONFIG_INPUT_DOUBLE_BUFFERING))
        { }

        ~Scanner()
        {
            doubleBuffer.reset();
        }

        const string& getFilePath() const { return filePath; }

        int getChar() {
            int ch = useDoubleBuffering ? doubleBuffer->myGetc() : fgetc(filePtr);
            pos += 1;
            if (ch == '\n') {
                lineNo += 1;
                columnNo = 0;
            } else {
                columnNo += 1;
            }
            return ch;
        }

        void ungetChar(int ch) {
            if (ch != EOF) {
                pos -= 1;
                if (ch == '\n') {
                    lineNo -= 1;
                } else {
                    columnNo -= 1;
                }
                if (useDoubleBuffering) {
                    doubleBuffer->myUngetc(ch);
                } else {
                    ungetc(ch, filePtr);
                }
            }
        }

        /*
         * Append a char to the end of stringBuf.
         */
        void Append(int ch) {
            if (nStringBuf < MAX_TEMP_BUF_SIZE) {
                stringBuf[nStringBuf++] = static_cast<char>(ch);
            } else {
                tmpValue.append(stringBuf, nStringBuf);
                nStringBuf = 1;
                stringBuf[0] = static_cast<char>(ch);
            }
        }

        /*
         * Append a \x escape sequence to the end of stringBuf.
         */
        void AppendEscaped(int ch) {
            switch (ch) {
            case 'n':
                Append('\n');
                break;
            case 't':
                Append('\t');
                break;
            case 'r':
                Append('\r');
                break;
            default:
                // Allow embedded special chars like []{} etc.
                Append(ch);
                break;
            }
        }

        /*
         * The get() function is reengineered so as to process the most frequent characters first.
         * The frequency information below is based on a real customer input file.
         *
         *  ,: 131805589
         *   : 87870466
         *  1: 67195493
         *  2: 65545353
         *  4: 63800051
         *  0: 59916857
         *  3: 53139522
         *  8: 51823424
         *  7: 51776908
         *  6: 51722637
         *  9: 51678571
         *  5: 51165942
         * \n: 43935255
         *  ): 43935211
         *  (: 43935211
         *  }: 44
         *  {: 44
         *  ]: 44
         *  [: 44
         *  ;: 44
         */

        /*
         * Below is the new version.
         */
        Token get()
        {
            int ch;

            while ((ch = getChar()) != EOF && isspace(ch))
                ; // ignore whitespaces

            if (isdigit(ch)) {
CommonCase:
                nStringBuf = 0;
                tmpValue.clear();

                while (true) {
                    if (isdigit(ch)) {
                        Append(ch);
                        ch = getChar();
                        continue;
                    }
                    else if (ch == '\\') {
                        ch = getChar();
                        if (ch == EOF)
                            break;
                        AppendEscaped(ch);
                        ch = getChar();
                        continue;
                    }
                    else if (ch==',' || isspace(ch) || ch=='(' || ch==')'
                             || ch == ']' || ch == '}' || ch == '{' || ch == '['
                             || ch == '*' || ch==EOF) {
                        break;
                    }
                    Append(ch);
                    ch = getChar();
                }
                if (nStringBuf==0 && tmpValue.size()==0)
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR14);
                ungetChar(ch);
                if (nStringBuf==4 && tmpValue.size()==0 &&
                    stringBuf[0]=='n' && stringBuf[1]=='u' && stringBuf[2]=='l' && stringBuf[3]=='l')
                {
                    missingReason = 0;
                }
                else {
                    missingReason = -1;
                }
                return TKN_LITERAL;
            }

            switch (ch)
            {
            case ',':
                return TKN_COMMA;
            case '(':
                return TKN_TUPLE_BEGIN;
            case ')':
                return TKN_TUPLE_END;
            case '\'':
            case '\"':
            {
                char begin = ch;
                nStringBuf = 0;
                tmpValue.clear();
                while (true)
                {
                    ch = getChar();
                    if (ch == '\\')
                    {
                        ch = getChar();
                        switch (ch)
                        {
                        case 'n':
                            ch = '\n';
                            break;
                        case 'r':
                            ch = '\r';
                            break;
                        case 'f':
                            ch = '\f';
                            break;
                        case 't':
                            ch = '\t';
                            break;
                        case '0':
                            ch = 0;
                            break;
                        }
                    }
                    else
                    {
                        if (ch == begin)
                        {
                            missingReason = -1;
                            return TKN_LITERAL;
                        }
                    }
                    if (ch == EOF)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR13);
                    Append(ch);
                }
                break;
            }
            case '{':
                return TKN_COORD_BEGIN;
            case '}':
                return TKN_COORD_END;
            case '*':
                return TKN_MULTIPLY;
            case '[':
                return TKN_ARRAY_BEGIN;
            case ']':
                return TKN_ARRAY_END;
            case ';':
                return TKN_SEMICOLON;
            case '?':
                nStringBuf = 0;
                tmpValue.clear();
                // Wrongly permits reasons > 127, or no numeric reason at all!
                // Deprecate in 14.11 and enforce "0 <= n <= 127, n required"
                // for text format in the next release.
                missingReason = 0;
                while ((ch = getChar()) >= '0' && ch <= '9')
                {
                    missingReason = missingReason * 10 + ch - '0';
                }
                ungetChar(ch);
                return TKN_LITERAL;
            case EOF:
                return TKN_EOF;

            default:
                goto CommonCase;
            }
        }
    };

} // namespace

#endif
