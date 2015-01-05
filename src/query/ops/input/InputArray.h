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

/*
 * InputArray.h
 *
 *  Created on: Sep 23, 2010
 */
#ifndef INPUT_ARRAY_H
#define INPUT_ARRAY_H

#define __EXTENSIONS__
#define _EXTENSIONS
#define _FILE_OFFSET_BITS 64
#if ! defined(HPUX11_NOT_ITANIUM) && ! defined(L64)
#define _LARGEFILE64_SOURCE 1 // access to files greater than 2Gb in Solaris
#define _LARGE_FILE_API     1 // access to files greater than 2Gb in AIX
#endif

#include <stdio.h>
#include <ctype.h>
#include <inttypes.h>
#include <limits.h>
#include <string>

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/MemArray.h"
#include "smgr/io/TemplateParser.h"
#include "util/BufferedFileInput.h"

#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/helpers/exception.h"

namespace scidb
{
    using namespace std;

    // declared static to prevent visibility of variable outside of this file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.ops.inputarray"));

    class InputArray;
    class InputArrayIterator;

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

    typedef enum
    {
        AS_EMPTY,
        AS_TEXT_FILE,
        AS_BINARY_FILE,
        AS_STRING        
    } InputType;

    class Scanner
    {
        FILE* f;
        string filePath;
        int missingReason;
        int lineNo;
        int columnNo;
        int64_t pos;
        char *buf;
        boost::shared_ptr<BufferedFileInput> doubleBuffer;

        // A temporary string which is used to hold the current token.
        // Normally, bytes are copied to stringBuf.
        // When stringBuf is filled up, or when getValue() is called, content in stringBuf is appended to the end of tmpValue.
        // This optimization is used to avoid expensive calls to string::operation+=() for individual bytes.
        string tmpValue;

        // Max size of the temporary stringBuf.
        static const size_t MAX_TEMP_BUF_SIZE = 100;

        // When constructing the string content, individual bytes are appended to the end of char[100].
        char stringBuf[MAX_TEMP_BUF_SIZE];

        // how many chars are in use in stringBuf
        size_t nStringBuf;
        
        void openStringStream(string const& input);


      public:
        inline bool isNull() const
        {
            return missingReason >= 0;
        }

        inline int getMissingReason() const
        {
            return missingReason;
        }

        inline string const& getValue()
        {
            tmpValue.append(stringBuf, nStringBuf);
            nStringBuf = 0;
            return tmpValue;
        }

        inline int64_t getPosition() const
        {
            return pos;
        }
 
        inline void setPosition(int64_t p) { 
            pos = p;
        }

        inline int getLine() const
        {
            return lineNo;
        }

        inline int getColumn() const
        {
            return columnNo;
        }

        bool open(string const& input, InputType inputType, boost::shared_ptr<Query> query);

        Scanner() : f(NULL), missingReason(0), lineNo(0), columnNo(0), pos(0), buf(NULL), nStringBuf(0)
        { }

        ~Scanner()
        {
            doubleBuffer.reset();
            if (f != NULL) { 
                fclose(f);
            }
            if (buf)
                delete[] buf;
        }

        const string& getFilePath() const { return filePath; }

        inline int getChar() {
            int ch = doubleBuffer->myGetc();
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
                doubleBuffer->myUngetc(ch);
            }
        }
        
        FILE* getFile() const
        {
            return f;
        }

        /*
         * Append a char to the end of stringBuf.
         */
        inline void Append(int ch) {
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
        inline Token get()
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

    class InputArrayIterator : public ConstArrayIterator
    {
        friend class InputArray;
      public:
        virtual ConstChunk const& getChunk();
        virtual bool end();
        virtual void operator ++();
        virtual Coordinates const& getPosition();
        InputArrayIterator(InputArray& array, AttributeID id);

      private:
        InputArray& array;
        AttributeID attr;
        bool hasCurrent;
        size_t currChunkIndex;
    };

    const size_t LOOK_AHEAD = 2;

    class InputArray : public Array
    {
        friend class InputArrayIterator;
      public:

        /**
         * Get the least restrictive access mode that the array supports.
         * @return SINGLE_PASS
         */
        virtual Access getSupportedAccess() const
        {
            return SINGLE_PASS;
        }


        virtual ArrayDesc const& getArrayDesc() const;
        virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const;

        InputArray(ArrayDesc const& desc, string const& input, string const& format, boost::shared_ptr<Query>& query, InputType inputType, int64_t maxCnvErrors = 0, string const& shadowArrayName = string(), bool parallelLoad = false);
        ~InputArray();

        bool moveNext(size_t chunkIndex);
        ConstChunk const& getChunk(AttributeID attr, size_t chunkIndex);

        void sg();
        void redistributeShadowArray(boost::shared_ptr<Query> const& query);
        void scheduleSG(boost::shared_ptr<Query> const& query);
        void completeShadowArrayRow();
        void handleError(Exception const& x, boost::shared_ptr<ChunkIterator> iterator, AttributeID i, int64_t pos);
        
        void resetShadowChunkIterators();
    
      private:
        bool loadOpaqueChunk(boost::shared_ptr<Query>& query, size_t chunkIndex);
        bool loadBinaryChunk(boost::shared_ptr<Query>& query, size_t chunkIndex);
        bool loadTextChunk(boost::shared_ptr<Query>& query, size_t chunkIndex);

        struct LookAheadChunks {
            MemChunk chunks[LOOK_AHEAD];
        };

        ArrayDesc desc;
        Scanner scanner;
        Coordinates chunkPos;
        Coordinates lastChunkPos;
        size_t currChunkIndex;
        vector< boost::shared_ptr<InputArrayIterator> > iterators;
        vector<LookAheadChunks> lookahead;
        vector< TypeId> types;
        vector< Value> attrVal;
        vector< Value> binVal;
        vector< FunctionPointer> converters;
        Value coordVal;
        Value strVal;
        AttributeID emptyTagAttrID;
        bool binaryLoad;
        ExchangeTemplate templ;
        uint64_t nLoadedCells;
        uint64_t nLoadedChunks;
        size_t nErrors;
        size_t maxErrors;
        uint32_t signature;
        boost::shared_ptr<Array> shadowArray;
        enum State
        {
            Init,
            EndOfStream,
            EndOfChunk,
            InsideArray,
            EmptyArray
        };
        State state;
        MemChunk tmpChunk;
        vector< shared_ptr<ArrayIterator> > shadowArrayIterators;
        vector< shared_ptr<ChunkIterator> > shadowChunkIterators;
        size_t nAttrs;
        int lastBadAttr;
        InstanceID myInstanceID;
        size_t nInstances;
        bool parallelLoad;
    };

} //namespace scidb

#endif /* INPUT_ARRAY_H */
