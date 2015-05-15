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
 * @file CsvChunkLoader.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#include "ChunkLoader.h"
#include "InputArray.h"

#include <util/StringUtil.h>    // for debugEncode
#include <util/CsvParser.h>

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

using namespace std;

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.ops.input.csvchunkloader"));

CsvChunkLoader::CsvChunkLoader()
    : _tooManyWarning(false)
{ }

CsvChunkLoader::~CsvChunkLoader()
{ }

void CsvChunkLoader::openHook()
{
    _csvParser
        .setFilePtr(fp())
        .setLogger(logger);

    if (hasOption('p')) {
        _csvParser.setDelim('|');
    } else if (hasOption('t')) {
        _csvParser.setDelim('\t');
    }

    if (hasOption('d')) {
        _csvParser.setQuote('\"');
    } else if (hasOption('s')) {
        _csvParser.setQuote('\'');
    }
}

void CsvChunkLoader::bindHook()
{
    // For now at least, flat arrays only.
    Dimensions const& dims = schema().getDimensions();
    if (dims.size() != 1) {
        throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR,
                             SCIDB_LE_MULTIDIMENSIONAL_ARRAY_NOT_ALLOWED);
    }
}

bool CsvChunkLoader::loadChunk(boost::shared_ptr<Query>& query, size_t chunkIndex)
{
    // Must do EOF check *before* nextImplicitChunkPosition() call, or
    // we risk stepping out of bounds.
    if (_csvParser.empty()) {
        int ch = ::getc(fp());
        if (ch == EOF) {
            return false;
        }
        ::ungetc(ch, fp());
    }

    // Reposition and make sure all is cool.
    nextImplicitChunkPosition(MY_CHUNK);
    enforceChunkOrder("csv loader");

    // Initialize a chunk and chunk iterator for each attribute.
    Attributes const& attrs = schema().getAttributes();
    size_t nAttrs = attrs.size();
    vector< boost::shared_ptr<ChunkIterator> > chunkIterators(nAttrs);
    for (size_t i = 0; i < nAttrs; i++) {
        Address addr(i, _chunkPos);
        MemChunk& chunk = getLookaheadChunk(i, chunkIndex);
        chunk.initialize(array(), &schema(), addr, attrs[i].getDefaultCompressionMethod());
        chunkIterators[i] = chunk.getIterator(query,
                                              ChunkIterator::NO_EMPTY_CHECK |
                                              ConstChunkIterator::SEQUENTIAL_WRITE);
    }

    char const *field = 0;
    int rc = 0;
    bool sawData = false;
    bool sawEof = false;

    while (!chunkIterators[0]->end()) {

        _column = 0;
        array()->countCell();
        
        // Parse and write out a line's worth of fields.  NB if you
        // have to 'continue;' after a writeItem() call, make sure the
        // iterator (and possibly the _column) gets incremented.
        //
        for (size_t i = 0; i < nAttrs; ++i) {
            try {
                // Handle empty tag...
                if (i == emptyTagAttrId()) {
                    attrVal(i).setBool(true);
                    chunkIterators[i]->writeItem(attrVal(i));
                    ++(*chunkIterators[i]); // ...but don't increment _column.
                    continue;
                }

                // Parse out next input field.
                rc = _csvParser.getField(field);
                if (rc == CsvParser::END_OF_FILE) {
                    sawEof = true;
                    break;
                }
                if (rc == CsvParser::END_OF_RECORD) {
                    // Got record terminator, but we have more attributes!
                    throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_TOO_FEW_FIELDS)
                        << _csvParser.getFileOffset() << _csvParser.getRecordNumber() << _column;
                }
                if (rc > 0) {
                    // So long as we never call _csvParser.setStrict(true), we should never see this.
                    throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_CSV_PARSE_ERROR)
                        << _csvParser.getFileOffset() << _csvParser.getRecordNumber()
                        << _column << csv_strerror(rc);
                }
                SCIDB_ASSERT(rc == CsvParser::OK);
                SCIDB_ASSERT(field);
                sawData = true;

                // Process input field.
                if (mightBeNull(field) && attrs[i].isNullable()) {
                    int8_t missingReason = parseNullField(field);
                    if (missingReason >= 0) {
                        attrVal(i).setNull(missingReason);
                        chunkIterators[i]->writeItem(attrVal(i));
                        ++(*chunkIterators[i]);
                        _column += 1;
                        continue;
                    }
                }
                if (converter(i)) {
                    Value v;
                    v.setString(field);
                    const Value* vp = &v;
                    (*converter(i))(&vp, &attrVal(i), NULL);
                    chunkIterators[i]->writeItem(attrVal(i));
                }
                else {
                    TypeId const &tid = typeIdOfAttr(i);
                    if (attrs[i].isNullable() &&
                        (*field == '\0' || (iswhitespace(field) && IS_NUMERIC(tid))))
                    {
                        // [csv2scidb compat] With csv2scidb, empty strings (or for numeric
                        // fields, whitespace) became nulls if the target attribute was
                        // nullable.  We keep the same behavior.  (We should *not* do this for
                        // TSV, that format requires explicit nulls!)
                        attrVal(i).setNull();
                    } else {
                        StringToValue(tid, field, attrVal(i));
                    }
                    chunkIterators[i]->writeItem(attrVal(i));
                }
            }
            catch (Exception& ex) {
                _badField = field;
                _fileOffset = _csvParser.getFileOffset();
                array()->handleError(ex, chunkIterators[i], i);
            }

            _column += 1;
            ++(*chunkIterators[i]);
        }

        if (sawEof) {
            break;
        }

        // We should be at EOL now, otherwise there are too many fields on this line.  Post a
        // warning: it seems useful not to complain too loudly about this or to abort the load, but
        // we do want to mention it.
        //
        rc = _csvParser.getField(field);
        if (!_tooManyWarning && (rc != CsvParser::END_OF_RECORD)) {
            _tooManyWarning = true;
            query->postWarning(SCIDB_WARNING(SCIDB_LE_OP_INPUT_TOO_MANY_FIELDS)
                               << _csvParser.getFileOffset() << _csvParser.getRecordNumber() << _column);
        }

        array()->completeShadowArrayRow(); // done with cell/record
    }

    for (size_t i = 0; i < nAttrs; i++) {
        if (chunkIterators[i]) {
            chunkIterators[i]->flush();
        }
    }

    return sawData;
}

} // namespace
