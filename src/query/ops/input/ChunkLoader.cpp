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
 * @file ChunkLoader.cpp
 * @brief Format-specific helper classes for loading chunks.
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#include "ChunkLoader.h"
#include "InputArray.h"

#include <util/StringUtil.h>    // for debugEncode
#include <util/TsvParser.h>

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

using namespace std;

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.ops.input.chunkloader"));

ChunkLoader* ChunkLoader::create(string const& format)
{
    string::size_type colon = format.find(':');
    string baseFmt = format.substr(0, colon);
    string fmtOptions;
    if (colon != string::npos) {
        fmtOptions = format.substr(colon + 1);
    }

    ChunkLoader *ret = 0;
    if (baseFmt.empty()) {
        ret = new TextChunkLoader(); // the default
    }
    else if (baseFmt[0] == '(') {
        ret = new BinaryChunkLoader(baseFmt);
    }
    else if (!compareStringsIgnoreCase(baseFmt, "opaque")) {
        ret = new OpaqueChunkLoader();
    }
    else if (!compareStringsIgnoreCase(baseFmt, "text")) {
        ret = new TextChunkLoader();
    }
    else if (!compareStringsIgnoreCase(baseFmt, "tsv")) {
        ret = new TsvChunkLoader();
    }
    else if (!compareStringsIgnoreCase(baseFmt, "csv")) {
        ret = new CsvChunkLoader();
    }

    if (ret) {
        ret->_options = fmtOptions;
    }
    return ret;
}

ChunkLoader::ChunkLoader()
    : _fileOffset(0)
    , _line(0)                  // for non-line-oriented input, record number
    , _column(0)
    , _inArray(0)
    , _fp(0)
    , _numInstances(0)
    , _myInstance(INVALID_INSTANCE)
    , _emptyTagAttrId(INVALID_ATTRIBUTE_ID)
    , _enforceDataIntegrity(false)
    , _isRegularFile(false)
    ,_hasDataIntegrityIssue(false)
{ }

ChunkLoader::~ChunkLoader()
{
    if (_fp) {
        ::fclose(_fp);
    }
}

int8_t ChunkLoader::parseNullField(const char* s)
{
    // Note we're not allowing leading or trailing whitespace here.

    SCIDB_ASSERT(s);
    if (*s == '\\' && *(s + 1) == 'N' && *(s + 2) == '\0') {
        // Per http://dataprotocols.org/linear-tsv/
        return 0;
    }
    if (*s == '?') {
        const char* cp = s + 1;
        if (!*cp) {
            return -1;          // lone ? does not cut it
        }
        int sum = 0;
        for (; *cp; ++cp) {
            if (!::isdigit(*cp)) {
                return -1;
            }
            sum = (sum * 10) + *cp - '0';
        }
        if (sum > INT8_MAX) {
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_BAD_MISSING_REASON) << s;
        }
        return static_cast<int8_t>(sum);
    }
    else if (*s == 'n'
             && *++s == 'u'
             && *++s == 'l'
             && *++s == 'l'
             && *++s == '\0')
    {
        return 0;
    }
    return -1;
}

int ChunkLoader::openFile(string const& fileName)
{
    SCIDB_ASSERT(_fp == NULL);
    _path = fileName;
    // On POSIX "rb" and "r" are equivalent, the 'b' is strictly for C89 compat.
    const char* mode = isBinary() ? "rb" : "r";
    _fp = ::fopen(fileName.c_str(), mode);
    if (_fp) {
        struct stat stbuf;
        int rc = ::fstat(fileno(_fp), &stbuf);
        _isRegularFile = ((rc == 0) && S_ISREG(stbuf.st_mode));
        this->openHook();
        return 0;
    }
    return errno;
}

int ChunkLoader::openString(string const& dataString)
{
    _path = "<string>";
    _fp = openMemoryStream(dataString.c_str(), dataString.size());
    SCIDB_ASSERT(_fp);
    this->openHook();
    return 0;
}

/// @return the schema we are loading into
ArrayDesc const& ChunkLoader::schema() const
{
    SCIDB_ASSERT(_inArray);
    return _inArray->getArrayDesc();
}

/// Validate and return the query pointer.
/// @return shared_ptr to valid Query object
boost::shared_ptr<Query> ChunkLoader::query()
{
    SCIDB_ASSERT(_inArray);
    return Query::getValidQueryPtr(_inArray->_query);
}

bool ChunkLoader::isParallelLoad() const
{
    SCIDB_ASSERT(_inArray);
    return _inArray->parallelLoad;
}

// This sort of activity would ordinarily happen in the constructor,
// but I prefer to delay it so that an attempt to construct a
// ChunkLoader for format "foo" can be used to determine that "foo" is
// a supported format, even when no @c InputArray* or @c boost::shared_ptr<Query> is
// present.  Otherwise the check for is-supported has to be coded in
// two places, which grosses me out.
//
void ChunkLoader::bind(InputArray* parent, boost::shared_ptr<Query>& query)
{
    _inArray = parent;
    _enforceDataIntegrity = parent->_enforceDataIntegrity;

    _numInstances = query->getInstancesCount();
    _myInstance = query->getInstanceID();
    AttributeDesc const* aDesc = schema().getEmptyBitmapAttribute();
    if (aDesc) {
        _emptyTagAttrId = aDesc->getId();
    }

    Dimensions const& dims = schema().getDimensions();
    size_t nDims = dims.size();

    _chunkPos.resize(nDims);
    for (size_t i = 0; i < nDims; i++) {
        _chunkPos[i] = dims[i].getStartMin();
    }

    // It's painful, but code in nextImplicitChunkPosition() and also
    // in the BinaryChunkLoader (where _chunkPos is used to select a
    // lookahead chunk) assumes that the initial _chunkPos is actually
    // "one step" prior to the start of the array.  So be it.
    //
    _chunkPos[nDims-1] -= dims[nDims-1].getChunkInterval();

    Attributes const& attrs = schema().getAttributes();
    size_t nAttrs = attrs.size();

    _lookahead.resize(nAttrs);
    _converters.resize(nAttrs);
    _attrTids.resize(nAttrs);
    for (size_t i = 0; i < nAttrs; ++i) {
        _attrTids[i] = attrs[i].getType();
        if (!isBuiltinType(_attrTids[i])) {
            _converters[i] = FunctionLibrary::getInstance()->findConverter(TID_STRING, _attrTids[i]);
        }
    }

    // For several subclasses, it's convenient to have a cell's worth
    // of Value objects pre-constructed with appropriate output type
    // and size.  For example, the TextChunkLoader's TKN_MULTIPLY
    // feature means the same parsed cell Values get written many
    // times.  And loaders that need to call conversion functions need
    // appropriately-sized Value objects as conversion targets.  That
    // said, there is no requirement that a subclass make use of this
    // Value vector, it's here in ChunkLoader as a convenience.
    //
    _attrVals.resize(nAttrs);
    for (size_t i = 0; i < nAttrs; i++) {
        _attrVals[i] = Value(TypeLibrary::getType(typeIdOfAttr(i)));
        if (attrs[i].isEmptyIndicator()) {
            _attrVals[i].setBool(true);
        }
    }

    // Tell derived classes they can look at the schema() now.
    this->bindHook();
}

void ChunkLoader::nextImplicitChunkPosition(WhoseChunk whose)
{
    Dimensions const& dims = schema().getDimensions();
    const size_t nDims = dims.size();
    size_t i = nDims-1;

    while (true) {
        _chunkPos[i] += dims[i].getChunkInterval();

        if (whose == MY_CHUNK) {
            // Keep bumping the _chunkPos until it points at one of *my* chunks.
            if (_chunkPos[i] <= dims[i].getEndMax()) {
                if (!isParallelLoad()
                    || schema().getHashedChunkNumber(_chunkPos) % numInstances() == myInstance())
                {
                    // _chunkPos points at one of my chunks.
                    break;
                }
            } else {
                // Stepped beyond end of dimension, start considering chunks at the start of the
                // next dimension...
                if (0 == i) {
                    // ...unless there are no dimensions left!
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_NEXT_CHUNK_OOB);
                }
                _chunkPos[i] = dims[i].getStartMin();
                i -= 1;
            }
        }
        else {
            // Just point me at the next chunk, I don't care if it's going to belong to my
            // instance or not.
            if (_chunkPos[i] <= dims[i].getEndMax()) {
                // This _chunkPos is good, quit bumping it.
                break;
            }
            // On to next dimension... *if* there is one.
            if (0 == i)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_NEXT_CHUNK_OOB);
            _chunkPos[i] = dims[i].getStartMin();
            i -= 1;
        }
    }

    LOG4CXX_TRACE(logger, "Implicit chunk coords " << CoordsToStr(_chunkPos));
}

void ChunkLoader::enforceChunkOrder(const char *caller)
{
    if (_lastChunkPos.size() == 0) {
        // First time, no previous chunk.
        _lastChunkPos = _chunkPos;
        return;
    }

    CoordinatesLess comp;

    // Check that this explicit chunkPos isn't inconsistent
    // (ie. out of order). We should always grow chunk
    // addresses.
    if (!comp(_lastChunkPos, _chunkPos)) {
        if (!_hasDataIntegrityIssue) {
            LOG4CXX_WARN(logger, "Given that the last chunk processed was " << CoordsToStr(_lastChunkPos)
                         << " this chunk " << CoordsToStr(_chunkPos) << " is out of sequence ("
                         << caller << ")"
                         << ". Add scidb.qproc.ops.input.chunkloader=TRACE to the log4cxx config file for more");
            _hasDataIntegrityIssue = true;
        } else {
            LOG4CXX_TRACE(logger, "Given that the last chunk processed was " << CoordsToStr(_lastChunkPos)
                          << " this chunk " << CoordsToStr(_chunkPos) << " is out of sequence ("
                          << caller << ")");
        }
        if (_enforceDataIntegrity) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_DUPLICATE_CHUNK_ADDR)
                << CoordsToStr(_chunkPos);
        }
    }

    _lastChunkPos = _chunkPos;
}

/**********************************************************************/

void OpaqueChunkLoader::bindHook()
{
    _signature = OpaqueChunkHeader::calculateSignature(schema());
    _templ = TemplateParser::parse(schema(), "opaque", true);
}

static void compareArrayMetadata(ArrayDesc const& a1, ArrayDesc const& a2)
{
    Dimensions const& dims1 = a1.getDimensions();
    Attributes const& attrs1 = a1.getAttributes();
    Dimensions const& dims2 = a2.getDimensions();
    Attributes const& attrs2 = a2.getAttributes();
    size_t nDims = dims1.size();
    size_t nAttrs = attrs1.size();
    if (nDims != dims2.size()) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
    }
    if (nAttrs != attrs2.size()) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
    }
    for (size_t i = 0; i < nDims; i++) {
        if (dims1[i].getChunkInterval() != dims2[i].getChunkInterval()) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
        }
        if (dims1[i].getChunkOverlap() != dims2[i].getChunkOverlap()) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
        }
    }
    for (size_t i = 0; i < nAttrs; i++) {
        if (attrs1[i].getType() != attrs2[i].getType()) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
        }
        if (attrs1[i].getFlags() != attrs2[i].getFlags()) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
        }
    }
}

// For good or ill, the opaque loader doesn't bother to upcall to
// handleError(), it just throws.  Assumption is that this data was
// saved by SciDB, so elaborate error reporting via shadow array isn't
// needed.
//
bool OpaqueChunkLoader::loadChunk(boost::shared_ptr<Query>& query, size_t chunkIndex)
{
    Dimensions const& dims = schema().getDimensions();
    Attributes const& attrs = schema().getAttributes();
    size_t nAttrs = attrs.size();
    size_t nDims = dims.size();

    // Can't call ftell/fseek/etc on a pipe, oh well.
    SCIDB_ASSERT(!canSeek() || _fileOffset == ::ftell(fp()));

    OpaqueChunkHeader hdr;
    for (size_t i = 0; i < nAttrs; i++) {
        if (fread(&hdr, sizeof hdr, 1, fp()) != 1) {
            if (i == 0) {
                return false;
            }
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(fp());
        }
        _fileOffset += sizeof(hdr);
        if (hdr.magic != OPAQUE_CHUNK_MAGIC) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR10);
        }
        if (hdr.version != SCIDB_OPAQUE_FORMAT_VERSION) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_MISMATCHED_OPAQUE_FORMAT_VERSION)
                  << hdr.version << SCIDB_OPAQUE_FORMAT_VERSION;
        }
        if (hdr.flags & OpaqueChunkHeader::ARRAY_METADATA)  {
            string arrayDescStr;
            arrayDescStr.resize(hdr.size);
            if (fread(&arrayDescStr[0], 1, hdr.size, fp()) != hdr.size) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(fp());
            }
            _fileOffset += hdr.size;
            stringstream ss;
            ss << arrayDescStr;
            ArrayDesc opaqueDesc;
            archive::text_iarchive ia(ss);
            ia & opaqueDesc;
            compareArrayMetadata(schema(), opaqueDesc);
            i -= 1; // compencate increment in for: repeat loop and try to load more mapping arrays
            continue;
        }
        if (hdr.signature != _signature) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
        }
        if (hdr.nDims != nDims) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_WRONG_NUMBER_OF_DIMENSIONS);
        }
        if (fread(&_chunkPos[0], sizeof(Coordinate), hdr.nDims, fp()) != hdr.nDims) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(fp());
        }
        _fileOffset += sizeof(Coordinate) * hdr.nDims;
        if (hdr.attrId != i) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_UNEXPECTED_DESTINATION_ATTRIBUTE) << attrs[i].getName();
        }
        if (i==0) {
            enforceChunkOrder("opaque loader");
        }
        Address addr(i, _chunkPos);
        MemChunk& chunk = getLookaheadChunk(i, chunkIndex);
        chunk.initialize(array(), &schema(), addr, hdr.compressionMethod);
        chunk.allocate(hdr.size);
        if (fread(chunk.getData(), 1, hdr.size, fp()) != hdr.size) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(fp());
        }
        _fileOffset += hdr.size;
        _line += chunk.getNumberOfElements(false /*no overlap*/);  // Unclear how useful this number is, but...
        chunk.write(query);
    }

    SCIDB_ASSERT(!canSeek() || _fileOffset == ::ftell(fp()));

    return true;
}


/**********************************************************************/

BinaryChunkLoader::BinaryChunkLoader(std::string const& format)
    : _format(format)
{ }

void BinaryChunkLoader::bindHook()
{
    _templ = TemplateParser::parse(schema(), _format, true);

    // We use this _binVal vector to minimize code churn in the
    // loadChunk method, but it would be so much better to have a
    // Value constructor that could point at pre-allocated external
    // memory, i.e. the buf vector in loadChunk.  That would avoid a
    // *lot* of realloc(3) calls on string attributes.
    //
    Attributes const& attrs = schema().getAttributes();
    _binVal.resize(attrs.size());
}

bool BinaryChunkLoader::loadChunk(boost::shared_ptr<Query>& query, size_t chunkIndex)
{
    // It would be nice to SCIDB_ASSERT(_fileOffset == ::ftell(fp())) in a
    // few places, but use of ungetc(3) makes that infeasible.

    Attributes const& attrs = schema().getAttributes();
    size_t nAttrs = attrs.size();

    vector< boost::shared_ptr<ChunkIterator> > chunkIterators(nAttrs);
    Value emptyTagVal;
    emptyTagVal.setBool(true);

    int ch = getc(fp());
    if (ch == EOF) {
        return false;
    }
    ungetc(ch, fp());

    nextImplicitChunkPosition(MY_CHUNK);
    enforceChunkOrder("binary loader");

    // Initialize a chunk for each attribute.  This initializes half
    // of the lookahead chunks, and obtains iterators for them.  (We
    // don't seem to be doing any actual lookahead in this code path.)
    for (size_t i = 0; i < nAttrs; i++) {
        Address addr(i, _chunkPos);
        MemChunk& chunk = getLookaheadChunk(i, chunkIndex);
        chunk.initialize(array(), &schema(), addr, attrs[i].getDefaultCompressionMethod());
        chunkIterators[i] = chunk.getIterator(query,
                                              ChunkIterator::NO_EMPTY_CHECK |
                                              ConstChunkIterator::SEQUENTIAL_WRITE);
    }

    size_t nCols = _templ.columns.size();
    vector<uint8_t> buf(8);
    uint32_t size = 0;
    bool conversionError = false;
    while (!chunkIterators[0]->end() && (ch = getc(fp())) != EOF) {
        ungetc(ch, fp());
        _line += 1;             // really record count
        _column = 0;
        array()->countCell();
        for (size_t i = 0, j = 0; i < nAttrs; i++, j++) {
            while (j < nCols && _templ.columns[j].skip) {
                ExchangeTemplate::Column const& column = _templ.columns[j++];
                if (column.nullable) {
                    int8_t missingReason;
                    if (fread(&missingReason, sizeof(missingReason), 1, fp()) != 1) {
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(fp());
                    }
                    _fileOffset += sizeof(missingReason);
                }
                size = static_cast<uint32_t>(column.fixedSize);
                if (size == 0) {
                    if (fread(&size, sizeof(size), 1, fp()) != 1) {
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(fp());
                    }
                    _fileOffset += sizeof(size);
                }
                if (buf.size() < size) {
                    buf.resize(size * 2);
                }
                if (fread(&buf[0], size, 1, fp()) != 1) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(fp());
                }
                _fileOffset += size;
            }
            try {
                if (j < nCols) {
                    ExchangeTemplate::Column const& column = _templ.columns[j];
                    int8_t missingReason = -1;
                    if (column.nullable) {
                        if (fread(&missingReason, sizeof(missingReason), 1, fp()) != 1) {
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(fp());
                        }
                        _fileOffset += sizeof(missingReason);
                    }
                    size = static_cast<uint32_t>(column.fixedSize);
                    if (size == 0) {
                        if (fread(&size, sizeof(size), 1, fp()) != 1) {
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(fp());
                        }
                        _fileOffset += sizeof(size);
                    }
                    if (missingReason >= 0) {
                        if (buf.size() < size) {
                            buf.resize(size * 2);
                        }
                        if (size && fread(&buf[0], size, 1, fp()) != 1) {
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(fp());
                        }
                        _fileOffset += size;
                        attrVal(i).setNull(missingReason);
                        chunkIterators[i]->writeItem(attrVal(i));
                    } else {
                        _binVal[i].setSize(size);
                        if (fread(_binVal[i].data(), 1, size, fp()) != size) {
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(fp());
                        }
                        _fileOffset += size;
                        if (column.converter) {
                            conversionError = false;
                            try {
                                Value const* v = &_binVal[i];
                                column.converter(&v, &attrVal(i), NULL);
                                chunkIterators[i]->writeItem(attrVal(i));
                            } catch (...) {
                                conversionError = true;
                                throw;
                            }
                        } else {
                            chunkIterators[i]->writeItem(_binVal[i]);
                        }
                    }
                } else {
                    // empty tag
                    chunkIterators[i]->writeItem(emptyTagVal);
                }
            } catch(Exception const& x) {
                if (conversionError) {
                    // We don't know _binVal[i]'s type, but this will
                    // at least show us the initial bytes of data.
                    char* s = static_cast<char*>(_binVal[i].data());
                    string badBinVal(s, _binVal[i].size());
                    _badField = badBinVal;
                } else {
                    // Probably an fread(3) failure.
                    _badField = "(unreadable)";
                }
                array()->handleError(x, chunkIterators[i], i);
            }
            _column += 1;
            ++(*chunkIterators[i]);
        }
        array()->completeShadowArrayRow(); // done with cell/record
    }
    for (size_t i = 0; i < nAttrs; i++) {
        if (chunkIterators[i]) {
            chunkIterators[i]->flush();
        }
    }

    return true;
}


/**********************************************************************/

void TextChunkLoader::openHook()
{
    _scanner.open(fp(), query());
}

bool TextChunkLoader::loadChunk(boost::shared_ptr<Query>& query, size_t chunkIndex)
{
    SCIDB_ASSERT(_where != W_EndOfStream);

    Dimensions const& dims = schema().getDimensions();
    Attributes const& attrs = schema().getAttributes();
    size_t nAttrs = attrs.size();
    size_t nDims = dims.size();
    vector< boost::shared_ptr<ChunkIterator> > chunkIterators(nAttrs);
    Value tmpVal;

    bool isSparse = false;
BeginScanChunk:
    {
        Token tkn = _scanner.get();
        if (tkn == TKN_SEMICOLON) {
            tkn = _scanner.get();
        }
        if (tkn == TKN_EOF) {
            _where = W_EndOfStream;
            return false;
        }
        bool explicitChunkPosition = false;
        if (_where != W_InsideArray) {
            if (tkn == TKN_COORD_BEGIN) {
                explicitChunkPosition = true;
                for (size_t i = 0; i < nDims; i++)
                {
                    if (i != 0 && _scanner.get() != TKN_COMMA)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << ",";
                    if (_scanner.get() != TKN_LITERAL)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR3);
                    StringToValue( TID_INT64, _scanner.getValue(), _coordVal);
                    _chunkPos[i] = _coordVal.getInt64();
                    if ((_chunkPos[i] - dims[i].getStartMin()) % dims[i].getChunkInterval() != 0)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR4);
                }

                enforceChunkOrder("text loader 1");

                if (_scanner.get() != TKN_COORD_END)
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "}";
                tkn = _scanner.get();
                LOG4CXX_TRACE(logger, "Explicit chunk coords are { " << CoordsToStr(_chunkPos) << " }");
            }
            if (tkn != TKN_ARRAY_BEGIN)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "[";
            tkn = _scanner.get();
        }
        for (size_t i = 1; i < nDims; i++) {
            if (tkn != TKN_ARRAY_BEGIN)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "[";
            tkn = _scanner.get();
        }

        if (tkn == TKN_ARRAY_BEGIN)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR6);
        if (!explicitChunkPosition) {
            nextImplicitChunkPosition(ANY_CHUNK);
        }
        Coordinates const* first = NULL;
        Coordinates const* last = NULL;
        Coordinates pos = _chunkPos;

        while (true) {
            if (tkn == TKN_COORD_BEGIN) {
                isSparse = true;
                for (size_t i = 0; i < nDims; i++) {
                    if (i != 0 && _scanner.get() != TKN_COMMA)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << ",";
                    if (_scanner.get() != TKN_LITERAL)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR3);
                    StringToValue( TID_INT64, _scanner.getValue(), _coordVal);
                    pos[i] = _coordVal.getInt64();
                }
                if (_scanner.get() != TKN_COORD_END)
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "}";
                tkn = _scanner.get();
            }
            bool inParen = false;
            if (tkn == TKN_TUPLE_BEGIN) {
                inParen = true;
                tkn = _scanner.get();
            }
            array()->countCell();
            if (tkn == TKN_LITERAL || (inParen && tkn == TKN_COMMA)) {
                for (size_t i = 0; i < nAttrs; i++) {
                    if (!chunkIterators[i]) {
                        if (isSparse && !explicitChunkPosition) {
                            _chunkPos = pos;
                            schema().getChunkPositionFor(_chunkPos);
                            LOG4CXX_TRACE(logger, "New chunk coords { " << CoordsToStr(_chunkPos) << " }");
                        }
                        if (i==0) {
                            enforceChunkOrder("text loader 2");
                        }
                        Address addr(i, _chunkPos);
                        MemChunk& chunk = getLookaheadChunk(i, chunkIndex);
                        chunk.initialize(array(), &schema(), addr,
                                         attrs[i].getDefaultCompressionMethod());
                        if (first == NULL) {
                            first = &chunk.getFirstPosition(true);
                            if (!isSparse) {
                                pos = *first;
                            }
                            last = &chunk.getLastPosition(true);
                        }
                        chunkIterators[i] = chunk.getIterator(query, ChunkIterator::NO_EMPTY_CHECK
                                                              | (!isSparse ? ConstChunkIterator::SEQUENTIAL_WRITE : 0));
                    }
                    if (!(chunkIterators[i]->setPosition(pos))) {
                        // Load from sparse/dense file {f} at coord {pos} is out of chunk bounds: {chunkPos}
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_OOB)
                            << (isSparse ? "sparse" : "dense")
                            << _scanner.getFilePath()
                            << CoordsToStr(pos)
                            << CoordsToStr(_chunkPos);
                    }
                    _fileOffset = _scanner.getPosition();
                    if ((inParen && (tkn == TKN_COMMA || tkn == TKN_TUPLE_END)) || (!inParen && i != 0)) {
                        if (i == emptyTagAttrId()) {
                            attrVal(i).setBool(true);
                            chunkIterators[i]->writeItem(attrVal(i));
                        } else {
                            chunkIterators[i]->writeItem(attrs[i].getDefaultValue());
                        }
                        if (inParen && tkn == TKN_COMMA) {
                            tkn = _scanner.get();
                        }
                    } else {
                        if (tkn != TKN_LITERAL)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR8);
                        try {
                            if (_scanner.isNull()) {
                                if (!schema().getAttributes()[i].isNullable())
                                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);
                                attrVal(i).setNull(_scanner.getMissingReason());
                            } else if (converter(i)) {
                                tmpVal.setString(_scanner.getValue().c_str());
                                const Value* v = &tmpVal;
                                (*converter(i))(&v, &attrVal(i), NULL);
                            } else {
                                StringToValue(typeIdOfAttr(i), _scanner.getValue(), attrVal(i));
                            }
                            if (i == emptyTagAttrId()) {
                                if (!attrVal(i).getBool())
                                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR9);
                            }
                            chunkIterators[i]->writeItem(attrVal(i));
                        } catch(Exception const& x) {
                            try
                            {
                                // Scanner tracks position w/in file, load position
                                // info into *this so handleError() can get it.
                                _fileOffset = _scanner.getPosition();
                                _line = _scanner.getLine();
                                _column = _scanner.getColumn();
                                _badField = _scanner.getValue();
                                array()->handleError(x, chunkIterators[i], i);
                            }
                            catch (Exception const& x)
                            {
                                if (x.getShortErrorCode() == SCIDB_SE_TYPE_CONVERSION && i == emptyTagAttrId())
                                {
                                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR15);
                                }
                                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR16);
                            }
                        }
                        tkn = _scanner.get();
                        if (inParen && i+1 < nAttrs && tkn == TKN_COMMA) {
                            tkn = _scanner.get();
                        }
                    }
                    if (!isSparse) {
                        ++(*chunkIterators[i]);
                    }
                }
            } else if (inParen && tkn == TKN_TUPLE_END && !isSparse) {
                for (size_t i = 0; i < nAttrs; i++) {
                    if (!chunkIterators[i]) {
                        if (i==0) {
                            enforceChunkOrder("text loader 3");
                        }
                        Address addr(i, _chunkPos);
                        MemChunk& chunk = getLookaheadChunk(i, chunkIndex);
                        chunk.initialize(array(), &schema(), addr,
                                         schema().getAttributes()[i].getDefaultCompressionMethod());
                        if (first == NULL) {
                            first = &chunk.getFirstPosition(true);
                            last = &chunk.getLastPosition(true);
                            pos = *first;
                        }
                        chunkIterators[i] = chunk.getIterator(query,
                                                              ChunkIterator::NO_EMPTY_CHECK|
                                                              ConstChunkIterator::SEQUENTIAL_WRITE);
                    }
                    if (emptyTagAttrId() == INVALID_ATTRIBUTE_ID) {
                        chunkIterators[i]->writeItem(attrs[i].getDefaultValue());
                    }
                    ++(*chunkIterators[i]);
                }
            }
            array()->completeShadowArrayRow(); // done with cell/record
            if (inParen) {
                if (tkn != TKN_TUPLE_END)
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << ")";
                tkn = _scanner.get();
                if (!isSparse && tkn == TKN_MULTIPLY) {
                    // Here's why text loader needs entire _attrVals[] vector.
                    tkn = _scanner.get();
                    if (tkn != TKN_LITERAL)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "multiplier";
                    Value countVal;
                    StringToValue(TID_INT64, _scanner.getValue(), countVal);
                    int64_t count = countVal.getInt64();
                    while (--count != 0) {
                        for (size_t i = 0; i < nAttrs; i++) {
                            chunkIterators[i]->writeItem(attrVal(i));
                            ++(*chunkIterators[i]);
                        }
                    }
                    tkn = _scanner.get();
                    pos = chunkIterators[0]->getPosition();
                    pos[nDims-1] -= 1;
                }
            }
            size_t nBrackets = 0;
            if (isSparse) {
                while (tkn == TKN_ARRAY_END) {
                    if (++nBrackets == nDims) {
                        if (first == NULL) { // empty chunk
                            goto BeginScanChunk;
                        }
                        _where = W_EndOfChunk;
                        goto EndScanChunk;
                    }
                    tkn = _scanner.get();
                }
            } else {
                if (NULL == last ) {
                    _where = W_EndOfStream;
                    return false;
                }
                for (size_t i = nDims-1; ++pos[i] > (*last)[i]; i--) {
                    if (i == 0) {
                        if (tkn == TKN_ARRAY_END) {
                            _where = W_EndOfChunk;
                        } else if (tkn == TKN_COMMA) {
                            _where = W_InsideArray;
                        } else {
                            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR2) << "]";
                        }
                        goto EndScanChunk;
                    }
                    if (tkn != TKN_ARRAY_END)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "]";
                    nBrackets += 1;
                    pos[i] = (*first)[i];
                    tkn = _scanner.get();
                }
            }
            if (tkn == TKN_COMMA) {
                tkn = _scanner.get();
            }
            while (nBrackets != 0 ) {
                if (tkn != TKN_ARRAY_BEGIN)
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "[";
                nBrackets -= 1;
                tkn = _scanner.get();
            }
        }
    }
EndScanChunk:
    if (!isSparse && emptyTagAttrId() == INVALID_ATTRIBUTE_ID) {
        for (size_t i = 0; i < nAttrs; i++) {
            if (chunkIterators[i]) {
                while (!chunkIterators[i]->end()) {
                    chunkIterators[i]->writeItem(attrs[i].getDefaultValue());
                    ++(*chunkIterators[i]);
                }
            }
        }
    }
    for (size_t i = 0; i < nAttrs; i++) {
        if (chunkIterators[i]) {
            chunkIterators[i]->flush();
        }
    }
    return true;
}

/**********************************************************************/

TsvChunkLoader::TsvChunkLoader()
    : _lineBuf(0)
    , _lineLen(0)
    , _errorOffset(0)
    , _tooManyWarning(false) // warnings squelch
{ }

TsvChunkLoader::~TsvChunkLoader()
{
    if (_lineBuf) {
        ::free(_lineBuf);
    }
}

void TsvChunkLoader::bindHook()
{
    // For now at least, flat arrays only.
    Dimensions const& dims = schema().getDimensions();
    if (dims.size() != 1) {
        throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR,
                             SCIDB_LE_MULTIDIMENSIONAL_ARRAY_NOT_ALLOWED);
    }
}

bool TsvChunkLoader::loadChunk(boost::shared_ptr<Query>& query, size_t chunkIndex)
{
    // Must do EOF check *before* nextImplicitChunkPosition() call, or
    // we risk stepping out of bounds.
    int ch = ::getc(fp());
    if (ch == EOF) {
        return false;
    }
    ::ungetc(ch, fp());

    // Reposition and make sure all is cool.
    nextImplicitChunkPosition(MY_CHUNK);
    enforceChunkOrder("tsv loader");

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

    TsvParser parser;
    if (hasOption('p')) {
        parser.setDelim('|');
    } else if (hasOption('c')) {
        // Seems sick and wrong---should use 'csv' format instead---but allow for now.
        parser.setDelim(',');
    }

    char const *field = 0;
    unsigned rc = 0;
    bool sawData = false;

    while (!chunkIterators[0]->end()) {

        ssize_t nread = ::getline(&_lineBuf, &_lineLen, fp());
        if (nread == EOF) {
            break;
        }

        sawData = true;
        _column = 0;
        _fileOffset += nread;
        _line += 1;
        parser.reset(_lineBuf);
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

                // Parse out next input record field.
                rc = parser.getField(field);
                if (rc == TsvParser::EOL) {
                    // Previous getField() set end-of-line, but we have more attributes!
                    throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_TOO_FEW_FIELDS)
                        << _fileOffset << _line << _column;
                }
                if (rc == TsvParser::ERR) {
                    throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_TSV_PARSE_ERROR);
                }
                SCIDB_ASSERT(field);

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
                    StringToValue(typeIdOfAttr(i), field, attrVal(i));
                    chunkIterators[i]->writeItem(attrVal(i));
                }
            }
            catch (Exception& ex) {
                _badField = field;
                _errorOffset = (_fileOffset - nread) + (field - _lineBuf);
                array()->handleError(ex, chunkIterators[i], i);
            }

            _column += 1;
            ++(*chunkIterators[i]);
        }

        // We should be at EOL now, otherwise there are too many fields on this line.  Post a
        // warning: it seems useful not to complain too loudly about this or to abort the load, but
        // we do want to mention it.
        //
        rc = parser.getField(field);
        if (!_tooManyWarning && (rc != TsvParser::EOL)) {
            _tooManyWarning = true;
            query->postWarning(SCIDB_WARNING(SCIDB_LE_OP_INPUT_TOO_MANY_FIELDS)
                               << _fileOffset << _line << _column);
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
