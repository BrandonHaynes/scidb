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
 * InputArray.cpp
 *
 *  Created on: Sep 23, 2010
 */
#include <boost/format.hpp>
#include <boost/foreach.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

#include <sstream>

#include "InputArray.h"

#include "system/SystemCatalog.h"
#include "system/Utils.h"
#include "util/StringUtil.h"
#include "array/DBArray.h"

#include <sys/socket.h>
#include <sys/utsname.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>


namespace scidb
{
    using namespace std;
    using namespace boost;
    using namespace boost::archive;

    const int GETHOSTBYNAME_BUF_SIZE = 1*KiB;


#ifdef SUPPORT_INPUT_FROM_SOCKET // sockets support is not confirmed
    static int openSocket(string const& addr)
    {
        LOG4CXX_DEBUG(logger, "Attempting to open socket " << addr);
                sockaddr_in sock_inet;
                struct hostent* hp;
                int rc;
        size_t at = addr.find('@');
        string host = addr.substr(0, at);
        int port = atoi(addr.substr(at+1).c_str());

#ifndef __APPLE__
        struct hostent ent;  // entry in hosts table
        char ghbn_buf[GETHOSTBYNAME_BUF_SIZE];
        int h_err;

        if (gethostbyname_r(host.c_str(), &ent, ghbn_buf, sizeof ghbn_buf, &hp, &h_err) != 0
            || hp == NULL
            || hp->h_addrtype != AF_INET)
        {
            LOG4CXX_DEBUG(logger, "Failed to resolve host " << host << ", errno = " << h_err);
            return -1;
        }
#else
        if ((hp = gethostbyname(host.c_str())) == NULL || hp->h_addrtype != AF_INET)
        {
            LOG4CXX_DEBUG(logger, "Failed to resolve host " << host << ", errno = " << errno);
            return -1;
        }
#endif

        sock_inet.sin_family = AF_INET;
        sock_inet.sin_port = htons(port);
        int sd = -1;
        for (int i = 0; hp->h_addr_list[i] != NULL; i++) {
            memcpy(&sock_inet.sin_addr, hp->h_addr_list[i], sizeof sock_inet.sin_addr);
            sd = socket(AF_INET, SOCK_STREAM, 0);
            if (sd < 0) {
                LOG4CXX_DEBUG(logger, "Attempt to create socket failed with errno = " << errno);
                return -1;
            }
            do {
                rc = ::connect(sd, (sockaddr*)&sock_inet, sizeof(sock_inet));
            } while (rc < 0 && errno == EINTR);

            if (rc < 0) {
                if (errno != ENOENT && errno != ECONNREFUSED) {
                    close(sd);
                    sd = -1;
                    break;
                }
            } else {
                break;
            }
            close(sd);
            sd = -1;
        }
        if (sd < 0) {
            LOG4CXX_DEBUG(logger, "Failed to connect to host " << host << ", errno = " << errno);
            close(sd);
            return -1;
        }
        return sd;
    }
#endif

    void Scanner::openStringStream(string const& input)
    {
        size_t size = input.size();
        buf = new char[size+1];
        memcpy(buf, input.c_str(), size+1);
        f = openMemoryStream(buf, size);
    }

    bool Scanner::open(string const& input, InputType inputType, boost::shared_ptr<Query> query)
    {
        missingReason = -1;
        lineNo = 1;
        columnNo = 0;
        pos = 0;
        if (AS_BINARY_FILE == inputType || AS_TEXT_FILE == inputType)
        {
#ifdef SUPPORT_INPUT_FROM_SOCKET // sockets support is not confirmed
            size_t at = input.find('@');
            if (at != string::npos) {
                int sd = openSocket(input);
                if (sd < 0) {
                    return false;
                }
                f = fdopen(sd, AS_BINARY_FILE == inputType ? "rb" : "r");
            }
            else
#endif
            {
                LOG4CXX_DEBUG(logger, "Attempting to open file '" << input << "' for input");
                f = fopen(input.c_str(), AS_BINARY_FILE == inputType ? "rb" : "r");
            }
            filePath = input;
            if (NULL == f) {
                LOG4CXX_DEBUG(logger,"Attempt to open input file '" << input << "' and failed with errno = " << errno);
                return false;
            }
        }
        else if (AS_STRING == inputType)
        {
            filePath = "<string>";
            openStringStream(input);
        }
        if (f != NULL && inputType != AS_BINARY_FILE) {
            doubleBuffer = shared_ptr<BufferedFileInput>(new BufferedFileInput(f, query));
        }
        return true;
    }

    void InputArray::resetShadowChunkIterators()
    {
        for (size_t i = 0, n = shadowChunkIterators.size(); i < n; i++) {
            shadowChunkIterators[i]->flush();
        }
        shadowChunkIterators.clear();
    }

    ConstChunk const& InputArrayIterator::getChunk()
    {
        return array.getChunk(attr, currChunkIndex);
    }

    bool InputArrayIterator::end()
    {
        try
        {
            if (!hasCurrent)
                hasCurrent = array.moveNext(currChunkIndex);
        }
        catch(Exception const& x)
        {
            array.resetShadowChunkIterators();
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_FILE_IMPORT_FAILED)
                << array.scanner.getFilePath()
                << Query::getQueryByID(Query::getCurrentQueryID())->getInstanceID()
                << array.getName()
                << array.scanner.getLine()
                << array.scanner.getColumn()
                << array.scanner.getPosition()
                << encodeString(array.scanner.getValue())
                << x.getErrorMessage();
        }
        catch(std::exception const& x)
        {
            array.resetShadowChunkIterators();
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_FILE_IMPORT_FAILED)
                << array.scanner.getFilePath()
                << Query::getQueryByID(Query::getCurrentQueryID())->getInstanceID()
                << array.getName()
                << array.scanner.getLine()
                << array.scanner.getColumn()
                << array.scanner.getPosition()
                << encodeString(array.scanner.getValue())
                << x.what();
        }
        catch(...)
        {
            array.resetShadowChunkIterators();
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_FILE_IMPORT_FAILED)
                << array.scanner.getFilePath()
                << Query::getQueryByID(Query::getCurrentQueryID())->getInstanceID()
                << array.getName()
                << array.scanner.getLine()
                << array.scanner.getColumn()
                << array.scanner.getPosition()
                << encodeString(array.scanner.getValue())
                << "unknown error of data load";
        }
        return !hasCurrent;
    }

    void InputArrayIterator::operator ++()
    {
        if (end())
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        hasCurrent = false;
        currChunkIndex += 1;
    }

    Coordinates const& InputArrayIterator::getPosition()
    {
        return getChunk().getFirstPosition(false);
    }

    InputArrayIterator::InputArrayIterator(InputArray& arr, AttributeID id)
    : array(arr), attr(id), hasCurrent(false), currChunkIndex(1)
    {
    }

    ArrayDesc const& InputArray::getArrayDesc() const
    {
        return desc;
    }

    boost::shared_ptr<ConstArrayIterator> InputArray::getConstIterator(AttributeID attr) const
    {
        InputArray& self = *(InputArray*)this;
        if (!self.iterators[attr]) {
            for (size_t i = 0, n = self.iterators.size(); i < n; i++) {
                self.iterators[i] = boost::shared_ptr<InputArrayIterator>(new InputArrayIterator(self, i));
            }
        }
        return self.iterators[attr];
    }

    static ArrayDesc generateShadowArraySchema(ArrayDesc const& targetArray, string const& shadowArrayName)
    {
        Attributes const& srcAttrs = targetArray.getAttributes(true);
        size_t nAttrs = srcAttrs.size();
        Attributes dstAttrs(nAttrs+2);
        for (size_t i = 0; i < nAttrs; i++) {
            dstAttrs[i] = AttributeDesc(i, srcAttrs[i].getName(), TID_STRING,  AttributeDesc::IS_NULLABLE, 0);
        }
        dstAttrs[nAttrs] = AttributeDesc(nAttrs, "row_offset", TID_INT64, 0, 0);
        dstAttrs[nAttrs+1] = AttributeDesc(nAttrs+1, DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME, TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0);
        return ArrayDesc(shadowArrayName, dstAttrs, targetArray.getDimensions());
    }

InputArray::InputArray(ArrayDesc const& array, string const& input, string const& format, boost::shared_ptr<Query>& query, InputType inputType, int64_t maxCnvErrors, string const& shadowArrayName, bool parallel)
    : desc(array),
      chunkPos(array.getDimensions().size()),
      currChunkIndex(0),
      iterators(array.getAttributes().size()),
      lookahead(iterators.size()),
      types(iterators.size()),
      attrVal(iterators.size()),
      binVal(iterators.size()),
      converters(iterators.size()),
      coordVal(TypeLibrary::getType(TID_INT64)),
      strVal(TypeLibrary::getType(TID_STRING)),
      emptyTagAttrID(array.getEmptyBitmapAttribute() != NULL ? array.getEmptyBitmapAttribute()->getId() : INVALID_ATTRIBUTE_ID),
      nLoadedCells(0),
      nLoadedChunks(0),
      nErrors(0),
      maxErrors(maxCnvErrors),
      signature(OpaqueChunkHeader::calculateSignature(array)),
      state(inputType == AS_EMPTY ? EmptyArray : Init),
      nAttrs(array.getAttributes(true).size()),
      myInstanceID(query->getInstanceID()),
      nInstances(query->getInstancesCount()),
      parallelLoad(parallel)
    {
        assert(query);
        _query=query;
        Dimensions const& dims = array.getDimensions();
        Attributes const& attrs = array.getAttributes();
        size_t nDims = chunkPos.size();
        size_t nAttrs = attrs.size();
        for (size_t i = 0; i < nDims; i++) {
            chunkPos[i] = dims[i].getStart();
        }
        for (size_t i = 0; i < nAttrs; i++) {
            types[i] = attrs[i].getType();
            attrVal[i] = Value(TypeLibrary::getType(types[i]));
            if (attrs[i].isEmptyIndicator()) {
                attrVal[i].setBool(true);
            }
        }
        if (!shadowArrayName.empty()) {
            shadowArray = boost::shared_ptr<Array>(new MemArray(generateShadowArraySchema(array, shadowArrayName),query));
        }
        if (!format.empty() && (format[0] == '(' || compareStringsIgnoreCase(format, "opaque") == 0)) {
            templ = TemplateParser::parse(array, format, true);
            binaryLoad = true;
        } else {
            binaryLoad = false;
            for (size_t i = 0; i < nAttrs; i++) {
                if (!isBuiltinType(types[i])) {
                    converters[i] = FunctionLibrary::getInstance()->findConverter( TID_STRING, types[i]);
                }
            }
        }
        chunkPos[nDims-1] -= dims[nDims-1].getChunkInterval();
        if (!scanner.open(input, inputType, query))  {
            LOG4CXX_DEBUG(logger,"Attempt to open input file '" << input<< "' and failed with errno = " << errno);
            if (parallel) {
                state = EmptyArray;
                LOG4CXX_WARN(logger, "Failed to open file " << input << " for input");
            } else {
                scheduleSG(query);
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CANT_OPEN_FILE) << input << errno;
            }
        }

    }
    //
    // This function simulates SG opertator, may there is better way to use SG code directly?
    //

    static void redistributeCallback(void* arg)
    {
        ((InputArray*)arg)->sg();
    }


    void InputArray::redistributeShadowArray(boost::shared_ptr<Query> const& query)
    {
        //All arrays are currently stored as round-robin. Let's store shadow arrays round-robin as well
        //TODO: revisit this when we allow users to store arrays with specified distributions
        PartitioningSchema ps = psHashPartitioned;
        ArrayDesc shadowArrayDesc = shadowArray->getArrayDesc();
        ArrayID arrayID = INVALID_ARRAY_ID;   /**< ID of new array */
        ArrayID updateableArrayID = INVALID_ARRAY_ID;   /**< ID of new array */
        string shadowArrayVersionName;
        string shadowArrayName = shadowArrayDesc.getName();
        boost::shared_ptr<SystemCatalog::LockDesc> lock;
        VersionID version;
        ArrayDesc desc;

        LOG4CXX_DEBUG(logger, "Redistribute shadow array " << shadowArrayName);
        if (query->getCoordinatorID() == COORDINATOR_INSTANCE) {
            lock = boost::shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(shadowArrayName,
                                                                                          query->getQueryID(),
                                                                                          Cluster::getInstance()->getLocalInstanceId(),
                                                                                          SystemCatalog::LockDesc::COORD,
                                                                                          SystemCatalog::LockDesc::WR));
            shared_ptr<Query::ErrorHandler> ptr(new UpdateErrorHandler(lock));
            query->pushErrorHandler(ptr);

            bool arrayExists = SystemCatalog::getInstance()->getArrayDesc(shadowArrayName, desc, false);
            VersionID lastVersion = 0;
            if (!arrayExists) {
                lock->setLockMode(SystemCatalog::LockDesc::CRT);
                bool updatedArrayLock = SystemCatalog::getInstance()->updateArrayLock(lock);
                SCIDB_ASSERT(updatedArrayLock);
                desc = shadowArrayDesc;
                SystemCatalog::getInstance()->addArray(desc, psHashPartitioned);
            } else {
                if (desc.getAttributes().size() != shadowArrayDesc.getAttributes().size() ||
                    desc.getDimensions().size() != shadowArrayDesc.getDimensions().size())
                {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAY_ALREADY_EXIST) << desc.getName();
                }
                lastVersion = SystemCatalog::getInstance()->getLastVersion(desc.getId());
            }
            version = lastVersion+1;
            LOG4CXX_DEBUG(logger, "Use version " << version << " of shadow array " << shadowArrayName);
            updateableArrayID = desc.getId();
            lock->setArrayId(updateableArrayID);
            lock->setArrayVersion(version);
            bool updatedArrayLock = SystemCatalog::getInstance()->updateArrayLock(lock);
            SCIDB_ASSERT(updatedArrayLock);

            shadowArrayVersionName = ArrayDesc::makeVersionedName(shadowArrayName, version);
            LOG4CXX_DEBUG(logger, "Create shadow array " << shadowArrayVersionName);
            shadowArrayDesc = ArrayDesc(shadowArrayVersionName,  desc.getAttributes(), desc.getDimensions());
            SystemCatalog::getInstance()->addArray(shadowArrayDesc, ps);

            arrayID = shadowArrayDesc.getId();
            lock->setArrayVersionId(shadowArrayDesc.getId());
            updatedArrayLock = SystemCatalog::getInstance()->updateArrayLock(lock);
            SCIDB_ASSERT(updatedArrayLock);
        } else {
            bool arrayExists = SystemCatalog::getInstance()->getArrayDesc(shadowArrayName, desc, false);
            VersionID lastVersion = 0;
            if (arrayExists) {
                lastVersion = SystemCatalog::getInstance()->getLastVersion(desc.getId());
            }
            version = lastVersion+1;
            LOG4CXX_DEBUG(logger, "Use version " << version << " of shadow array " << shadowArrayName);
            shadowArrayVersionName = ArrayDesc::makeVersionedName(shadowArrayName, version);
        }

        shadowArray = redistribute(shadowArray, query, ps); // it will synchronize all nodes with coordinator

        if (arrayID == INVALID_ARRAY_ID) { // worker node
            assert(!lock);
            lock = boost::shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(shadowArrayName,
                                                                                          query->getQueryID(),
                                                                                          Cluster::getInstance()->getLocalInstanceId(),
                                                                                          SystemCatalog::LockDesc::WORKER,
                                                                                          SystemCatalog::LockDesc::WR));
            lock->setArrayVersion(version);
            shared_ptr<Query::ErrorHandler> ptr(new UpdateErrorHandler(lock));
            query->pushErrorHandler(ptr);

            Query::Finalizer f = bind(&UpdateErrorHandler::releaseLock, lock, _1);
            query->pushFinalizer(f);
            SystemCatalog::ErrorChecker errorChecker = bind(&Query::validate, query);
            if (!SystemCatalog::getInstance()->lockArray(lock, errorChecker)) {
                throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_INCREMENT_LOCK) << shadowArrayName;
            }
        }
        shared_ptr<DBArray> persistentShadowArray(DBArray::newDBArray(shadowArrayVersionName, query));
        ArrayDesc const& dstArrayDesc = persistentShadowArray->getArrayDesc();

        query->getReplicationContext()->enableInboundQueue(dstArrayDesc.getId(), persistentShadowArray);

        persistentShadowArray->populateFrom(shadowArray);

        query->getReplicationContext()->replicationSync(dstArrayDesc.getId());
        query->getReplicationContext()->removeInboundQueue(dstArrayDesc.getId());
        StorageManager::getInstance().flush();

        if (updateableArrayID != INVALID_ARRAY_ID) {
            VersionID newVersionID = SystemCatalog::getInstance()->createNewVersion(updateableArrayID, arrayID);
            LOG4CXX_DEBUG(logger, "Create new version " << newVersionID << " of shadow array " << shadowArrayName);
        }
        // XXX TODO: add: getInjectedErrorListener().check();
    }

    void InputArray::sg()
    {
        shared_ptr<Query> query(Query::getValidQueryPtr(_query));
        if (shadowArray) {
            redistributeShadowArray(query);
        }
    }

    void InputArray::scheduleSG(boost::shared_ptr<Query> const& query)
    {
        boost::shared_ptr<SGContext> sgCtx =  boost::dynamic_pointer_cast<SGContext>(query->getOperatorContext());
        resetShadowChunkIterators();
        shadowArrayIterators.clear();
        if (sgCtx) {
            sgCtx->onSGCompletionCallback = redistributeCallback;
            sgCtx->callbackArg = this;
        } else {
            sg();
        }
    }

    void InputArray::handleError(Exception const& x,
                                 boost::shared_ptr<ChunkIterator> iterator,
                                 AttributeID i,
                                 int64_t pos)
    {
        scanner.setPosition(pos);
        string const& msg = x.getErrorMessage();
        Attributes const& attrs = desc.getAttributes();
        LOG4CXX_ERROR(logger, "Failed to convert attribute " << attrs[i].getName()
                      << " at position " << pos << " line " << scanner.getLine()
                      << " column " << scanner.getColumn() << ": " << msg);
        if (++nErrors > maxErrors) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR16);
        }
        if (attrs[i].isNullable()) {
            attrVal[i].setNull();
        } else {
            attrVal[i].setSize(TypeLibrary::getType(attrs[i].getType()).byteSize());
            attrVal[i] = TypeLibrary::getDefaultValue(attrs[i].getType());
        }
        iterator->writeItem(attrVal[i]);
        if (shadowArray) {
            if (shadowChunkIterators.empty()) {
                shared_ptr<Query> query(Query::getValidQueryPtr(_query));
                if (shadowArrayIterators.empty()) {
                    shadowArrayIterators.resize(nAttrs+1);
                    for (size_t j = 0; j < nAttrs; j++) {
                        shadowArrayIterators[j] = shadowArray->getIterator(j);
                    }
                    shadowArrayIterators[nAttrs] = shadowArray->getIterator(nAttrs);
                }
                shadowChunkIterators.resize(nAttrs+1);
                for (size_t j = 0; j < nAttrs; j++) {
                    shadowChunkIterators[j] = shadowArrayIterators[j]->newChunk(chunkPos, 0).getIterator(query, ChunkIterator::NO_EMPTY_CHECK|ChunkIterator::SEQUENTIAL_WRITE);
                }
                shadowChunkIterators[nAttrs] = shadowArrayIterators[nAttrs]->newChunk(chunkPos, 0).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
            }
            Coordinates const& currPos = iterator->getPosition();
            if (lastBadAttr < 0) {
                Value rowOffset;
                rowOffset.setInt64(pos);
                shadowChunkIterators[nAttrs]->setPosition(currPos);
                shadowChunkIterators[nAttrs]->writeItem(rowOffset);
            }
            strVal.setNull();
            while (AttributeID(++lastBadAttr) < i) {
                shadowChunkIterators[lastBadAttr]->setPosition(currPos);
                shadowChunkIterators[lastBadAttr]->writeItem(strVal);
            }
            shadowChunkIterators[i]->setPosition(currPos);
            strVal.setString(msg.c_str());
            shadowChunkIterators[i]->writeItem(strVal);
        }
    }

    void InputArray::completeShadowArrayRow()
    {
        if (lastBadAttr >= 0) {
            strVal.setNull();
            Coordinates const& currPos =  shadowChunkIterators[nAttrs]->getPosition(); // rowOffset attribute should be already set
            while (AttributeID(++lastBadAttr) < nAttrs) {
                shadowChunkIterators[lastBadAttr]->setPosition(currPos);
                shadowChunkIterators[lastBadAttr]->writeItem(strVal);
            }
        }
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

    bool InputArray::loadOpaqueChunk(boost::shared_ptr<Query>& query, size_t chunkIndex)
    {
        Dimensions const& dims = desc.getDimensions();
        Attributes const& attrs = desc.getAttributes();
        size_t nAttrs = attrs.size();
        size_t nDims = dims.size();

        FILE* f = scanner.getFile();
        OpaqueChunkHeader hdr;
        for (size_t i = 0; i < nAttrs; i++) {
            if (fread(&hdr, sizeof hdr, 1, f) != 1) {
                if (i == 0) {
                    state = EndOfStream;
                    scheduleSG(query);
                    return false;
                } else {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(f);
                }
            }
            if (hdr.magic != OPAQUE_CHUNK_MAGIC) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR10);
            }
            if (hdr.version != SCIDB_OPAQUE_FORMAT_VERSION) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_MISMATCHED_OPAQUE_FORMAT_VERSION) << hdr.version << SCIDB_OPAQUE_FORMAT_VERSION;
            }
            if (hdr.flags & OpaqueChunkHeader::ARRAY_METADATA)  {
                string arrayDescStr;
                arrayDescStr.resize(hdr.size);
                if (fread(&arrayDescStr[0], 1, hdr.size, f) != hdr.size) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(f);
                }
                stringstream ss;
                ss << arrayDescStr;
                ArrayDesc opaqueDesc;
                text_iarchive ia(ss);
                ia & opaqueDesc;
                compareArrayMetadata(desc, opaqueDesc);
                i -= 1; // compencate increment in for: repeat loop and try to load more mapping arrays
                continue;
            }
            if (hdr.signature != signature) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
            }
            if (fread(&chunkPos[0], sizeof(Coordinate), hdr.nDims, f) != hdr.nDims) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(f);
            }
            if (hdr.flags & OpaqueChunkHeader::COORDINATE_MAPPING)
            {
                //TODO-3667: remove this in next release (give folks one release cycle to adjust with proper error message)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ILLEGAL_OPERATION) << "Cannot re-load old style non-integer dimensions";
            } else {
                if (hdr.nDims != nDims) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_WRONG_NUMBER_OF_DIMENSIONS);
                }
                if (hdr.attrId != i) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_UNEXPECTED_DESTINATION_ATTRIBUTE) << attrs[i].getName();
                }
                Address addr(i, chunkPos);
                MemChunk& chunk = lookahead[i].chunks[chunkIndex % LOOK_AHEAD];
                chunk.initialize(this, &desc, addr, hdr.compressionMethod);
                chunk.setRLE((hdr.flags & OpaqueChunkHeader::RLE_FORMAT) != 0);
                chunk.setSparse((hdr.flags & OpaqueChunkHeader::SPARSE_CHUNK) != 0);
                chunk.allocate(hdr.size);
                if (fread(chunk.getData(), 1, hdr.size, f) != hdr.size) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(f);
                }
                chunk.write(query);
            }
        }
        return true;
    }

    bool InputArray::loadBinaryChunk(boost::shared_ptr<Query>& query, size_t chunkIndex)
    {
        Dimensions const& dims = desc.getDimensions();
        Attributes const& attrs = desc.getAttributes();
        size_t nAttrs = attrs.size();
        size_t nDims = dims.size();
        vector< boost::shared_ptr<ChunkIterator> > chunkIterators(nAttrs);

        FILE* f = scanner.getFile();
        int ch = getc(f);
        if (ch != EOF) {
            ungetc(ch, f);
        } else {
            state = EndOfStream;
            scheduleSG(query);
            return false;
        }
        size_t i = nDims-1;
        while (true) {
            chunkPos[i] += dims[i].getChunkInterval();
            if (chunkPos[i] <= dims[i].getEndMax()) {
                if (!parallelLoad || desc.getHashedChunkNumber(chunkPos) % nInstances == myInstanceID) {
                    break;
                }
            } else {
                if (0 == i) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR7);
                }
                chunkPos[i] = dims[i].getStart();
                i -= 1;
            }
        }
        for (size_t i = 0; i < nAttrs; i++) {
            Address addr(i, chunkPos);
            MemChunk& chunk =  lookahead[i].chunks[chunkIndex % LOOK_AHEAD];
            chunk.initialize(this, &desc, addr, attrs[i].getDefaultCompressionMethod());
            chunkIterators[i] = chunk.getIterator(query, ChunkIterator::NO_EMPTY_CHECK | ConstChunkIterator::SEQUENTIAL_WRITE);
        }
        size_t nCols = templ.columns.size();
        vector<uint8_t> buf(8);
        while (!chunkIterators[0]->end() && (ch = getc(f)) != EOF) {
            ungetc(ch, f);
            lastBadAttr = -1;
            nLoadedCells += 1;
            for (size_t i = 0, j = 0; i < nAttrs; i++, j++) {
                while (j < nCols && templ.columns[j].skip) {
                    ExchangeTemplate::Column const& column = templ.columns[j++];
                    if (column.nullable) {
                        int8_t missingReason;
                        if (fread(&missingReason, sizeof(missingReason), 1, f) != 1) {
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(f);
                        }
                    }
                    uint32_t size = (uint32_t)column.fixedSize;
                    if (size == 0) {
                        if (fread(&size, sizeof(size), 1, f) != 1) {
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(f);
                        }
                    }
                    if (buf.size() < size) {
                        buf.resize(size * 2);
                    }
                    if (fread(&buf[0], size, 1, f) != 1) {
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(f);
                    }
                }
                long pos = ftell(f);
                try {
                    if (j < nCols) {
                        ExchangeTemplate::Column const& column = templ.columns[j];
                        int8_t missingReason = -1;
                        if (column.nullable) {
                            if (fread(&missingReason, sizeof(missingReason), 1, f) != 1) {
                                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(f);
                            }
                        }
                        uint32_t size = (uint32_t)column.fixedSize;
                        if (size == 0) {
                            if (fread(&size, sizeof(size), 1, f) != 1) {
                                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(f);
                            }
                        }
                        if (missingReason >= 0) {
                            if (buf.size() < size) {
                                buf.resize(size * 2);
                            }
                            if (size && fread(&buf[0], size, 1, f) != 1) {
                                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(f);
                            }
                            attrVal[i].setNull(missingReason);
                            chunkIterators[i]->writeItem(attrVal[i]);
                        } else {
                            binVal[i].setSize(size);
                            if (fread(binVal[i].data(), 1, size, f) != size) {
                                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(f);
                            }
                            if (column.converter) {
                                Value const* v = &binVal[i];
                                column.converter(&v, &attrVal[i], NULL);
                                chunkIterators[i]->writeItem(attrVal[i]);
                            } else {
                                chunkIterators[i]->writeItem(binVal[i]);
                            }
                        }
                    } else {
                        // empty tag
                        chunkIterators[i]->writeItem(attrVal[i]);
                    }
                } catch(Exception const& x) {
                    handleError(x, chunkIterators[i], i, pos);
                }
                ++(*chunkIterators[i]);
            }
            completeShadowArrayRow();
        }
        for (size_t i = 0; i < nAttrs; i++) {
            if (chunkIterators[i]) {
                chunkIterators[i]->flush();
            }
        }
        resetShadowChunkIterators();
        return true;
    }


    bool InputArray::loadTextChunk(boost::shared_ptr<Query>& query, size_t chunkIndex)
    {
        Dimensions const& dims = desc.getDimensions();
        Attributes const& attrs = desc.getAttributes();
        size_t nAttrs = attrs.size();
        size_t nDims = dims.size();
        vector< boost::shared_ptr<ChunkIterator> > chunkIterators(nAttrs);

        bool isSparse = false;
    BeginScanChunk:
        {
            Token tkn = scanner.get();
            if (tkn == TKN_SEMICOLON) {
                tkn = scanner.get();
            }
            if (tkn == TKN_EOF) {
                state = EndOfStream;
                scheduleSG(query);
                return false;
            }
            bool explicitChunkPosition = false;
            if (state != InsideArray) {
                if (tkn == TKN_COORD_BEGIN) {
                    explicitChunkPosition = true;
                    for (size_t i = 0; i < nDims; i++)
                    {
                        if (i != 0 && scanner.get() != TKN_COMMA)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << ",";
                        if (scanner.get() != TKN_LITERAL)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR3);
                        StringToValue( TID_INT64, scanner.getValue(), coordVal);
                        chunkPos[i] = coordVal.getInt64();
                        if ((chunkPos[i] - dims[i].getStart()) % dims[i].getChunkInterval() != 0)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR4);
                    }
                    //
                    // Check that this explicit chunkPos isn't inconsistent
                    // (ie. out of order). We should always grow chunk
                    // addresses.
                    for (size_t i = 0; i < lastChunkPos.size(); i++) {

                        if (!(lastChunkPos[i] <= chunkPos[i])) {
                            std::stringstream ss;
                            ss << "Given that the last chunk processed was { " << lastChunkPos << " } this chunk { " << chunkPos << " } is out of sequence";
                            LOG4CXX_DEBUG(logger, ss.str());
                        }
                        if (lastChunkPos[i] > chunkPos[i])
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR5);
                        if (lastChunkPos[i] < chunkPos[i]) {
                            break;
                        }
                    }
                    lastChunkPos = chunkPos;
                    if (scanner.get() != TKN_COORD_END)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "}";
                    tkn = scanner.get();
                    std::stringstream ss;
                    ss << "Explicit chunk coords are { " << chunkPos << " }";
                    LOG4CXX_TRACE(logger, ss.str());
                }
                if (tkn != TKN_ARRAY_BEGIN)
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "[";
                tkn = scanner.get();
            }
            for (size_t i = 1; i < nDims; i++) {
                if (tkn != TKN_ARRAY_BEGIN)
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "[";
                tkn = scanner.get();
            }

            if (tkn == TKN_ARRAY_BEGIN)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR6);
            if (!explicitChunkPosition) {
                size_t i = nDims-1;
                while (true) {
                    chunkPos[i] += dims[i].getChunkInterval();
                    if (chunkPos[i] <= dims[i].getEndMax()) {
                        break;
                    }
                    if (0 == i)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR7);
                    chunkPos[i] = dims[i].getStart();
                    i -= 1;
                }
                // Do not write into stream if TRACE is disabled.
                if (logger->isTraceEnabled())
                {
                    std::stringstream ss;
                    ss << "Implicit chunk coords { " << chunkPos << " }";
                    LOG4CXX_TRACE(logger, ss.str());
                }
            }
            Coordinates const* first = NULL;
            Coordinates const* last = NULL;
            Coordinates pos = chunkPos;

            while (true) {
                if (tkn == TKN_COORD_BEGIN) {
                    isSparse = true;
                    for (size_t i = 0; i < nDims; i++) {
                        if (i != 0 && scanner.get() != TKN_COMMA)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << ",";
                        if (scanner.get() != TKN_LITERAL)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR3);
                        StringToValue( TID_INT64, scanner.getValue(), coordVal);
                        pos[i] = coordVal.getInt64();
                    }
                    if (scanner.get() != TKN_COORD_END)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "}";
                    tkn = scanner.get();
                }
                bool inParen = false;
                if (tkn == TKN_TUPLE_BEGIN) {
                    inParen = true;
                    tkn = scanner.get();
                }
                nLoadedCells += 1;
                lastBadAttr = -1;
                if (tkn == TKN_LITERAL || (inParen && tkn == TKN_COMMA)) {
                    for (size_t i = 0; i < nAttrs; i++) {
                        if (!chunkIterators[i]) {
                            if (isSparse && !explicitChunkPosition) {
                                chunkPos = pos;
                                desc.getChunkPositionFor(chunkPos);
                            }
                            Address addr(i, chunkPos);
                            MemChunk& chunk =  lookahead[i].chunks[chunkIndex % LOOK_AHEAD];
                            chunk.initialize(this, &desc, addr, attrs[i].getDefaultCompressionMethod());
                            if (first == NULL) {
                                first = &chunk.getFirstPosition(true);
                                if (!isSparse) {
                                    pos = *first;
                                }
                                last = &chunk.getLastPosition(true);
                            }
                            chunkIterators[i] = chunk.getIterator(query, ChunkIterator::NO_EMPTY_CHECK
                                                                  | (isSparse ? ConstChunkIterator::SPARSE_CHUNK : ConstChunkIterator::SEQUENTIAL_WRITE));
                        }
                        if (isSparse) {
                            if (!( chunkIterators[i]->setPosition(pos) )) {
                                std::stringstream ss;
                                ss << "From sparse load file '" << scanner.getFilePath() << "' at coord " << pos << " is out of chunk bounds :" << chunkPos;
                                LOG4CXX_DEBUG(logger, ss.str());

                                if (!chunkIterators[i]->setPosition(pos))
                                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR7);
                            }
                        } else {
                            if (!(chunkIterators[i]->getPosition() == pos)) {
                                std::stringstream ss;
                                ss << "From dense load file '" << scanner.getFilePath() << "' at coord " << pos << " is out of chunk bounds :" << chunkPos;
                                LOG4CXX_DEBUG(logger, ss.str());
                            }
                            if (chunkIterators[i]->getPosition() != pos)
                                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR7);
                        }
                        int64_t pos = scanner.getPosition();
                        if ((inParen && (tkn == TKN_COMMA || tkn == TKN_TUPLE_END)) || (!inParen && i != 0)) {
                            if (i == emptyTagAttrID) {
                                attrVal[i].setBool(true);
                                chunkIterators[i]->writeItem(attrVal[i]);
                            } else if (chunkIterators[i]->getChunk().isRLE()/* && emptyTagAttrID != INVALID_ATTRIBUTE_ID*/) {
                                chunkIterators[i]->writeItem(attrs[i].getDefaultValue());
                            }
                            if (inParen && tkn == TKN_COMMA) {
                                tkn = scanner.get();
                            }
                        } else {
                            if (tkn != TKN_LITERAL)
                                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR8);
                            try {
                                if (scanner.isNull()) {
                                    if (!desc.getAttributes()[i].isNullable())
                                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);
                                    attrVal[i].setNull(scanner.getMissingReason());
                                } else if (converters[i]) {
                                    strVal.setString(scanner.getValue().c_str());
                                    const Value* v = &strVal;
                                    (*converters[i])(&v, &attrVal[i], NULL);
                                } else {
                                    StringToValue(types[i], scanner.getValue(), attrVal[i]);
                                }
                                if (i == emptyTagAttrID) {
                                    if (!attrVal[i].getBool())
                                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR9);
                                }
                                chunkIterators[i]->writeItem(attrVal[i]);
                            } catch(Exception const& x) {
                                try
                                {
                                    handleError(x, chunkIterators[i], i, pos);
                                }
                                catch (Exception const& x)
                                {
                                    if (x.getShortErrorCode() == SCIDB_SE_TYPE_CONVERSION && i == emptyTagAttrID)
                                    {
                                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR15);
                                    }
                                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR16);
                                }
                            }
                            tkn = scanner.get();
                            if (inParen && i+1 < nAttrs && tkn == TKN_COMMA) {
                                tkn = scanner.get();
                            }
                        }
                        if (!isSparse) {
                            ++(*chunkIterators[i]);
                        }
                    }
                } else if (inParen && tkn == TKN_TUPLE_END && !isSparse) {
                    for (size_t i = 0; i < nAttrs; i++) {
                        if (!chunkIterators[i]) {
                            Address addr(i, chunkPos);
                            MemChunk& chunk =  lookahead[i].chunks[chunkIndex % LOOK_AHEAD];
                            chunk.initialize(this, &desc, addr, desc.getAttributes()[i].getDefaultCompressionMethod());
                            if (first == NULL) {
                                first = &chunk.getFirstPosition(true);
                                last = &chunk.getLastPosition(true);
                                pos = *first;
                            }
                            chunkIterators[i] = chunk.getIterator(query, ChunkIterator::NO_EMPTY_CHECK|ConstChunkIterator::SEQUENTIAL_WRITE);
                        }
                        if (chunkIterators[i]->getChunk().isRLE() && emptyTagAttrID == INVALID_ATTRIBUTE_ID) {
                            chunkIterators[i]->writeItem(attrs[i].getDefaultValue());
                        }
                        ++(*chunkIterators[i]);
                    }
                }
                completeShadowArrayRow();
                if (inParen) {
                    if (tkn != TKN_TUPLE_END)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << ")";
                    tkn = scanner.get();
                    if (!isSparse && tkn == TKN_MULTIPLY) {
                        tkn = scanner.get();
                        if (tkn != TKN_LITERAL)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "multiplier";
                        Value countVal;
                        StringToValue(TID_INT64, scanner.getValue(), countVal);
                        int64_t count = countVal.getInt64();
                        while (--count != 0) {
                            for (size_t i = 0; i < nAttrs; i++) {
                                chunkIterators[i]->writeItem(attrVal[i]);
                                ++(*chunkIterators[i]);
                            }
                        }
                        tkn = scanner.get();
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
                            state = EndOfChunk;
                            goto EndScanChunk;
                        }
                        tkn = scanner.get();
                    }
                } else {
                    if (NULL == last ) {
                        state = EndOfStream;
                        scheduleSG(query);
                        return false;
                        /*
                          std::stringstream ss;
                          ss << "Dense load files need chunks of regular sizes - don't know size of current chunk " << chunkPos;
                          LOG4CXX_DEBUG(logger, ss.str());
                        */
                    }
                    if (!last)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR10);
                    for (size_t i = nDims-1; ++pos[i] > (*last)[i]; i--) {
                        if (i == 0) {
                            if (tkn == TKN_ARRAY_END) {
                                state = EndOfChunk;
                            } else if (tkn == TKN_COMMA) {
                                state = InsideArray;
                            } else {
                                throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR2) << "]";
                            }
                            goto EndScanChunk;
                        }
                        if (tkn != TKN_ARRAY_END)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "]";
                        nBrackets += 1;
                        pos[i] = (*first)[i];
                        tkn = scanner.get();
                    }
                }
                if (tkn == TKN_COMMA) {
                    tkn = scanner.get();
                }
                while (nBrackets != 0 ) {
                    if (tkn != TKN_ARRAY_BEGIN)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "[";
                    nBrackets -= 1;
                    tkn = scanner.get();
                }
            }
        }
    EndScanChunk:
        if (!isSparse && emptyTagAttrID == INVALID_ATTRIBUTE_ID) {
            for (size_t i = 0; i < nAttrs; i++) {
                if (chunkIterators[i] && chunkIterators[i]->getChunk().isRLE()) {
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
        resetShadowChunkIterators();
        return true;
    }

    bool InputArray::moveNext(size_t chunkIndex)
    {
        if (chunkIndex > currChunkIndex+1) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR1);
        }
        shared_ptr<Query> query(Query::getValidQueryPtr(_query));
        if (chunkIndex <= currChunkIndex) {
            return true;
        }
        if (state == EmptyArray) {
            state = EndOfStream;
            scheduleSG(query);
            return false;
        }
        if (state == EndOfStream) {
            return false;
        }

        bool result;
        if (binaryLoad) {
            if (templ.opaque) {
                result = loadOpaqueChunk(query, chunkIndex);
            } else {
                result = loadBinaryChunk(query, chunkIndex);
            }
        } else {
            result = loadTextChunk(query, chunkIndex);
        }
        if (result) {
            nLoadedChunks += 1;
            LOG4CXX_TRACE(logger, "Loading of " << desc.getName() << " is in progress: load at this moment " << nLoadedChunks << " chunks and " << nLoadedCells << " cells with " << nErrors << " errors");

            currChunkIndex += 1;
        }
        LOG4CXX_TRACE(logger, "Finished scan of chunk number " << currChunkIndex << ", result=" << result);
        return result;
    }

    InputArray::~InputArray()
    {
        LOG4CXX_INFO(logger, "Loading of " << desc.getName() << " is completed: loaded " << nLoadedChunks << " chunks and " << nLoadedCells << " cells with " << nErrors << " errors");
    }


    ConstChunk const& InputArray::getChunk(AttributeID attr, size_t chunkIndex)
    {
        Query::getValidQueryPtr(_query);
        if (chunkIndex > currChunkIndex || chunkIndex + LOOK_AHEAD <= currChunkIndex) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR11);
        }
        MemChunk& chunk = lookahead[attr].chunks[chunkIndex % LOOK_AHEAD];
        if (emptyTagAttrID != attr && emptyTagAttrID != INVALID_ATTRIBUTE_ID) {
            chunk.setBitmapChunk(&lookahead[emptyTagAttrID].chunks[chunkIndex % LOOK_AHEAD]);
        }
        return chunk;
    }
}
