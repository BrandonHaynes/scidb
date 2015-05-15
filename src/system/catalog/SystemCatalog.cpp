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

/*!
 *      @file
 *
 *      @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 *      @brief API for fetching and updating system catalog metadata.
 */

#include <vector>
#include <stdint.h>
#include <unistd.h>
#include <assert.h>
#include <fstream>
#include <sstream>
#include <iostream>
#include <limits.h>

#include <boost/random.hpp>

#include <pqxx/connection>
#include <pqxx/transaction>
#include <pqxx/prepared_statement>
#include <pqxx/except>
#include <pqxx/binarystring>
#include <libpq-fe.h>

#include <log4cxx/logger.h>

#include "system/Config.h"
#include "smgr/io/Storage.h"
#include "system/Exceptions.h"
#include "system/ErrorCodes.h"
#include "util/Mutex.h"

#include "system/SystemCatalog.h"
#include "system/SciDBConfigOptions.h"
#include "system/ErrorCodes.h"

#include "query/Expression.h"
#include "query/Serialize.h"

#include "system/catalog/data/CatalogMetadata.h"

using namespace std;
using namespace pqxx;
using namespace pqxx::prepare;

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.catalog"));

    Mutex SystemCatalog::_pgLock;


     SystemCatalog::LockDesc::LockDesc(const std::string& arrayName,
                                      QueryID  queryId,
                                      InstanceID   instanceId,
                                      InstanceRole instanceRole,
                                      LockMode lockMode)
    : _arrayName(arrayName),
      _arrayId(0),
      _queryId(queryId),
      _instanceId(instanceId),
      _arrayVersionId(0),
      _arrayVersion(0),
      _instanceRole(instanceRole),
      _lockMode(lockMode),
      _isLocked(false)
     {}

     std::string SystemCatalog::LockDesc::toString()
     {
        std::ostringstream out;
        out << "Lock: arrayName="
            << _arrayName
            << ", arrayId="
            << _arrayId
            << ", queryId="
            << _queryId
            << ", instanceId="
            << _instanceId
            << ", instanceRole="
            << (_instanceRole==COORD ? "COORD" : "WORKER")
            << ", lockMode="
            << _lockMode
            << ", arrayVersion="
            << _arrayVersion
            << ", arrayVersionId="
            << _arrayVersionId;
        return out.str();
    }

    void SystemCatalog::invalidateTempArrays()
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_invalidateTempArrays, this);
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_invalidateTempArrays()
    {
        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        LOG4CXX_TRACE(logger, "SystemCatalog::removeTempArrays()");

        try
        {
         /* Add the 'INVALID' flag to all entries of the 'array' table whose
          * 'flags' field currently has the 'TRANSIENT' bit set... */

            string s("update \"array\" set flags = (flags | $1) where (flags & $2)!=0");

            _connection->prepare(s,s)("int",treat_direct)("int",treat_direct);

            work tr(*_connection);
            tr.prepared(s)(int(ArrayDesc::INVALID))(int(ArrayDesc::TRANSIENT)).exec();
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            LOG4CXX_ERROR(logger, "SystemCatalog::invalidateTempArrays: postgress exception:"<< e.what());
            LOG4CXX_ERROR(logger, "SystemCatalog::invalidateTempArrays: query:"              << e.query());
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT,SCIDB_LE_PG_QUERY_EXECUTION_FAILED)       << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }

        LOG4CXX_TRACE(logger, "Invalidated temp arrays");
    }

    const std::string& SystemCatalog::initializeCluster()
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_initializeCluster, this);
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
        return _uuid;
    }

    void SystemCatalog::_initializeCluster()
    {
        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        LOG4CXX_TRACE(logger, "SystemCatalog::initializeCluster()");

        try
        {
            work tr(*_connection);
            tr.exec(string(CURRENT_METADATA));

            result query_res = tr.exec("select get_cluster_uuid as uuid from get_cluster_uuid()");
            _uuid = query_res[0].at("uuid").as(string());
            query_res = tr.exec("select get_metadata_version as version from get_metadata_version()");
            _metadataVersion = query_res[0].at("version").as(int());
            assert(METADATA_VERSION == _metadataVersion);
            _initialized = true;

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }

        LOG4CXX_TRACE(logger, "Initialized cluster uuid = " << _uuid << ", metadata version = " << _metadataVersion);
    }

    bool SystemCatalog::isInitialized() const
    {
        return _initialized;
    }

    const std::string& SystemCatalog::getClusterUuid() const
    {
        return _uuid;
    }

    int totalNewArrays;
    int maxTotalNewArrays;

    //Not thread safe. Must be called with active connection under _pgLock.
    inline void fillArrayIdentifiers(pqxx::connection *connection,
                                     pqxx::basic_transaction* tr,
                                     string const& array_name,
                                     ArrayID arrId,
                                     ArrayUAID& uaid,
                                     VersionID& vid)
    {
        assert(tr);
        uaid = arrId;
        vid = 0;
        if(ArrayDesc::isNameVersioned(array_name))
        {
            vid = ArrayDesc::getVersionFromName(array_name);
            string unv_name = ArrayDesc::makeUnversionedName(array_name);
            string sql_u = "select id, name, partitioning_schema, flags from \"array\" where name = $1";

            connection->prepare("find-by-name2", sql_u)("varchar", treat_string);
            result query_res_u = tr->prepared("find-by-name2")(unv_name).exec();
            if (query_res_u.size() <= 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << unv_name;
            }
            uaid = query_res_u[0].at("id").as(int64_t());
        }
    }

    void SystemCatalog::addArray(ArrayDesc &array_desc, PartitioningSchema ps)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_addArray,
                this, ref(array_desc), ps);
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_addArray(ArrayDesc &array_desc, PartitioningSchema ps)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::addArray ( array_name = " << array_desc.getName() << ")");
        LOG4CXX_TRACE(logger, "New Array    = " << array_desc );
        LOG4CXX_TRACE(logger, "Partitioning = " << ps );

        ScopedMutexLock mutexLock(_pgLock);

        if (++totalNewArrays > maxTotalNewArrays) {
            maxTotalNewArrays = totalNewArrays;
        }

        assert(_connection);
        ArrayID arrId = array_desc.getId();
        string array_name = array_desc.getName();

        try
        {
            work tr(*_connection);

            if (arrId == 0)
            {
                result query_res = tr.exec("select nextval from nextval('array_id_seq')");
                arrId = query_res[0].at("nextval").as(int64_t());
            }
            else
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << "Calling addArray with populated descriptor";
            }

            ArrayUAID uaid;
            VersionID vid;
            fillArrayIdentifiers(_connection, &tr, array_name, arrId, uaid, vid);

            string sql1 = "insert into \"array\"(id, name, partitioning_schema, flags) values ($1, $2, $3, $4)";
            _connection->prepare(sql1, sql1)
                ("bigint", treat_direct)
                ("varchar", treat_string)
                ("integer", treat_direct)
                ("integer", treat_direct);
            tr.prepared(sql1)
                (arrId)
                (array_desc.getName())
                ((int) ps)
                (array_desc.getFlags()).exec();

            string sql2 = "insert into \"array_attribute\"(array_id, id, name, type, flags, "
                " default_compression_method, reserve, default_missing_reason, default_value) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
            _connection->prepare(sql2, sql2)
                ("bigint", treat_direct)
                ("int", treat_direct)
                ("varchar", treat_string)
                ("bigint", treat_direct)
                ("int", treat_direct)
                ("int", treat_direct)
                ("int", treat_direct)
                ("int", treat_direct)
                ("varchar", treat_string);

            Attributes const& attributes = array_desc.getAttributes();
            Attributes cachedAttributes(attributes.size());
            for (int i = 0, n = attributes.size(); i < n; i++)
            {
                AttributeDesc const& attr = attributes[i];
                tr.prepared(sql2)
                    (arrId)
                    (i)
                    (attr.getName())
                    (attr.getType())
                    (attr.getFlags())
                    (attr.getDefaultCompressionMethod())
                    (attr.getReserve())
                    (attr.getDefaultValue().getMissingReason())
                    (attr.getDefaultValueExpr()).exec();

                //Attribute in descriptor has no some data before adding to catalog so build it manually for
                //caching
                cachedAttributes[i] =
                    AttributeDesc(i, attr.getName(), attr.getType(), attr.getFlags(),
                                  attr.getDefaultCompressionMethod(), std::set<std::string>(), attr.getReserve(),
                                  &attr.getDefaultValue(), attr.getDefaultValueExpr());
            }

            //
            // string sql3 = "insert into \"array_dimension\"(array_id, id, name,"
            //      " start, length, chunk_interval, chunk_overlap) values ($1, $2, $3, $4, $5, $6, $7)";
            //
            string sql3 = "insert into \"array_dimension\"(array_id, id, name,"
                " startMin, currStart, currEnd, endMax, chunk_interval, chunk_overlap) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
            _connection->prepare(sql3, sql3)
                ("bigint", treat_direct)
                ("int", treat_direct)
                ("varchar", treat_string)
                ("bigint", treat_direct)
                ("bigint", treat_direct)
                ("bigint", treat_direct)
                ("bigint", treat_direct)
                ("bigint", treat_direct)
                ("bigint", treat_direct)
                ;

            Dimensions const& dims = array_desc.getDimensions();
            for (int i = 0, n = dims.size(); i < n; i++)
            {
                DimensionDesc const& dim = dims[i];
                tr.prepared(sql3)
                    (arrId)
                    (i)
                    (dim.getBaseName())
                    (dim.getStartMin())
                    (dim.getCurrStart())
                    (dim.getCurrEnd())
                    (dim.getEndMax())
                    (dim.getChunkInterval())
                    (dim.getChunkOverlap()).exec();
                //TODO: If DimensionDesc will store IDs in future, here must be building vector of
                //dimensions for caching as for attributes.
            }

            tr.commit();
            LOG4CXX_DEBUG(logger, "Create array " << array_desc.getName() << "(" << arrId << ") in query " << Query::getCurrentQueryID());
            array_desc.setIds(arrId, uaid, vid);
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    void SystemCatalog::updateArray(const ArrayDesc &array_desc)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_updateArray,
                this, cref(array_desc));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_updateArray(const ArrayDesc &array_desc)
    {

        LOG4CXX_TRACE(logger, "SystemCatalog::updateArray ( old_array_ID = " << array_desc.getId() << ", new array_name = " << array_desc.getName() << ")");

        ScopedMutexLock mutexLock(_pgLock);

        boost::shared_ptr<ArrayDesc> oldArrayDesc = getArrayDesc(array_desc.getId());

        LOG4CXX_TRACE(logger, "Previously = " << *oldArrayDesc);
        LOG4CXX_TRACE(logger, "New        = " << array_desc);

        assert(_connection);
        ArrayID array_id = array_desc.getId();

        try
        {
            work tr(*_connection);

            if (array_desc.getName() != oldArrayDesc->getName()) {
                _connection->prepare("updateArray", "update \"array\" set name = t.new_name from\
                    (select name || '_' || version_id as old_name, $1 || '_' || version_id as new_name from \"array\" join array_version on array_id = id where id = $2) as t\
                    where \"array\".name = t.old_name")
                    ("varchar", treat_string)
                    ("bigint", treat_direct);
                tr.prepared("updateArray")
                    (array_desc.getName())
                    (array_id).exec();
                _connection->unprepare("updateArray");

                string sql1 = "update \"array\" set name=$2, flags=$3 where id=$1";
                _connection->prepare(sql1, sql1)
                    ("bigint", treat_direct)
                    ("varchar", treat_string)
                    ("integer", treat_direct);
                tr.prepared(sql1)
                    (array_id)
                    (array_desc.getName())
                    (array_desc.getFlags()).exec();
            } else {
                string sql1 = "update \"array\" set flags=$2 where id=$1";
                _connection->prepare(sql1, sql1)
                    ("bigint", treat_direct)
                    ("integer", treat_direct);
                tr.prepared(sql1)
                    (array_id)
                    (array_desc.getFlags()).exec();
            }

            string sql2 = "update \"array_attribute\" set name=$3, type=$4, flags=$5, default_compression_method=$6, reserve=$7, default_missing_reason=$8, default_value=$9 where array_id=$1 and id=$2";
            _connection->prepare(sql2, sql2)
                ("bigint", treat_direct)
                ("int", treat_direct)
                ("varchar", treat_string)
                ("bigint", treat_direct)
                ("int", treat_direct)
                ("int", treat_direct)
                ("int", treat_direct)
                ("int", treat_direct)
                ("varchar", treat_string);

            Attributes const& attributes = array_desc.getAttributes();
            for (int i = 0, n = attributes.size(); i < n; i++)
            {
                AttributeDesc const& attr = attributes[i];
                tr.prepared(sql2)
                    (array_id)
                    (i)
                    (attr.getName())
                    (attr.getType())
                    (attr.getFlags())
                    (attr.getDefaultCompressionMethod())
                    (attr.getReserve())
                    (attr.getDefaultValue().getMissingReason())
                    (attr.getDefaultValueExpr()).exec();
            }


            string sql3 = "update \"array_dimension\" set name=$3, startMin=$4, endMax=$5, chunk_interval=$6, chunk_overlap=$7 where array_id=$1 and id=$2";

            _connection->prepare(sql3, sql3)
                ("bigint", treat_direct)
                ("int", treat_direct)
                ("varchar", treat_string)
                ("bigint", treat_direct)
                ("bigint", treat_direct)
                ("bigint", treat_direct)
                ("bigint", treat_direct)
                ;

            Dimensions const& dims = array_desc.getDimensions();
            for (int i = 0, n = dims.size(); i < n; i++)
            {
                DimensionDesc const& dim = dims[i];
                tr.prepared(sql3)
                    (array_id)
                    (i)
                    (dim.getBaseName())
                    (dim.getStartMin())
                    (dim.getEndMax())
                    (dim.getChunkInterval())
                    (dim.getChunkOverlap()).exec();
            }

            tr.commit();
            *oldArrayDesc = array_desc;
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    void SystemCatalog::getArrays(std::vector<std::string> &arrays)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_getArrays,
                this, ref(arrays));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_getArrays(std::vector<std::string> &arrays)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getArrays()");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);
            result query_res = tr.exec("select name from \"array\" where name is not null order by name");

            arrays.clear();
            arrays.reserve(query_res.size());

            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
            {
                arrays.push_back(i.at("name").c_str());
            }

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        LOG4CXX_TRACE(logger, "Retrieved " << arrays.size() << " arrays from catalogs");

    }


    bool SystemCatalog::containsArray(const ArrayID array_id)
    {
        boost::function<bool()> work = boost::bind(&SystemCatalog::_containsArray,
                this, array_id);
        return Query::runRestartableWork<bool, broken_connection>(work, _reconnectTries);
    }

    bool SystemCatalog::_containsArray(const ArrayID array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::containsArray( id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock);

        LOG4CXX_TRACE(logger, "Failed to find array_id = " << array_id << " locally.");
        assert(_connection);

        try
        {
            work tr(*_connection);
            string sql1 = "select id from \"array\" where id = $1";
            _connection->prepare(sql1, sql1)("bigint", treat_direct);
            result query_res1 = tr.prepared(sql1)(array_id).exec();
            return query_res1.size() != 0;
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return false;
    }

    bool SystemCatalog::containsArray(const string &array_name)
    {
        return findArrayByName(array_name) != INVALID_ARRAY_ID;
    }

    ArrayID SystemCatalog::findArrayByName(const std::string &array_name)
    {
        boost::function<ArrayID()> work = boost::bind(&SystemCatalog::_findArrayByName,
                this, cref(array_name));
        return Query::runRestartableWork<ArrayID, broken_connection>(work, _reconnectTries);
    }

    ArrayID SystemCatalog::_findArrayByName(const std::string &array_name)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::containsArray( name = " << array_name << ")");

        ScopedMutexLock mutexLock(_pgLock);

        LOG4CXX_TRACE(logger, "Failed to find array_name = " << array_name << " locally.");

        assert(_connection);

        try
        {
            work tr(*_connection);
            string sql1 = "select id from \"array\" where name = $1";
            _connection->prepare(sql1, sql1)("varchar", treat_string);
            result query_res1 = tr.prepared(sql1)(array_name).exec();
            if (query_res1.size() != 0) {
                return query_res1[0].at("id").as(int64_t());
            }
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return INVALID_ARRAY_ID;
    }

    bool SystemCatalog::getArrayDesc(const string &array_name, VersionID version, ArrayDesc &array_desc, const bool throwException)
    {
        if (getArrayDesc(array_name, array_desc, throwException)) {
            std::stringstream ss;
            if (version == LAST_VERSION) {
                version = getLastVersion(array_desc.getId());
                if (version == 0) {
                    return true;
                }
            }
            ss << array_name << "@" << version;
            return getArrayDesc(ss.str(), array_desc, throwException);
        } else {
            return false;
        }
    }

    bool SystemCatalog::getArrayDesc(const string &array_name, ArrayDesc &array_desc, const bool throwException)
    {
        try {
            getArrayDesc(array_name, array_desc);
        } catch (const Exception& e) {
            if (!throwException &&
                e.getLongErrorCode() == SCIDB_LE_ARRAY_DOESNT_EXIST) {
                return false;
            }
            throw;
        }
        return true;
    }

    void SystemCatalog::getArrayDesc(const std::string &array_name,
                                     ArrayDesc &array_desc)
    {
        const bool ignoreOrphanAttributes = false;
        boost::function<void()> work = boost::bind(&SystemCatalog::_getArrayDesc,
                                                   this,
                                                   cref(array_name),
                                                   ref(array_desc),
                                                   ignoreOrphanAttributes);
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }



void SystemCatalog::_getArrayDesc(const std::string &array_name,
                                  ArrayDesc &array_desc,
                                  bool ignoreOrphanAttributes)
{
    LOG4CXX_TRACE(logger, "SystemCatalog::_getArrayDesc( name = " << array_name << ")");

    ScopedMutexLock mutexLock(_pgLock);

    assert(_connection);

    pqxx::transaction<pqxx::serializable> tr(*_connection);

    _getArrayDesc(array_name, array_desc, ignoreOrphanAttributes, &tr);

    tr.commit();
}

void SystemCatalog::_getArrayDesc(const std::string &array_name,
                                  ArrayDesc &array_desc,
                                  bool ignoreOrphanAttributes,
                                  pqxx::basic_transaction* tr)
{
    assert(_connection);

    string sql1 = "select id, name, partitioning_schema, flags from \"array\" where name = $1";
    _connection->prepare("find-by-name", sql1)("varchar", treat_string);
    result query_res1 = tr->prepared("find-by-name")(array_name).exec();
    if (query_res1.size() <= 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << array_name;
    }

    ArrayID array_id = query_res1[0].at("id").as(int64_t());
    ArrayUAID uaid(0);
    VersionID vid(0);
    fillArrayIdentifiers(_connection, tr, array_name, array_id, uaid, vid);
    int flags = query_res1[0].at("flags").as(int());

    string sql2 = "select id, name, type, flags, default_compression_method, reserve, default_missing_reason, default_value"
    " from \"array_attribute\" where array_id = $1 order by id";
    _connection->prepare(sql2, sql2)("integer", treat_direct);
    result query_res2 = tr->prepared(sql2)(array_id).exec();

    Attributes attributes;
    attributes.reserve(query_res2.size());
    for (result::const_iterator i = query_res2.begin(); i != query_res2.end(); ++i)
    {
        Value defaultValue;
        int missingReason = i.at("default_missing_reason").as(int());
        string defaultValueExpr;
        if (missingReason >= 0) {
            defaultValue.setNull(missingReason);
        } else {
            defaultValueExpr = i.at("default_value").as(string());
            try {
                // Do type check before constructor check in the next if.
                TypeId typeId = i.at("type").as(TypeId());
                defaultValue = Value(TypeLibrary::getType(typeId));

                // Evaluate expression if present and set default value
                if (defaultValueExpr != "")
                {
                    Expression expr = deserializePhysicalExpression(defaultValueExpr);
                    defaultValue = expr.evaluate();
                }
                // Else fallback to null or zero as before
                else
                {
                    if (i.at("flags").as(int16_t()) & AttributeDesc::IS_NULLABLE)
                    {
                        defaultValue.setNull();
                    }
                    else
                    {
                        defaultValue = TypeLibrary::getDefaultValue(typeId);
                    }
                }
            } catch(const scidb::Exception& e) {
                if (ignoreOrphanAttributes &&
                    (e.getLongErrorCode() == SCIDB_LE_TYPE_NOT_REGISTERED ||
                     e.getLongErrorCode() == SCIDB_LE_FUNCTION_NOT_FOUND)) {

                    flags |= ArrayDesc::INVALID;
                    Value tmp;
                    defaultValue.swap(tmp);

                } else { throw; }
            }
        }
        AttributeDesc att(
                          i.at("id").as(AttributeID()),
                          i.at("name").as(string()),
                          i.at("type").as(TypeId()),
                          i.at("flags").as(int16_t()),
                          i.at("default_compression_method").as(uint16_t()),
                          std::set<std::string>(),
                          i.at("reserve").as(int16_t()),
                          &defaultValue,
                          defaultValueExpr
                          );
        attributes.push_back(att);
    }

    // string sql3 = "select name, start, length, chunk_interval, chunk_overlap "
    //          " from \"array_dimension\" where array_id = $1 order by id";
    string sql3 = "select name, startmin, currstart, currend, endmax, chunk_interval, chunk_overlap "
    " from \"array_dimension\" where array_id = $1 order by id";

    _connection->prepare(sql3, sql3)("integer", treat_direct);
    result query_res3 = tr->prepared(sql3)(array_id).exec();

    Dimensions dimensions;
    dimensions.reserve(query_res3.size());
    for (result::const_iterator i = query_res3.begin(); i != query_res3.end(); ++i)
    {
        dimensions.push_back(
                             DimensionDesc(
                                           i.at("name").as(string()),
                                           i.at("startmin").as(int64_t()),
                                           i.at("currstart").as(int64_t()),
                                           i.at("currend").as(int64_t()),
                                           i.at("endmax").as(int64_t()),
                                           i.at("chunk_interval").as(int64_t()),
                                           i.at("chunk_overlap").as(int64_t())
                                           ));
    }
    ArrayDesc newDesc(array_id, uaid, vid,
                      query_res1[0].at("name").as(string()),
                      attributes,
                      dimensions,
                      flags);
    newDesc.setPartitioningSchema((PartitioningSchema)query_res1[0].at("partitioning_schema").as(int()));

    array_desc = newDesc;

    assert(array_desc.getUAId()!=0);
}

    void SystemCatalog::getArrayDesc(const ArrayID array_id, ArrayDesc &array_desc)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getArrayDesc( id = " << array_id << ", array_desc )");
        boost::shared_ptr<ArrayDesc> desc = getArrayDesc(array_id);
        array_desc = *desc;
    }

    boost::shared_ptr<ArrayDesc> SystemCatalog::getArrayDesc(const ArrayID array_id)
    {
        boost::function<boost::shared_ptr<ArrayDesc>()> work =
                boost::bind(&SystemCatalog::_getArrayDesc, this, array_id);
        return Query::runRestartableWork<boost::shared_ptr<ArrayDesc>, broken_connection>(work, _reconnectTries);
    }

    boost::shared_ptr<ArrayDesc> SystemCatalog::_getArrayDesc(const ArrayID array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getArrayDesc( id = " << array_id << ")");
        ScopedMutexLock mutexLock(_pgLock);

        LOG4CXX_TRACE(logger, "Failed to find array_id = " << array_id << " locally.");
        assert(_connection);
        boost::shared_ptr<ArrayDesc> newDesc;
        try
        {
            work tr(*_connection);
            string sql1 = "select id, name, partitioning_schema, flags from \"array\" where id = $1";
            _connection->prepare("find-by-id", sql1)("bigint", treat_direct);
            result query_res1 = tr.prepared("find-by-id")(array_id).exec();
            if (query_res1.size() <= 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAYID_DOESNT_EXIST) << array_id;
            }

            assert(array_id ==  query_res1[0].at("id").as(uint64_t()));
            string array_name = query_res1[0].at("name").as(string());
            ArrayUAID uaid;
            VersionID vid;
            fillArrayIdentifiers(_connection, &tr, array_name, array_id, uaid, vid);

            string sql2 = "select id, name, type, flags, default_compression_method, reserve, default_missing_reason, default_value"
                " from \"array_attribute\" where array_id = $1 order by id";
            _connection->prepare(sql2, sql2)("integer", treat_direct);
            result query_res2 = tr.prepared(sql2)(array_id).exec();

            Attributes attributes;
            if (query_res2.size() > 0)
            {
                attributes.reserve(query_res2.size());
                for (result::const_iterator i = query_res2.begin(); i != query_res2.end(); ++i)
                {
                    Value defaultValue;
                    string defaultValueExpr = "";
                    int missingReason = i.at("default_missing_reason").as(int());
                    if (missingReason >= 0) {
                        defaultValue.setNull(missingReason);
                    } else {
                        defaultValueExpr = i.at("default_value").as(string());

                        // Evaluate expression if present and set default value
                        if (defaultValueExpr != "")
                        {
                            Expression expr = deserializePhysicalExpression(defaultValueExpr);
                            defaultValue = expr.evaluate();
                        }
                        // Else fallback to null or zero as before
                        else
                        {
                            TypeId typeId = i.at("type").as(TypeId());
                            defaultValue = Value(TypeLibrary::getType(typeId));
                            if (i.at("flags").as(int16_t()) & AttributeDesc::IS_NULLABLE)
                            {
                                defaultValue.setNull();
                            }
                            else
                            {
                                defaultValue = TypeLibrary::getDefaultValue(typeId);
                            }
                        }
                    }
                    AttributeDesc att(
                        i.at("id").as(AttributeID()),
                        i.at("name").as(string()),
                        i.at("type").as (TypeId()),
                        i.at("flags").as(int16_t()),
                        i.at("default_compression_method").as(uint16_t()),
                        std::set<std::string>(),
                        i.at("reserve").as(int16_t()),
                        &defaultValue,
                        defaultValueExpr
                        );

                    attributes.push_back(att);
                }
            }

            // string sql3 = "select name, start, length, chunk_interval, chunk_overlap "
            //      " from \"array_dimension\" where array_id = $1 order by id";

            string sql3 = "select name, startmin, currstart, currend, endmax, chunk_interval, chunk_overlap "
                " from \"array_dimension\" where array_id = $1 order by id";
            _connection->prepare(sql3, sql3)("integer", treat_direct);
            result query_res3 = tr.prepared(sql3)(array_id).exec();

            Dimensions dimensions;
            if (query_res3.size() > 0)
            {
                attributes.reserve(query_res3.size());
                for (result::const_iterator i = query_res3.begin(); i != query_res3.end(); ++i)
                {
                    dimensions.push_back(
                        DimensionDesc(
                            i.at("name").as(string()),
                            i.at("startmin").as(int64_t()),
                            i.at("currstart").as(int64_t()),
                            i.at("currend").as(int64_t()),
                            i.at("endmax").as(int64_t()),
                            i.at("chunk_interval").as(int64_t()),
                            i.at("chunk_overlap").as(int64_t())));
                }
            }
            newDesc = boost::shared_ptr<ArrayDesc>(new ArrayDesc(array_id, uaid, vid,
                                                                 query_res1[0].at("name").as(string()),
                                                                 attributes,
                                                                 dimensions,
                                                                 query_res1[0].at("flags").as(int())));
            newDesc->setPartitioningSchema((PartitioningSchema)query_res1[0].at("partitioning_schema").as(int()));
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        assert(newDesc->getUAId()!=0);
        return newDesc;
    }

    PartitioningSchema SystemCatalog::getPartitioningSchema(const ArrayID array_id)
    {
        boost::function<PartitioningSchema()> work = boost::bind(&SystemCatalog::_getPartitioningSchema,
                this, array_id);
        return Query::runRestartableWork<PartitioningSchema, broken_connection>(work, _reconnectTries);
    }

    PartitioningSchema SystemCatalog::_getPartitioningSchema(const ArrayID array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getPartitioningSchema( id = " << array_id << ")");
        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);
            string sql = "select partitioning_schema from \"array\" where id = $1";
            _connection->prepare(sql, sql)("integer", treat_direct);

            result query_res = tr.prepared(sql)(array_id).exec();

            if (query_res.size() <= 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAYID_DOESNT_EXIST) << array_id;
            }

            return (PartitioningSchema)query_res[0].at("partitioning_schema").as(int());
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    bool SystemCatalog::deleteArray(const string &array_name)
    {
        boost::function<bool()> work = boost::bind(&SystemCatalog::_deleteArrayByName,
                this, cref(array_name));
        return Query::runRestartableWork<bool, broken_connection>(work, _reconnectTries);
    }

    bool SystemCatalog::_deleteArrayByName(const string &array_name)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::deleteArray( name = " << array_name << ")");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);
        bool rc = false;
        try
        {
            work tr(*_connection);

            const string deleteArraySql = "delete from \"array\" where name = $1 or (name like $1||'@%' and name not like '%:%')";
            _connection->prepare("delete-array-name", deleteArraySql)("varchar", treat_string);
            result query_res = tr.prepared("delete-array-name")(array_name).exec();
            totalNewArrays -= query_res.affected_rows();
            rc = (query_res.affected_rows() > 0);

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return rc;
    }

    bool SystemCatalog::deleteArrayVersions(const std::string &array_name, const VersionID array_version)
    {
        boost::function<bool()> work = boost::bind(&SystemCatalog::_deleteArrayVersions,
                                                   this, cref(array_name), array_version);
        return Query::runRestartableWork<bool, broken_connection>(work, _reconnectTries);
    }

    bool SystemCatalog::_deleteArrayVersions(const std::string &array_name, const VersionID array_version)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::deleteArrayVersions( array_name = " <<
                      array_name << ", array_version = " << array_version << ")");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);
        bool rc = false;
        try
        {
            work tr(*_connection);
            stringstream ss;
            ss << "delete from \"array\" where name like $1||'@%' and id < "
               << "(select id from \"array\" where name like $1||'@'||$2)";
            _connection->prepare("delete-array-versions", ss.str())
                ("varchar", treat_string)("integer", treat_direct);
            result query_res =
                tr.prepared("delete-array-versions")(array_name)(array_version).exec();
            totalNewArrays -= query_res.affected_rows();
            rc = (query_res.affected_rows() > 0);
            tr.commit();
        }
        catch (const pqxx::broken_connection &e)
        {
            throw;
        }
        catch (const pqxx::sql_error &e)
        {
            LOG4CXX_ERROR(logger, "SystemCatalog::deleteArrayVersions: postgress exception:"<< e.what());
            LOG4CXX_ERROR(logger, "SystemCatalog::deleteArrayVersions: query:"<< e.query());
            LOG4CXX_ERROR(logger, "SystemCatalog::deleteArrayVersions: "
                          << array_name << " version:" << array_version);
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT,
                                   SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }

        return rc;
    }

    void SystemCatalog::deleteArray(const ArrayID array_id)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_deleteArrayById,
                this, array_id);
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_deleteArrayById(const ArrayID array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::deleteArray( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);
        try
        {
            work tr(*_connection);

            const string sql1 = "delete from \"array\" where id = $1";
            _connection->prepare("delete-array-id", sql1)("integer", treat_direct);
            totalNewArrays -= tr.prepared("delete-array-id")(array_id).exec().affected_rows();

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    VersionID SystemCatalog::createNewVersion(const ArrayID array_id, const ArrayID version_array_id)
    {
        boost::function<VersionID()> work = boost::bind(&SystemCatalog::_createNewVersion,
                this, array_id, version_array_id);
        return Query::runRestartableWork<VersionID, broken_connection>(work, _reconnectTries);
    }

    VersionID SystemCatalog::_createNewVersion(const ArrayID array_id, const ArrayID version_array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::createNewVersion( array_id = " << array_id << ")");
        VersionID version_id = (VersionID)-1;
        {
            ScopedMutexLock mutexLock(_pgLock);

            assert(_connection);

            try
            {
                work tr(*_connection);

                string sql = "select COALESCE(max(version_id),0) as vid from \"array_version\" where array_id=$1";
                _connection->prepare(sql, sql)
                    ("bigint", treat_direct);
                result query_res = tr.prepared(sql)(array_id).exec();
                version_id = query_res[0].at("vid").as(uint64_t());

                version_id += 1;

                string sql1 = "insert into \"array_version\"(array_id, version_array_id, version_id, time_stamp)"
                    " values ($1, $2, $3, $4)";
                int64_t timestamp = time(NULL);
                _connection->prepare(sql1, sql1)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct);
                tr.prepared(sql1)(array_id)(version_array_id)(version_id)(timestamp).exec();


                tr.commit();
            }
            catch (const broken_connection &e)
            {
                throw;
            }
            catch (const sql_error &e)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
            }
            catch (const pqxx::failure &e)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
            }
        }
        return version_id;
    }

    void SystemCatalog::deleteVersion(const ArrayID array_id, const VersionID version_id)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_deleteVersion,
                this, array_id, version_id);
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_deleteVersion(const ArrayID array_id, const VersionID version_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::deleteVersion( array_id = " << array_id << ", version_id = " << version_id << ")");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);

            _connection->prepare("delete-version", "delete from \"array_version\" where array_id=$1 and version_id = $2")
                ("bigint", treat_direct)
                ("bigint", treat_direct);
            tr.prepared("delete-version")(array_id)(version_id).exec();
            _connection->unprepare("delete-version");
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    VersionID SystemCatalog::getLastVersion(const ArrayID array_id)
    {
        boost::function<VersionID()> work = boost::bind(&SystemCatalog::_getLastVersion,
                this, array_id);
        return Query::runRestartableWork<VersionID, broken_connection>(work, _reconnectTries);
    }

    ArrayID SystemCatalog::getOldestArrayVersion(const ArrayID id)
    {
        boost::function<VersionID()> work = boost::bind(&SystemCatalog::_getOldestArrayVersion,
                this, id);
        return Query::runRestartableWork<VersionID, broken_connection>(work, _reconnectTries);
    }

/*
** TODO: We will need to rework this so that we only need to go back to the
**       persistent meta-data store when the local cache is invalidated
**       due to a new version being created with an UPDATE. This can be
**       piggy-backed on a "heartbeat" message, which contains the latest
**       lamport clock value. Meta-data additions to the persistent store
**       will increment the lamport clock value, which will be propogated
**       throughout the instances. When the local instance's clock value < the
**       'global', and the local is obliged to check it's local catalogs,
**       then it can reload meta-data from the persistent store.
*/
    VersionID SystemCatalog::_getLastVersion(const ArrayID array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getLastVersion( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock);
        try
        {
            work tr(*_connection);

            string sql = "select COALESCE(max(version_id),0) as vid from \"array_version\" where array_id=$1";
            _connection->prepare("select-last-version", sql)
                ("bigint", treat_direct);
            result query_res = tr.prepared("select-last-version")(array_id).exec();
            VersionID version_id = query_res[0].at("vid").as(uint64_t());
            tr.commit();
            return version_id;
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return 0;
    }

    ArrayID SystemCatalog::_getOldestArrayVersion(const ArrayID id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getOldestArrayVersion( id = " << id << ")");

        ScopedMutexLock mutexLock(_pgLock);
        try
        {
            work tr(*_connection);

            string sql =
                "select COALESCE(min(version_array_id),0) as vid from \"array_version\" where array_id=$1";
            _connection->prepare("select-oldest-version", sql)
                ("bigint", treat_direct);
            result query_res = tr.prepared("select-oldest-version")(id).exec();
            ArrayID array_version_id = query_res[0].at("vid").as(uint64_t());
            tr.commit();
            return array_version_id;
        }
        catch (const pqxx::broken_connection &e)
        {
            throw;
        }
        catch (const pqxx::sql_error &e)
        {
            LOG4CXX_ERROR(logger, "SystemCatalog::getOldestArrayVersion: postgress exception:"<< e.what());
            LOG4CXX_ERROR(logger, "SystemCatalog::getOldestArrayVersion: query:"<< e.query());
            LOG4CXX_ERROR(logger, "SystemCatalog::getOldestArrayVersion: "
                          << " arrayId:" << id);
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT,
                                   SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }

        return 0;
    }

    VersionID SystemCatalog::lookupVersionByTimestamp(const ArrayID array_id, const uint64_t timestamp)
    {
        boost::function<VersionID()> work = boost::bind(&SystemCatalog::_lookupVersionByTimestamp,
                this, array_id, timestamp);
        return Query::runRestartableWork<VersionID, broken_connection>(work, _reconnectTries);
    }

    VersionID SystemCatalog::_lookupVersionByTimestamp(const ArrayID array_id, const uint64_t timestamp)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::lookupVersionByTimestamp( array_id = " << array_id << ", timestamp = " << timestamp << ")");

        ScopedMutexLock mutexLock(_pgLock);

        try
        {
            work tr(*_connection);

            string sql = "select COALESCE(max(version_id),0) as vid from \"array_version\" where array_id=$1 and time_stamp <= $2";
            _connection->prepare("select-version-by-timestamp", sql)
                ("bigint", treat_direct)
                ("bigint", treat_direct);
            result query_res = tr.prepared("select-version-by-timestamp")(array_id)(timestamp).exec();
            VersionID version_id = query_res[0].at("vid").as(uint64_t());
            tr.commit();
            return version_id;
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return 0;
    }

    std::vector<VersionDesc> SystemCatalog::getArrayVersions(const ArrayID array_id)
    {
        boost::function<std::vector<VersionDesc>()> work = boost::bind(&SystemCatalog::_getArrayVersions,
                this, array_id);
        return Query::runRestartableWork<std::vector<VersionDesc>, broken_connection>(work, _reconnectTries);
    }

    std::vector<VersionDesc> SystemCatalog::_getArrayVersions(const ArrayID array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getArrayVersions( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock);
        try
        {
            work tr(*_connection);

            string sql = "select \"version_array_id\", \"version_id\", \"time_stamp\" from \"array_version\" where \"array_id\"=$1 order by \"version_id\";";
            _connection->prepare("select-all-versions", sql)
                ("bigint", treat_direct);
            result query_res = tr.prepared("select-all-versions")(array_id).exec();
            std::vector<VersionDesc> versions(query_res.size());
            int j = 0;
            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
            {
                versions[j++] = VersionDesc(i.at("version_array_id").as(uint64_t()), i.at("version_id").as(uint64_t()), i.at("time_stamp").as(time_t()));
            }
            tr.commit();
            return versions;
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return std::vector<VersionDesc>();
    }

    Coordinates SystemCatalog::getHighBoundary(const ArrayID array_id)
    {
        boost::function<Coordinates()> work = boost::bind(&SystemCatalog::_getHighBoundary,
                this, array_id);
        return Query::runRestartableWork<Coordinates, broken_connection>(work, _reconnectTries);
    }

    Coordinates SystemCatalog::_getHighBoundary(const ArrayID array_id)
    {

        LOG4CXX_TRACE(logger, "SystemCatalog::getHighBoundary( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock);
        try
        {
            work tr(*_connection);

            string sql = "select currEnd from \"array_dimension\" where array_id=$1 order by id";
            _connection->prepare("select-high-boundary", sql)
                ("bigint", treat_direct);
            result query_res = tr.prepared("select-high-boundary")(array_id).exec();
            Coordinates highBoundary(query_res.size());
            int j = 0;
            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
            {
                highBoundary[j++] = i.at("currEnd").as(int64_t());
            }

            if (0 == j) {
                throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAYID_DOESNT_EXIST) <<array_id ;
            }
            tr.commit();
            return highBoundary;
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return Coordinates();
    }

    Coordinates SystemCatalog::getLowBoundary(const ArrayID array_id)
    {
        boost::function<Coordinates()> work = boost::bind(&SystemCatalog::_getLowBoundary,
                this, array_id);
        return Query::runRestartableWork<Coordinates, broken_connection>(work, _reconnectTries);
    }

    Coordinates SystemCatalog::_getLowBoundary(const ArrayID array_id)
    {

        LOG4CXX_TRACE(logger, "SystemCatalog::getLowBoundary( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock);
        try
        {
            work tr(*_connection);

            string sql = "select currStart from \"array_dimension\" where array_id=$1 order by id";
            _connection->prepare("select-low-boundary", sql)
                ("bigint", treat_direct);
            result query_res = tr.prepared("select-low-boundary")(array_id).exec();
            Coordinates lowBoundary(query_res.size());
            int j = 0;
            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
            {
                lowBoundary[j++] = i.at("currStart").as(int64_t());
            }

            if (0 == j) {
                throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAYID_DOESNT_EXIST) <<array_id ;
            }
            tr.commit();
            return lowBoundary;
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return Coordinates();
    }

    void SystemCatalog::updateArrayBoundaries(ArrayDesc const& desc, PhysicalBoundaries const& bounds)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_updateArrayBoundaries,
                this, cref(desc), ref(bounds));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_updateArrayBoundaries(ArrayDesc const& desc, PhysicalBoundaries const& bounds)
    {
        PhysicalBoundaries trimmed = bounds.trimToDims(desc.getDimensions());
        Coordinates const& low = trimmed.getStartCoords();
        Coordinates const& high = trimmed.getEndCoords();
        ArrayID array_id = desc.getId();

        LOG4CXX_DEBUG(logger, "SystemCatalog::updateArrayBoundaries( array_id = " << desc.getId() << ", low = [" << low << "], high = [" << high << "])");

        ScopedMutexLock mutexLock(_pgLock);
        try
        {
            work tr(*_connection);

            string sql1 = "update \"array_dimension\" set currStart=$1 where array_id=$2 and id=$3 and currStart>$1";
            _connection->prepare("update-low-boundary", sql1)
                ("bigint", treat_direct)
                ("bigint", treat_direct)
                ("int", treat_direct);
            string sql2 = "update \"array_dimension\" set currEnd=$1 where array_id=$2 and id=$3 and currEnd<$1";
            _connection->prepare("update-high-boundary", sql2)
                ("bigint", treat_direct)
                ("bigint", treat_direct)
                ("int", treat_direct);
            for (int i = 0, n = low.size(); i < n; i++) {
                tr.prepared("update-low-boundary")(low[i])(array_id)(i).exec();
                tr.prepared("update-high-boundary")(high[i])(array_id)(i).exec();
            }
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    uint32_t SystemCatalog::getNumberOfInstances()
    {
        boost::function<uint32_t()> work = boost::bind(&SystemCatalog::_getNumberOfInstances,
                this);
        return Query::runRestartableWork<uint32_t, broken_connection>(work, _reconnectTries);
    }

    uint32_t SystemCatalog::_getNumberOfInstances()
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getNumberOfInstances()");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);
        uint32_t n_instances;
        try
        {
            work tr(*_connection);

            result query_res = tr.exec("select count(*) as cnt from \"instance\"");
            n_instances = query_res[0].at("cnt").as(uint32_t());
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return n_instances;
    }

    InstanceID SystemCatalog::addInstance(const InstanceDesc &instance)
    {
        boost::function<InstanceID()> work = boost::bind(&SystemCatalog::_addInstance,
                this, cref(instance));
        return Query::runRestartableWork<InstanceID, broken_connection>(work, _reconnectTries);
    }

    InstanceID SystemCatalog::_addInstance(const InstanceDesc &instance)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::addInstance( " << instance << ")");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        int64_t instance_id = 0;
        try
        {
            work tr(*_connection);

            result query_res = tr.exec("select nextval from nextval('instance_id_seq')");
            instance_id = query_res[0].at("nextval").as(int64_t());

            string sql1 = "insert into \"instance\"(instance_id, host, port, path, online_since) values ($1, $2, $3, $4, 'infinity')";
            _connection->prepare(sql1, sql1)
                ("bigint", treat_direct)
                ("varchar", treat_string)
                ("int", treat_direct)
                ("varchar", treat_string);
            tr.prepared(sql1)(instance_id)(instance.getHost())(instance.getPort())(instance.getPath()).exec();

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return instance_id;
    }

    void SystemCatalog::getInstances(Instances &instances)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_getInstances,
                this, ref(instances));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_getInstances(Instances &instances)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getInstances()");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);

            string sql = "select instance_id, host, port, path, date_part('epoch', online_since)::bigint as ts from \"instance\" order by instance_id";
            result query_res = tr.exec(sql);

            if (query_res.size() > 0)
            {
                instances.reserve(query_res.size());
                for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
                {
                    instances.push_back(
                        InstanceDesc(
                            i.at("instance_id").as(uint64_t()),
                            i.at("host").as(string()),
                            i.at("port").as(uint16_t()),
                            i.at("ts").as(uint64_t()),
                            i.at("path").as(string())
                                     ));

                }
            }

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        LOG4CXX_TRACE(logger, "Retrieved " << instances.size() << " instances from catalogs");
    }

    void SystemCatalog::getClusterInstance(InstanceID instance_id, InstanceDesc &instance)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_getClusterInstance,
                this, instance_id, ref(instance));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_getClusterInstance(InstanceID instance_id, InstanceDesc &instance)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getInstance( instance_id = " << instance_id << " InstanceDesc& )");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);

            string sql = "select instance_id, host, port, path, date_part('epoch', online_since)::bigint as ts from \"instance\" where instance_id = $1";
            _connection->prepare(sql, sql)("bigint", treat_direct);
            result query_res = tr.prepared(sql)(instance_id).exec();

            if (query_res.size() <= 0) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_INSTANCE_DOESNT_EXIST) << instance_id;
            }
            instance = InstanceDesc(
                query_res[0].at("instance_id").as(uint64_t()),
                query_res[0].at("host").as(string()),
                query_res[0].at("port").as(uint16_t()),
                query_res[0].at("ts").as(uint64_t()),
                query_res[0].at("path").as(string())
                                    );

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        LOG4CXX_TRACE(logger, "instance_id = " << instance_id << " is instance " << instance);
    }

    void SystemCatalog::markInstanceOnline(InstanceID instance_id, const std::string& host, uint16_t port)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_markInstanceOnline,
                this, instance_id, cref(host), port);
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_markInstanceOnline(InstanceID instance_id, const std::string& host, uint16_t port)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::markInstanceOnline( instance_id = " << instance_id << ", host = " << host << ", port = " << port << ")");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);

            string sql = "update \"instance\" set host = $1, port = $2, online_since = 'now' where instance_id = $3";
            _connection->prepare(sql, sql)
                ("varchar", treat_string)
                ("int", treat_direct)
                ("bigint", treat_direct);

            tr.prepared(sql)(host)(port)(instance_id).exec();

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    void SystemCatalog::markInstanceOffline(InstanceID instance_id)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_markInstanceOffline,
                this, instance_id);
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_markInstanceOffline(InstanceID instance_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::markInstanceOffline( instance_id = " << instance_id << ")");
        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);

            string sql = "update \"instance\" set online_since = 'infinity' where instance_id = $1";
            _connection->prepare(sql, sql)
                ("bigint", treat_direct);

            tr.prepared(sql)(instance_id).exec();

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    void SystemCatalog::connect(const std::string& connectionString, bool doUpgrade)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::connect( connect string ='" << connectionString << ")");

        if (!PQisthreadsafe())
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_LIBPQ_NOT_THREADSAFE);

        try
        {
            _connection = new pqxx::connection(connectionString);

            work tr(*_connection);
            result query_res = tr.exec("select count(*) from pg_tables where tablename = 'cluster'");
            _initialized = query_res[0].at("count").as(bool());

            if (_initialized)
            {
                result query_res = tr.exec("select get_cluster_uuid as uuid from get_cluster_uuid()");
                _uuid = query_res[0].at("uuid").as(string());

                query_res = tr.exec("select count(*) from pg_proc where proname = 'get_metadata_version'");
                if (query_res[0].at("count").as(bool()))
                {
                    query_res = tr.exec("select get_metadata_version as version from get_metadata_version()");
                    _metadataVersion = query_res[0].at("version").as(int());
                }
                else
                {
                    LOG4CXX_WARN(logger, "Can not find procedure get_metadata_version in catalog. Assuming catalog metadata version is 0");
                    _metadataVersion = 0;
                }
            }
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query()
                << e.what();
        }
        catch (const PGSTD::runtime_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_CONNECT_PG) << e.what();
        }

        if (_initialized && doUpgrade)
        {
            if (_metadataVersion > METADATA_VERSION)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CATALOG_NEWER_THAN_SCIDB)
                        << METADATA_VERSION << _metadataVersion;
            }
            else if (_metadataVersion < METADATA_VERSION)
            {
                if (Config::getInstance()->getOption<bool>(CONFIG_ENABLE_CATALOG_UPGRADE) == false)
                {
                    string const& configName = Config::getInstance()->getOptionName(CONFIG_ENABLE_CATALOG_UPGRADE);
                    ostringstream message;
                    message <<"In order to proceed, SciDB needs to perform an upgrade of the system "<<
                              "catalog. This is not reversible. To confirm, please restart the system "<<
                              "with the setting \'"<<configName<<"\' set to 'true'";
                    LOG4CXX_ERROR(logger, message.str());
                    std::cerr<<message.str()<<"\n";
                    throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_NEED_UPGRADE_CONFIRMATION);
                }

                LOG4CXX_WARN(logger, "Catalog metadata version (" << _metadataVersion
                        << ") lower than SciDB metadata version (" << METADATA_VERSION
                        << "). Trying to upgrade catalog...");

                try
                {
                    work tr(*_connection);
                    sleep(5); //XXX why sleep ?
                    for(int ver = _metadataVersion + 1; ver <= METADATA_VERSION; ++ver)
                    {
                        LOG4CXX_WARN(logger, "Upgrading metadata from " << ver-1 << " to " << ver);
                        string upgradeScript(METADATA_UPGRADES_LIST[ver]);
                        tr.exec(string(METADATA_UPGRADES_LIST[ver]));
                    }
                    tr.commit();
                    _metadataVersion = METADATA_VERSION;
                }
                catch (const sql_error &e)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
                }
                catch (const pqxx::failure &e)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
                }
            }
        }
    }

    bool SystemCatalog::isConnected() const
    {
        if (_connection)
            return _connection->is_open();
        return false;
    }

    void SystemCatalog::addLibrary(const string& libraryName)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_addLibrary,
                this, cref(libraryName));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_addLibrary(const string& libraryName)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::addLibrary( libraryName ='" << libraryName << ")");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);

            result query_res = tr.exec("select nextval from nextval('libraries_id_seq')");
            int64_t lid = query_res[0].at("nextval").as(int64_t());

            string sql1 = "insert into \"libraries\"(id, name)"
                " values ($1, $2)";
            _connection->prepare("addLibrary", sql1)
                ("bigint", treat_direct)
                ("varchar", treat_string);
            tr.prepared("addLibrary")
                (lid)
                (libraryName).exec();
            _connection->unprepare("addLibrary");
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    void SystemCatalog::getLibraries(vector<string >& libraries)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_getLibraries,
                this, ref(libraries));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_getLibraries(vector< string >& libraries)
    {

        LOG4CXX_TRACE(logger, "SystemCatalog::getLibraries ( &libraries )");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);

            string sql1 = "select name from \"libraries\"";
            _connection->prepare("getLibraries", sql1);
            result query_res = tr.prepared("getLibraries").exec();
            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i) {
                libraries.push_back(i.at("name").as(string()));
            }
            _connection->unprepare("getLibraries");
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        LOG4CXX_TRACE(logger, "Loaded " << libraries.size() << " libraries.");
    }

    void SystemCatalog::removeLibrary(const string& libraryName)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_removeLibrary,
                this, cref(libraryName));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_removeLibrary(const string& libraryName)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::removeLibrary ( " << libraryName << ")");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);
            string sql1 = "delete from \"libraries\" where name = $1";
            _connection->prepare("removeLibrary", sql1)
                ("varchar", treat_string);
            tr.prepared("removeLibrary")
                (libraryName).exec();
            _connection->unprepare("removeLibrary");
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }


    SystemCatalog::SystemCatalog() :
    _initialized(false),
    _connection(NULL),
    _uuid(""),
    _metadataVersion(-1),
    _reconnectTries(Config::getInstance()->getOption<int>(CONFIG_CATALOG_RECONNECT_TRIES))
    {

    }

    SystemCatalog::~SystemCatalog()
    {
        if (_connection)
        {
            try
            {
                if (isConnected())
                {
                    _connection->disconnect();
                }
                delete _connection;
                _connection = NULL;
            }
            catch (...)
            {
                LOG4CXX_DEBUG(logger, "Error when disconnecting from PostgreSQL.");
            }
        }
    }

int SystemCatalog::getMetadataVersion() const
{
    return _metadataVersion;
}

std::string SystemCatalog::getLockInsertSql(const boost::shared_ptr<LockDesc>& lockDesc)
{
   assert(lockDesc);
   assert(lockDesc->getInstanceRole() == LockDesc::COORD ||
          lockDesc->getInstanceRole() == LockDesc::WORKER);
   string lockInsertSql;


   if (lockDesc->getLockMode() == LockDesc::RD) {
      if (lockDesc->getInstanceRole() == LockDesc::COORD) {
         lockInsertSql = "insert into array_version_lock"
         " (array_name, array_id, query_id, instance_id, array_version_id, array_version, instance_role, lock_mode)"
         "(select $1::VARCHAR,$2,$3,$4,$5,$6,$7,$8 where not exists"
         "  (select AVL.array_name from array_version_lock as AVL where AVL.array_name=$1::VARCHAR and AVL.lock_mode>$9 and AVL.instance_role=$10))";

      } else {
          assert(false);
          throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "lock mode";
      }
   } else if (lockDesc->getLockMode() == LockDesc::WR ||
              lockDesc->getLockMode() == LockDesc::CRT) {
      if (lockDesc->getInstanceRole() == LockDesc::COORD) {
         lockInsertSql = "insert into array_version_lock"
         " (array_name, array_id, query_id, instance_id, array_version_id, array_version, instance_role, lock_mode)"
         "(select $1::VARCHAR,$2,$3,$4,$5,$6,$7,$8 where not exists"
         "  (select AVL.array_name from array_version_lock as AVL where AVL.array_name=$1::VARCHAR and AVL.query_id<>$3 and AVL.lock_mode>$9))";

      } else if (lockDesc->getInstanceRole() == LockDesc::WORKER) {
          if (lockDesc->getLockMode() == LockDesc::CRT) {
              assert(false);
              throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "lock mode";
          }
         lockInsertSql = "insert into array_version_lock"
         " ( array_name, array_id, query_id, instance_id, array_version_id, array_version, instance_role, lock_mode)"
         "  (select AVL.array_name, AVL.array_id, AVL.query_id, $3, AVL.array_version_id, AVL.array_version, $4, AVL.lock_mode"
         " from array_version_lock as AVL where AVL.array_name=$1::VARCHAR"
         " and AVL.query_id=$2 and AVL.instance_role=1 and (AVL.lock_mode=$5 or AVL.lock_mode=$6))";
      }
   }  else if (lockDesc->getLockMode() == LockDesc::RM) {

      if (lockDesc->getInstanceRole() == LockDesc::COORD) {
         lockInsertSql = "insert into array_version_lock"
         " ( array_name, array_id, query_id, instance_id, array_version_id, array_version, instance_role, lock_mode)"
         "(select $1::VARCHAR,$2,$3,$4,$5,$6,$7,$8 where not exists"
         "  (select array_name from array_version_lock where array_name=$1::VARCHAR and query_id<>$3))";
      } else {
         assert(false);
         throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "lock mode";
      }
   }  else if (lockDesc->getLockMode() == LockDesc::RNF) {
        if (lockDesc->getInstanceRole() == LockDesc::COORD) {
            lockInsertSql = "insert into array_version_lock"
            " ( array_name, array_id, query_id, instance_id, array_version_id, array_version, instance_role, lock_mode)"
            "(select $1::VARCHAR,$2,$3,$4,$5,$6,$7,$8 where not exists"
            "  (select array_name from array_version_lock where array_name=$1::VARCHAR and query_id<>$3))";
      } else if (lockDesc->getInstanceRole() == LockDesc::WORKER) {
         lockInsertSql = "insert into array_version_lock"
         " ( array_name, array_id, query_id, instance_id, array_version_id, array_version, instance_role, lock_mode)"
         "  (select AVL.array_name, AVL.array_id, AVL.query_id, $3, AVL.array_version_id, AVL.array_version, $4, AVL.lock_mode"
         " from array_version_lock as AVL where AVL.array_name=$1::VARCHAR"
         " and AVL.query_id=$2 and AVL.instance_role=$5 and AVL.lock_mode=$6)";
      }
   } else {
      assert(false);
      throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "lock mode";
   }
   return lockInsertSql;
}

bool SystemCatalog::lockArray(const boost::shared_ptr<LockDesc>& lockDesc, ErrorChecker& errorChecker)
{
    boost::function<bool()> work = boost::bind(&SystemCatalog::_lockArray,
                                               this, cref(lockDesc), ref(errorChecker));
    return Query::runRestartableWork<bool, broken_connection>(work, _reconnectTries);
}

bool SystemCatalog::_lockArray(const boost::shared_ptr<LockDesc>& lockDesc, ErrorChecker& errorChecker)
{
   assert(lockDesc);
   LOG4CXX_TRACE(logger, "SystemCatalog::lockArray: "<<lockDesc->toString());
   try
   {
      assert(_connection);
      string lockInsertSql = getLockInsertSql(lockDesc);

      string lockTableSql = "LOCK TABLE array_version_lock";
      {
          ScopedMutexLock mutexLock(_pgLock);
          work tr(*_connection);
          result query_res;

          _connection->prepare(lockTableSql, lockTableSql);
          tr.prepared(lockTableSql).exec();

          if (lockDesc->getLockMode() == LockDesc::RD) {

              if (lockDesc->getInstanceRole() == LockDesc::COORD) {
                  string uniquePrefix("COORD-RD-");
                  _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
                  ("varchar", treat_string)
                  ("bigint", treat_direct)
                  ("bigint", treat_direct)
                  ("bigint", treat_direct)
                  ("bigint", treat_direct)
                  ("bigint", treat_direct)
                  ("integer", treat_direct)
                  ("integer", treat_direct)
                  ("integer", treat_direct)
                  ("integer", treat_direct);

                  query_res = tr.prepared(uniquePrefix+lockInsertSql)
                  (lockDesc->getArrayName())
                  (lockDesc->getArrayId())
                  (lockDesc->getQueryId())
                  (lockDesc->getInstanceId())
                  (lockDesc->getArrayVersionId())
                  (lockDesc->getArrayVersion())
                  ((int)lockDesc->getInstanceRole())
                  ((int)lockDesc->getLockMode())
                  ((int)LockDesc::RD)
                  ((int)LockDesc::COORD).exec();
              } else { assert(false);}
          } else if (lockDesc->getLockMode() == LockDesc::WR
                     || lockDesc->getLockMode() == LockDesc::CRT) {

              if (lockDesc->getInstanceRole() == LockDesc::COORD) {
                  string uniquePrefix("COORD-WR-");
                  _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
                  ("varchar", treat_string)
                  ("bigint", treat_direct)
                  ("bigint", treat_direct)
                  ("bigint", treat_direct)
                  ("bigint", treat_direct)
                  ("bigint", treat_direct)
                  ("integer", treat_direct)
                  ("integer", treat_direct)
                  ("integer", treat_direct);

                  query_res = tr.prepared(uniquePrefix+lockInsertSql)
                  (lockDesc->getArrayName())
                  (lockDesc->getArrayId())
                  (lockDesc->getQueryId())
                  (lockDesc->getInstanceId())
                  (lockDesc->getArrayVersionId())
                  (lockDesc->getArrayVersion())
                  ((int)lockDesc->getInstanceRole())
                  ((int)lockDesc->getLockMode())
                  ((int)LockDesc::INVALID_MODE).exec();

              } else if (lockDesc->getInstanceRole() == LockDesc::WORKER) {
                  assert(lockDesc->getLockMode() != LockDesc::CRT);
                  string uniquePrefix("WORKER-WR-");
                  _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
                  ("varchar", treat_string)
                  ("bigint", treat_direct)
                  ("bigint", treat_direct)
                  ("integer", treat_direct)
                  ("integer", treat_direct)
                  ("integer", treat_direct);

                  query_res = tr.prepared(uniquePrefix+lockInsertSql)
                  (lockDesc->getArrayName())
                  (lockDesc->getQueryId())
                  (lockDesc->getInstanceId())
                  ((int)LockDesc::WORKER)
                  ((int)LockDesc::WR)
                  ((int)LockDesc::CRT).exec();

                  if (query_res.affected_rows() == 1) {
                      string lockReadSql = "select array_id, array_version_id, array_version"
                      " from array_version_lock where array_name=$1::VARCHAR and query_id=$2 and instance_id=$3";

                      _connection->prepare(lockReadSql, lockReadSql)
                      ("varchar", treat_string)
                      ("bigint", treat_direct)
                      ("bigint", treat_direct);

                      result query_res_read = tr.prepared(lockReadSql)
                      (lockDesc->getArrayName())
                      (lockDesc->getQueryId())
                      (lockDesc->getInstanceId()).exec();

                      assert(query_res_read.size() == 1);

                      lockDesc->setArrayVersion(query_res_read[0].at("array_version").as(VersionID()));
                      lockDesc->setArrayId(query_res_read[0].at("array_id").as(ArrayID()));
                      lockDesc->setArrayVersionId(query_res_read[0].at("array_version_id").as(ArrayID()));
                  }
              } else { assert(false); }
          }  else if (lockDesc->getLockMode() == LockDesc::RM) {

              assert(lockDesc->getInstanceRole() == LockDesc::COORD);

              string uniquePrefix("RM-");
              _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
              ("varchar", treat_string)
              ("bigint", treat_direct)
              ("bigint", treat_direct)
              ("bigint", treat_direct)
              ("bigint", treat_direct)
              ("bigint", treat_direct)
              ("integer", treat_direct)
              ("integer", treat_direct);

              query_res = tr.prepared(uniquePrefix+lockInsertSql)
              (lockDesc->getArrayName())
              (lockDesc->getArrayId())
              (lockDesc->getQueryId())
              (lockDesc->getInstanceId())
              (lockDesc->getArrayVersionId())
              (lockDesc->getArrayVersion())
              ((int)lockDesc->getInstanceRole())
              ((int)lockDesc->getLockMode()).exec();
          }  else if (lockDesc->getLockMode() == LockDesc::RNF) {
              if (lockDesc->getInstanceRole() == LockDesc::COORD) {

                  string uniquePrefix("COORD-RNF-");
                  _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
                  ("varchar", treat_string)
                  ("bigint", treat_direct)
                  ("bigint", treat_direct)
                  ("bigint", treat_direct)
                  ("bigint", treat_direct)
                  ("bigint", treat_direct)
                  ("integer", treat_direct)
                  ("integer", treat_direct);

                  query_res = tr.prepared(uniquePrefix+lockInsertSql)
                  (lockDesc->getArrayName())
                  (lockDesc->getArrayId())
                  (lockDesc->getQueryId())
                  (lockDesc->getInstanceId())
                  (lockDesc->getArrayVersionId())
                  (lockDesc->getArrayVersion())
                  ((int)lockDesc->getInstanceRole())
                  ((int)lockDesc->getLockMode()).exec();

              } else if (lockDesc->getInstanceRole() == LockDesc::WORKER) {

                  string uniquePrefix("WORKER-RNF-");
                  _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
                  ("varchar", treat_string)
                  ("bigint", treat_direct)
                  ("bigint", treat_direct)
                  ("integer", treat_direct)
                  ("integer", treat_direct)
                  ("integer", treat_direct);

                  query_res = tr.prepared(uniquePrefix+lockInsertSql)
                  (lockDesc->getArrayName())
                  (lockDesc->getQueryId())
                  (lockDesc->getInstanceId())
                  ((int)LockDesc::WORKER)
                  ((int)LockDesc::COORD)
                  ((int)LockDesc::RNF).exec();
              } else { assert(false); }
          } else {
              assert(false);
          }
          if (query_res.affected_rows() == 1) {
              tr.commit();
              lockDesc->setLocked(true);
              LOG4CXX_DEBUG(logger, "SystemCatalog::lockArray: locked "<<lockDesc->toString());
              return true;
          }
          if (lockDesc->getInstanceRole() == LockDesc::WORKER &&
              query_res.affected_rows() != 1) {
              // workers must error out immediately
              assert(query_res.affected_rows()==0);
              tr.commit();
              return false;
          }
          tr.commit();
      }
      if (errorChecker && !errorChecker()) {
          return false;
      }
      throw LockBusyException(REL_FILE, __FUNCTION__, __LINE__);
   }
   catch (const pqxx::unique_violation &e)
   {
       if (!lockDesc->isLocked()) {
           assert(false);
           throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
       }

       // On coordinator we may try to acquire the same lock
       // multiple times. If the lock is already acquired,
       // we should just return success.
       // XXX tigor 6/20/2013 TODO: Technically, just checking isLocked()
       // before running the query should be sufficient.
       // After debugging we should/can switch to doing just that.

       assert(lockDesc->getInstanceRole() == LockDesc::COORD);

       return true;
   }
   catch (const pqxx::broken_connection &e)
   {
       throw;
   }
   catch (const pqxx::sql_error &e)
   {
      LOG4CXX_ERROR(logger, "SystemCatalog::lockArray: postgress exception:"<< e.what());
      LOG4CXX_ERROR(logger, "SystemCatalog::lockArray: query:"<< e.query());
      LOG4CXX_ERROR(logger, "SystemCatalog::lockArray: "
                    << ((!lockDesc) ? string("lock:NULL") : lockDesc->toString()));
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   return false;
}

bool SystemCatalog::unlockArray(const boost::shared_ptr<LockDesc>& lockDesc)
{
    boost::function<bool()> work = boost::bind(&SystemCatalog::_unlockArray,
            this, cref(lockDesc));
    return Query::runRestartableWork<bool, broken_connection>(work, _reconnectTries);
}

bool SystemCatalog::_unlockArray(const boost::shared_ptr<LockDesc>& lockDesc)
{
   assert(lockDesc);
   LOG4CXX_DEBUG(logger, "SystemCatalog::unlockArray: "<<lockDesc->toString());
   bool rc = false;
   try
   {
      assert(_connection);
      string lockDeleteSql("delete from array_version_lock where array_name=$1::VARCHAR and query_id=$2 and instance_id=$3");

      {
         ScopedMutexLock mutexLock(_pgLock);

         work tr(*_connection);

         _connection->prepare(lockDeleteSql, lockDeleteSql)
         ("varchar", treat_string)
         ("bigint", treat_direct)
         ("bigint", treat_direct);

         result query_res = tr.prepared(lockDeleteSql)
         (lockDesc->getArrayName())
         (lockDesc->getQueryId())
         (lockDesc->getInstanceId()).exec();

         rc = (query_res.affected_rows() == 1);
         tr.commit();
      }
   }
   catch (const broken_connection &e)
   {
       throw;
   }
   catch (const sql_error &e)
   {
      LOG4CXX_ERROR(logger, "SystemCatalog::unlockArray: postgress exception:"<< e.what());
      LOG4CXX_ERROR(logger, "SystemCatalog::unlockArray: query:"<< e.query());
      LOG4CXX_ERROR(logger, "SystemCatalog::unlockArray: "
                    << ((!lockDesc) ? string("lock:NULL") : lockDesc->toString()));
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   return rc;
}

bool SystemCatalog::updateArrayLock(const boost::shared_ptr<LockDesc>& lockDesc)
{
    boost::function<bool()> work = boost::bind(&SystemCatalog::_updateArrayLock,
            this, cref(lockDesc));
    return Query::runRestartableWork<bool, broken_connection>(work, _reconnectTries);
}

bool SystemCatalog::_updateArrayLock(const boost::shared_ptr<LockDesc>& lockDesc)
{
   assert(lockDesc);

   LOG4CXX_TRACE(logger, "SystemCatalog::updateArrayLock: "<<lockDesc->toString());
   bool rc = false;
   try
   {
      assert(_connection);
      string lockUpdateSql = "update array_version_lock set array_id=$4, array_version_id=$5, array_version=$6, lock_mode=$7"
                             " where array_name=$1::VARCHAR and query_id=$2 and instance_id=$3";
      {
         ScopedMutexLock mutexLock(_pgLock);

         work tr(*_connection);

         _connection->prepare(lockUpdateSql, lockUpdateSql)
         ("varchar", treat_string)
         ("bigint", treat_direct)
         ("bigint", treat_direct)
         ("bigint", treat_direct)
         ("bigint", treat_direct)
         ("bigint", treat_direct)
         ("integer", treat_direct);

         result query_res = tr.prepared(lockUpdateSql)
         (lockDesc->getArrayName())
         (lockDesc->getQueryId())
         (lockDesc->getInstanceId())
         (lockDesc->getArrayId())
         (lockDesc->getArrayVersionId())
         (lockDesc->getArrayVersion())
         ((int)lockDesc->getLockMode()).exec();

         rc = (query_res.affected_rows() == 1);
         tr.commit();
      }
   }
   catch (const broken_connection &e)
   {
       throw;
   }
   catch (const sql_error &e)
   {
      LOG4CXX_ERROR(logger, "SystemCatalog::updateArrayLock: postgress exception:"<< e.what());
      LOG4CXX_ERROR(logger, "SystemCatalog::updateArrayLock: query:"<< e.query());
      LOG4CXX_ERROR(logger, "SystemCatalog::updateArrayLock: "
                    << ((!lockDesc) ? string("lock:NULL") : lockDesc->toString()));
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   return rc;
}

void SystemCatalog::readArrayLocks(const InstanceID instanceId,
                                   std::list<boost::shared_ptr<LockDesc> >& coordLocks,
                                   std::list<boost::shared_ptr<LockDesc> >& workerLocks)
{
    boost::function<void()> work = boost::bind(&SystemCatalog::_readArrayLocks,
            this, instanceId, ref(coordLocks), ref(workerLocks));
    Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
}

void SystemCatalog::_readArrayLocks(const InstanceID instanceId,
                                   std::list<boost::shared_ptr<LockDesc> >& coordLocks,
                                   std::list<boost::shared_ptr<LockDesc> >& workerLocks)
{
   LOG4CXX_TRACE(logger, "SystemCatalog::getArrayLocks(instanceId = " << instanceId);

   ScopedMutexLock mutexLock(_pgLock);
   assert(_connection);
   try
   {
      work tr(*_connection);

      string sql = "select array_name, array_id, query_id, array_version_id, array_version, instance_role, lock_mode"
      " from array_version_lock where instance_id=$1";
      _connection->prepare(sql, sql)("bigint", treat_direct);

      result query_res = tr.prepared(sql)(instanceId).exec();
      size_t size = query_res.size();
      LOG4CXX_TRACE(logger, "SystemCatalog::getArrayLocks: found "<< size <<" locks");

      for (size_t i=0; i < size; ++i) {
         boost::shared_ptr<LockDesc> lock( new LockDesc(query_res[i].at("array_name").as(string()),
                                                        query_res[i].at("query_id").as(QueryID()),
                                                        instanceId,
                                                        static_cast<LockDesc::InstanceRole>(query_res[i].at("instance_role").as(int())),
                                                        static_cast<LockDesc::LockMode>(query_res[i].at("lock_mode").as(int()))
                                                        ));
         lock->setArrayVersion(query_res[i].at("array_version").as(VersionID()));
         lock->setArrayId(query_res[i].at("array_id").as(ArrayID()));
         lock->setArrayVersionId(query_res[i].at("array_version_id").as(ArrayID()));

         if (lock->getInstanceRole() == LockDesc::COORD) {
            coordLocks.push_back(lock);
         } else {
            workerLocks.push_back(lock);
         }
         LOG4CXX_TRACE(logger, lock->toString());
      }
      tr.commit();
   }
   catch (const broken_connection &e)
   {
       throw;
   }
   catch (const sql_error &e)
   {
      LOG4CXX_ERROR(logger, "SystemCatalog::readArrayLocks: postgress exception:"<< e.what());
      LOG4CXX_ERROR(logger, "SystemCatalog::readArrayLocks: query:"<< e.query());
      LOG4CXX_ERROR(logger, "SystemCatalog::readArrayLocks: instance ID = " << instanceId);
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
}

uint32_t SystemCatalog::deleteCoordArrayLocks(InstanceID instanceId)
{
    return deleteArrayLocks(instanceId, INVALID_QUERY_ID, LockDesc::COORD);
}

uint32_t SystemCatalog::deleteWorkerArrayLocks(InstanceID instanceId)
{
    return deleteArrayLocks(instanceId, INVALID_QUERY_ID,LockDesc::WORKER);
}

uint32_t SystemCatalog::deleteArrayLocks(InstanceID instanceId, QueryID queryId, LockDesc::InstanceRole role)
{
    boost::function<uint32_t()> work = boost::bind(&SystemCatalog::_deleteArrayLocks,
                                                   this, instanceId, queryId, role);
    return Query::runRestartableWork<uint32_t, broken_connection>(work, _reconnectTries);
}

uint32_t SystemCatalog::_deleteArrayLocks(InstanceID instanceId, QueryID queryId, LockDesc::InstanceRole role)
{
    LOG4CXX_DEBUG(logger, "SystemCatalog::deleteArrayLocks instanceId = "
                  << instanceId
                  << " role = "<<role
                  << " queryId = "<<queryId);
   size_t numLocksDeleted = 0;
   ScopedMutexLock mutexLock(_pgLock);

   try
   {
      assert(_connection);

      uint16_t argNum = 1;
      string lockDeleteSql("delete from array_version_lock where instance_id=$1");
      bool isQuerySpecified = (queryId != INVALID_QUERY_ID && queryId != 0);
      bool isRoleSpecified = (role !=LockDesc::INVALID_ROLE);

      work tr(*_connection);

      result query_res;

      if (isQuerySpecified) {
          stringstream ss;
          ss << " and query_id=$"<< ++argNum;
          lockDeleteSql += ss.str();
      }

      if (isRoleSpecified) {
          stringstream ss;
          ss << " and instance_role=$"<< ++argNum;
          lockDeleteSql += ss.str();
      }

      pqxx::prepare::declaration decl = _connection->prepare(lockDeleteSql, lockDeleteSql)
          ("bigint", treat_direct);

      if (isQuerySpecified) {
          decl("bigint", treat_direct);
      }
      if (isRoleSpecified) {
          decl("integer", treat_direct);
      }

      pqxx::prepare::invocation invc = tr.prepared(lockDeleteSql)(instanceId);

      if (isQuerySpecified) {
          invc(queryId);
      }
      if (isRoleSpecified) {
          invc((int)role);
      }

      query_res = invc.exec();

      numLocksDeleted = query_res.affected_rows();

      LOG4CXX_TRACE(logger, "SystemCatalog::deleteArrayLocks: deleted "
                    << numLocksDeleted
                    <<"locks for instance " << instanceId);
      tr.commit();
   }
   catch (const broken_connection &e)
   {
       throw;
   }
   catch (const sql_error &e)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   return numLocksDeleted;
}

boost::shared_ptr<SystemCatalog::LockDesc>
SystemCatalog::checkForCoordinatorLock(const string& arrayName, QueryID queryId)
{
    boost::function<boost::shared_ptr<SystemCatalog::LockDesc>()> work = boost::bind(&SystemCatalog::_checkForCoordinatorLock,
            this, cref(arrayName), queryId);
    return Query::runRestartableWork<boost::shared_ptr<SystemCatalog::LockDesc>, broken_connection>(work, _reconnectTries);
}

boost::shared_ptr<SystemCatalog::LockDesc>
SystemCatalog::_checkForCoordinatorLock(const string& arrayName, QueryID queryId)
{
   LOG4CXX_TRACE(logger, "SystemCatalog::checkForCoordinatorLock:"
                 << " arrayName = " << arrayName
                 << " queryID = " << queryId);

   boost::shared_ptr<LockDesc> coordLock;

   ScopedMutexLock mutexLock(_pgLock);

   assert(_connection);
   try
   {
      work tr(*_connection);

      string sql = "select array_id, instance_id, array_version_id, array_version, lock_mode"
      " from array_version_lock where array_name=$1::VARCHAR and query_id=$2 and instance_role=$3";
      _connection->prepare(sql, sql)
      ("varchar", treat_string)
      ("bigint", treat_direct)
      ("integer", treat_direct);

      result query_res = tr.prepared(sql)(arrayName)(queryId)((int)LockDesc::COORD).exec();
      size_t size = query_res.size();
      LOG4CXX_TRACE(logger, "SystemCatalog::checkForCoordinatorLock found "<< size <<" locks");

      assert(size < 2);
      if (size > 0) {
         coordLock = boost::shared_ptr<LockDesc>(new LockDesc(arrayName,
                                                              queryId,
                                                              query_res[0].at("instance_id").as(InstanceID()),
                                                              LockDesc::COORD,
                                                              static_cast<LockDesc::LockMode>(query_res[0].at("lock_mode").as(int()))
                                                     ));
         coordLock->setArrayVersion(query_res[0].at("array_version").as(VersionID()));
         coordLock->setArrayId(query_res[0].at("array_id").as(ArrayID()));
         coordLock->setArrayVersionId(query_res[0].at("array_version_id").as(ArrayID()));
         LOG4CXX_TRACE(logger, coordLock->toString());
      }
      tr.commit();
   }
   catch (const broken_connection &e)
   {
       throw;
   }
   catch (const sql_error &e)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   return coordLock;
}

void SystemCatalog::renameArray(const string &old_array_name, const string &new_array_name)
{
    boost::function<void()> work = boost::bind(&SystemCatalog::_renameArray,
            this, cref(old_array_name), cref(new_array_name));
    Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
}

void SystemCatalog::_renameArray(const string &old_array_name, const string &new_array_name)
{
   LOG4CXX_TRACE(logger, "SystemCatalog::renameArray( old name = "
                 << old_array_name << ")"
                 << "new name = " << new_array_name << ")");

   // replace all AAA, AAA@y with BBB, BBB@y correspondingly
   string renameSql = "update \"array\" set name=regexp_replace(name, '^'||$1::VARCHAR||'(@.+)?$', $2::VARCHAR||E'\\\\1')";
   ScopedMutexLock mutexLock(_pgLock);

   assert(_connection);
   try
   {
      work tr(*_connection);

      _connection->prepare(renameSql, renameSql)("varchar", treat_string)("varchar", treat_string);
      result query_res = tr.prepared(renameSql)(old_array_name)(new_array_name).exec();

      bool rc = (query_res.affected_rows() > 0);
      if (!rc) {
          throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << old_array_name;
      }
      tr.commit();
   }
   catch (const broken_connection &e)
   {
       throw;
   }
   catch (const unique_violation& e)
   {
       LOG4CXX_ERROR(logger, "SystemCatalog::renameArray: unique constraint violation:"<< e.what());
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_ALREADY_EXIST) << new_array_name;
   }
   catch (const sql_error &e)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
}

void SystemCatalog::getArrays(std::vector<ArrayDesc> &arrays,
                              bool ignoreOrphanAttributes,
                              bool ignoreVersions)
{
    boost::function<void()> work = boost::bind(&SystemCatalog::_getArrays,
                                               this, ref(arrays),
                                               ignoreOrphanAttributes,
                                               ignoreVersions);
    Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
}

void SystemCatalog::_getArrays(std::vector<ArrayDesc> &arrays,
                               bool ignoreOrphanAttributes,
                               bool ignoreVersions)
{
    LOG4CXX_TRACE(logger, "SystemCatalog::getArrays(ArrayDescs)");

    string sql ("select name from \"array\" where name is not null");
    if (ignoreVersions) {
        sql += " and name not like '%@%'";
    }
    sql += " order by name";
    ScopedMutexLock mutexLock(_pgLock);

    assert(_connection);

    try
    {
        pqxx::transaction<pqxx::serializable> tr(*_connection);
        result query_res = tr.exec(sql);

        arrays.clear();
        arrays.resize(query_res.size());
        size_t i=0;
        for (result::const_iterator cursor = query_res.begin();
             cursor != query_res.end(); ++cursor, ++i) {

            const string arrName(cursor.at("name").c_str());
            ArrayDesc& arrDesc = arrays[i];

            _getArrayDesc(arrName, arrDesc, ignoreOrphanAttributes, &tr);
        }

        tr.commit();
    }
    catch (const broken_connection &e)
    {
        throw;
    }
    catch (const sql_error &e)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
    }
    catch (const pqxx::failure &e)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
    }

    LOG4CXX_TRACE(logger, "Retrieved " << arrays.size() << " arrays from catalogs");
}

} // namespace catalog
