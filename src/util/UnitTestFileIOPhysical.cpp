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

#include <query/Operator.h>
#include <array/Metadata.h>
#include <system/Cluster.h>
#include <query/Query.h>
#include <boost/make_shared.hpp>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <log4cxx/logger.h>
#include <util/NetworkMessage.h>
#include <array/RLE.h>
#include <util/FileIO.h>

using namespace boost;
using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.unittest"));

class UnitTestFileIOPhysical: public PhysicalOperator
{
public:

    UnitTestFileIOPhysical(const string& logicalName,
                           const string& physicalName,
                           const Parameters& parameters,
                           const ArrayDesc& schema)
    : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        /* Simple test:  create MAX_OPEN_FD + 10 open file objects.  write data into each.  read data
           back and verify.
         */
        string                       basepath = 
            getDir(Config::getInstance()->getOption<string>(CONFIG_STORAGE));

        uint32_t                     nfileobjs;
        uint32_t                     i;
        vector< shared_ptr< File > > fileobjs(0);

        nfileobjs = Config::getInstance()->getOption<int>(CONFIG_MAX_OPEN_FDS) + 10;

        for (i = 0; i < nfileobjs; ++i)
        {
            stringstream filename;

            filename << basepath << '/' << i << ".fileio-test";
            fileobjs.push_back(FileManager::getInstance()->openFileObj(filename.str().c_str(), O_CREAT | O_EXCL | O_RDWR));
        }

        for (i = 0; i < nfileobjs; ++i)
        {
            uint32_t* buf = new uint32_t[i];

            for (uint32_t j = 0; j < i; ++j)
            {
                buf[j] = i;
            }
            fileobjs[i]->writeAll(reinterpret_cast<char*>(buf), (sizeof(uint32_t) * i), 0);
            
            delete [] buf;
        }

        for (i = 0; i < nfileobjs; ++i)
        {
            uint32_t* buf = new uint32_t[i];
            
            fileobjs[i]->readAll(reinterpret_cast<char*>(buf), (sizeof(uint32_t) * i), 0);
            for (uint32_t j = 0; j < i; ++j)
            {
                if (buf[j] != i)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED) 
                        << "UnitTestFileIOPhysical" << "read data mismatch";
                }
            }
            fileobjs[i]->removeOnClose();

            delete [] buf;
        }

        return shared_ptr<Array> (new MemArray(_schema,query));
    }

};

REGISTER_PHYSICAL_OPERATOR_FACTORY(UnitTestFileIOPhysical, "test_file_io", "UnitTestFileIOPhysical");
}
