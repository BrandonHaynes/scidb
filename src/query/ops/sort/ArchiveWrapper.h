/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2014 SciDB, Inc.
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
 * @file ArchiveWrapper.h
 *
 * @brief Wrapper utilities for boost::archive, that can be used to serialize arbitrary data (both ways).
 *
 *  Created on: Nov 25, 2014
 *      Author: Donghui Zhang
 *
 * @example
 *   - Suppose you want one SciDB instance to send a scidb::Value to another instance.
 *   - Background 1: scidb::Value has a data member template<Archive>serialize() that serialize from/into a boost::archive object.
 *   - Background 2: There are BufSend() and BufReceive() functions that send/receive shared_ptr<SharedBuffer>.
 *
 *   - The sender may:
 *     OArchiveWrapper oaw;
 *     boost::archive::binary_oarchive* oarchive = oaw.reset();
 *     Value v = ...
 *     v.serialize(oarchive, 0);
 *     shared_ptr<SharedBuffer> buffer = oaw.getSharedBuffer();
 *     BufSend(...buffer...)
 *
 *   - The receiver may:
 *     shared_ptr<SharedBuffer> buffer = BufReceive(...)
 *     IArchiveWrapper iaw;
 *     boost::archive::binary_oarchive* iarchive = iaw.reset(buffer);
 *     Value v;
 *     v.serialize(iaw.getArchive(), 0);
 *
 * @note
 *   This class uses binary archives (boost::archive::binary_iarchive and binary_oarchive).
 *   The code only works assuming a homogeneous cluster, where all machines have the same architecture.
 *   If portability is an issue, the code should be changed to use text archives, which are slower.
 */

#include <sstream>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <array/Array.h>
#include <util/PointerRange.h>
#include <assert.h>

namespace scidb
{
/**
 * Wrapper for an output archive, to be used on a sender.
 */
class OArchiveWrapper
{
    boost::scoped_ptr<std::ostringstream> _ss;
    boost::scoped_ptr<boost::archive::binary_oarchive> _oa;

public:
    /**
     * @return the serialized data, in the form of shared_ptr<SharedBuffer> that may be passed to BufSend().
     *
     * @param copy  whether serialized data should be copied into the SharedBuffer.
     *              Default is to make a copy: slower but does not require OArchiveWrapper to remain valid.
     */
    boost::shared_ptr<SharedBuffer> getSharedBuffer(bool copy=true)
    {
        if (_ss) {
            _ss->flush();
            return boost::make_shared<MemoryBuffer>(_ss->str().data(), _ss->str().size(), copy);
        }
        return shared_ptr<SharedBuffer>();
    }

    /**
     * Reset to the initial state.
     * This function allows an OArchiveWrapper object to be reused.
     * @return a pointer to an archive::binary_oarchive object, to serialize data out.
     */
    boost::archive::binary_oarchive* reset()
    {
        _oa.reset();
        _ss.reset(new std::ostringstream());
        _oa.reset(new boost::archive::binary_oarchive(*_ss));
        return _oa.get();
    }

    /**
     * @return the serialized data in raw (i.e. char*) format.
     */
    char const* getData()
    {
        if (_ss) {
            _ss->flush();
            return _ss->str().data();
        }
        return NULL;
    }

    /**
     * @return the size of the serialized data.
     */
    size_t getSize()
    {
        if (_ss) {
            _ss->flush();
            return _ss->str().size();
        }
        return 0;
    }
};

/**
 * Wrapper for an input archive, to be used in a receiver.
 *
 * @note
 *   This class seemingly can be replaced with a function, that turns a shared_ptr<SharedBuffer> to a shared_ptr<binary_iarchive>.
 *   However, we prefer a class.
 *   The reason is that the stringbuf (or some equivalent stream object) needs to stay alive as long as the binary_iarchive is in use.
 *   Defining a class frees the caller from the responsibility of maintaining the stringbuf.
 */
class IArchiveWrapper
{
    std::stringbuf _sb;
    boost::scoped_ptr<boost::archive::binary_iarchive> _ia;
    boost::shared_ptr<SharedBuffer> _sharedBuffer;

public:
    /**
     * @param sharedBuffer  a shared_ptr of SharedBuffer, typically received with BufReceive().
     * @return a pointer to a binary_iarchive object to serialize data in.
     */
    boost::archive::binary_iarchive* reset(boost::shared_ptr<SharedBuffer> const& sharedBuffer)
    {
        _sharedBuffer = sharedBuffer;
        _sb.pubsetbuf(static_cast<char*>(sharedBuffer->getData()), sharedBuffer->getSize());
        _ia.reset(new boost::archive::binary_iarchive(_sb));
        return _ia.get();
    }

    boost::archive::binary_iarchive* reset(PointerRange<const char> range)
    {
        _sharedBuffer.reset();
        _sb.pubsetbuf(const_cast<char*>(range.begin()),range.size());

        _ia.reset(new boost::archive::binary_iarchive(_sb));
        return _ia.get();
    }
};

} // namespace
