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
 *    @file TileInterface.h
 *
 * @note
 * This file contains the virtual classes to interface with tiles.
 *
 *    BaseTile     - abstract tile interface
 *    BaseEncoding - abstract encoding interface
 *    TileFactory  - concrete class for producing concrete tiles based on scidb::TypeId & BaseEncoding::EncodingID
 */
#ifndef __TILE_INTERFACE__
#define __TILE_INTERFACE__

#include <assert.h>
#include <string.h>
#include <stdint.h>         // For the various int types
#include <iostream>         // For the operator<< and the dump().
#include <boost/unordered_map.hpp>
#include <boost/function.hpp>
#include <util/Singleton.h>
#include <query/TypeSystem.h>
#include <array/RLE.h>

namespace scidb {

    /**
     * Abstract interface to various data formats, e.g. RLE encoded values, unencoded contiguous sequence of values a.k.a. array, etc.
     * It is quite simplistic and meant only to enforce type checking and to provide a couple of bootstrapping hooks for tiles.
     * This is mainly because we dont expect SciDB operators to manipulate data encodings directly via the virtual interface.
     * The encodings should be manipulated using type-specific interfaces. In fact, concrete implementations of scidb::BaseTile
     * use specific encodings internally to maipulate the data.
     */
    class BaseEncoding {
    public:

        /// Supported encodings
        typedef enum  {
        NONE,
        ARRAY,
        RLE
        } EncodingID;

    protected:
        const EncodingID _encodingID;
        const scidb::TypeId _typeID;

        /// Constructor
        BaseEncoding ( EncodingID encodingID, TypeId typeID )
        : _encodingID(encodingID), _typeID(typeID)
        {
            if (encodingID!=ARRAY && encodingID!=RLE) {
                assert(false);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
                << "invalid BaseEncoding type";
            }
        }
    public:
        /// Destructor
        virtual ~BaseEncoding() {}

        /// @return this encoding's ID
        const EncodingID& getEncodingID() const { return _encodingID; }

        /// @return the type ID of the data contained in this encoding
        const scidb::TypeId& getTypeID()  const { return _typeID; }

        /**
         * An abstract interface for any context required in construction of an encoding
         */
        class Context {
        protected:
            Context () {}
            virtual ~Context() {}
        };
        /// Encoding initialization hook
        virtual void initialize(const Context* ctx=NULL) = 0;
    };

    /**
     * Abstract interface to a tile of data.
     * It represents a sequence of data values
     * which can be first constructed using these steps:
     * initialize(), push_back(), ..., push_back(), finalized()
     * and then examined by: at(), ..., at()
     * The purpose of BaseTile is to provide an abstract interface
     * for exchanging array data between operators (via iterators).
     * @see scidb::ConstChunkIterator::getData()
     */
    class BaseTile : public boost::noncopyable
    {
    public:

        /**
         * An abstract interface for any context required in construction of a tile
         */
        class Context {
        protected:
            Context() {}
            virtual ~Context() {}
        };
        virtual ~BaseTile() {}

        /// @return number of data elements in the tile
        virtual size_t size() const = 0;

        /// @return true iff the tile is empty
        virtual bool empty() const = 0;

        /// @return the size in bytes of each data element in the tile, or 0 if variable
        virtual size_t typeSize() const = 0;

        /// Tile initialization hook
        virtual void initialize() = 0;

        /// Get the internal encoding
        /// @note for internal use only
        //XXX HACK: to make RLEConstChunkIterator more efficient
        virtual BaseEncoding* getEncoding() = 0;

        /// Tile finalization hook
        virtual void finalize() = 0;

        /// Erase all data from the tile
        virtual void clear() = 0;

        /// Append a new element to the tile
        virtual void push_back( const Value& ) = 0;

        /// produce a data element at a specified index in the tile
        virtual void at( size_t, Value& ) const = 0;

        /// reserve space for a given number of elements
        virtual void reserve(size_t) = 0;
    };

    /**
     * A concrete factory for building tiles based on data and encoding types
     */
    class TileFactory : public scidb::Singleton<TileFactory>, public boost::noncopyable
    {
    public:

        TileFactory ()
        {
            registerBuiltinTypes(); //must be non-virtual
        }

        typedef boost::function< boost::shared_ptr<BaseTile>(const scidb::TypeId,
                                                             const BaseEncoding::EncodingID,
                                                             const BaseTile::Context*) >
        TileConstructor;

        void registerConstructor(const scidb::TypeId tID,
                                 const BaseEncoding::EncodingID eID,
                                 const TileConstructor& constructor);

        boost::shared_ptr<BaseTile> construct(const scidb::TypeId tID,
                                              const BaseEncoding::EncodingID eID,
                                              const BaseTile::Context* ctx=NULL);
    private:

        template<typename T, template<typename T> class E>
        void registerBuiltin(scidb::TypeId typeId, BaseEncoding::EncodingID encodingId);
        void registerBuiltinTypes();

        typedef std::pair<BaseEncoding::EncodingID, scidb::TypeId> KeyType;
        typedef boost::unordered_map<KeyType, TileConstructor > TileFactoryMap;
        TileFactoryMap _factories;
    };

} // scidb namespace
#endif //__TILE_INTERFACE__
