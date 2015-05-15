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

%define MODULE_DOCSTRING
"This module provides classes for accessing SciDB.  These Python classes were generate by
an interface compiler called SWIG (www.swig.org). There are SWIG-generated docstrings for
the Python classes, but since these are merely wrappers for the C++ API, it is possible that
for a full understanding of the Python interface, you will have to reference the SciDB C API
(TBD reference here).  Questions about how SWIG translates aspects of a C++ API into
Python classes can be found in the SWIG documentation, in particular the 'SWIG and Python' chapter
(http://www.swig.org/Doc2.0/SWIGDocumentation.html#Python).  Information on improving this
docstring documentation of the Python wrapper classes / API can be found in the 'Docstring Features'
section of that chapter."
%enddef

%module(docstring=MODULE_DOCSTRING) libscidbpython
#pragma SWIG nowarn=362,389,319,325,309,401,520,403,503

%{
#define SWIG_FILE_WITH_INIT
#define SCIDB_CLIENT
#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include "boost/shared_ptr.hpp"
using namespace std;
using namespace boost;
#include "log4cxx/logger.h"
#include "query/TypeSystem.h"
//Igor1: my comment
#include "query/Value.h"
#include "array/Metadata.h"
#include "network/BaseConnection.h"
#include "system/Exceptions.h"
#include "array/Array.h"
#include "array/MemChunk.h"
#include "array/StreamArray.h"
#include "array/ParallelAccumulatorArray.h"
#include "Python.h"
#include "../include/SciDBAPI.h"
#include "../src/array/Metadata.cpp"

#if PY_MAJOR_VERSION == 2 && PY_MINOR_VERSION == 4
#ifndef PyInt_FromSize_t
#define PyInt_FromSize_t(x) PyInt_FromLong((long)x)
#endif
#endif

using namespace scidb;
%}

/*
* let stdint.i, inttypes.i, swigarch.i know that on a 64-bit machine
* we want to allow a 64bit wide interface to be built, and not to
*/
#define SWIG_FILE_WITH_INIT
#define SCIDB_CLIENT
#if defined(SWIGWORDSIZE32) || defined(SWIGWORDSIZE64)
# /* good */
#else
# error This file needs SWIGWORDSIZE32 or -64 set so that stdint.i will adapt correctly
#endif
%include "stdint.i"  // try <> here?

%include "typemaps.i"
%include "std_string.i"
%include "std_vector.i"
%include "boost_shared_ptr.i"

%include "exception.i"
%exception {

try {
$action
}
catch (scidb::SystemException& e) {
PyErr_SetString(PyExc_IndexError,e.what()); SWIG_fail;
}
catch (scidb::UserQueryException& e) {
PyErr_SetString(PyExc_IndexError,e.what()); SWIG_fail;
}
catch (scidb::UserException& e) {
PyErr_SetString(PyExc_IndexError,e.what()); SWIG_fail;
}
catch (std::exception& e) {
PyErr_SetString(PyExc_IndexError,"STD Exception"); SWIG_fail;
}
catch (...) {
PyErr_SetString(PyExc_IndexError,"Unknown Exception"); SWIG_fail;
}

}

namespace scidb {
%rename(increment_to_next) ConstIterator::operator++;
%rename(increment_to_next) ConstChunkIterator::operator++;
%rename(increment_to_next) ConstArrayIterator::operator++;

}

%shared_ptr(scidb::ConstIterator);
%shared_ptr(scidb::ConstChunkIterator);
%shared_ptr(scidb::ChunkIterator);
%shared_ptr(scidb::MemChunkIterator);
%shared_ptr(scidb::ConstItemIterator);
%shared_ptr(scidb::SparseChunkIterator);
%shared_ptr(scidb::ArrayIterator);
%shared_ptr(scidb::ConstArrayIterator);
%shared_ptr(scidb::StreamArrayIterator);
%shared_ptr(scidb::Array);
%shared_ptr(scidb::StreamArray);
%shared_ptr(scidb::AccumulatorArray);
%shared_ptr(scidb::ParallelAccumulatorArray);
%shared_ptr(scidb::MultiStreamArray);
%shared_ptr(scidb::MergeStreamArray);
%shared_ptr(scidb::ClientArray);
%shared_ptr(scidb::BaseChunkIterator);
%shared_ptr(scidb::BaseTileChunkIterator);
%shared_ptr(scidb::RLEConstChunkIterator);
%shared_ptr(scidb::RLETileConstChunkIterator);
%shared_ptr(scidb::RLEBitmapChunkIterator);
%shared_ptr(scidb::RLEChunkIterator);

using namespace std;
using namespace boost;
// SWIG does not understand the GCC function attribute syntax, so disable this macro temporarily
#pragma push_macro("SCIDB_FORCEINLINE")
#undef SCIDB_FORCEINLINE
#define SCIDB_FORCEINLINE 
%include "query/Value.h"
%include "query/TypeSystem.h"
#undef SCIDB_FORCEINLINE
#pragma pop_macro("SCIDB_FORCEINLINE")

%include "array/Metadata.h"
%include "network/BaseConnection.h"
%include "array/Array.h"
%include "array/MemChunk.h"
%include "array/StreamArray.h"
%apply std::string *INPUT {std::string& connectionString };
%apply std::string *INPUT {std::string& queryString };
%include "../../include/SciDBAPI.h"
%clear std::string& connectionString;
%clear std::string& queryString;

typedef std::string TypeId;
typedef scidb::Value Value;
typedef uint64_t QueryID;
typedef int64_t Coordinate;
typedef std::vector<Coordinate> Coordinates;
typedef uint32_t AttributeID;
typedef uint64_t ArrayID;
typedef std::vector<AttributeDesc> Attributes;
typedef std::vector<DimensionDesc> Dimensions;

namespace std {
%template(dimensiondesc) vector<scidb::DimensionDesc,std::allocator< scidb::DimensionDesc > >;
%template(attributedesc) vector<scidb::AttributeDesc,std::allocator< scidb::AttributeDesc > >;
%template(constarrayiterator) vector<boost::shared_ptr<scidb::ConstArrayIterator> > ;
%template(constchunkiterator) vector<boost::shared_ptr<scidb::ConstChunkIterator> > ;
%template(coordinates) std::vector<scidb::Coordinate>;
%template(attributetypes) vector<TypeId>;
}

