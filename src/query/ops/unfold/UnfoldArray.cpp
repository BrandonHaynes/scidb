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
 * UnfoldArray.cpp
 *
 *  Created on: 13 May 2014
 *      Author: Dave Gosselin
 */

#include "UnfoldArray.h"

namespace scidb {

  void copyCoordinates(Coordinates& dst, const Coordinates& src)
  {
    assert(dst.size() >= src.size());
    for (Coordinates::size_type i = 0; i < src.size(); ++i) {
      dst[i] = src[i];
    }
  }

  UnfoldArrayIter::UnfoldArrayIter(DelegateArray const& delegate,
				   AttributeID attrID,
				   const shared_ptr<Array>& inputArray)
    : DelegateArrayIterator(delegate, attrID,
			    boost::shared_ptr<ConstArrayIterator>()),
      _inputArrayIterators(0),
      _position(0)
  {
    assert(attrID == 0 || attrID == 1);

    const ArrayDesc& parrayDesc = inputArray->getArrayDesc();
    const AttributeDesc* pattrDesc = parrayDesc.getEmptyBitmapAttribute();

    // Set the inputIterator on the base since it was initialized to NULL
    // in the DelegateArrayIterator base constructor above.
    inputIterator = 
      inputArray->getConstIterator(attrID == 0 ? 0 : pattrDesc->getId());

    _position.resize(parrayDesc.getDimensions().size()+1);

    // There are only two attributes in the output array: the data 
    // attribute and the empty tag attribute.
    if (attrID == 0) {
      // The array resulting from this operator has only two attributes:  the
      // attribute containing the data chunks and the (hidden) attribute
      // containing the empty bitmap.
      // The output chunks for attribute 0 depend on the input chunks for
      // the data attributes.
      AttributeID nAttrs = parrayDesc.getAttributes(true).size();
      _inputArrayIterators.reserve(nAttrs);
      _inputArrayIterators.push_back(inputIterator);
      for (AttributeID i = 1; i < nAttrs; ++i) {
	_inputArrayIterators.push_back(inputArray->getConstIterator(i));
      }
    }
    else {  // attrID == 1, the empty bitmap attribute
      // The output chunk for attribute 1 depends on the input chunk
      // for the empty bitmap attribute but not on the input chunks of the
      // data attributes.
      _inputArrayIterators.push_back(inputIterator);
    }
  }

  UnfoldArrayIter::~UnfoldArrayIter()
  {
  }

  bool
  UnfoldArrayIter::end()
  {
    if (isDebug()) {
      // end() is true as soon as any one of the input
      // iterators has reached the end.
      for (std::vector<boost::shared_ptr<ConstArrayIterator> >::const_iterator citer =
	     _inputArrayIterators.begin();
	   citer != _inputArrayIterators.end();
	   ++citer) {
	if ((*citer)->end()) {
	  return true;
	}
      }
      return false;
    }
    else {
      return _inputArrayIterators.empty() ||
	_inputArrayIterators[0]->end();
    }
  }

  void
  UnfoldArrayIter::operator ++()
  {
    // Increment all of the attribute iterators in lock-step.
    for (std::vector<boost::shared_ptr<ConstArrayIterator> >::const_iterator citer =
	   _inputArrayIterators.begin();
	 citer != _inputArrayIterators.end();
	 ++citer) {
      ++(**citer);
    }
  }

  Coordinates const&
  UnfoldArrayIter::getPosition()
  {
    // The position from the first iterator is sufficient as
    // all iterators are kept at the same position during
    // setPosition and operator++.
    const Coordinates& pposition = _inputArrayIterators[0]->getPosition();
    assert(_position.size() == pposition.size()+1);
    copyCoordinates(_position, pposition);
    *(_position.end()-1) = 0;
    return _position;
  }

  bool
  UnfoldArrayIter::setPosition(Coordinates const& pos)
  {
    // Set the position on all of the input iterators at once.
    // Slice-off the last value of the coordinates vector because
    // that is not a valid coordinate for the input chunks.
    Coordinates mapped(pos.begin(), pos.end()-1);
    bool success = true;
  
    for (std::vector<boost::shared_ptr<ConstArrayIterator> >::const_iterator citer =
	   _inputArrayIterators.begin();
	 (citer != _inputArrayIterators.end()) && success;
	 ++citer) {
      success = success && (*citer)->setPosition(mapped);
    }
    return success;
  }

  void
  UnfoldArrayIter::reset()
  {
    // Reset all of the attribute iterators in lock-step.
    for (std::vector<boost::shared_ptr<ConstArrayIterator> >::const_iterator citer =
	   _inputArrayIterators.begin();
	 citer != _inputArrayIterators.end();
	 ++citer) {
      (*citer)->reset();
    }
  }

  DelegateChunkIterator*
  UnfoldArray::createChunkIterator(DelegateChunk const* chunk,
				   int iterationMode) const
  {
    const AttributeDesc& pattrDesc = chunk->getAttributeDesc();

    if (pattrDesc.isEmptyIndicator()) {
      // In the case of the empty bitmap attribute,
      // the output attribute is built from only one
      // input attribute.
      return new UnfoldBitmapChunkIter(chunk, iterationMode, pattrDesc.getId());
    }
    else {
      // Many input attributes are used to build-up the
      // one output data attribute.
      return new UnfoldChunkIter(chunk, iterationMode);
    }
  }

  DelegateArrayIterator*
  UnfoldArray::createArrayIterator(AttributeID id) const
  {
    // Return an iterator to this array.  This array will have only one 
    // data attribute.  The number of dimensions will be:  the number of 
    // dimensions in the input array plus 1.
    // This is a pipelined operator which means:  as the consumer pulls
    // on the data via the iterators we provide, we, in turn, need to
    // pull on the data provided by iterators given by operators below
    // us.
    return new UnfoldArrayIter(*this, id, inputArray);
  }

  DelegateChunk*
  UnfoldArray::createChunk(DelegateArrayIterator const* iterator,
			   AttributeID id) const
  {
    // An operator-specific specialization of DelegateChunk
    // is required as overrides of getFirstPosition and
    // getLastPosition are required since this operator
    // produces chunks whose dimensions are different than the
    // input chunks.
    return new UnfoldChunk(*this, *iterator, id, isClone);
  }

  UnfoldArray::UnfoldArray(ArrayDesc const& schema,
			   const shared_ptr<Array>& pinputArray,
			   const shared_ptr<Query>& pquery)
    : DelegateArray(schema, pinputArray)
  {
    Array::_query = pquery;
  }

  UnfoldArray::~UnfoldArray()
  {
  }

  UnfoldChunk::UnfoldChunk(DelegateArray const& array,
			   DelegateArrayIterator const& iterator,
			   AttributeID attrID,
			   bool isClone)
    : DelegateChunk(array, iterator, attrID, isClone),
      _firstPosition(0),
      _lastPosition(0),
      _unfoldedDimensionUpperBound(0)
  {
    assert(array.getArrayDesc().getDimensions().size() > 1);

    // Get a reference to the unfold array's output dimensions to
    // use throughout construction.
    const Dimensions& unfoldDims = array.getArrayDesc().getDimensions();

    // The first and last position have dimensions matching those set
    // during the "infer schema" step.
    _firstPosition.resize(unfoldDims.size());
    _lastPosition.resize(unfoldDims.size());

    // The maximum of the dimension added to hold the attributes is the
    // coordinate of the last position in that dimension.
    const DimensionDesc& addedDimension = *(unfoldDims.end()-1);
    _unfoldedDimensionUpperBound = addedDimension.getEndMax();
  }

  Coordinates const&
  UnfoldChunk::getFirstPosition(bool withOverlap) const
  {
    // Must evaluate the first position on every call because
    // the first position may change for sparse arrays.
    const Coordinates& pposition =
      iterator.getInputIterator()->getChunk().getFirstPosition(withOverlap);
    assert(_firstPosition.size() == pposition.size()+1);
    copyCoordinates(_firstPosition, pposition);
    *(_firstPosition.end()-1) = 0;
    return _firstPosition;
  }

  Coordinates const&
  UnfoldChunk::getLastPosition(bool withOverlap) const
  {
    // Must evaluate the last position on every call because
    // the last position may change for sparse arrays.
    const Coordinates& pposition =
      iterator.getInputIterator()->getChunk().getLastPosition(withOverlap);
    assert(_lastPosition.size() == pposition.size()+1);
    copyCoordinates(_lastPosition, pposition);
    *(_lastPosition.end()-1) = _unfoldedDimensionUpperBound;
    return _lastPosition;
  }

  UnfoldChunkIter::UnfoldChunkIter(const DelegateChunk* chunk,
				   int iterationMode)
    : DelegateChunkIterator(chunk, iterationMode),
      _inputChunkIterators(0),
      _visitingAttribute(0),
      _currentPosition(0)
  {
    // Get a chunk iterator from each of the input array's attributes
    // and store them all internally.  The UnfoldChunkIter will walk
    // the input chunks to build-up its resulting output chunk.
    const DelegateArrayIterator& parrayIter = chunk->getArrayIterator();
    const UnfoldArrayIter& paacIter =
      dynamic_cast<const UnfoldArrayIter&>(parrayIter);
    size_t nInputIters = paacIter._inputArrayIterators.size();
    _inputChunkIterators.reserve(nInputIters);
    _inputChunkIterators.push_back(inputIterator);
    for (size_t i = 1; i < nInputIters; ++i) {
      _inputChunkIterators.push_back(paacIter._inputArrayIterators[i]->getChunk().
				     getConstIterator(iterationMode & ~INTENDED_TILE_MODE));
    }
    _currentPosition.resize(chunk->getArrayDesc().getDimensions().size());
  }

  UnfoldChunkIter::~UnfoldChunkIter()
  {
  }

  Value&
  UnfoldChunkIter::getItem()
  {
    return _inputChunkIterators[_visitingAttribute]->getItem();
  }

  bool
  UnfoldChunkIter::isEmpty()
  {
    return _inputChunkIterators[_visitingAttribute]->isEmpty();
  }

  bool
  UnfoldChunkIter::end()
  {
    if (isDebug()) {
      // end() is true when when any one of the
      // input chunks is at its end.
      for (std::vector<boost::shared_ptr<ConstChunkIterator> >::const_iterator citer =
	     _inputChunkIterators.begin();
	   citer != _inputChunkIterators.end();
	   ++citer) {
	if ((*citer)->end()) {
	  return true;
	}
      }
      return false;
    }
    else {
      return _inputChunkIterators.empty() ||
	_inputChunkIterators[0]->end();
    }
  }

  void
  UnfoldChunkIter::operator ++()
  {
    // Once we've visited all of the attributes, reset the
    // _visitingAttribute to zero and go to the next chunk
    // along all of the input arrays.  Think of it like going
    // to the next line on a typewriter.
    ++_visitingAttribute;
    if (_visitingAttribute >= _inputChunkIterators.size()) {
      for (std::vector<boost::shared_ptr<ConstChunkIterator> >::const_iterator citer =
	     _inputChunkIterators.begin();
	   citer != _inputChunkIterators.end();
	   ++citer) {
	++(**citer);
      }
      _visitingAttribute = 0;
    }
  }

  Coordinates const&
  UnfoldChunkIter::getPosition()
  {
    const Coordinates& pposition =
      _inputChunkIterators[_visitingAttribute]->getPosition();
    assert(_currentPosition.size() == pposition.size()+1);
    copyCoordinates(_currentPosition, pposition);
    *(_currentPosition.end()-1) = _visitingAttribute;
    return _currentPosition;
  }

  bool
  UnfoldChunkIter::setPosition(Coordinates const& pos)
  {
    // 'pos' will have N coordinates, according to the ArrayDesc returned
    // from inferSchema.  Set the position along the input chunks
    // according to the first N-1 coordinates.  The last coordinate is
    // the attribute index which must be set independently.
    Coordinates mapped(pos.begin(), pos.end()-1);
    bool success = true;
    for (std::vector<boost::shared_ptr<ConstChunkIterator> >::const_iterator citer =
	   _inputChunkIterators.begin();
	 (citer != _inputChunkIterators.end()) && success;
	 ++citer) {
      success = success && (*citer)->setPosition(mapped);
    }

    AttributeID visitingAttr = *(pos.end()-1);
    if (success && visitingAttr < _inputChunkIterators.size()) {
        _visitingAttribute = visitingAttr;
        return true;
    }
    _visitingAttribute = 0;
    return false;
  }

  void
  UnfoldChunkIter::reset()
  {
    // Reset each of the input chunk iterators and reset the
    // _visitingAttribute back to zero (initial state).
    for (std::vector<boost::shared_ptr<ConstChunkIterator> >::const_iterator citer =
	   _inputChunkIterators.begin();
	 citer != _inputChunkIterators.end();
	 ++citer) {
      (*citer)->reset();
    }
    _visitingAttribute = 0;
  }

  UnfoldBitmapChunkIter::UnfoldBitmapChunkIter(const DelegateChunk* chunk,
					 int iterationMode,
					 AttributeID attrId)
    : DelegateChunkIterator(chunk, iterationMode),
      _value(TypeLibrary::getType(TID_BOOL)),
      _nAttrs(chunk->getDelegateArray().getInputArray()->getArrayDesc().getAttributes(true).size()),
      _visitingAttribute(0),
      _currentPosition(0)
  {
    _currentPosition.resize(chunk->getArrayDesc().getDimensions().size());
  }

  UnfoldBitmapChunkIter::~UnfoldBitmapChunkIter()
  {
  }

  Value&
  UnfoldBitmapChunkIter::getItem()
  {
    _value.setBool(inputIterator->getItem().getBool());
    return _value;
  }

  void
  UnfoldBitmapChunkIter::operator ++()
  {
    // Walking the empty bitmap attribute is different
    // than walking a data attribute.  I have to generate
    // N output bits for every 1 bit present in the bitmap
    // (where N is the number of data attributes in the
    // input array).
    ++_visitingAttribute;
    if (_visitingAttribute >= _nAttrs) {
      ++(*inputIterator);
      _visitingAttribute = 0;
    }
  }

  Coordinates const&
  UnfoldBitmapChunkIter::getPosition()
  {
    const Coordinates& pposition = inputIterator->getPosition();
    assert(_currentPosition.size() == pposition.size()+1);
    copyCoordinates(_currentPosition, pposition);
    *(_currentPosition.end()-1) = _visitingAttribute;
    return _currentPosition;
  }

  bool
  UnfoldBitmapChunkIter::setPosition(Coordinates const& pos)
  {
    // 'pos' will have N coordinates, according to the ArrayDesc returned
    // from inferSchema.  Set the position along the input chunks
    // according to the first N-1 coordinates.  The last coordinate is
    // the attribute index which must be set independently.
    Coordinates mapped(pos.begin(), pos.end()-1);
    AttributeID visitingAttr = *(pos.end()-1);
    if (visitingAttr < _nAttrs && 
        inputIterator->setPosition(mapped)) {
        _visitingAttribute = visitingAttr;
        return true;
    }
    _visitingAttribute = 0;
    return false;
  }

  void
  UnfoldBitmapChunkIter::reset()
  {
    inputIterator->reset();
    _visitingAttribute = 0;
  }

}
