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
 *  Q2Cook.cpp
 *  scidb_overlap
 *
 *  Created by Philippe Cudre-Mauroux on 12/14/09.
 *
 */



#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/foreach.hpp>

#include "../include/Q2Cook.h"
#include "../include/Polygonizer.h"

#include "IntraChunkIterator.h"
#include "Catalog.h"



namespace Storage {


	Q2Cook::Q2Cook(ArrayHandle handleIN, int32_t threshold) 
	{
		// initializations
		_handleIN = handleIN;
		_threshold = threshold;
		
		totalObjects = 0;
		
		// create the input / output structures
		createInput();
		createOutput();
		
		// cook specific
		_nextOid = 0;
		
		_maxEdges = 0;
	}
	
	Q2Cook::~Q2Cook() 
	{
	}
	
	
	void Q2Cook::createInput()
	{
		// initialize input data structures
		Array::Pointer inarray =  Catalog::getInstance().getArray(_handleIN);
		Array* parray = dynamic_cast<Array*>(boost::get_pointer(inarray));
		
		_chunksIN = parray->getReadChunkIterator(false); // no need to buffer the input
		
		
		// filter the chunks
		std::vector<CellPosition> chunksToRetrieve;
		chunksToRetrieve.push_back(0);
		chunksToRetrieve.push_back(1);
		chunksToRetrieve.push_back(2);
		chunksToRetrieve.push_back(3);
		
		//chunksToRetrieve.push_back(15);
		//chunksToRetrieve.push_back(16);
		//chunksToRetrieve.push_back(17);
		//chunksToRetrieve.push_back(18);
		
		//chunksToRetrieve.push_back(30);
		//chunksToRetrieve.push_back(31);
		//chunksToRetrieve.push_back(32);
		//chunksToRetrieve.push_back(33);
		
		//chunksToRetrieve.push_back(45);
		//chunksToRetrieve.push_back(46);
		//chunksToRetrieve.push_back(47);
		//chunksToRetrieve.push_back(48);
		
		//_chunksIN->setChunksToRetrieve(chunksToRetrieve);
		
		_descriptorIN = parray->getDescriptor();
	}
	
	
	void Q2Cook::createOutput()
	{
		// initialize output data sturctures, create output array
		_descriptorOUT1.arrayType = "FIXED_LENGTH";
		
		// attributes are the same as those for the input array
		_descriptorOUT1.attributes = _descriptorIN.attributes;
		_descriptorOUT1.dimensions = _descriptorIN.dimensions;
		
		// create the array and get a new array handle
		Array::Pointer inarray =  Catalog::getInstance().getArray(_handleIN);
		std::string pixSumArrayName = inarray->getName();
		pixSumArrayName.append(".pixsum");
		
		try{
			Catalog::getInstance().destroyArray(pixSumArrayName);
		} catch(SciDB::Exception& e) {}
		Catalog::getInstance().createArray(pixSumArrayName, _descriptorOUT1);
		_handleOUT1 = Catalog::getInstance().getArray(pixSumArrayName)->getHandle();
		Array::Pointer outarray =  Catalog::getInstance().getArray(_handleOUT1);
		Array* parray = dynamic_cast<Array*>(boost::get_pointer(outarray));
		
		// create the chunk iterator
		_chunksOUT1 = parray->getWriteChunkIterator();
		
		
		/////// second out array
		_descriptorOUT2.arrayType = "FIXED_LENGTH";
		
		// attributes are the same as those for the input array
		_descriptorOUT2.attributes = _descriptorIN.attributes;
		
		// add a dimension
		Dimension newDimension(0, 80, 80, "poly");
		_descriptorOUT2.dimensions.push_back(newDimension);
		_descriptorOUT2.dimensions.push_back(_descriptorIN.dimensions[0]);
		_descriptorOUT2.dimensions.push_back(_descriptorIN.dimensions[1]);
		
		
		// create the array and get a new array handle
		std::string polyArrayName = inarray->getName();
		polyArrayName.append(".poly");
		
		try{
			Catalog::getInstance().destroyArray(polyArrayName);
		} catch(const SciDB::Exception& e) {}
		
/*
		Array::Pointer outarray2 = Catalog::getInstance().createNewCompressedArray(polyArrayName, _descriptorOUT2, "LempelZiv");
		Array* parray2 = dynamic_cast<Array*>(boost::get_pointer(outarray));
		_handleOUT2 = parray2->getHandle();
 */
///*		
		Catalog::getInstance().createArray(polyArrayName, _descriptorOUT2);
		_handleOUT2 = Catalog::getInstance().getArray(polyArrayName)->getHandle();
		Array::Pointer outarray2 =  Catalog::getInstance().getArray(_handleOUT2);
		Array* parray2 = dynamic_cast<Array*>(boost::get_pointer(outarray2));
//*/		
		// create the chunk iterator
		_chunksOUT2 = parray2->getWriteChunkIterator();
	}
	
	
	int32_t Q2Cook::apply(int nbWorkers)
	{
		boost::thread* threadArray = NULL;
		threadArray = new boost::thread[nbWorkers];
		workers = new Worker[nbWorkers];
		for(int i=0; i<nbWorkers; i++)
		{
			_workerNumber = i;
			threadArray[i] = boost::thread(boost::bind(&Storage::Q2Cook::processChunks, this, i));
		}
		for(int i=0; i<nbWorkers; i++)
			threadArray[i].join();
		
		delete[] threadArray;
		delete[] workers;
		threadArray = NULL;
		
		return 0;
	}
	
	
	void Q2Cook::processChunks(int workerNumber) 
	{	
		int nbObjects = 0;
		int nbIterations = 0;
		workers[workerNumber]._current = new Oid[2000];
		workers[workerNumber]._previous = new Oid[2000];
		
		ChunkDescriptor::Pointer descriptorIN;
		while((descriptorIN = _chunksIN->getChunkAndIterate())->nbCells != -1)
		{
//int aboveTh = 0;
			nbIterations++;
			
			// read each chunk using a intra chunk iterator
			IntraChunkIterator iterIN(descriptorIN);
			
			// get the corresponding output chunk
			ChunkDescriptor::Pointer descriptorOUT = _chunksOUT1->getChunk(descriptorIN->dChunkPosition);
			workers[workerNumber]._iterOUT1 = IntraChunkIterator(descriptorOUT);
			
			
///*			
			std::vector<CellPosition> pos2;
			pos2.push_back(0);
			pos2.push_back(descriptorIN->dChunkPosition[0]);
			pos2.push_back(descriptorIN->dChunkPosition[1]);
			//std::cout << descriptorIN->dChunkPosition[0] << " /// " << descriptorIN->dChunkPosition[1]<< std::endl;
			//pos2.push_back(0);
			//pos2.push_back(0);
			ChunkDescriptor::Pointer descriptorOUT2 = _chunksOUT2->getChunk(pos2);
			workers[workerNumber]._iterOUT2 = IntraChunkIterator(descriptorOUT2);
//*/			
			
			// set the out chunk to zero
			memset(descriptorOUT->beginningOfChunk, 0, descriptorOUT->chunkSize);
//			memset(descriptorOUT2->beginningOfChunk, 0, descriptorOUT2->chunkSize);
			
			//iterIN.moveToCore();
			
			workers[workerNumber]._width = descriptorIN->dEndOfChunk[0] - descriptorIN->dStartOfChunk[0] + 1;
 
			memset(workers[workerNumber]._current, 0, workers[workerNumber]._width*sizeof(Oid));
			memset(workers[workerNumber]._previous, 0, workers[workerNumber]._width*sizeof(Oid));
			
			workers[workerNumber]._minX = descriptorIN->dStartOfChunk[0];
			workers[workerNumber]._maxX = descriptorIN->dEndOfChunk[0];

			workers[workerNumber]._currentY = descriptorIN->dStartOfChunk[1];
			workers[workerNumber]._minY = descriptorIN->dStartOfChunk[1];
			workers[workerNumber]._maxY = descriptorIN->dEndOfChunk[1];
			
			while(workers[workerNumber]._currentY <= workers[workerNumber]._maxY)
			{
				CellPosition x = 0;
				while(x < workers[workerNumber]._width)
				{
					// ingest
					int32_t pixVal = iterIN.getInt32();
					//std::cout << "pixval : " << pixVal << std::endl;					
					if(pixVal >= _threshold) {
						//aboveTh++;
						Oid objid;
						Oid left = leftNeighbor(workerNumber, x);
						if(left) {
							Oid up = upNeighbor(workerNumber, x);
							if(up) {
								if (left != up) { // Need to merge upper and left objects
									if (left < up) {
										objid = left;
										mergeObjects(workerNumber, left, up);
									} else {
										objid = up;
										mergeObjects(workerNumber, up, left);
									}
								} else { // don't need to merge
									objid = left;
								}
							} else {
								objid = left;
							}
						} else { // no left object
							Oid up = upNeighbor(workerNumber, x); // optimize this out?
							if(up) {
								mergeUpper(workerNumber, x); // may merge upper-left and upper-right objs
								objid = up;
							} else {
								objid = newObjId();
							}
						}
						workers[workerNumber]._current[x] = objid;
						addPixel(workerNumber, objid, x+workers[workerNumber]._minX, workers[workerNumber]._currentY, pixVal);
					}
					
					iterIN.nextCell();
					x++;
				}

				nextLine(workerNumber);
				workers[workerNumber]._currentY++;
//std::cout << "obsmap : " << workers[workerNumber]._obs.size() << std::endl;

//iterIN.reset();
//iterIN.setPosition(0, workers[workerNumber]._currentY);
				
			}
			
			// flush all objects 
			nbObjects += workers[workerNumber]._obs.size();
//			std::cout << "Current Iteration: " << nbIterations << " Current Objects: " << workers[workerNumber]._obs.size() << std::endl;
//			std::cout << "Above th: " << aboveTh << std::endl;
			flushObjects(workerNumber);
		}
		delete [] workers[workerNumber]._current;
		delete [] workers[workerNumber]._previous;
		
		std::cout << "Iterations: " << nbIterations << " Objects: " << nbObjects << " _maxEdges: " << _maxEdges << std::endl;
		totalObjects += nbObjects;
	}
	
	
	Oid Q2Cook::leftNeighbor(int workerNumber, CellPosition x)  {
		if(x > 0)
			return workers[workerNumber]._current[x-1];
		return 0;
	}

	
	Oid Q2Cook::upNeighbor(int workerNumber, CellPosition x) {
		// Check:
		// A B C <-- previous
		// D ?  <-- current 
		// ... need to check A, B, and C.  Will return smallest oid if 
		// multiple possibilities (e.g., A=4, C=5).  Note that oid(C) <
		// oid(A) is possible too. 
		
		if(workers[workerNumber]._currentY > workers[workerNumber]._minY) {
			Oid current = workers[workerNumber]._previous[x];
			
			if((x > 0) && (workers[workerNumber]._previous[x-1]) 
			   && ((workers[workerNumber]._previous[x-1] < current) || !current)) 
				current = workers[workerNumber]._previous[x-1];
			
			if((x+1 <= workers[workerNumber]._maxX ) && (workers[workerNumber]._previous[x+1]) 
			   && ((workers[workerNumber]._previous[x+1] < current) || !current)) 
				current = workers[workerNumber]._previous[x+1];
			
			return current;
		}
		return 0;
	}
	
	
	Oid Q2Cook::newObjId() {
		// synchronize?
		return _nextOid++;
	}
	
	
	void Q2Cook::addPixel(int workerNumber, Oid obsid, CellPosition x, CellPosition y, int32_t pixVal)
	{
		//updateFinalizable(workerNumber, obsid);
		// Get obj
		workers[workerNumber]._obs[obsid].pixels.push_back(PixVal(x,y, pixVal));
	}
	
	
	void Q2Cook::updateFinalizable(int workerNumber, Oid o)
	{
		//_fresh[o] = 1;
		//_finalizable.erase(o);
	}
	
	
	void Q2Cook::mergeUpper(int workerNumber, CellPosition x) 
	{
		// Merge upper pixel objects into a single object.  This only
		// happens when:
		// A 0 B -- upper
		// 0 q   -- lower
		// where A, B belong to different objects (currently), and q is
		// qualifying pixel.  (Idx x) refers to the x coordinate of q.
		// 
		// Correct behavior is simple: Find oids of A and B, and merge them 
		// to the dominant object, which we'll define as the one with the
		// lower (earlier) oid.
		
		// check whether we're on bounds:
		if((x <= workers[workerNumber]._minX) || (x >= workers[workerNumber]._maxX)) {
			return; // Since we're on a boundary-- won't have to do anything.
		}
		Oid leftoid = workers[workerNumber]._previous[x-1];
		Oid rightoid = workers[workerNumber]._previous[x+1];
		if (!leftoid || !rightoid) {
			return; // A or B do not belong to objects.
		}
		if (leftoid < rightoid) {
			mergeObjects(workerNumber, leftoid, rightoid);
		} else if(rightoid < leftoid) {
			mergeObjects(workerNumber, rightoid, leftoid);
		}
	}
	
	
	void Q2Cook::mergeObjects(int workerNumber, Oid majoroid, Oid minoroid) 
	{
		// Put pixels of the minor oid object into the major oid object.
		// Then fix the two-line buffer so that the pixels belonging to the
		// minor object get patched with the major object's id.
		
		if(majoroid == minoroid) return; // nothing to do.
		
		Observ& minor = workers[workerNumber]._obs[minoroid];
		Observ& major = workers[workerNumber]._obs[majoroid];
		
		// Copy minor's pixels to major's.
		major.pixels.insert(major.pixels.end(), 
							minor.pixels.begin(), minor.pixels.end());
		
		// Patch the pixels in the minor that occur in our buffer (lines
		// indexed _currentY and _currentY-1).
		//for_each(minor.pixels.begin(), minor.pixels.end(), 
		//		 _obsSet(majoroid, 
		//				 std::pair<CellPosition, LineBufferIter> (_currentY, _current), 
		//				 std::pair<CellPosition, LineBufferIter> (_currentY-1, _previous)));
		
		// Remove the minor object.
		workers[workerNumber]._obs.erase(minoroid);		
	}
	
	
	void Q2Cook::nextLine(int workerNumber) 
	{
		// To save memory, we may want to "finalize" objects once we know
		// all their boundaries.  For now, don't do this.
		
		// Swap buffers and zero-out buffer.
		Oid* temp = workers[workerNumber]._previous;

		workers[workerNumber]._previous = workers[workerNumber]._current;
		workers[workerNumber]._current = temp;
		
		memset(workers[workerNumber]._current, 0, workers[workerNumber]._width);
	}

	
	void Q2Cook::flushObjects(int workerNumber) 
	{
		
		BOOST_FOREACH( ObsInMap& o, workers[workerNumber]._obs )
		{
			obsPostProc(workerNumber, o.second);
		}
		
		// write 
		
		// printout
		//printObjects(workerNumber);
		
		// erase
		workers[workerNumber]._obs.clear();
	}
	
	
	// compute bounding box, center and polygons
	void Q2Cook::obsPostProc(int workerNumber, Observ& o) 
	{
		// Compute props
		int32_t pixSum = 0;
		int64_t weightSumX = 0;
		int64_t weightSumY = 0;
		
		// todo: we should remove those!!
		if(o.pixels.size()==0)
		{
			std::cout << "obs without pixel" << std::endl;
			return;
		}
		
		for(PixVect::iterator i = o.pixels.begin(); i != o.pixels.end(); i++) 
		{
			pixSum += i->val;
			weightSumX += i->val * i->coord.first;
			weightSumY += i->val * i->coord.second;
		}
		
		o.centroidX = (float32_t)weightSumX / pixSum;
		o.centroidY = (float32_t)weightSumY / pixSum;
		o.pixelSum = pixSum;
		
//		std::cout << "x :" << o.centroidX << " y :" << o.centroidY << " pixsum :" << o.pixelSum << std::endl;
		
		// Compute polygon edges.
		computePoly(workerNumber, o.pixels, o.polyVertices);
		if(o.polyVertices.size() > _maxEdges)
			_maxEdges = o.polyVertices.size();
		
		// write the results
		//iterOUT.set<float32_t>
		
/*		
		if(o.centroidX == 1500)
		{
			for(PixVect::iterator i = o.pixels.begin(); i != o.pixels.end(); i++) {
				std::cout << "(" << i->coord.first << ", " << i->coord.second << ") ";
			}
			
			std::cout << std::endl << "Poly vertices: " ;
			for(CoordVect::iterator i = o.polyVertices.begin(); i != o.polyVertices.end(); i++) 
			{
				std::cout << "(" << i->first << ", " << i->second << ") ";
			}
			std::cout << std::endl;
			
			std::cout << "Centroid: " << o.centroidX << ", " << o.centroidY << std::endl << " pixelSum: " << o.pixelSum << std::endl;
		}
*/		
		workers[workerNumber]._iterOUT1.setPosition((int)o.centroidX, (int)o.centroidY);
		workers[workerNumber]._iterOUT1.setInt32(pixSum);
///*		
		std::vector<CellPosition> pos;
		pos.push_back(0);
		pos.push_back((int)o.centroidX);
		pos.push_back((int)o.centroidY);
		
		
		pos.push_back(0);
		pos.push_back(0);
		
		workers[workerNumber]._iterOUT2.setPosition(pos);
		
		for(int p=0; p<o.polyVertices.size(); p++)
		{
			workers[workerNumber]._iterOUT2.setInt32((int32_t)o.polyVertices[p].first);
			workers[workerNumber]._iterOUT2.nextCell();
			workers[workerNumber]._iterOUT2.setInt32((int32_t)o.polyVertices[p].second);
			if(p==38)
				break;
			workers[workerNumber]._iterOUT2.nextCell();
		}
//*/
	}
	
	
	void Q2Cook::computePoly(int workerNumber, PixVect const& p, CoordVect& vertices) 
	{
		Polygonizer<int> pol;
		// Adapt pixel values to a plain coordinate vector.
		std::vector<Coord> cv(p.size());
		int cursor=0;
		
		for(PixVect::const_iterator i = p.begin(); 
			i != p.end(); i++) {
			cv[cursor] = i->coord;
			cursor++;
		}
		pol.findPoly(cv, vertices);
	}
	
	
	void Q2Cook::printObjects(int workerNumber) 
	{
		std::cout << "************************************************" << std::endl;
		std::cout << "printing" << std::endl;

		BOOST_FOREACH( ObsInMap p, workers[workerNumber]._obs )
		{
			obsPrint(workerNumber, p);
		}
	}
	
	
	void Q2Cook::obsPrint (int workerNumber, ObsInMap const& p) 
	{
		Observ o = p.second;
		std::cout << "Observation " << p.first << " ";
		for(PixVect::iterator i = o.pixels.begin(); i != o.pixels.end(); i++) {
			std::cout << "(" << i->coord.first << ", " << i->coord.second << ") ";
		}
		
		std::cout << std::endl << "Poly vertices: " ;
		for(CoordVect::iterator i = o.polyVertices.begin(); i != o.polyVertices.end(); i++) 
		{
			std::cout << "(" << i->first << ", " << i->second << ") ";
		}
		std::cout << std::endl;
		
		std::cout << "Centroid: " << o.centroidX << ", " << o.centroidY << std::endl << " pixelSum: " << o.pixelSum << std::endl;
	}
	
	
	
/*	
	

	
// simply go to next line	
void ops::Q2Cook::_nextLine() {
	// To save memory, we may want to "finalize" objects once we know
	// all their boundaries.  For now, don't do this.
		
	// Swap buffers and zero-out buffer.
	LineBufferIter temp = _previous;
	if(temp == _lines.end()) temp = _current + _width; // previous line was
	// null.
	_flushFinalizable();
	_previous = _current;
	_current = temp;
	std::fill_n(temp, _width, 0);
}
	

void ops::Q2Cook::_ingest(Common::Cell const& c, Idx x, Idx y) {
	

}


int ops::Q2Cook::_newObjId() {
	return _nextOid++;
}



void ops::Q2Cook::_nextTime() {
    // In flight?
    if(_currentT >= 0) {
		// Flush the current Observations
		_finalizeTimeStep();  // Post process
		_writeOutTimeStep();  // Write out
    }
    // Increment and clear-out old structures.
    _obs.erase(_obs.begin(), _obs.end()); // Clear obs buffer
    std::fill(_lines.begin(), _lines.end(), 0); // Clear line buffer
    ++_timePos[1]; // advance position.
    _prepareWriteIncr();       
    assert(_timePos[1] == _currentT + 1);
    
}


// simply check if the pixel on the left is part of the same object
ops::Q2Cook::Oid ops::Q2Cook::_leftNeighbor(Idx x)  {
	if(x > 0)
		return _current[x-1];
	return 0;
}

ops::Q2Cook::Oid ops::Q2Cook::_upNeighbor(Idx x) {
	// Check:
	// A B C <-- previous
	// D ?  <-- current 
	// ... need to check A, B, and C.  Will return smallest oid if 
	// multiple possibilities (e.g., A=4, C=5).  Note that oid(C) <
	// oid(A) is possible too. 
	
	if(_currentY > 0) {
		int current = _previous[x];
		if((x > 0) && (_previous[x-1]) 
		   && ((_previous[x-1] < current) || !current)) 
			current = _previous[x-1];
		if((x+1 < _width) && (_previous[x+1]) 
		   && ((_previous[x+1] < current) || !current)) 
			current = _previous[x+1];
		
		return current;
	}
	return 0;
	
}

void ops::Q2Cook::_mergeUpper(Idx x) {
    // Merge upper pixel objects into a single object.  This only
    // happens when:
    // A 0 B -- upper
    // 0 q   -- lower
    // where A, B belong to different objects (currently), and q is
    // qualifying pixel.  (Idx x) refers to the x coordinate of q.
    // 
    // Correct behavior is simple: Find oids of A and B, and merge them 
    // to the dominant object, which we'll define as the one with the
    // lower (earlier) oid.
	
    // check whether we're on bounds:
    if((x < 1) || (x == _width)) {
		return; // Since we're on a boundary-- won't have to do anything.
    }
    int leftoid = _previous[x-1];
    int rightoid = _previous[x+1];
    if (!leftoid || !rightoid) {
		return; // A or B do not belong to objects.
    }
    if (leftoid < rightoid) {
		_mergeObjects(leftoid, rightoid);
    } else if(rightoid < leftoid) {
		_mergeObjects(rightoid, leftoid);
    }
	
}


void ops::Q2Cook::_mergeObjects(int majoroid, int minoroid) {
	// Put pixels of the minor oid object into the major oid object.
	// Then fix the two-line buffer so that the pixels belonging to the
	// minor object get patched with the major object's id.
	if(majoroid == minoroid) return; // nothing to do.
	
	Observ& minor = _obs[minoroid];
	Observ& major = _obs[majoroid];
	
	// Copy minor's pixels to major's.
	major.pixels.insert(major.pixels.end(), 
						minor.pixels.begin(), minor.pixels.end());
	
	// Patch the pixels in the minor that occur in our buffer (lines
	// indexed _currentY and _currentY-1).
	for_each(minor.pixels.begin(), minor.pixels.end(), 
			 _obsSet(majoroid, 
					 std::pair<Idx,LineBufferIter>(_currentY, _current), 
					 std::pair<Idx,LineBufferIter>(_currentY-1, _previous)));
	
	// Remove the minor object.
	_obs.erase(minoroid);
	// Obj scratch();
	// remove_copy_if(minor.second.begin(), minor.second.end(), 
	// 		 back_inserter(scratch),
	// 		 bind2nd(less<int>(),_currentY-1));
	// We could do this without the scratch vector.  Optimize this
	// later.
	
}

void ops::Q2Cook::_addPixel(ops::Q2Cook::Oid obsid, Idx x, Idx y, ops::Q2Cook::Val pixval) {
    _updateFinalizable(obsid);
    // Get obj
    _obs[obsid].pixels.push_back(PixVal(x,y, pixval));
    //std::cout << obsid << " adding " << x << "," << y << std::endl;
}



void ops::Q2Cook::_writeOutTimeStep() {
    // For each object, write to the output array.
    int obsCount = _obs.size();
    int writeCount = 0;
    // std::cerr << "outarray size is " 
    // 	      << _outputArray->getDescriptor().getArraySize() 
    // 	      << std::endl;
    //assert(_outputArray->getDescriptor().getArraySize() >= (unsigned)obsCount);
    // Should assert that this dimension's size is big enough.
    Common::ArrayIterator::Pointer ip = _outputArray->getIterator();
    ip->setPosition(_timePos);
    //printPosition(_outputArray->getDescriptor(), _timePos);
	
    boost::shared_ptr<StorageContext> sc(StorageContext::get(_outputArray));
    for_each(_obs.begin(), _obs.end(), _obsWrite(writeCount, ip, *sc));
	
    if(writeCount != obsCount) {
		throw std::logic_error("Bug! Didn't write all observations.");
    }
    
}

void ops::Q2Cook::_prepareWriteIncr() {
    _writeCount = 0;
    Common::ArrayIterator::Pointer ip = _outputArray->getIterator();
    _sContext = StorageContext::get(_outputArray);
    _obsWriteHelperP.reset(new _obsWrite(_writeCount, ip, *_sContext));
	
    ip->setPosition(_timePos);
}

void ops::Q2Cook::_computePoly(const ops::PixVect& p, 
								 ops::CoordVect& vertices) {
    Polygonizer<int> pol;
    // Adapt pixel values to a plain coordinate vector.
    std::vector<Coord> cv(p.size());
    int cursor=0;
	
    for(PixVect::const_iterator i = p.begin(); 
		i != p.end(); i++) {
		cv[cursor] = i->coord;
		cursor++;
    }
    pol.findPoly(cv, vertices);
}


void ops::Q2Cook::_finalizeTimeStep() {
    for_each(_obs.begin(), _obs.end(), _obsPostProc());
}

void ops::Q2Cook::_updateFinalizable(Oid o) {
    _fresh[o] = 1;
    _finalizable.erase(o);
}

void ops::Q2Cook::_flushFinalizable() {
    // Apply postprocessing.
    _obsPostProc opp;
    for_each(_finalizable.begin(), _finalizable.end(), 
			 _applyMap<_obsPostProc>(_obs, opp));
    // Write out.
    for_each(_finalizable.begin(), _finalizable.end(), 
			 _applyMap<_obsWrite>(_obs, *_obsWriteHelperP));
    // Stop tracking
    for(OidIter i = _finalizable.begin(); i != _finalizable.end(); ++i) {
		_obs.erase(i->first);
    }
    // Reset Oid list.
    _finalizable = _fresh;
    _fresh.clear();
}


void ops::Q2Cook::_printObjects() {
    
    std::cout << "printing" << std::endl;
    for_each(_obs.begin(), _obs.end(), _obsPrint());
    std::cout << std::endl;
}
	

*/ 
 
}
