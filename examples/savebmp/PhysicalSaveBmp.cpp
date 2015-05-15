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
 * PhysicalSaveBmp.cpp
 *
 *  Created on: 8/15/12
 *      Author: poliocough@gmail.com
 */

#include <query/Operator.h>
#include <array/Metadata.h>

using namespace std;
using namespace boost;

namespace scidb {

/**
 * A simple bitmap image that can be populated with pixels and then saved to a BMP file.
 */
class SimpleImage
{
private:
    size_t _nRows;
    size_t _nCols;
    size_t _numCells;
    uint32_t* _imageData;

    inline size_t rcToPos(size_t const row, size_t const col)
    {
        assert(row<_nRows && col<_nCols);
        return (row) * _nCols + col;
    }

public:
    /**
     * Create a new transparent image with specified number of rows and columns. The memory is allocated at this phase and the
     * image is initialized to fully transparent.
     * @param numRows the number of rows in the image (i.e. 768)
     * @param numColumns the number of columns in the image (i.e. 1024)
     */
    SimpleImage(size_t numRows, size_t numColumns):
        _nRows(numRows),
        _nCols(numColumns),
        _numCells(numRows * numColumns)
    {
        assert(_nRows > 0 && _nCols > 0);
        _imageData = new uint32_t[_numCells];
        memset(_imageData, 0, _numCells * sizeof(uint32_t));
    }

    ~SimpleImage()
    {
        delete[] _imageData;
    }

    /**
     * Set a pixel value to a desired color.
     * @param row the row of the pixel; must be between 0 and numRows-1 inclusive; row 0 is the bottom most row of the image
     * @param col the column of the pixel; must be between 0 and numColumns-1 inclusive; column 0 is the left most column of the image
     * @param blue the blue color component
     * @param green the green color component
     * @param red the red color component
     */
    inline void setPixel(size_t const row, size_t const col, uint8_t const blue, uint8_t const green, uint8_t const red)
    {
        size_t pos = rcToPos(row,col);
        uint8_t* cell = (uint8_t*)  &(_imageData[pos]);
        *cell = blue;
        cell++;
        *cell = green;
        cell++;
        *cell = red;
        cell++;
        *cell = 255;
    }

    /**
     * Save the image to the specified file in bmp format. Caller is responsible for properly opening the file before the call
     * and closing the file afterwards.
     * @param file the file handle; must be already open for writing and positioned at the start of file
     * @return the total size of the file that was written, or 0 if there was an error
     */
    size_t saveToBmp(FILE* file)
    {
        //Here we write out the bmp file header. This is taken from the example at http://en.wikipedia.org/wiki/BMP_file_format
        uint32_t const headerSize = 122;

        uint32_t const dataSize = _numCells*sizeof(uint32_t);
        uint8_t* dataSizePtr = (uint8_t*) &dataSize;
        uint32_t const totalSize = headerSize + dataSize;
        uint8_t* totalSizePtr = (uint8_t*) &totalSize;
        uint32_t const nRows = _nRows;
        uint8_t* nRowsPtr = (uint8_t*) &nRows;
        uint32_t const nCols = _nCols;
        uint8_t* nColsPtr = (uint8_t*) &nCols;
        uint8_t header[headerSize];
        memset(&header[0], 0, headerSize);

        header[0]='B';
        header[1]='M';

        header[2] = totalSizePtr[0];
        header[3] = totalSizePtr[1];
        header[4] = totalSizePtr[2];
        header[5] = totalSizePtr[3];
        header[10]=122;
        header[14]=108;
        header[18]=nColsPtr[0];
        header[19]=nColsPtr[1];
        header[20]=nColsPtr[2];
        header[21]=nColsPtr[3];
        header[22]=nRowsPtr[0];
        header[23]=nRowsPtr[1];
        header[24]=nRowsPtr[2];
        header[25]=nRowsPtr[3];
        header[26]=1;
        header[28]=32;
        header[30]=3;
        header[34] = dataSizePtr[0];
        header[35] = dataSizePtr[1];
        header[36] = dataSizePtr[2];
        header[37] = dataSizePtr[3];
        header[38]=19;
        header[39]=11;
        header[42]=19;
        header[43]=11;
        header[56]=255;
        header[59]=255;
        header[62]=255;
        header[69]=255;
        header[70]=0x20;
        header[71]=0x6E;
        header[72]=0x69;
        header[73]=0x57;

        if( fwrite(&header, headerSize, 1, file) != 1)
            return 0;

        if( fwrite(&_imageData[0], _numCells*sizeof(uint32_t), 1, file) != 1 )
            return 0;

        return totalSize;
    }
};


/**
 * Physical savebmp operator.
 */
class PhysicalSaveBmp: public PhysicalOperator
{
  public:
    PhysicalSaveBmp(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    /**
     * Require that the input array is located entirely on instance 0.
     */
    virtual DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const
    {
        vector<ArrayDistribution> requiredDistribution(1);
        requiredDistribution[0] = ArrayDistribution(psLocalInstance, boost::shared_ptr<DistributionMapper>(), 0);
        return DistributionRequirement(DistributionRequirement::SpecificAnyOrder, requiredDistribution);
    }

    /**
     * Run.
     */
    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        if (query->getInstanceID() != 0)
        {
            //I am not instance 0 - I don't need to do anything. Return an empty array.
            return shared_ptr<Array>(new MemArray(_schema,query));
        }

        //I am instance 0, let's save the array to a bmp image.
        string filepath = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getString();

        ArrayDesc const& inputSchema = inputArrays[0]->getArrayDesc();

        size_t nRows = inputSchema.getDimensions()[0].getLength();
        size_t rowStart = inputSchema.getDimensions()[0].getStartMin();

        size_t nCols = inputSchema.getDimensions()[1].getLength();
        size_t colStart = inputSchema.getDimensions()[1].getStartMin();

        SimpleImage image(nRows, nCols);

        vector<shared_ptr<ConstArrayIterator> > aiters(3);
        aiters[0] = inputArrays[0]->getConstIterator(0);
        aiters[1] = inputArrays[0]->getConstIterator(1);
        aiters[2] = inputArrays[0]->getConstIterator(2);

        int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS | ConstChunkIterator::IGNORE_EMPTY_CELLS;
        vector<shared_ptr<ConstChunkIterator> > citers(3);
        while(!aiters[0]->end())
        {
            citers[0] = aiters[0]->getChunk().getConstIterator(iterationMode);
            citers[1] = aiters[1]->getChunk().getConstIterator(iterationMode);
            citers[2] = aiters[2]->getChunk().getConstIterator(iterationMode);

            while(!citers[0]->end())
            {
                Coordinates const& pos = citers[0]->getPosition();
                size_t row = pos[0] - rowStart;
                size_t col = pos[1] - colStart;

                Value const& valueRed = citers[0]->getItem();
                Value const& valueGreen = citers[1]->getItem();
                Value const& valueBlue = citers[2]->getItem();

                if (valueRed.isNull() || valueGreen.isNull() || valueBlue.isNull())
                {
                    image.setPixel(row,col,0,0,0);
                }
                else
                {
                    uint8_t red = valueRed.getUint8();
                    uint8_t green = valueGreen.getUint8();
                    uint8_t blue = valueBlue.getUint8();
                    image.setPixel(row,col,blue,green,red);
                }

                ++(*citers[0]);
                ++(*citers[1]);
                ++(*citers[2]);
            }

            ++(*aiters[0]);
            ++(*aiters[1]);
            ++(*aiters[2]);
        }

        FILE* f = fopen(filepath.c_str(), "wb");
        if(!f)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "savebmp can't open the target file!";
        }

        size_t fileSize = image.saveToBmp(f);
        if(fileSize == 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "savebmp can't write the target file!";
        }
        fclose(f);

        shared_ptr<Array> dstArray(new MemArray(_schema,query));
        Coordinates outPos(1);
        outPos[0]=0;
        Value outValue;

        shared_ptr<ArrayIterator> daiter = dstArray->getIterator(0);
        Chunk& outChunk = daiter->newChunk(outPos);
        shared_ptr<ChunkIterator> dciter = outChunk.getIterator(query);
        dciter->setPosition(outPos);
        outValue.setString("File Saved Successfully");
        dciter->writeItem(outValue);
        dciter->flush();

        shared_ptr<ArrayIterator>daiter2 = dstArray->getIterator(1);
        Chunk& outChunk2 = daiter2->newChunk(outPos);
        shared_ptr<ChunkIterator>dciter2 = outChunk2.getIterator(query);
        dciter2->setPosition(outPos);
        outValue.setDouble(fileSize * 1.0 / MiB);
        dciter2->writeItem(outValue);
        dciter2->flush();

        return dstArray;
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalSaveBmp, "savebmp", "physicalSaveBmp");

}  // namespace scidb
