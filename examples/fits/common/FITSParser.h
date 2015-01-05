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
#ifndef FITS_PARSER_H
#define FITS_PARSER_H

#include <fstream>


namespace scidb
{
using namespace std;

class FITSParser
{
public:
    enum BitPixType {
        INT16,
        INT16_SCALED,
        INT32,
        INT32_SCALED,
        FLOAT32_SCALED
    };

    FITSParser(string const& filePath);
    ~FITSParser();

    int                 getNumberOfHDUs();

    bool                moveToHDU(uint32_t hdu, string& error);
    int                 getBitPix() const;
    int                 getBitPixType() const;
    const vector<int>&  getAxisSizes() const;
    float               getBZero() const;
    float               getBScale() const;

    void                moveToCell(int cell);
    short int           readInt16();
    int                 readInt32();
    float               readFloat32();

private:
    bool                validateHDU(uint32_t hdu, string& error);

    string              readKeyword();
    void                readAndIgnoreValue();
    bool                hasKey(string const& key);

    bool                readFixedLogicalKeyword(string const& key);
    int                 readFixedIntegerKeyword(string const& key);
    void                readFreeStringKeyword(string const& key, string &value, bool &undefined);
    float               readFreeFloatingValue();
    int                 readFreeIntegerValue();

    static const int    kBlockSize = 2880;

    string              filePath;
    uint32_t            hdu;

    char                buffer[kBlockSize];
    ifstream            file;
    filebuf             *pbuffer;
    int                 bufferPos;              // Current position in buffer
    int                 dataPos;                // Position in file where the data part of the HDU begins

    int                 bitpix;
    int                 bitpixsize;             // bitpix converted to bytes
    BitPixType          bitpixtype;
    int                 naxis;
    vector<int>         axissize;
    bool                scale;                  // Set to true only if bscale/bzero are present
    float               bscale;
    float               bzero;
    int                 pcount;
    int                 gcount;
    string              xtension;
};
    
}

#endif
