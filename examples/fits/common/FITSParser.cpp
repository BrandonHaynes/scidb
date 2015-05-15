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
#include <cerrno>

#include "system/Exceptions.h"

#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/helpers/exception.h"

#include "FITSParser.h"

/*
 * References:
 *
 * [FITS3.0] "Definition of the Flexible Image Transport System (FITS), version 3.0",
 *           W.D.Pence, L.Chiappetti, C.G.Page, R.A.Shaw, and E.Stobie
 *           A&A 524, A42 (2010), DOI: 10.1051/0004-6361/201015362, ESO 2010.
 */

namespace scidb
{
using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.impl_fits_input"));

FITSParser::FITSParser(string const& filePath)
    : filePath(filePath)
{
    file.open(filePath.c_str());
    if (file.fail()) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CANT_OPEN_FILE)
            << filePath << ::strerror(errno) << errno;
    }
    pbuffer = file.rdbuf();
}

FITSParser::~FITSParser()
{
    file.close();
}

/**
 * Scan through FITS file to get the number of HDUs. This method is rather
 * slow since it needs to scan most of the file, only jumping over data
 * areas.
 */
int FITSParser::getNumberOfHDUs()
{
    int hdu = 0;
    while (true) {
        try {
            string error;
            moveToHDU(hdu, error);
        } catch (Exception& e) {
            break;
        }
        ++hdu;
    }
    return hdu;
}

/**
 * Parse FITS file from the beginning until the desired HDU is reached.
 * Reads relevant header variables and stores them in class members.
 *
 * Refer to sections 4.4.1.1 and 4.4.1.2 of [FITS3.0].
 */
bool FITSParser::moveToHDU(uint32_t hdu, string& error)
{
    pbuffer->pubseekoff(0, ios_base::beg);
    bufferPos = 0;

    if (pbuffer->sgetn(buffer, kBlockSize) != kBlockSize) {
        throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
    }

    // Check whether FITS file reportedly follows the standard
    if (!readFixedLogicalKeyword("SIMPLE")) {
        error = "File does not follow the standard";
        return false;
    }

    // Parse HDUs until we reach the requested HDU
    for (uint32_t i = 0; i <= hdu; i++) {
        int totalaxissize = 1;

        // If not PRIMARY HDU, expect XTENSION keyword
        if (i > 0) {
            bool undefined;
            readFreeStringKeyword("XTENSION", xtension, undefined);
            if (undefined) {
                error = "XTENSION keyword is undefined";
                return false;
            }
        }

        // Parse BITPIX keyword
        bitpix = readFixedIntegerKeyword("BITPIX");
        bitpixsize = abs(bitpix) / 8;

        // Parse NAXIS keyword
        naxis = readFixedIntegerKeyword("NAXIS");
        if (naxis < 0 || naxis > 999) {
            error = "NAXIS must be between 0 and 999";
            return false;
        }

        // Parse NAXIS1, ..., NAXISn keywords
        axissize.resize(naxis);
        if (naxis > 0) {
            for (int j = 0; j < naxis; j++) {
                stringstream ss;
                ss << "NAXIS" << (j + 1);

                int size = readFixedIntegerKeyword(ss.str());

                axissize[naxis - (j + 1)] = size;
                totalaxissize *= size;
            }
        }

        // Default values
        scale = false;
        bscale = 1.0;
        bzero = 0.0;
        pcount = 0;
        gcount = 1;

        // Parse remaining keywords until END keyword
        string key = "";
        while ((key = readKeyword()) != "END") {
            if (key == "BSCALE") {
                bscale = readFreeFloatingValue();
                scale = true;
            } else if (key == "BZERO") {
                bzero = readFreeFloatingValue();
                scale = true;
            } else if (key == "PCOUNT") {
                pcount = readFreeIntegerValue();
            } else if (key == "GCOUNT") {
                gcount = readFreeIntegerValue();
            } else {
                readAndIgnoreValue();
            }

            // Read more blocks as needed
            if (bufferPos == kBlockSize) {
                if (pbuffer->sgetn(buffer, kBlockSize) != kBlockSize) {
                    throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
                }
                bufferPos = 0;
            }
        }

        // My understanding of the spec: if BSCALE and BZERO have their default values
        // then do not scale, since scaling could indirectly convert int to float or double.
        if (bscale == 1.0 && bzero == 0.0) {
            scale = false;
        }

        // Haven't reached desired HDU yet
        if (i != hdu) {

            // If there is data, calculate its size and jump over
            if (naxis > 0) {
                int datasize = (bitpixsize * gcount * (pcount + totalaxissize));
                if ((datasize % kBlockSize) != 0) {
                    datasize = datasize - (datasize % kBlockSize) + kBlockSize;
                }
                pbuffer->pubseekoff(datasize, ios_base::cur);
            }

            // Read next block
            int nread = pbuffer->sgetn(buffer, kBlockSize);
            if (nread == 0) {
                LOG4CXX_ERROR(logger, "HDU does not exist");
                throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
            } else if (nread < kBlockSize) {
                throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
            }
            bufferPos = 0;
        }
    }

    // We are now in the correct HDU

    // Store start of data position
    dataPos = file.tellg();

    return validateHDU(hdu, error);
}

/**
 * Check if the current HDU is an image with content.
 */
bool FITSParser::validateHDU(uint32_t hdu, string& error)
{
    if (naxis == 0) {
        error = "HDU is empty";
        return false;
    }
    if (hdu > 0 && xtension != "IMAGE   ") {
        error = "XTENSION must be IMAGE (found " + xtension + ")";
        return false;
    }
    if (pcount != 0) {
        error = "PCOUNT must have value 0";
        return false;
    }
    if (gcount != 1) {
        error = "GCOUNT must have value 1";
        return false;
    }

    // If valid HDU with image content, set bitpix type
    switch (bitpix) {
        case 16:
            if (!scale) {
                bitpixtype = INT16;
            } else {
                bitpixtype = INT16_SCALED;
            }
            break;
        case 32:
            if (!scale) {
                bitpixtype = INT32;
            } else {
                bitpixtype = INT32_SCALED;
            }
            break;
        case -32:
            bitpixtype = FLOAT32_SCALED;
            break;
        default:
            error = "Unsupported BITPIX value";
            return false;
    }

    return true;
}

int FITSParser::getBitPix() const
{
    return bitpix;
}

int FITSParser::getBitPixType() const
{
    return bitpixtype;
}

const vector<int>& FITSParser::getAxisSizes() const
{
    return axissize;
}

float FITSParser::getBZero() const
{
    return bzero;
}

float FITSParser::getBScale() const
{
    return bscale;
}

void FITSParser::moveToCell(int cell)
{
    pbuffer->pubseekoff(dataPos + cell * bitpixsize, ios_base::beg);
}

/**
 * Read big endian int16 from file.
 */
short int FITSParser::readInt16()
{
    unsigned short ubitpix;

    if (pbuffer->sgetn((char *) &ubitpix, 2) != 2) {
        throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
    }
    return (ubitpix >> 8) | (ubitpix << 8);
}

/**
 * Read big endian int32 from file.
 */
int FITSParser::readInt32()
{
    int bitpix;

    if (pbuffer->sgetn((char *) &bitpix, 4) != 4) {
        throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
    }

    // Swap bytes
    bitpix = (bitpix >> 24) |
             ((bitpix << 8) & 0x00FF0000) |
             ((bitpix >> 8) & 0x0000FF00) |
             (bitpix << 24);
    return bitpix;
}

/**
 * Read big endian float32 from file.
 */
float FITSParser::readFloat32()
{
    int bitpix = readInt32();
    return *((float *) &bitpix);
}

/**
 * Return key in the header's key/value pair.
 */
string FITSParser::readKeyword()
{
    char key[8];
    int i;

    for (i = 0; i < 8; i++) {
        char ch = buffer[bufferPos + i];
        if (ch == ' ' || ch == '=') {
            key[i] = '\0';
            break;
        }
        key[i] = ch;
    }

    bufferPos += 8;

    return key;
}

/**
 * Jump over a value in the set header's key/value pairs.
 */
void FITSParser::readAndIgnoreValue()
{
    bufferPos += 72;
}

/**
 * Check if the header key/value pair contains the given key.
 */
bool FITSParser::hasKey(string const& key)
{
    size_t size = key.length();
    if (strncmp(buffer + bufferPos, key.c_str(), size) != 0) {
        return false;
    }
    bufferPos += size;

    while (size < 8) {
        if (buffer[bufferPos] != ' ') {
            return false;
        }
        ++size;
        ++bufferPos;
    }

    return true;
}

/**
 * Refer to Section 4.2.1 of [FITS3.0].
 */
void FITSParser::readFreeStringKeyword(string const& key, string &value, bool &undefined)
{
    bool prevQuote = false;
    stringstream vstr;
    int i;

    if (!hasKey(key)) {
        LOG4CXX_ERROR(logger, key + " keyword missing");
        throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
    }

    bufferPos += 2;

    for (i = 0; i < 70; i++, bufferPos++) {
        char ch = buffer[bufferPos];

        if (ch == ' ') {
            continue;
        } else if (ch == '\'') {
            break;
        } else {
            LOG4CXX_ERROR(logger, "Unexpected character in character string");
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
        }
    }

    if (i == 70) {
        undefined = true;
        return;
    }

    for (; i < 70; i++, bufferPos++) {
        char ch = buffer[bufferPos];

        if (ch == '\'') {
            if (prevQuote) {
                vstr << "'";
                prevQuote = false;
            } else {
                prevQuote = true;
            }
        } else if (ch == ' ' && prevQuote) {
            break;
        } else if (ch >= 32 && ch <= 126) {
            vstr << ch;
            prevQuote = false;
        } else {
            LOG4CXX_ERROR(logger, "Unexpected character in character string");
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
        }
    }

    bufferPos += 70 - i;

    if (!prevQuote) {
        LOG4CXX_ERROR(logger, "Missing ' in character string");
        throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
    }
    undefined = false;

    value = vstr.str();
}

/**
 * Refer to Section 4.2.2 of [FITS3.0].
 */
bool FITSParser::readFixedLogicalKeyword(string const& key)
{
    bool v;

    if (!hasKey(key)) {
        LOG4CXX_ERROR(logger, key + " keyword missing");
        throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
    }

    bufferPos += 21;
    switch (buffer[bufferPos]) {
        case 'T':
            v = true;
            break;
        case 'F':
            v = false;
            break;
        default:
            LOG4CXX_ERROR(logger, "Fixed-format logical does not contain either T or F");
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
    }
    bufferPos += 51;

    return v;
}

/**
 * Refer to Section 4.2.3 of [FITS3.0] (Fixed-format).
 */
int FITSParser::readFixedIntegerKeyword(string const& key)
{
    int i = 0;
    int j = 1;
    int v = 0;

    if (!hasKey(key) != 0) {
        LOG4CXX_ERROR(logger, key + " keyword missing");
        throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
    }

    bufferPos += 21;
    for (i = 0; i < 20; i++, bufferPos--) {
        char c = buffer[bufferPos];

        if (c >= '0' && c <= '9') {
            v += j * (c - '0');
            j *= 10;
        } else if (c == '+') {
            break;
        } else if (c == '-') {
            v = -v;
            break;
        } else {
            break;
        }
    }
    bufferPos += i + 51;

    return v;
}

/**
 * Refer to Section 4.2.3 of [FITS3.0] (Free-format).
 *
 * NOTE: This is almost as slow as readFreeFloatingValue() below.
 * Refer to that comment for more info.
 */
int FITSParser::readFreeIntegerValue()
{
    stringstream vstr;
    int v;
    int i;

    bufferPos += 2;

    for (i = 0; i < 69 && buffer[bufferPos] == ' '; i++, bufferPos++)
        ;

    switch (buffer[bufferPos]) {
        case '-':
            vstr << '-';
            ++bufferPos;
            break;
        case '+':
            vstr << '+';
            ++bufferPos;
            break;
    }

    for (; i < 70; i++, bufferPos++) {
        char ch = buffer[bufferPos];

        if (ch >= '0' && ch <= '9') {
            vstr << ch;
        } else if (ch == ' ') {
            break;
        } else {
            LOG4CXX_ERROR(logger, "Unexpected character in integer");
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
        }
    }

    errno = 0;
    v = atoi(vstr.str().c_str());
    if (errno != 0) {
        LOG4CXX_ERROR(logger, "Error occurred during conversion to integer");
        throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
    }

    bufferPos += 70 - i;

    return v;
}

/**
 * Refer to Section 4.2.4 of [FITS3.0].
 *
 * NOTE: This implementation is *absolutely not* efficient but it is safe.
 * It simply composes a string and then uses strtod() to do the conversion.
 * It does check that the string is well formed according to [FITS3.0].
 * Even though this is not efficient, it is not terribly important because
 * we're only parsing headers at this point.
 */
float FITSParser::readFreeFloatingValue()
{
    enum { INTEGER, FRACTION, EXPONENT } mode = INTEGER;
    bool integerNull = true;
    bool fractionNull = true;
    bool exponentNull = true;
    stringstream vstr;
    float v;
    int i;

    bufferPos += 2;

    for (i = 0; i < 69 && buffer[bufferPos] == ' '; i++, bufferPos++)
        ;

    if (buffer[bufferPos] == '-') {
        vstr << '-';
        ++bufferPos;
    }

    for (; i < 70; i++, bufferPos++) {
        char ch = buffer[bufferPos];

        if (ch >= '0' && ch <= '9') {
            if (mode == INTEGER) {
                integerNull = false;
            } else if (mode == FRACTION) {
                fractionNull = false;
            } else {
                exponentNull = false;
            }
            vstr << ch;

        } else if (ch == '.') {

            if (mode != INTEGER) {
                LOG4CXX_ERROR(logger, "Fraction can only appear after integer in floating-point");
                throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
            }
            mode = FRACTION;
            vstr << '.';

        } else if (ch == 'E' || ch == 'D') {

            if (mode != FRACTION) {
                LOG4CXX_ERROR(logger, "Exponent can only appear after fraction in floating-point");
                throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
            }
            mode = EXPONENT;
            vstr << 'e';

        } else if (ch == '+' || ch == '-') {

            if (mode != EXPONENT || !exponentNull) {
                LOG4CXX_ERROR(logger, "Unexpected character in floating-point");
                throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
            }
            if (ch == '-') {
                vstr << '-';
            }

        } else if (ch == ' ') {
            break;
        } else {
            LOG4CXX_ERROR(logger, "Unexpected character in floating-point");
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
        }
    }

    if (integerNull) {
        LOG4CXX_ERROR(logger, "Missing integer part in floating-point");
        throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
    }

    if (mode == FRACTION && fractionNull) {
        LOG4CXX_ERROR(logger, "Missing fraction part in floating-point");
        throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
    }

    if (mode == EXPONENT && exponentNull) {
        LOG4CXX_ERROR(logger, "Missing exponent part in floating-point");
        throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
    }

    errno = 0;
    v = strtof(vstr.str().c_str(), NULL);
    if (errno != 0) {
        LOG4CXX_ERROR(logger, "Error occurred during conversion to floating-point");
        throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
    }

    bufferPos += 70 - i;

    return v;
}

}
