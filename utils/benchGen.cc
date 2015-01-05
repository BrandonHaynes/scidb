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
 * XLDB Science Benchmark Data Generator
 * version 1.4, 2010-04-15
 *
 * Author: Kian-Tat Lim, SLAC National Accelerator Laboratory
 *
 * This code is in the public domain.
 *
 * This code should be accompanied by an "tileData" file.  The information in
 * tileData is based on observations obtained with MegaPrime/MegaCam, a joint
 * project of CFHT and CEA/DAPNIA, at the Canada-France-Hawaii Telescope (CFHT)
 * which is operated by the National Research Council (NRC) of Canada, the
 * Institut National des Science de l'Univers of the Centre National de la
 * Recherche Scientifique (CNRS) of France, and the University of Hawaii. This
 * work is based in part on data products produced at TERAPIX and the Canadian
 * Astronomy Data Centre as part of the Canada-France-Hawaii Telescope Legacy
 * Survey, a collaborative project of NRC and CNRS.
 */

/* Change log:
 * v1.0: initial implementation
 * v1.1: fix bug causing crash, reduce spatial variation for small datasets,
 *       make many attributes random Gaussians instead of booleans, pull pixel
 *       generator into a separate function, generate CSV in chunks, add
 *       filename to metadata, enable per-attribute output.
 * v1.2: reduce spatial variation even more for small datasets, ensure tileData
 *       exists, add very-small configuration, add -p option to generate
 *       positions only.
 * v1.3: double raw datasets to ensure grouping, speed up random number
 *       generation, rename configurations, add round-robin generation, add
 *       ability to use any tileData, prefix tileData with sizes.
 * v1.4: changed csv format for SciDB v1. added minute mode.
 */

#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <stdint.h> // Needed on some platforms.
#include <unistd.h>

/*---------------------------------------------------------------------------*/

// Utility functions

inline int lcg(int x) {
    return (1103515245 * x + 12345) & 0x3fffffff;
}

double benchRand(int t) {
    // Compute a pseudo-random number between 0 and 1 that is always the same
    // for a given value of t.
    for (int i = 0; i < 10; ++i) {
        t = lcg(t);
    }
    return t / double(0x40000000);
}

double benchRandNorm(int t) {
    // Compute a pseudo-random pseudo-normally-distributed number with mean 0
    // and standard deviation 1 that is always the same for a given value of t.
    double norm = 0.0;
    for (int i = 0; i < 12; ++i) {
        norm += benchRand(t * 12 + i);
    }
    return norm - 6.0;
}

std::pair<int, int> fudge(int llcX, int llcY, int t) {
    // Fudge the coordinates to depend on time, giving an apparent motion to
    // the pixels/objects.
    llcX += (t % 3) - 1;
    llcY += (t / 3 % 3) - 1;
    if (llcX < 0) {
        llcX = 0;
    }
    if (llcY < 0) {
        llcY = 0;
    }
    return std::pair<int, int>(llcX, llcY);
}

int fudgePix(int pix, int t) {
    // Fudge the pixel values to depend on time, giving some variation to the
    // pixels/objects.
    pix += ((t % 5) - 2) * 10;
    if (pix < 0) {
        pix = 0;
    }
    if (pix > 65535) {
        pix = 65535;
    }
    return pix;
}

/*---------------------------------------------------------------------------*/

struct Pixel {
    // Structure containing attributes for a pixel.
    int pix;
    int var;
    int valid;
    int sat;
    int v0;
    int v1;
    int v2;
    int v3;
    int v4;
    int v5;
    int v6;
};

/*---------------------------------------------------------------------------*/

class Tiles {
    // Class used to generate benchmark data.  Contains image tiles.

public:
    enum Mode { CSV, BINARY, ATTRIBUTE, POS_ONLY };

    Tiles(std::string const& inputFile);
    ~Tiles(void);
    void genData(std::string const& fileBase, int n, int i, bool roundRobin,
                 int total, int side, int range, Mode outputMode);
private:
    std::pair<int, int> _llc(int size, int range, int t, int total);
    Pixel const& _pixelGen(int x, int y, int t);
    void _extract(std::ofstream& md, int size, int range, int t, int total,
                  std::string const& fileName, Mode outputMode, bool &firstImg);

    uint16_t* _tiles;
    Pixel _p;

    static int const OUTPUT_BUFFER_SIZE = 10 * (1 << 20); // 10 MB
    char* _outputBuffer;

    // Input tile sizes
    int _xSize;
    int _ySize;

    // Tiles are repeated at these intervals (larger than their size).
    // Spaces between tiles are filled with zeros.
    int _xRepeat;
    int _yRepeat;

    // Variation in locations for normal data set.
    static int const WORLD_VARIATION = 100000;

    static int const WORLD_SIZE = 1000000;

    // The sequence of tiles to use, starting in the lower left corner and
    // going across in X and then up in Y.  Taken from the first 101 digits of
    // pi.
    static char const TILE_SEQ[];
    static int const TILE_SEQ_LEN;
};

Tiles::Tiles(std::string const& inputFile) {
    std::ifstream f(inputFile.c_str());
    if (!f.is_open()) {
        throw std::runtime_error("Unable to open tileData. Is it readable?");
    }
    f.read(reinterpret_cast<char*>(&_xSize), sizeof(_xSize));
    f.read(reinterpret_cast<char*>(&_ySize), sizeof(_ySize));
    f.read(reinterpret_cast<char*>(&_xRepeat), sizeof(_xRepeat));
    f.read(reinterpret_cast<char*>(&_yRepeat), sizeof(_yRepeat));

    _tiles = new uint16_t[10 * _xSize * _ySize];
    _outputBuffer = new char[OUTPUT_BUFFER_SIZE];

    f.read(reinterpret_cast<char*>(_tiles),
           10 * _xSize * _ySize * sizeof(uint16_t));
    f.close();
}

Tiles::~Tiles(void) {
    delete[] _tiles;
    delete[] _outputBuffer;
}

void Tiles::genData(std::string const& fileBase, int n, int i, bool roundRobin,
                    int total, int side, int range, Mode outputMode) {
    std::ofstream md((fileBase + ".pos").c_str());
    md << "[";
    bool firstImg = true;
    if (roundRobin) {
        for (int t = 0; t < total; ++t) {
            if (t % n != i) continue;
            std::stringstream fname;
            fname << fileBase << '_' << std::setfill('0') << std::setw(4) << t;
            _extract(md, side, range, t, total, fname.str(), outputMode, firstImg);
        }
    }
    else {
        int const low = total * i / n;
        int const high = total * (i + 1) / n;
        for (int t = low; t < high; ++t) {
            std::stringstream fname;
            fname << fileBase << '_' << std::setfill('0') << std::setw(4) << t;
            _extract(md, side, range, t, total, fname.str(), outputMode, firstImg);
        }
    }
    md << "]";
    md.close();
}

std::pair<int, int> Tiles::_llc(int size, int range, int t, int total) {
    // Pick the coordinates of the lower left corner of the array.
    int x;
    int y;
    if (t >= total / 2) {
        t -= total / 2;
    }
    if (benchRand(t * 3 + 0) < 0.8) {
        x = (WORLD_SIZE - range) / 2 + benchRand(t * 3 + 1) * (range - size);
        y = (WORLD_SIZE - range) / 2 + benchRand(t * 3 + 2) * (range - size);
    }
    else {
        x = 0 + benchRand(t * 3 + 1) * (WORLD_SIZE - size);
        y = 0 + benchRand(t * 3 + 2) * (WORLD_SIZE - size);
    }
    return std::pair<int, int>(x, y);
}

Pixel const& Tiles::_pixelGen(int x, int y, int t) {
    int tileSeq = x / _xRepeat + (y / _yRepeat) * (WORLD_SIZE / _xRepeat + 1);
    int tileNum = TILE_SEQ[tileSeq % TILE_SEQ_LEN] - '0';
    int tileX = x % _xRepeat;
    int tileY = y % _yRepeat;
    if (tileX >= _xSize || tileY >= _ySize) {
        _p.valid = 0;
        _p.pix = 0;
    }
    else {
        _p.valid = 1;
        _p.pix = _tiles[tileNum * _xSize * _ySize + tileY * _xSize + tileX];
    }

    // Mask indicating the pixel is saturated.
    _p.sat = (_p.pix == 65535) ? 1 : 0;

    // Fudge the pixel value.
    _p.pix = fudgePix(_p.pix, t);

    // Approximate the variance by the sqrt of the pixel value.
    _p.var = static_cast<int>(sqrt(static_cast<double>(_p.pix)));

    // Simulate a background subtraction and rescaling.
    _p.v0 = static_cast<int>((_p.pix - 3000) * 65535.0 / (65535 - 3000));
    if (_p.v0 < 0) {
        _p.v0 = 0;
    }

    // Add some random outputs.
    int seed = (t * 3141 + x * 592 + y) % 65359;
    _p.v1 = 0          + 65535   * benchRandNorm(seed * 6 + 1);
    _p.v2 = 32768      + 32768   * benchRandNorm(seed * 6 + 2);
    _p.v3 = -500000000 + 1000000 * benchRandNorm(seed * 6 + 3);
    _p.v4 = 0          + 10      * benchRandNorm(seed * 6 + 4);
    _p.v5 = 1000       + 100     * benchRandNorm(seed * 6 + 5);
    _p.v6 = 1          + 0.5     * benchRandNorm(seed * 6 + 6);

    return _p;
}

void Tiles::_extract(std::ofstream& md, int size, int range, int t, int total,
                     std::string const& fileName, Mode outputMode, bool &firstImg) {
    // Extract a pixel array of given size with the given range of variations
    // in position at the specified time into a file with the given
    // fileName (or multiple files with the filename as a base), writing its
    // position to the given metadata file.  Output CSV, BINARY, ATTRIBUTE,
    // or POS_ONLY according to the outputMode.

    // Pick the nominal lower left corner.
    std::pair<int, int> l = _llc(size, range, t, total);
    int llcX = l.first;
    int llcY = l.second;

    // Save the lower left corner and the filename to the metadata file.
    // md << t << ',' << llcX << ',' << llcY << ',' << fileName << std::endl;
    if (firstImg) {
        firstImg = false;
    } else {
        md << ',';
    }
    md << '(' << llcX << ',' << llcY << ')';
    if (outputMode == POS_ONLY) return;

    // Fudge the nominal lower left corner into the one we will actually
    // use.
    l = fudge(llcX, llcY, t);
    llcX = l.first;
    llcY = l.second;

    if (outputMode == BINARY) {
        std::ofstream out(fileName.c_str());
        out.rdbuf()->pubsetbuf(_outputBuffer, OUTPUT_BUFFER_SIZE);
        for (int y = llcY; y < llcY + size; ++y) {
            for (int x = llcX; x < llcX + size; ++x) {
                Pixel const& p = _pixelGen(x, y, t);

                out.write(reinterpret_cast<char const*>(&p.pix),
                          sizeof(p.pix));
                out.write(reinterpret_cast<char const*>(&p.var),
                          sizeof(p.var));
                out.write(reinterpret_cast<char const*>(&p.valid),
                          sizeof(p.valid));
                out.write(reinterpret_cast<char const*>(&p.sat),
                          sizeof(p.sat));
                out.write(reinterpret_cast<char const*>(&p.v0), sizeof(p.v0));
                out.write(reinterpret_cast<char const*>(&p.v1), sizeof(p.v1));
                out.write(reinterpret_cast<char const*>(&p.v2), sizeof(p.v2));
                out.write(reinterpret_cast<char const*>(&p.v3), sizeof(p.v3));
                out.write(reinterpret_cast<char const*>(&p.v4), sizeof(p.v4));
                out.write(reinterpret_cast<char const*>(&p.v5), sizeof(p.v5));
                out.write(reinterpret_cast<char const*>(&p.v6), sizeof(p.v6));
            }
        }
        out.close();
    }
    else if (outputMode == CSV) {
        std::ofstream out(fileName.c_str());
        out.rdbuf()->pubsetbuf(_outputBuffer, OUTPUT_BUFFER_SIZE);

        static int const ROWBUF_SIZE = 14 * (11 + 1) + 1;
            // 14 numbers, each up to 10 digits plus sign plus separator,
            // and one for terminating null.
        char rowbuf[14 * (11 + 1) + 1];
        out << '[';

        // static int const CHUNK_SIZE = 100; // so far no chunking in CSV
        bool firstY = true;
        for (int yOffset = 0; yOffset < size; ++yOffset) {
            if (firstY) {
                firstY = false;
            } else {
                out << ',';
            }
            out << '[';
            bool firstX = true;
            for (int xOffset = 0; xOffset < size; ++xOffset) {
                const char *temp;
                if (firstX) {
                    firstX = false;
                    temp = "(%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)";
                } else {
                    temp = ",(%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)";
                }
                int x = xOffset + llcX;
                int y = yOffset + llcY;

                Pixel const& p = _pixelGen(x, y, t);

                // Write the output.
                int written = ::snprintf(
                    rowbuf, ROWBUF_SIZE,
                    // "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
                    temp,
                    /*chunk, xOffset, yOffset,*/ p.pix, p.var, p.valid,
                    p.sat, p.v0, p.v1, p.v2, p.v3, p.v4, p.v5, p.v6);
                if (written <= 0) {
                    throw std::runtime_error("Failed to snprintf");
                }
                out.write(rowbuf, written);
            } // for xOffset
            out << "]\n";
        } // for yOffset
        out << ']';
        out.close();
    }
    else if (outputMode == ATTRIBUTE) {
        int const NUM_ATTRIBUTES = 11;
        char const* attributeNames[NUM_ATTRIBUTES] = {
            "pix", "var", "valid", "sat",
            "v0", "v1", "v2", "v3", "v4", "v5", "v6"
        };
        std::ofstream* outs[NUM_ATTRIBUTES];
        for (int i = 0; i < NUM_ATTRIBUTES; ++i) {
            std::string attrFileName = fileName + "_" + attributeNames[i];
            outs[i] = new std::ofstream(attrFileName.c_str());
        }
        for (int y = llcY; y < llcY + size; ++y) {
            for (int x = llcX; x < llcX + size; ++x) {
                Pixel const& p = _pixelGen(x, y, t);

                outs[0]->write(reinterpret_cast<char const*>(&p.pix),
                               sizeof(p.pix));
                outs[1]->write(reinterpret_cast<char const*>(&p.var),
                               sizeof(p.var));
                outs[2]->write(reinterpret_cast<char const*>(&p.valid),
                               sizeof(p.valid));
                outs[3]->write(reinterpret_cast<char const*>(&p.sat),
                               sizeof(p.sat));
                outs[4]->write(reinterpret_cast<char const*>(&p.v0),
                               sizeof(p.v0));
                outs[5]->write(reinterpret_cast<char const*>(&p.v1),
                               sizeof(p.v1));
                outs[6]->write(reinterpret_cast<char const*>(&p.v2),
                               sizeof(p.v2));
                outs[7]->write(reinterpret_cast<char const*>(&p.v3),
                               sizeof(p.v3));
                outs[8]->write(reinterpret_cast<char const*>(&p.v4),
                               sizeof(p.v4));
                outs[9]->write(reinterpret_cast<char const*>(&p.v5),
                               sizeof(p.v5));
                outs[10]->write(reinterpret_cast<char const*>(&p.v6),
                                sizeof(p.v6));
            }
        }
        for (int i = 0; i < NUM_ATTRIBUTES; ++i) {
            outs[i]->close();
            delete outs[i];
        }
    }
    else { // outputMode
        throw std::runtime_error("Invalid outputMode");
    }
}

char const Tiles::TILE_SEQ[] = "31415926535897932384626433832795028841971693993751058209749445923078164062862089986280348253421170679";
int const Tiles::TILE_SEQ_LEN = sizeof(Tiles::TILE_SEQ) / sizeof(char) - 1;

/*---------------------------------------------------------------------------*/

void usage(char* argv0) {
    std::cerr << "Usage: " << argv0 <<
        " [-f BASE] [-n N] [-i I] [-r] [-c CONFIG] [-t] [-p] [-h] TILEDATA" << std::endl;
    std::cerr << "\t-f BASE: use BASE as filename base (default=bench)" << std::endl;
    std::cerr << "\t-n N: divide output image set into N pieces" << std::endl;
    std::cerr << "\t-i I: produce only I'th piece of N (0-based)" << std::endl;
    std::cerr << "\t-r: (with -i and -n) produce pieces in round robin" << std::endl;
    std::cerr << "\t-c CONFIG: minute, tiny (default), small, normal, large, very-large" << std::endl;
    std::cerr << "\t-t: produce text (CSV) output (default=binary)" << std::endl;
    std::cerr << "\t-p: produce image positions (bench.pos) only" << std::endl;
    std::cerr << "\t-h: produce this message" << std::endl;
    std::exit(1);
}

int main(int argc, char** argv) {
    std::string fbase = "bench";
    int denom = 1;
    int index = 0;
    std::string config = "tiny";
    Tiles::Mode outputMode = Tiles::BINARY;
    bool roundRobin = false;
    int ch;
    while ((ch = ::getopt(argc, argv, "f:n:i:c:tahpr")) != -1) {
        switch (ch) {
        case 'f':
            fbase = std::string(::optarg);
            break;
        case 'n':
            denom = std::atoi(::optarg);
            break;
        case 'i':
            index = std::atoi(::optarg);
            break;
        case 'c':
            config = std::string(::optarg);
            break;
        case 't':
            outputMode = Tiles::CSV;
            break;
        case 'p':
            outputMode = Tiles::POS_ONLY;
            break;
        case 'a':
            outputMode = Tiles::ATTRIBUTE;
            break;
        case 'r':
            roundRobin = true;
            break;
        case 'h':
        case '?':
        default:
            usage(argv[0]);
            break;
        }
    }

    if (index >= denom) {
        usage(argv[0]);
    }

    if (::optind != argc - 1) {
        usage(argv[0]);
    }

    Tiles tiles(argv[::optind]);

    if (config == "minute") {
        tiles.genData(fbase, denom, index, roundRobin, 1, 10, 10, outputMode);
    }
    else if (config == "tiny") {
        tiles.genData(fbase, denom, index, roundRobin, 10, 1000, 1200, outputMode);
    }
    else if (config == "very-small") {
        tiles.genData(fbase, denom, index, roundRobin, 40, 1600, 3162, outputMode);
    }
    else if (config == "small") {
        tiles.genData(fbase, denom, index, roundRobin, 160, 3750, 10000, outputMode);
    }
    else if (config == "normal") {
        tiles.genData(fbase, denom, index, roundRobin, 400, 7500, 31623, outputMode);
    }
    else if (config == "large") {
        tiles.genData(fbase, denom, index, roundRobin, 1000, 15000, 100000, outputMode);
    }
    else if (config == "very-large") {
        tiles.genData(fbase, denom, index, roundRobin, 2500, 30000, 316228, outputMode);
    }
    else {
        usage(argv[0]);
    }
    return 0;
}
