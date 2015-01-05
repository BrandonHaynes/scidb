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
 * Initial Developer: GP
 * Created: September, 2012
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/* Input arguments. */
int _numOutputFiles = 0;
long _chunkSize = 1;
long _numLinesToSkip = 0;
char* _inputFileName = NULL;
char* _outputFileBaseName = NULL;

/* Custom data types */
typedef struct {
    char* data;
    int count;
} chunk_line;

/* Shared Variables. */
FILE* _inputFile = NULL;
FILE** _outputFiles = NULL;
long _linesSkipped = 0;
unsigned long long _newlinesFound = 0;
unsigned long long _linesProcessed = 0;
int _outputFileIndex = 0;
chunk_line*** _chunks = NULL;
int* _chunkIndices = NULL;

/* Read Buffer. */
#define RB_SIZE 262144
char _rb[RB_SIZE];
int _rbBeginPos = 0;
int _rbEndPos = -1;

inline void printUsage() {
    printf("Utility to split a CSV file into smaller files.\n"
           "USAGE: splitcsv -n NUMBER [-c CHUNK] [-s SKIP] [-i INPUT] [-o OUTPUT]\n"
           "   -n NUMBER\tNumber of files to split the input file into.\n"
           "   -c CHUNK\tChunk size (Default = 1).\n"
           "   -s SKIP\tNumber of lines to skip from the beginning of the input file (Default = 0).\n"
           "   -i INPUT\tInput file. (Default = stdin).\n"
           "   -o OUTPUT\tOutput file base name. (Default = INPUT or \"stdin.csv\").\n");
}

inline void handleArgError(const char* error) {
    fprintf(stderr, "ERROR: %s\n", error);
    printUsage();
    exit(EXIT_FAILURE);
}

inline void parseArgs(int argc, char* argv[]) {
    int i = 0;

    /* Sanity check. */
    if (argc <= 1) {
        handleArgError("This utility has a required command-line argument.");
    }

    /* Iterate over the command-line arguments. */
    for (i = 1; i < argc; i++) {
        /* Number option. */
        if (strcmp(argv[i], "-n") == 0) {
            _numOutputFiles = atoi(argv[++i]);
            continue;
        }

        /* Chunk size. */
        if (strcmp(argv[i], "-c") == 0) {
            _chunkSize = atol(argv[++i]);
            continue;
        }

        /* Skip option. */
        if (strcmp(argv[i], "-s") == 0) {
            _numLinesToSkip = atol(argv[++i]);
            continue;
        }

        /* Input file. */
        if (strcmp(argv[i], "-i") == 0) {
            _inputFileName = argv[++i];
            continue;
        }

        /* Output file base name. */
        if (strcmp(argv[i], "-o") == 0) {
            _outputFileBaseName = argv[++i];
            continue;
        }

        /* Help option. */
        if (strcmp(argv[i], "-h") == 0) {
            printUsage();
            exit(EXIT_SUCCESS);
        }
    }

    /* Validation. */
    if (_numOutputFiles <= 0) {
        handleArgError("NUMBER is required and must be a positive number.");
    }

    if (_chunkSize <= 0) {
        handleArgError("CHUNK must be a positive number.");
    }

    if (_numLinesToSkip < 0) {
        handleArgError("SKIP must be a number >= 0.");
    }
}

inline int fillReadBuffer() {
    int numRead = fread(_rb, 1, RB_SIZE, _inputFile);
    if (numRead == 0) {
        _rbBeginPos = 0;
        _rbEndPos = -1;
        if (ferror(_inputFile)) {
            fprintf(stderr, "ERROR: Problem encountered while reading input file.\n");
            exit(EXIT_FAILURE);
        }
    } else {
        _rbBeginPos = 0;
        _rbEndPos = numRead - 1;
    }
    return numRead;
}

inline int getNextNlPos() {
    int ch = 0;
    int rbCurPos = _rbBeginPos;
    while (rbCurPos <= _rbEndPos) {
        ch = _rb[rbCurPos];
        if (ch == '\n') {
            return rbCurPos;
        }
        rbCurPos++;
    }
    return -1;
}

inline void skipLines() {
    while (_linesSkipped < _numLinesToSkip) {
        int nextNlPos = getNextNlPos();
        if (nextNlPos != -1) {
            _linesSkipped++;
            _rbBeginPos = nextNlPos + 1;
        } else {
            if (!fillReadBuffer()) {
                fprintf(stdout, "WARNING: All lines in the file have been skipped.\n");
                break;
            }
        }
    }
}

inline void allocChunks() {
    int i = 0;
    int j = 0;
    _chunkIndices = malloc(sizeof (int) * _numOutputFiles);
    _chunks = malloc(sizeof (chunk_line**) * _numOutputFiles);
    for (i = 0; i < _numOutputFiles; i++) {
        _chunkIndices[i] = -1;
        _chunks[i] = malloc(sizeof (chunk_line*) * _chunkSize);
        for (j = 0; j < _chunkSize; j++) {
            _chunks[i][j] = malloc(sizeof (chunk_line));
            _chunks[i][j]->data = NULL;
            _chunks[i][j]->count = 0;
        }
    }
}

inline void freeChunks() {
    int i = 0;
    int j = 0;
    for (i = 0; i < _numOutputFiles; i++) {
        for (j = 0; j < _chunkSize; j++) {
            if (_chunks[i][j] != NULL) {
                if (_chunks[i][j]->data != NULL) {
                    free(_chunks[i][j]->data);
                    _chunks[i][j]->data = NULL;
                }
                free(_chunks[i][j]);
                _chunks[i][j] = NULL;
            }
        }
        if (_chunks[i] != NULL) {
            free(_chunks[i]);
            _chunks[i] = NULL;
        }
    }
    if (_chunks != NULL) {
        free(_chunks);
        _chunks = NULL;
    }
    if (_chunkIndices != NULL) {
        free(_chunkIndices);
        _chunkIndices = NULL;
    }
}

inline void closeFile(FILE* file) {
    if (file != NULL) {
        fclose(file);
    }
}

inline void writeData() {
    int i = 0;
    int curIndex = 0;
    char lineWasWritten = 1;
    chunk_line* chunkLine = NULL;
    for (i = 0; i < _numOutputFiles; i++) {
        if (_chunkIndices[i] < 0) {
            closeFile(_outputFiles[i]);
            _outputFiles[i] = NULL;
        }
    }
    while (lineWasWritten) {
        lineWasWritten = 0;
        for (i = 0; i < _numOutputFiles; i++) {
            if (_chunkIndices[i] >= curIndex) {
                chunkLine = _chunks[i][curIndex];
                if (chunkLine != NULL) {
                    if (chunkLine->count > 0) {
                        int numWritten = fwrite(chunkLine->data, 1, chunkLine->count, _outputFiles[i]);
                        if (numWritten != chunkLine->count) {
                            fprintf(stderr, "ERROR: Problem encountered while writing to output file.\n");
                            exit(EXIT_FAILURE);
                        }
                        lineWasWritten = 1;
                        _linesProcessed++;
                        chunkLine->count = 0;
                    }
                }
            } else {
                closeFile(_outputFiles[i]);
                _outputFiles[i] = NULL;
            }
        }
        if (++curIndex >= _chunkSize) {
            break;
        }
    }
    for (i = 0; i < _numOutputFiles; i++) {
        _chunkIndices[i] = -1;
    }
}

inline void fillChunk(char* offset, int count, int outputFileIndex, char fullLine) {
    int oldCount = 0;
    int newCount = 0;
    chunk_line* chunkLine = NULL;
    if (count <= 0) {
        return;
    }
    if (_chunkIndices[outputFileIndex] < 0) {
        _chunkIndices[outputFileIndex] = 0;
    }
    chunkLine = _chunks[outputFileIndex][_chunkIndices[outputFileIndex]];
    oldCount = chunkLine->count;
    newCount = oldCount + count;
    chunkLine->data = realloc(chunkLine->data, newCount);
    memcpy(chunkLine->data + oldCount, offset, count);
    chunkLine->count = newCount;
    if (fullLine) {
        _chunkIndices[outputFileIndex]++;
        if ((outputFileIndex == _numOutputFiles - 1) && (_chunkIndices[outputFileIndex] == _chunkSize)) {
            /* All of the chunk buffers are full. Time to drain them. */
            writeData();
        }
    }
}

inline void splitFile() {
    while (1) {
        int nextNlPos = getNextNlPos();
        char* offset = (char *) (_rb + _rbBeginPos);
        if (nextNlPos != -1) {
            /* We have found a newline. */
            int count = nextNlPos - _rbBeginPos + 1;
            fillChunk(offset, count, _outputFileIndex, 1);
            _newlinesFound++;
            _outputFileIndex = (_newlinesFound / _chunkSize) % _numOutputFiles;
            _rbBeginPos = nextNlPos + 1;
        } else {
            if (_rbBeginPos <= _rbEndPos) {
                /* We have a line fragment left in the buffer. */
                int count = _rbEndPos - _rbBeginPos + 1;
                fillChunk(offset, count, _outputFileIndex, 0);
            }
            if (!fillReadBuffer()) {
                /* End of the file. Drain the chunk buffers and finish up. */
                writeData();
                break;
            }
        }
    }
}

inline void openInputFile() {
    if (_inputFileName == NULL) {
        _inputFileName = "stdin.csv";
        _inputFile = stdin;
    } else {
        _inputFile = fopen(_inputFileName, "r");
        if (_inputFile == NULL) {
            fprintf(stderr, "ERROR: Failed to open specified CSV input file.\n");
            exit(EXIT_FAILURE);
        }
    }
}

inline void openOutputFiles() {
    int i = 0;
    if (_outputFileBaseName == NULL) {
        if (_inputFileName == NULL) {
            _outputFileBaseName = "stdin.csv";
        } else {
            _outputFileBaseName = _inputFileName;
        }
    }
    _outputFiles = malloc(sizeof (FILE*) * _numOutputFiles);
    for (i = 0; i < _numOutputFiles; i++) {
        char outputFileName[2048];
        sprintf(outputFileName, "%.1024s_%04d", _outputFileBaseName, i);
        _outputFiles[i] = fopen(outputFileName, "w");
        if (_outputFiles[i] == NULL) {
            fprintf(stderr, "ERROR: Could not open output file '%s' for writing.\n", outputFileName);
            exit(EXIT_FAILURE);
        }
    }
    allocChunks();
}

inline void closeOutputFiles() {
    int i = 0;
    for (i = 0; i < _numOutputFiles; i++) {
        closeFile(_outputFiles[i]);
    }
    if (_outputFiles != NULL) {
        free(_outputFiles);
        _outputFiles = NULL;
    }
    freeChunks();
}

inline void closeInputFile() {
    closeFile(_inputFile);
    _inputFile = NULL;
}

int main(int argc, char* argv[]) {
    parseArgs(argc, argv);
    openInputFile();
    openOutputFiles();
    skipLines();
    splitFile();
    fprintf(stdout, "Lines Skipped: %ld / Lines Processed: %llu\n", _linesSkipped, _linesProcessed);
    closeOutputFiles();
    closeInputFile();
    exit(EXIT_SUCCESS);
}

