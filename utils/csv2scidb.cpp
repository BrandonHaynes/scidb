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
 *  
 * @file csv2scidb.cpp
 *
 * @author paulgeoffreybrown@gmail.com
 * 
 *  csv2scidb is a utility tool designed to convert data in .csv formated 
 *  files into the ASCII load format that SciDB can understand
 * 
 *  TODO: Add support for binary formats, although I suspect that doing so 
 *        will require a re-write. 
 * 
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <limits.h>
#include "system/Constants.h"
#include <string>

using namespace std;

/**
 *  
 * Depending on the type being formatted for load() into SciDB, we use 
 * different delimiters to surround the value. Numbers and strings that 
 * are to be unadorned (because they're already surrounded by the appropriate 
 * delimiters) are unchanged. But if the user has requested that we delim 
 * char attributes then we prepend and append a "'" char, or if the user has 
 * requested that we delim a string then we prepend and append a """ char. 
 */
inline void
putdelim(char type, FILE * out) {
    switch (type) {
        case 'N':
        case 's':
        case 'c':
            break;
        case 'S':
            putc('"', out);
            break;
        case 'C':
            putc('\'', out);
            break;
        default:
            fprintf(stdout, "invalid type provided in type string\n");
            break;
    }
}

/**
 * 
 *  Get the char from the string handed to us with the -p flag for the 
 * attribute number. 
 */
inline char
getFormatCh(const char * chTypeString, const int attrCnt, const int nAttrs)
{
    if (attrCnt < nAttrs) {
        return chTypeString[attrCnt];
    }
    return 'N';
}

/**
 * 
 * Provide help about how to use this utility. 
 */
void
usage ( const char * execName, const long int nChunkLen )
{
    fprintf(stdout, 
        "%s: Convert CSV file to SciDB input text format.\n"
        "Usage:   csv2scidb [options] [ < input-file ] [ > output-file ]\n"
        "Default: -f 0 -c %ld -q\n"
        "Options:\n"
        "  -v        version information\n"
        "  -i PATH   input file\n"
        "  -o PATH   output file\n"
        "  -a PATH   appended output file\n"
        "  -c INT    length of chunk\n"
        "  -f INT    starting coordinate\n"
        "  -n INT    number of instances\n"
        "  -d CHAR   delimiter: defaults to ,\n"
        "  -p STR    type pattern: N number, S string, s nullable-string,\n"
        "            C char, c nullable-char\n"
        "  -q        quote the input line exactly by wrapping it in ()\n"
        "  -s N      skip N lines at the beginning of the file\n"
        "  -h        prints this helpful message\n"
        "\n"
        "Note: the -q and -p options are mutually exclusive.\n",
        execName, nChunkLen);
}

int 
main(int argc, char* argv[]) {
    int i, j, ch;
    char delim = ',';
    long count = 0;
    FILE* in = stdin;
    FILE* out = stdout;
    long skip = 0;
    long nChunkLen = 1000000, outCnt = 0;
    int valSize = 0;
    int numAttrs = 0, attrCnt = 0;
    long startChunk = 0;
    long chunknum = 0;
    long instances = 1;
    bool quoteLine = true;
    bool quoteLineOrTypeString[2] = {false,false};
    char *chTypeString = NULL;

    for (i = 1; i < argc; i++) {
        if (i > (argc - 1)) {
            fprintf(stderr, "invalid command line args\n");
            return EXIT_FAILURE;
        }

        if (strcmp(argv[i], "-i") == 0) {
            in = fopen(argv[++i], "r");
            if (in == NULL) {
                perror("open input file\n");
                return EXIT_FAILURE;
            }
        } else if (strcmp(argv[i], "-o") == 0) {
            out = fopen(argv[++i], "w");
            if (out == NULL) {
                perror("open output file\n");
                return EXIT_FAILURE;
            }
        } else if (strcmp(argv[i], "-a") == 0) {
            out = fopen(argv[++i], "a");
            if (out == NULL) {
                perror("open output file\n");
                return EXIT_FAILURE;
            }
        } else if (strcmp(argv[i], "-s") == 0) {
            skip = atol(argv[++i]);
        } else if (strcmp(argv[i], "-c") == 0) {
            nChunkLen = atol(argv[++i]);
            if (0 >= nChunkLen) {
                fprintf(stderr, "chunk size must be > 0\n");
                return EXIT_FAILURE;
            }
        } else if (strcmp(argv[i], "-f") == 0) {
            startChunk = atol(argv[++i]);
            if (0 > startChunk) {
                fprintf(stderr, "starting coordinate must be >= 0\n");
                return EXIT_FAILURE;
            }
        } else if (strcmp(argv[i], "-n") == 0) {
            instances = atol(argv[++i]);
            if (0 >= instances) {
                fprintf(stderr, "instances must be > 0\n");
                return EXIT_FAILURE;
            }
        } else if (strcmp(argv[i], "-d") == 0) {
            i++;
            if ((('"' == argv[i][0]) || ('\'' == argv[i][0])) && (3 == strlen(argv[i]))) {
                delim = argv[i][1];
            } else if (strcmp(argv[i], "\\t") == 0) {
                delim = '\t';
            } else if (0 == (delim = argv[i][0])) {
                fprintf(stderr, "delim required\n");
                return EXIT_FAILURE;
            }
        } else if (strcmp(argv[i], "-p") == 0) {
            if (true == quoteLineOrTypeString[1]) { 
                fprintf(stderr, "Cannot specify both -p and -q options\n");
                usage(argv[0], nChunkLen);
                return EXIT_FAILURE;
            }
            quoteLineOrTypeString[0]=true;
            quoteLine = false;
            chTypeString = argv[++i];
            for (j = 0, numAttrs = strlen(chTypeString); j < numAttrs; j++) {
                switch (chTypeString[j]) {
                    case 'N':
                    case 'S':
                    case 's':
                    case 'C':
                    case 'c':
                        break;
                    default:
                        fprintf(stderr, "type string must contain only N, S, s, C and c characters\n");
                        return EXIT_FAILURE;
                }
            }
        } else if (strcmp(argv[i], "-v")==0) {
            printf("SciDB File Conversion Utility Version: %s\n",scidb::SCIDB_VERSION_PUBLIC());
            printf("Build Type: %s\n",scidb::SCIDB_BUILD_TYPE());
            printf("%s\n",scidb::SCIDB_COPYRIGHT());
            return EXIT_SUCCESS;
        } else if (strcmp(argv[i], "-q") == 0) {
            if (true == quoteLineOrTypeString[0]) { 
                fprintf(stderr, "Cannot specify both -q and -p options\n");
                usage(argv[0], nChunkLen);
                return EXIT_FAILURE;
            }
            quoteLineOrTypeString[1]=true;
            quoteLine = true;
        } else if (strcmp(argv[i], "-h") == 0) {
            usage(argv[0], nChunkLen);
            return EXIT_SUCCESS;
        } else {
            usage(argv[0], nChunkLen);
            return 2;
        }
    }

    while (skip != 0 && (ch = getc(in)) != EOF) {
        if (ch == '\n') {
            skip -= 1;
        }
    }

    outCnt = 0;
    chunknum = startChunk;
    //
    // TODO: Make this more efficient by reading blocks of data larger 
    //       than a single char at a time. 
    //
    while ((ch = getc(in)) != EOF) {
        bool firstCh = true;
        attrCnt = 0;
        valSize = 0;
        do {
            if (iscntrl(ch) && ch != '\t')
                continue;

            if (firstCh) {
                if (outCnt) {
                    fprintf(out, ",\n");
                } else {
                    fprintf(out, "{%ld}", chunknum);
                    chunknum = chunknum + (nChunkLen * instances);
                    fprintf(out, "[\n");
                }

                putc('(', out);
                if (!quoteLine)
                    putdelim(getFormatCh(chTypeString, attrCnt, numAttrs), out);
                firstCh = false;
            }

            if (quoteLine) {
                //
                // If we're just printing out each line and wrapping it in a 
                // pair of (), then just print the char. 
                //
                putc(ch, out);
            } else if (delim == ch) {
                //
                // We're at the end of an attribute in the .csv file.
                //
                if (valSize == 0 && (getFormatCh(chTypeString, attrCnt, numAttrs) == 'N' || getFormatCh(chTypeString, attrCnt, numAttrs) == 's' || getFormatCh(chTypeString, attrCnt, numAttrs) == 'c')) {
                    fputs("null", out);
                } else if (valSize > 0 && getFormatCh(chTypeString, attrCnt, numAttrs) == 's') {
                    putc('"', out);
                }

                valSize = 0;
                putdelim(getFormatCh(chTypeString, attrCnt, numAttrs), out);
                putc(',', out); // Marker for end of attribute in SciDB
                attrCnt++;
                if (numAttrs > 0 && attrCnt > numAttrs) {
                    fprintf(stderr, "too many attributes in csv file\n");
                    return EXIT_FAILURE;
                }
                putdelim(getFormatCh(chTypeString, attrCnt, numAttrs), out);
            } else {
                if (valSize == 0 && getFormatCh(chTypeString, attrCnt, numAttrs) == 's') {
                    putc('"', out);
                }
                valSize += 1;
                putc(ch, out);
            }
        } while ((ch = getc(in)) != EOF && ch != '\n');
        //
        // We're at the end of the line. 
        //
        if (attrCnt > 0 || (numAttrs == 1 && !firstCh) || (quoteLine)) {
            if (attrCnt < numAttrs) {
                if (valSize == 0 && (getFormatCh(chTypeString, attrCnt, numAttrs) == 'N' || getFormatCh(chTypeString, attrCnt, numAttrs) == 's')) {
                    fputs("null", out);
                } else if (valSize > 0 && getFormatCh(chTypeString, attrCnt, numAttrs) == 's') {
                    if (!quoteLine)
                        putc('"', out);
                }

                if (!quoteLine)
                    putdelim(getFormatCh(chTypeString, attrCnt, numAttrs), out);
            }
            //
            // Only if the line had something in it do we need to append 
            // the ')' character, indicating the end of the cell of attributes. 
            //
            if (!firstCh) {
                putc(')', out);
            }
            count++;
        }

        if (!firstCh && ++outCnt >= nChunkLen && nChunkLen) {
            (void) fprintf(out, "\n];\n");
            outCnt = 0;
        }
    }
    if (outCnt) {
        putc('\n', out);
        putc(']', out);
        putc('\n', out);
    }

    fclose(out);
    fclose(in);

    return EXIT_SUCCESS;
}
