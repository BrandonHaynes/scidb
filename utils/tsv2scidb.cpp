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
 * @file tsv2scidb.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 *
 * @brief Convert data in .tsv format into the SciDB ASCII load format.
 *
 * @note Rather than radically modify csv2scidb so late in the release
 * cycle (mods would require integrating it with libcsv), I chose to
 * add a new utility that parses the very simple TSV format.  The new
 * splitcsv program can write TSV, and so loadcsv.py can invoke it and
 * this program and only parse CSV once.  (TSV can be parsed in linear
 * time.)
 *
 * @see http://dataprotocols.org/linear-tsv/
 */

#include <system/Constants.h>
#include <util/Utility.h>       // for scidb::tsv_parse

#include <boost/lexical_cast.hpp>
#include <cassert>
#include <cerrno>
#include <getopt.h>
#include <iostream>
#include <string>
#include <string.h>             // for strerror
#include <sys/param.h>          // for roundup
#include <vector>

using namespace boost;
using namespace std;

const size_t DEFAULT_INSTANCE_COUNT = 1;
const string PATTERN_CHARS("NSsCc");
const char* SCIDB_ESCAPED_CHARS = "()[]{},*";

/* Options */
bool gAppend = false;
size_t gChunkSize = 1;
char gDelim = '\t';
string gInFile("-");            // Gin often leads to gout.
string gOutFile("-");
size_t gSkip = 0;
bool gVerbose = false;
unsigned gNumInstances = DEFAULT_INSTANCE_COUNT;
size_t gStartChunk = 0;
string gTypePattern;

/* A few non-option globals */
string gPgm;
bool gCtype[256];

/* Error checking, mostly for output calls. */
#define CKRET(_stmt, _successCond)                                      \
do {                                                                    \
    int _rc = _stmt ;                                                   \
    if (!(_rc _successCond)) {                                          \
        cerr << gPgm << ": \"" << #_stmt << "\" at line " << __LINE__   \
             << " returned " << _rc << ": " << ::strerror(errno) << endl; \
        ::exit(2);                                                      \
    }                                                                   \
} while (0)

/**
 * Initialize the table that tells which characters must be escaped.
 *
 * The escaped characters are 8-bit or listed in SCIDB_ESCAPED_CHARS.
 */
void initCtypeTable()
{
    ::memset(&gCtype[0], 0, sizeof(gCtype)/2); // 7-bit
    ::memset(&gCtype[128], 1, sizeof(gCtype)/2); // 8-bit
    for (const char *cp = SCIDB_ESCAPED_CHARS; *cp; ++cp)
        gCtype[*cp & 0xFF] = true;
}

/**
 * Is this a character that requires backquoting in a SciDB load file?
 *
 * We need to escape the SCIDB_ESCAPED_CHARS (already loaded into the
 * gCtype table) and also anything non-ASCII, which InputArray::Scanner
 * hates unless they are quoted.
 */
inline bool isescape(int ch)
{
    return gCtype[ch & 0xFF];
}

/**
 * Massage input field in column for use inside a tuple.
 *
 * @param col column index of field, used to choose "pattern" character
 * @param field pointer to raw input field
 * @param buf work buffer where we'll place the result
 * @return pointer to null-terminated string in buf
 *
 * @note For the usual case, we start constructing the result at workbuf[1]
 * so that we can decide late whether to use single- or double-quote.
 */
char* toScidbField(size_t col, const char* field, vector<char>& buf)
{
    static vector<int8_t> cache(32, -1);  // Cache whether column needs quotes.
    assert(buf.size() > 8);     // else we can't write "null"
    buf[0] = buf[1] = '\0';

    // Default behavior is N for backward compatibility, BUT we want
    // to be flexible: use '\0' to remember we took the default, and
    // if the field *looks* like it should be quoted/encoded, do so.
    //
    const char NO_FORMAT = '\0';
    char format = (col < gTypePattern.size() ? gTypePattern[col] : NO_FORMAT);

    // Empty field handling.
    if (*field == '\0') {
        switch (format) {
        case NO_FORMAT:
        case 'N':
        case 's':
        case 'c':
            // Emptiness is null in SciDB load format.
            break;
        case 'S':
            ::memcpy(&buf[0], "\"\"", 3);
            break;
        case 'C':
            ::memcpy(&buf[0], "''", 3);
            break;
        }
        return &buf[0];
    }

    // Treat LinearTSV \N  and our own "null" as null.
    // Note that nulls don't affect the column cache[].
    if ((field[0] == '\\' && field[1] == 'N' && field[2] == '\0')
        || !::strcmp(field, "null"))
    {
        ::memcpy(&buf[0], "null", 5);
        return &buf[0];
    }

    // Decide whether the field must be quoted.  First, did we already
    // cache an answer?
    int8_t wantQuote = -1;
    if (col < cache.size()) {
        wantQuote = cache[col];
    } else {
        // Need to expand the cache.
        cache.resize(roundup(col, 8), -1);
    }

    // If we didn't find an answer in the cache, figure it out.
    if (wantQuote < 0) {
        switch (format) {
        case 'N':
            wantQuote = 0;
            cache[col] = 0;     // superfluous really
            if (gVerbose)
                cerr << gPgm << ": don't quote column " << col << ": N" << endl;
            break;
        case NO_FORMAT:
            //  If this regex match is too slow for you, use
            // --type-pattern to avoid it.
            wantQuote = scidb::isnumber(field) ? 0 : 1;
            cache[col] = wantQuote;
            if (gVerbose) {
                cerr << gPgm << ": " << (wantQuote ? "" : "don't ")
                     << "quote column " << col << ": number regex" << endl;
            }
            break;
        default:
            wantQuote = 1;
            cache[col] = 1;
            if (gVerbose)
                cerr << gPgm << ": quote column " << col << ": SsCc" << endl;
            break;
        }
    }
    assert(wantQuote > -1);

    // If quotes, then leave room at beginning of buffer for the
    // yet-to-be-chosen quote character.
    char *wp = wantQuote ? &buf[1] : &buf[0];  // write ptr
    const char *rp = field;                    // read ptr
    char *wend = &buf[buf.size()];
    for (size_t i = 0; *rp && wp < wend; ++rp, ++i) {

        // No quotes?  Easy.
        if (!wantQuote) {
            *wp++ = *rp;
            continue;
        }

        // Field may already contain quotes.  If *rp is a quote and no
        // quoting style chosen yet, choose the opposite kind.  (If
        // the raw field contains both kinds of quotes, you're
        // screwed---currently the InputArray::Scanner can't deal with
        // that, even if we backslash-quote them.)
        //
        if (*rp == '\'' && buf[0] == '\0')
            buf[0] = '"';
        if (*rp == '"' && buf[0] == '\0')
            buf[0] = '\'';

        // If this is a character that would mess up the SciDB format
        // parse, backquote it.
        if (::strchr(SCIDB_ESCAPED_CHARS, *rp))
            *wp++ = '\\';

        // Copy it on!
        *wp++ = *rp;
    }
    if (wp == wend) {
        // Out of space?  Bah, humbug.  Restart with more.
        buf.resize(2 * buf.size());
        return toScidbField(col, field, buf);
    }

    // Supply matching close-quote for quoted stuff.
    if (wantQuote) {
        // If no quote chosen yet, pick " for [sS], ' for [cC]
        if (buf[0] == '\0')
            buf[0] = (format == 'c' || format == 'C') ? '\'' : '"';
        *wp++ = buf[0];
    }

    *wp++ = '\0';
    return &buf[0];
}

/**
 * Translate parsed fields into SciDB tuple.
 * Leaves the output positioned after the closing paren.
 */
void toScidbTuple(FILE* fout, vector<char*>& fields)
{
    static vector<char> workbuf(128);

    CKRET(::fputc('(', fout), >= 0);
    for (size_t i = 0; i < fields.size(); ++i) {
        if (i) {
            CKRET(::fputc(',', fout), != EOF);
        }
        const char *field = fields[i];
        CKRET(::fprintf(fout, "%s", toScidbField(i, field, workbuf)), >= 0);
    }
    CKRET(::fputc(')', fout), != EOF);
}

/**
 * Parse TSV file and translate record by record.
 *
 * @note Empty input files must produce empty output files.
 */
void tsvToScidb(FILE* fin, FILE* fout)
{
    size_t buflen = 128;
    char *bufp = static_cast<char*>(::malloc(buflen));
    vector<char *> fields;
    size_t lineno = 0;
    size_t tupleno = 0;
    size_t chunkno = 0;
    size_t coordinate = gStartChunk;

    while (EOF != ::getline(&bufp, &buflen, fin)) {
        ++lineno;
        if (gSkip) {
            --gSkip;
            continue;
        }

        if (tupleno == 0) {
            // We've read a line but haven't seen a tuple in this chunk
            // yet, so we know it's time to open the chunk.
            if (chunkno) {
                // Not the first chunk, so emit chunk separator.
                CKRET(::fprintf(fout, ";\n"), >= 0);
            }
            CKRET(::fprintf(fout, "{%ld}[\n", coordinate), >= 0);
            coordinate += (gChunkSize * gNumInstances);
        }
        else {
            // Already wrote a tuple into this chunk, so emit tuple separator.
            CKRET(::fprintf(fout, ",\n"), >= 0);
        }

        // Parse line and write tuple!
        if (!scidb::tsv_parse(bufp, fields, gDelim)) {
            cerr << gPgm << ": TSV parse error at [" << gInFile << ':' << lineno
                 << "], probably caused by \\<TAB>" << endl;
            ::exit(2);
        }
        toScidbTuple(fout, fields);

        // Close old chunk?
        if (++tupleno == gChunkSize) {
            tupleno = 0;        // ...so we'll open next chunk when we see data.
            ++chunkno;
            CKRET(::fprintf(fout, "\n]"), >= 0);
        }
    }

    // Close final chunk nicely.
    if (tupleno) {
        // Inside an unclosed chunk, close it!
        CKRET(::fprintf(fout, "\n]\n"), >= 0);
    }
    else if (chunkno) {
        // Loop closed the chunk, but a final newline would be nice.
        CKRET(::fputc('\n', fout), != EOF);
    }

    if (gVerbose) {
        cerr << gPgm << " pid " << ::getpid() << " wrote " << chunkno
             << " " << gChunkSize << "-tuple chunks plus one chunk of "
             << tupleno << " tuples (" << ((chunkno * gChunkSize) + tupleno)
             << " total tuples)" << endl;
    }
}

void printUsage()
{
    cerr << gPgm <<
        ": Convert TSV file to SciDB input text format.\n"
        "Usage:   tsv2scidb [options] [ < input-file ] [ > output-file ]\n"
        "Default: --start-coord=0 --chunk-size=" << gChunkSize << "\n"
        "Options:\n"

        "  -i PATH, --input=PATH\n"
        "\tinput file\n"

        "  -o PATH, --output=PATH\n"
        "\toutput file\n"

        "  -a PATH, --append=PATH\n"
        "\tappended output file\n"

        "  -c INT, --chunk-size=INT\n"
        "\tlength of chunk\n"

        "  -f INT, --start-coord=INT\n"
        "\tstarting coordinate\n"

        "  -n INT, --instances=INT\n"
        "\tnumber of instances\n"

        "  -d CHAR, --delim=CHAR\n"
        "\tdelimiter: defaults to TAB (ascii 0x09)\n"

        "  -p STR, --type-pattern=STR\n"
        "\ttype pattern: N number, S string, s nullable-string,\n"
        "\tC char, c nullable-char\n"

        "  -s N, --skip-lines=N\n"
        "\tskip N lines at the beginning of the file\n"

        "  -v, --version\n"
        "\tversion information\n"

        "  -V, --verbose\n"
        "\twrite debug info to stderr\n"

        "  -h, --help\n"
        "\tprints this helpful message\n"
         << flush;
}

void printVersion()
{
    // Make it look vaguely the same as 'iquery --version':
    cout << gPgm << " file conversion utility"
         << "\nSciDB Version: " << scidb::SCIDB_VERSION_PUBLIC()
         << "\nBuild Type: " << scidb::SCIDB_BUILD_TYPE()
         << "\n" << scidb::SCIDB_COPYRIGHT()
         << endl;
}

/**
 *  Accept single character delimiter x, 'x', or "x".
 *  For backward compat, \t is also OK.
 */
char parseDelim(const char *optarg)
{
    char ch;
    char delim = '\t';

    switch (::strlen(optarg)) {
    case 1:
        delim = optarg[0];
        break;
    case 2:
        // backward compat w/ csv2scidb
        if (optarg[0] == '\\' && optarg[1] == 't') {
            delim = '\t';
        } else {
            throw runtime_error(string("Bad delimiter: ") + optarg);
        }
        break;
    case 3:
        delim = '\0'; // unset
        ch = optarg[0];
        if (ch == optarg[2] && (ch == '"' || ch == '\'')) {
            delim = optarg[1];
        }
        break;
    default:
        throw runtime_error(string("Bad delimiter: ") + optarg);
    }

    if (!delim) {
        throw runtime_error(string("Bad delimiter: ") + optarg);
    }
    return delim;
}

// Don't forget to change both short_ and long_options!
const char* short_options = "a:c:d:f:hi:n:o:s:p:vV";
struct option long_options[] = {
    { "append",       required_argument, 0, 'a' },
    { "chunk-size",   required_argument, 0, 'c' },
    { "delim",        required_argument, 0, 'd' },
    { "help",         no_argument,       0, 'h' },
    { "input",        required_argument, 0, 'i' },
    { "instances",    required_argument, 0, 'n' },
    { "start-coord",  required_argument, 0, 'f' },
    { "output",       required_argument, 0, 'o' },
    { "skip-lines",   required_argument, 0, 's' },
    { "type-pattern", required_argument, 0, 'p' }, // XXX -p differs from splitcsv usage
    { "version",      no_argument,       0, 'v' },
    { "verbose",      no_argument,       0, 'V' },
    {0, 0, 0, 0}
};

int main(int ac, char** av)
{
    gPgm = av[0];
    string::size_type slash = gPgm.find_last_of("/");
    if (slash != string::npos)
        gPgm = gPgm.substr(slash + 1);

    initCtypeTable();

    // Take some defaults from environment...
    char *cp = ::getenv("SCIDB_INSTANCE_NUM");
    if (cp) {
        try {
            gNumInstances = lexical_cast<unsigned>(cp);
        } catch (bad_lexical_cast&) {
            cerr << gPgm << ": Ignoring bad SCIDB_INSTANCE_NUM value" << endl;
            gNumInstances = DEFAULT_INSTANCE_COUNT; // paranoid
        }
    }

    char c;
    int optidx = 0;
    try {
        while (1) {
            c = ::getopt_long(ac, av, short_options, long_options, &optidx);
            if (c == -1)
                break;
            switch (c) {
            case 'a':
                gAppend = true;
                gOutFile = optarg;
                break;
            case 'c':
                gChunkSize = lexical_cast<size_t>(optarg);
                break;
            case 'd':
                gDelim = parseDelim(optarg);
                break;
            case 'f':
                gStartChunk = lexical_cast<size_t>(optarg);
                break;
            case 'h':
                printUsage();
                return 0;
            case 'i':
                gInFile = optarg;
                break;
            case 'n':
                gNumInstances = lexical_cast<unsigned>(optarg);
                break;
            case 'o':
                gOutFile = optarg;
                break;
            case 's':
                gSkip = lexical_cast<size_t>(optarg);
                break;
            case 'p':
                gTypePattern = (optarg ? optarg : "");
                break;
            case 'v':
                printVersion();
                return 0;
            case 'V':
                // Beware: test harness hates stderr and this is a
                // filter so can't use stdout.
                gVerbose = true;
                break;
            default:
                printUsage();
                return 2;
            }
        }
    }
    catch (bad_lexical_cast& exc) {
        cerr << "Bad or missing option value: " << exc.what()
             << "\nType '" << gPgm << " -h' for help." << endl;
        return 2;
    }
    catch (std::exception& exc) {
        cerr << "Option parsing error: " << exc.what()
             << "\nType '" << gPgm << " -h' for help." << endl;
        return 2;
    }

    // Validate options and arguments here.
    if (gInFile.empty())
        gInFile = "-";
    if (gOutFile.empty())
        gOutFile = "-";
    if (gNumInstances == 0) {
        cerr << "Instance count of zero is meaningless" << endl;
        return 2;
    }
    if (gChunkSize == 0) {
        cerr << "Chunk size of zero is meaningless" << endl;
        return 2;
    }
    if (!gTypePattern.empty()
        && gTypePattern.find_first_not_of(PATTERN_CHARS) != string::npos)
    {
        cerr << gPgm << ": Type string must contain only these characters: "
             << PATTERN_CHARS << endl;
        return 2;
    }
    if (gVerbose) {
        cerr << "---- " << gPgm << " pid " << ::getpid() << " parameters: ----"
             << "\nchunk-size  : " << gChunkSize
             << "\nstart-chunk : " << gStartChunk
             << "\ninput-file  : " << gInFile
             << "\noutput-file : " << gOutFile
             << "\ninstances   : " << gNumInstances
             << "\nskip-lines  : " << gSkip
             << "\ndelimiter   : '" << gDelim << '\''
             << "\ntype-pattern: '" << gTypePattern << '\''
             << endl;
    }

    //
    //  Open the files and do the work!!
    //

    FILE* fin = 0;
    if (gInFile == "-") {
        fin = ::stdin;
        gInFile = "(stdin)"; // nicer for later display
    } else {
        fin = ::fopen(gInFile.c_str(), "rb");
        if (!fin) {
            cerr << "Cannot fopen for input: " << gInFile << ": "
                 << ::strerror(errno) << endl;
            return 2;
        }
    }

    FILE* fout = 0;
    if (gOutFile == "-") {
        fout = ::stdout;
        gOutFile = "(stdout)"; // nicer for later display
    } else {
        fout = ::fopen(gOutFile.c_str(), (gAppend ? "a" : "w"));
        if (!fout) {
            cerr << "Cannot fopen for " << (gAppend ? "append: " : "output: ")
                 << gOutFile << ": " << ::strerror(errno) << endl;
            return 2;
        }
    }

    tsvToScidb(fin, fout);
    ::fclose(fin);
    ::fclose(fout);

    return 0;
}
