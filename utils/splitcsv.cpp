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
 * @file splitcsvx.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 *
 * @brief Use a reliable CSV parser to reimplement splitcsv.
 */

#include <util/Utility.h>

#include <boost/lexical_cast.hpp>
#include <boost/shared_ptr.hpp>
#include <cassert>
#include <cerrno>
#include <csv.h>
#include <getopt.h>
#include <iomanip>
#include <iostream>
#include <limits.h>
#include <string>
#include <string.h>             // for strerror
#include <sys/param.h>          // for roundup
#include <sys/user.h>           // for NBPG
#include <vector>

using namespace boost;
using namespace std;

const size_t KiB = 1024UL;
const size_t BAD_SIZE = ~0;
const char * const DEFAULT_OUTPUT_BASE = "stdin.csv";
const string TSV_ESCAPED_CHARS("\t\n\r\\");
const char OUTPUT_QUOTE = '\'';

// See http://www.parashift.com/c++-faq/macro-for-ptr-to-memfn.html
#define CALL_MEMBER_FN(_object, _ptrToMbrFun) ((_object).*(_ptrToMbrFun))

#define SCIDB_ASSERT(_cond_) do { size_t _rc_ = sizeof(_cond_); assert(bool(_cond_)); _rc_ = _rc_; /* avoid compiler warning */ } while(0)

/* Options */
size_t gChunkSize = 1;
string gInFile;
size_t gSplitCount = BAD_SIZE;
string gOutputBase;
char gInputQuote = '\0';        // Guess automatically if not set.
size_t gSkip = 0;
bool gVerbose = false;
string gOutFormat("csv");
unsigned char gDelim = '\0';
string gTypePattern;
string gPgm;                    // Program name (to be used in warnings and errors but *not* verbose output).

/**
 * Should value 'val' in column 'col' get the CSV quote treatment on output?
 *
 * @descripton Since we now have to actually parse the input rather
 * than pass it along in pretty much raw form, we now may need to
 * replace the quotes that parsing removed.  This routine figures it
 * out and caches the answer for later calls.
 */
static bool csvShouldQuote(unsigned col, const string& val)
{
    static vector<int8_t> cache(32, -1); // 1:true, 0:false, -1:undecided_yet

    // If given a "--type-pattern" switch, that should dictate.
    size_t end = gTypePattern.size();
    if (end && col < end) {
        // Quote string fields only... for now.
        char ch = gTypePattern[col];
        switch (ch) {
        case 's':
        case 'S':
            return true;
        default:
            return false;
        }
    }
        
    // Use cached answer if we have one.
    if (col < cache.size()) {
        if (cache[col] >= 0) {
            // Cache hit!
            return static_cast<bool>(cache[col]);
        }
    } else {
        // Need to expand the cache.
        cache.resize(roundup(col, 8), -1);
    }

    // Field is not covered by the type pattern, so make our best
    // guess.  Look for reasons *not* to quote it.

    // First, don't quote numbers.  Note this checks for "nan" as well.
    if (scidb::isnumber(val.c_str())) {
        cache[col] = 0;
        return false;
    }

    // Don't quote null.  Null gives no clue as to attribute type, so
    // don't update the cache.
    if (val == "null") {
        return false;
    }

    // Out of reasons not to quote it, so quote it.
    cache[col] = 1;
    return true;
}

/** Parsed row of CSV input. Tracks total bytes in row. */
class Row
{
    size_t         _total;
    vector<string> _fields;
public:
    Row() : _total(0) {}

    size_t length() const
    {
        size_t delim_count = _fields.empty() ? 0 : _fields.size() - 1;
        return _total + delim_count;
    }
    size_t field_count() const { return _fields.size(); }
    const string& operator[](size_t i) const { return _fields[i]; }
    const string& back() const { return _fields.back(); }

    void push_back(const string& s)
    {
        _total += s.size();
        _fields.push_back(s);
    }

    void clear()
    {
        _total = 0;
        _fields.clear();
    }

    string str() const
    {
        stringstream ss;
        for (unsigned i = 0; i < _fields.size(); ++i) {
            if (i) ss << ' ';
            ss << '[' << _fields[i] << ']';
        }
        ss << "<EOR>";
        return ss.str();
    }
};

/**
 * This class does the buffering needed to prevent deadlocks during load.
 *
 * @description We want to round-robin through all output files so
 * that the chunk data is written "evenly" to all instances.  To do
 * that, entire chunks must be buffered up, since the input must be
 * read linearly and divided among the chunks as we go.
 *
 * @note There doesn't seem to be any choice but to buffer up N
 * complete chunks of data at a time, given the way SciDB wants all
 * instances to be receiving data more or less simultaneously.
 *
 * @see Ticket #3211.
 */
class ChunkBufferedFile
{
    FILE*       _fp;            // File we are buffering
    size_t      _bufLen;        // Total size of data buffer
    char*       _bufPtr;        // The data buffer
    char*       _dataPtr;       // Points to unwritten data in buffer
    size_t      _dataLen;       // Byte count of valid data
    size_t      _eor;           // Offset of last end-of-record mark
    bool        _dirty;         // Buffer had data to flush
    mutable string _name;       // Pretty name for debug output

    void resize(size_t);

    static size_t       s_recLenHwm;   // Longest record seen so far
    static size_t       s_chunkSize;   // Chunk size (# of records per chunk)

    static size_t computeGoal()
    { 
        assert(s_chunkSize);
        assert(s_recLenHwm);
        return roundup(s_chunkSize * s_recLenHwm, NBPG);
    }

public:
    ChunkBufferedFile(FILE* fp, const string& name);
    ~ChunkBufferedFile();

    /** For older code that doesn't use buffering. */
    FILE* getFile() { return _fp; }

    /** For debug output. */
    const string& str() const { return _name; }

    /** Buffer up these bytes for eventual flush. */
    void write(const void* buf, size_t count);

    /**
     * Give client a buffer to write into.
     *
     * @description This method and @c putBuf allow library code to
     * write into a buffer without having to know about this class.
     *
     * @param bufp OUT pointer to writeable buffer
     * @param buflen OUT remaining bytes in buffer
     * @param need IN if non-zero client needs buffer at least this big
     */
    void getBuf(char*& bufp, size_t& buflen, size_t need = 0);

    /** Client done writing buffer, here's where it stopped. */
    void putBuf(char* newBufp);

    /** Write record-terminating newline and bump record count. */
    void putNewline();

    /** Write n bytes to output FILE*, return true iff something got written. */
    bool flush(size_t n);

    /** Close now if nothing has been written. */
    void maybeCloseEarly();

    /** Provide chunk size for buffer guesstimation. */
    static void setChunkSize(size_t sz) { s_chunkSize = sz; }

    /** Provide line length hint so we'll allocate adequate buffers. */
    static void setLineHint(size_t sz) { s_recLenHwm = sz; }
};

typedef shared_ptr<ChunkBufferedFile> CbfPtr;

size_t ChunkBufferedFile::s_chunkSize = 0;
size_t ChunkBufferedFile::s_recLenHwm = 0;

ChunkBufferedFile::ChunkBufferedFile(FILE *fp, const string& name)
    : _fp(fp)
    , _bufLen(computeGoal())
    , _bufPtr(static_cast<char*>(::malloc(_bufLen)))
    , _dataPtr(_bufPtr)    // Only moves from _bufPtr during Splitter::drain().
    , _dataLen(0)
    , _eor(0)
    , _dirty(false)
{
    assert(s_chunkSize != 0);
    stringstream ss;
    ss << name << "[fd=" << (_fp ? ::fileno(_fp) : -1) << ']';
    _name = ss.str();
}

ChunkBufferedFile::~ChunkBufferedFile()
{
    if (_fp) {
        flush(LONG_MAX);
        ::fclose(_fp);
        _fp = NULL;
    }
    assert(_bufPtr);
    ::free(_bufPtr);
    _bufPtr = _dataPtr = 0;
}

/**
 * Increase buffer space to meet the immediate need.
 *
 * @description We must have encountered some longer-than usual input
 * lines, so reallocate the buffer according to the new line length
 * high water mark.
 *
 * @param need number of buffer bytes we need but do not have
 */
void ChunkBufferedFile::resize(size_t need)
{
    assert(need);
    assert(_dataPtr == _bufPtr);

    size_t remaining = _bufLen - _dataLen;
    SCIDB_ASSERT(need > remaining);

    // Did we run out while writing the new longest line?
    size_t rlen = (_dataLen - _eor) + need;
    if (rlen > s_recLenHwm) {
        s_recLenHwm = rlen;
    }

    // Newly computed goal should (a) be bigger than what we've got
    // and (b) satisfy the need, otherwise someone hasn't been
    // maintaining s_recLenHwm correctly!
    //
    size_t goal = computeGoal();
    assert(goal > _bufLen);
    assert(need <= (goal - _dataLen));
    
    // OK, reach the goal and we should be good.
    _bufPtr = static_cast<char*>(::realloc(_bufPtr, goal));
    if (_bufPtr == NULL)
        throw runtime_error("Out of memory while reallocating to goal");
    if (gVerbose) {
        cerr << str() << ": realloc to " << goal << endl;
    }
    _dataPtr = _bufPtr;
    _bufLen = goal;
}

void ChunkBufferedFile::write(const void* src, size_t slen)
{
    assert(_dataPtr == _bufPtr);
    size_t remaining = _bufLen - _dataLen;
    if (remaining < slen) {
        resize(slen);
        remaining = _bufLen - _dataLen;
    }
    ::memcpy(&_bufPtr[_dataLen], src, slen);
    _dataLen += slen;
    _dirty = true;
}

void ChunkBufferedFile::getBuf(char*& outBuf, size_t& outLen, size_t need)
{
    assert(_dataPtr == _bufPtr);
    size_t remaining = _bufLen - _dataLen;
    if (remaining == 0 || need > remaining) {
        resize(need ? need : 1);
        remaining = _bufLen - _dataLen;
        assert(remaining);
        assert(!need || need <= remaining);
    }
    assert(remaining);
    outBuf = &_bufPtr[_dataLen];
    outLen = remaining;
}

void ChunkBufferedFile::putBuf(char* bufp)
{
    assert(_dataPtr == _bufPtr);
    assert(bufp >= &_bufPtr[_dataLen]);
    assert(bufp < &_bufPtr[_bufLen]);
    _dataLen += bufp - &_bufPtr[_dataLen];
    _dirty = true;
}

void ChunkBufferedFile::putNewline()
{
    assert(_dataPtr == _bufPtr);
    if (_dataLen == _bufLen)
        resize(1);
    _bufPtr[_dataLen++] = '\n';

    // Record length HWM tracking.
    size_t reclen = _dataLen - _eor;
    if (reclen > s_recLenHwm)
        s_recLenHwm = reclen;
    _eor = _dataLen;
    _dirty = true;
}

bool ChunkBufferedFile::flush(size_t nbytes)
{
    if (_dataLen == 0) {
        // Nothing to flush, fully drained.  Just reset stuff.
        // (We know this'll happen at least once per file per
        // call to Splitter::drain().)
        _dataPtr = _bufPtr;
        _eor = 0;
        _dirty = false;
        return false;
    }

    assert(_fp);
    size_t n = std::min(nbytes, _dataLen);
    size_t nwritten = ::fwrite(_dataPtr, 1, n, _fp);
    if (nwritten != n) {
        cerr << gPgm << ": ERROR: Cannot write to " << str()
             << ": " << ::strerror(errno) << endl;
        ::exit(1);
    }
    _dataPtr += n;
    _dataLen -= n;
    return true;
}

void ChunkBufferedFile::maybeCloseEarly()
{
    // We never had any data, and we'll never see any again, so it's
    // time to close the output pipe.
    if (!_dirty && _fp) {
        ::fclose(_fp);
        _fp = NULL;
    }
}

/**
 * Splits the input rows across the buffered per-chunk output files.
 * Write out the chunks only when <splitCount> chunks are buffered,
 * avoiding deadlock conditions.
 */
class Splitter
{
public:
    Splitter();
    ~Splitter();

    Splitter& setOutputBase(const string& baseName) {
        _outputBase = baseName;
        return *this;
    }
    Splitter& setSplitCount(size_t n) {
        _splitCount = n;
        return *this;
    }
    Splitter& setChunkSize(size_t n) {
        _chunkSize = n;
        return *this;
    }
    Splitter& setSkipCount(size_t n) {
        _skipCount = n;
        return *this;
    }
    Splitter& setOutputFormat(const string& s);

    bool open();
    void close();
    void drain();
    void writeRow(const Row& r);

private:
    void          sanityCheck(const Row& r);
    static string encode(const string& s);

    typedef void (Splitter::*MbrFun)(const Row&);
    void        writeTsvRow(const Row&);
    void        writeCsvRow(const Row&);
    void        writeDebugRow(const Row&);

    MbrFun      _writeRowInternal;
    size_t      _splitCount;
    size_t      _chunkSize;
    size_t      _skipCount;
    string      _outputBase;
    vector<CbfPtr> _files;
    vector<string> _fnames;

    // Internal cursor: what file are we on, how many rows of chunk written.
    size_t      _currFile;
    size_t      _rowsWritten;

    // Bookkeeping for sanity checks.
    size_t      _rowsRead;
    size_t      _maxRowLen;
};

Splitter::Splitter()
    : _writeRowInternal(&Splitter::writeCsvRow)
    , _splitCount(0)
    , _chunkSize(0)
    , _skipCount(0)
    , _currFile(0)
    , _rowsWritten(0)
    , _maxRowLen(0)
{ }

Splitter::~Splitter()
{
    close();
}

bool Splitter::open()
{
    assert(!_outputBase.empty());
    assert(_files.empty());
    assert(_rowsWritten == 0);
    assert(_currFile == 0);

    _fnames.resize(_splitCount);
    _files.resize(_splitCount);

    for (unsigned i = 0; i < _splitCount; ++i) {
        stringstream fileName;
        fileName << _outputBase << '_' << setfill('0') << setw(4) << i;
        _fnames[i] = fileName.str();
        FILE *fp = ::fopen(_fnames[i].c_str(), "wb");
        if (!fp) {
            cerr << gPgm << ": ERROR: Cannot open " << _fnames[i] << ": "
                 << ::strerror(errno) << endl;
            return false;
        }
        _files[i].reset(new ChunkBufferedFile(fp, _fnames[i]));
    }
    return true;
}

void Splitter::close()
{
    drain();
    _files.clear();
}

/**
 * Drain buffered output in a way that keeps 'load' and 'input' operators happy.
 *
 * @description Internally, the several SciDB instances are
 * synchronizing on a barrier.  To make sure that they all see data at
 * a nice even rate (and hence don't lock up waiting), we drain the
 * output buffers a pipe-load at a time in round-robin order.
 *
 * @note This should be superior to the old implementation since we
 * use PIPE_BUF buffers instead of writing record by record (and
 * having to track record boundaries, yuck).
 */
void Splitter::drain()
{
    // Empty chunks?  Need to close pipes ASAP to avoid deadlocks.
    for (unsigned i = 0; i < _files.size(); ++i) {
        _files[i]->maybeCloseEarly();
    }

    bool active;
    do {
        active = false;
        for (unsigned i = 0; i < _files.size(); ++i) {
            active |= _files[i]->flush(PIPE_BUF);
        }
    } while (active);
}

/**
 * A row that is way too long probably indicates a quoting problem.
 *
 * @note Inspired by Alex's infamous places2.csv file.
 *
 * @note
 * Unfortunately our input is buffer oriented and not line oriented,
 * so the @c rowsRead count is not necessarily the same as the line
 * number (since CSV records can span lines).
 */
void Splitter::sanityCheck(const Row& row)
{
    static size_t rowsChecked = 0;

    const size_t MIN_SAMPLES = 10;
    if (++rowsChecked < MIN_SAMPLES) {
        // Not enough rows seen to judge what's too long.
        if (row.length() > _maxRowLen) {
            _maxRowLen = row.length();
        }
        return;
    }

    const size_t WAY_TOO_LONG = _maxRowLen * 3;
    if (row.length() > WAY_TOO_LONG) {
        size_t rowsRead = rowsChecked + _skipCount;
        string r(row.str().substr(0,60));
        cerr << gPgm << ": WARNING: Long " << row.length() << "-byte record"
             << " at or near line " << rowsRead
             << " may indicate a quoting error.\n"
             << "Record: " << r << " ..." << endl;
    }
    else if (row.length() > _maxRowLen) {
        _maxRowLen = row.length();
    }
}

Splitter& Splitter::setOutputFormat(const string& fmt)
{
    if (!::strcasecmp(fmt.c_str(), "tsv")) {
        _writeRowInternal = &Splitter::writeTsvRow;
    } else if (!::strcasecmp(fmt.c_str(), "csv")) {
        _writeRowInternal = &Splitter::writeCsvRow;
    } else if (!::strcasecmp(fmt.c_str(), "debug")) {
        _writeRowInternal = &Splitter::writeDebugRow;
    } else {
        cerr << gPgm << ": ERROR: Unrecogized output format '" << fmt << '\''
             << endl;
        ::exit(1);
    }
    return *this;
}

void Splitter::writeRow(const Row& row)
{
    ++_rowsRead;
    if (_skipCount) {
        _skipCount--;
        return;
    }

    sanityCheck(row);
    CALL_MEMBER_FN(*this, _writeRowInternal)(row);

    // Is it time to move on to next file in the rotation?
    if (++_rowsWritten == _chunkSize) {
        _rowsWritten = 0;
        if (++_currFile == _files.size()) {
            // All N buffers full, drain 'em.
            drain();
            _currFile = 0;
        }
    }
}

void Splitter::writeDebugRow(const Row& row)
{
    FILE* fp = _files[_currFile]->getFile();
    string s(row.str());
    size_t n = ::fwrite(s.c_str(), 1, s.size(), fp);
    if (n != s.size()) {
        cerr << gPgm << ": ERROR: Cannot write to " << _fnames[_currFile]
             << ": " << ::strerror(errno) << endl;
        ::exit(1);
    }
}

void Splitter::writeCsvRow(const Row& row)
{
    size_t n = 0;
    char* bufp = 0;
    size_t buflen = 0;
    CbfPtr file = _files[_currFile];

    for (unsigned i = 0; i < row.field_count(); ++i) {
        if (i) {
            // Ensure enuf space for this comma and at least one more byte.
            if (buflen < 2) {
                file->putBuf(bufp); // Record our progress so far.
                file->getBuf(bufp, buflen, row.length()); // Get more space!
            }
            *bufp++ = ',';
            --buflen;
            file->putBuf(bufp);
        }

        // Ensure enuf space for this field, assuming lots of quoting.
        file->getBuf(bufp, buflen, 2 * (1 + row[i].size()));

        if (csvShouldQuote(i, row[i])) {
            n = csv_write2(bufp, buflen, row[i].c_str(), row[i].size(), OUTPUT_QUOTE);
        } else {
            // Raw write-to-buffer.
            n = row[i].size();
            assert(n <= buflen);
            ::memcpy(bufp, row[i].c_str(), n);
        }

        assert(n <= buflen);
        bufp += n;
        buflen -= n;
        file->putBuf(bufp);
    }

    file->putNewline();
}

void Splitter::writeTsvRow(const Row& row)
{
    // Compose the TSV row.
    stringstream ss;
    for (unsigned i = 0; i < row.field_count(); ++i) {
        if (i) ss << '\t';
        if (row[i].find_first_of(TSV_ESCAPED_CHARS) != string::npos) {
            ss << encode(row[i]);
        } else {
            ss << row[i];
        }
    }
    string tsvRow(ss.str());

    // Write it.  Easy.
    _files[_currFile]->write(tsvRow.c_str(), tsvRow.size());
    _files[_currFile]->putNewline();
}

string Splitter::encode(const string& s)
{
    stringstream ss;
    for (unsigned i = 0; i < s.size(); ++i) {
        switch (s[i]) {

        // These characters MUST be encoded per the TSV standard.
        // See http://dataprotocols.org/linear-tsv/ .
        case '\t':      ss << "\\t";    break;
        case '\n':      ss << "\\n";    break;
        case '\r':      ss << "\\r";    break;
        case '\\':      ss << "\\\\";   break;
        default:
            ss << s[i];
            break;
        }
    }
    return ss.str();
}

/**
 * Examine the first buffer of input to learn important things.
 *
 * @description In particular we want to know what quoting style and
 * delimiter is used in the input, and what the length of a long input
 * line might be.  We want to program the CSV parser to expect the
 * correct quote character, and we want to make a reasonable first
 * guess at how much memory will be needed to buffer up a chunk's
 * worth of lines.
 *
 * @param buf [IN] the input buffer
 * @param len [IN] the length of the input buffer
 * @param quote [OUT] first quote char found in buffer, or '\0'
 * @param delim [OUT] most likely delimiter character being used
 * @param maxLineLen [OUT] length of longest line seen in the buffer
 */
void studyInputBuffer(const char *buf,
                      size_t len,
                      unsigned char& quote,
                      unsigned char& delim,
                      size_t& maxLineLen)
{
    assert(buf);
    assert(len);
    quote = '\0';
    delim = '\0';
    maxLineLen = 0;

    // Counters for comma, tab, or pipe delimiter.
    unsigned commas = 0, tabs = 0, pipes = 0;

    size_t llen = 0;
    size_t lastNewline = 0;
    unsigned char firstQuote = '\0';

    size_t i = 0;
    for (const char *cp = buf; i < len; ++cp, ++i) {
        switch (*cp) {
        case '\n':
            llen = i - lastNewline;
            if (llen > maxLineLen)
                maxLineLen = llen;
            lastNewline = i;
            break;
        case '\'':
        case '"':
            if (!firstQuote)
                firstQuote = *cp;
            break;
        case '|':
            pipes += 1;
            break;
        case '\t':
            tabs += 1;
            break;
        case ',':
            commas += 1;
            break;
        }
    }
    quote = firstQuote;

    // Buffer ends in midst of a really huge line?
    llen = i - lastNewline;
    if (llen > maxLineLen)
        maxLineLen = llen + (llen>>2);

    // Simple heuristic: whichever candidate delimiter was seen most,
    // must be the actual delimiter.  Ties favor "more popular" delimiter.
    unsigned votes = commas;
    delim = ',';
    if (tabs > votes) {
        delim = '\t';
        votes = tabs;
    }
    if (pipes > votes)
        delim = '|';

    if (gVerbose) {
        cerr << "Guessing quote=" << quote << " delim='" << delim
             << "' maxline=" << maxLineLen << endl;
    }
}

/** Application state passed to libcsv parser's callbacks. */
struct ParseState {
    ParseState() : records(0) {}
    int records;
    Row currentRow;
    Splitter splitter;
};

/** Per-field callback for libcsv parser. */
void fieldCbk(void* s, size_t n, void* state)
{
    ParseState* ps = static_cast<ParseState*>(state);
    string field;
    if (s)
        field = static_cast<const char*>(s);
    ps->currentRow.push_back(field);
}

/** Per-record callback for libcsv parser. */
void recordCbk(int endChar, void* state)
{
    ParseState* ps = static_cast<ParseState*>(state);
    ps->records++;

    // Changing field count is worth a warning... but not too many.
    static unsigned fields = ~0U;
    static unsigned warnings = 0;
    const unsigned MAX_WARNINGS = 8;
    if (fields == ~0U) {
        fields = ps->currentRow.field_count();
    } else if (warnings < MAX_WARNINGS) {
        if (fields != ps->currentRow.field_count()) {
            cerr << gPgm << ": WARNING: Field count changed from "
                 << fields << " to " << ps->currentRow.field_count()
                 << " at input record " << ps->records;
            if (++warnings == MAX_WARNINGS) {
                cerr << " (Done complaining about this!)";
            }
            cerr << endl;
            fields = ps->currentRow.field_count();
        }
    }

    // Write the row to the correct output file.
    ps->splitter.writeRow(ps->currentRow);
    ps->currentRow.clear();
}

/**
 * Tell the libcsv parser what constitutes a space character.
 *
 * @description The parser ordinarily removes spaces and tabs from the
 * beginning and end of unquoted fields.  Apparently this is
 * undesirable, so we supply this callback to indicate that unquoted
 * fields should be left as is.
 *
 * @see Ticket #4353.
 */
int spaceFunc(unsigned char ch)
{
    return 0;
}

void splitCsvFile(FILE* fp)
{
    csv_parser parser;

    int rc = csv_init(&parser, CSV_APPEND_NULL);
    if (rc != 0) {
        cerr << gPgm << ": csv_init: " << rc << endl;
        ::exit(rc);
    }
    csv_set_space_func(&parser, spaceFunc);
    if (gDelim) {
        csv_set_delim(&parser, gDelim);
    }
    if (gInputQuote) {
        csv_set_quote(&parser, gInputQuote);
    }
    ChunkBufferedFile::setChunkSize(gChunkSize);

    // NOTE we do *not* open the splitter just yet!
    ParseState state;
    state.splitter
        .setOutputBase(gOutputBase)
        .setChunkSize(gChunkSize)
        .setSplitCount(gSplitCount)
        .setSkipCount(gSkip)
        .setOutputFormat(gOutFormat);

    const size_t SZ = 8 * KiB;
    char buf[SZ];
    size_t nparse, nread;
    bool firstLoop = true;
    while ((nread = ::fread(&buf[0], 1, SZ, fp)) > 0) {

        // Ponder the first input buffer and learn things.
        if (firstLoop) {
            firstLoop = false;
            unsigned char quote, delim;
            size_t longLine;
            studyInputBuffer(&buf[0], nread, quote, delim, longLine);
            if (!gInputQuote)
                gInputQuote = quote ? quote : csv_get_quote(&parser);
            csv_set_quote(&parser, gInputQuote);
            if (delim && !gDelim)
                csv_set_delim(&parser, delim);
            ChunkBufferedFile::setLineHint(longLine);
            // Delay open until line hint is known.
            if (!state.splitter.open()) {
                // Problem already logged.
                ::exit(2);
            }
        }

        nparse = csv_parse(&parser, buf, nread, fieldCbk, recordCbk, &state);
        if (nparse != nread) {
            cerr << gPgm << ": CSV parse error, record " << state.records
                 << " near field " << state.currentRow.field_count()
                 << " '" << state.currentRow.back() << "': "
                 << csv_strerror(csv_error(&parser)) << endl;
            ::exit(2);
        }
    }

    csv_fini(&parser, fieldCbk, recordCbk, &state);
    csv_free(&parser);
    state.splitter.close();
}

// Allow single ordinary character or \t for <TAB>.
unsigned char parseDelim(const char* arg)
{
    if (arg == NULL || *arg == '\0')
        throw runtime_error("Bad delimiter");
    if (arg[0] == '\\') {
        if (arg[1] == 't' && arg[2] == '\0')
            return '\t';
        throw runtime_error(string("Bad delimiter: ") + arg);
    }
    if (arg[1] != '\0')
        throw runtime_error(string("Bad delimiter: ") + arg);
    return arg[0];
}

void printUsage()
{
    cout <<
        "Utility to split a CSV file into smaller files.\n"
        "USAGE: " << gPgm << " -n NUMBER [-c CHUNK] [-s SKIP] [-i INPUT] [-o OUTPUT]\n"

        " -n N, --split-count N\n"
        "    Number of files to split the input file into.  Required.\n"

        " -c CHUNK, --chunksize CHUNK\n"
        "    Chunk size (Default = 1).\n"

        " -f FORMAT, --format FORMAT\n"
        "    Output format, one of: tsv, csv (Default = csv).\n"

        " -s SKIP, --skip-lines SKIP\n"
        "    Number of lines to skip from the beginning of the input\n"
        "    file (Default = 0).\n"

        " -i INPUT, --input INPUT\n"
        "    Input file. (Default = stdin).\n"

        " -o OUTPUT, --output-base OUTPUT\n"
        "    Output file base name. (Default = INPUT or \"" << DEFAULT_OUTPUT_BASE << "\").\n"

        " -t PATTERN, --type-pattern PATTERN\n"
        "    Type pattern:  N number, S string, s nullable-string, C char, c nullable-char\n"

        " -v, --verbose\n"
        "    Turn on more logging to stdout.\n"

        " --single-quote, --double-quote\n"
        "   Force input quoting style.  Normally " << gPgm << " guesses based on first read.\n"

        " -h, --help\n"
        "    Print this help message.\n"
         << flush;
}

// Don't forget to change both short_ and long_options!
const char* short_options = "c:d:f:hi:n:o:s:t:v";
struct option long_options[] = {
    { "chunk-size",   required_argument, 0, 'c' },
    { "delim",        required_argument, 0, 'd' },
    { "format",       required_argument, 0, 'f' },
    { "help",         no_argument,       0, 'h' },
    { "input",        required_argument, 0, 'i' },
    { "split-count",  required_argument, 0, 'n' },
    { "output-base",  required_argument, 0, 'o' },
    { "skip-lines",   required_argument, 0, 's' },
    { "type-pattern", required_argument, 0, 't' }, // XXX -t same as loadcsv but not csv2scidb
    { "verbose",      no_argument,       0, 'v' },

    // These long options intentionally have no short equivalents, so
    // use control characters to avoid future conflicts.
    { "single-quote", no_argument,       0, '\001' },
    { "double-quote", no_argument,       0, '\002' },
    {0, 0, 0, 0}
};

int main(int ac, char** av)
{
    gPgm = av[0];
    string::size_type slash = gPgm.find_last_of("/");
    if (slash != string::npos)
        gPgm = gPgm.substr(slash + 1);

    char c;
    int optidx = 0;
    try {
        while (1) {
            c = ::getopt_long(ac, av, short_options, long_options, &optidx);
            if (c == -1)
                break;
            switch (c) {
            case 'c':
                gChunkSize = lexical_cast<size_t>(optarg);
                break;
            case 'd':
                gDelim = parseDelim(optarg);
                break;
            case 'f':
                gOutFormat = (optarg ? optarg : "");
                break;
            case 'h':
                printUsage();
                return 0;
            case 'i':
                gInFile = optarg;
                break;
            case 'n':
                gSplitCount = lexical_cast<size_t>(optarg);
                break;
            case 'o':
                gOutputBase = optarg;
                break;
            case 's':
                gSkip = lexical_cast<size_t>(optarg);
                break;
            case 't':
                gTypePattern = (optarg ? optarg : "");
                break;
            case 'v':
                gVerbose = true;
                break;
            case '\001':
                gInputQuote = '\'';
                break;
            case '\002':
                gInputQuote = '"';
                break;
            default:
                printUsage();
                return 2;
            }
        }
    }
    catch (boost::bad_lexical_cast& exc) {
        cerr << gPgm << ": Bad or missing option value: " << exc.what() << endl;
        return 2;
    }
    catch (std::exception& exc) {
        cerr << gPgm << ": Option parsing error: " << exc.what() << endl;
        return 2;
    }

    // Input file can be specified as an ordinary non-option argument,
    // but like the Highlander, there can be only one!  Kind of an
    // artificial restriction, but it's backward compatible.
    //
    if ((optind + 1) == ac) {
        if (!gInFile.empty()) {
            cerr << gPgm << ": Too many input files: " << av[optind]
                 << ", " << gInFile << endl;
            return 2;
        }
        gInFile = av[optind];
    }
    else if ((optind + 1) < ac) {
        cerr << gPgm << ": Too many input files: " << av[optind]
             << ", " << av[optind+1] << ", ..." << endl;
        return 2;
    }

    // For unset options that have environment vars, set them now.
    if (gSplitCount == BAD_SIZE) {
        char *cp = ::getenv("SCIDB_INSTANCE_NUM");
        if (cp) {
            try {
                gSplitCount = lexical_cast<size_t>(cp);
            }
            catch (boost::bad_lexical_cast& exc) {
                cerr << gPgm << ": Bad SCIDB_INSTANCE_NUM '" << cp << "': "
                     << exc.what() << "\nUse explicit -n/--split-count option."
                     << endl;
                return 2;
            }
        }
    }

    // Validate options and arguments.
    if (gInFile.empty()) {
        gInFile = "-";          // stdin
    }
    if (gSplitCount == BAD_SIZE || gSplitCount == 0) {
        cerr << gPgm << ": Assuming --split-count=1" << endl;
        gSplitCount = 1;
    }
    if (gChunkSize == 0) {
        cerr << gPgm << ": Chunk size of zero is meaningless" << endl;
        return 2;
    }
    if (gOutputBase.empty()) {
        if (gInFile == "-") {
            gOutputBase = DEFAULT_OUTPUT_BASE;
        } else {
            gOutputBase = gInFile;
        }
    }
    if (gVerbose) {
        cout <<   "chunk-size : " << gChunkSize
             << "\ninput-file : " << gInFile
             << "\nsplit-count: " << gSplitCount
             << "\noutput-base: " << gOutputBase
             << "\nskip-lines : " << gSkip
             << "\nin-delim   : '" << gDelim << '\''
             << "\nout-format : " << gOutFormat
             << "\ntype-pattrn: '" << gTypePattern << '\''
             << endl;
    }

    //
    //  Here it is!  Open the input file and do the work!!
    //
    FILE* fp = 0;
    if (gInFile == "-") {
        fp = ::stdin;
    } else {
        fp = ::fopen(gInFile.c_str(), "rb");
        if (!fp) {
            cerr << gPgm << ": fopen: " << gInFile << ": " << ::strerror(errno)
                 << endl;
            return 2;
        }
    }
    splitCsvFile(fp);
    ::fclose(fp);

    return 0;
}
