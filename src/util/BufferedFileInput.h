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
 * BufferedFileInput.h
 *
 * Description: Buffered scan of an input file.
 * @author: donghui
 * Created on: May 7, 2012
 *
 */
#ifndef BUFFERED_FILE_INPUT_H
#define BUFFERED_FILE_INPUT_H

#include <stdio.h>
#include <ctype.h>
#include <inttypes.h>
#include <limits.h>
#include <string>
#include <errno.h>

#include "boost/format.hpp"

#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/helpers/exception.h"

#include "system/Exceptions.h"
#include "query/TypeSystem.h"
#include "query/FunctionDescription.h"
#include "query/FunctionLibrary.h"
#include "query/Operator.h"
#include "array/DBArray.h"
#include "smgr/io/Storage.h"
#include "system/SystemCatalog.h"
#include "util/BufferedFileInput.h"

namespace scidb
{

/*
 * A class that enables myGetc() and myUngetc() over a file.
 *
 * Restriction 1: myUngetc() cannot be followed by aother myUngetc().
 * Restriction 2: if myGet() returns EOF, no further calls of myGet() or myUnget() can be made.
 * Restriction 3: myUngetc(ch) pushes back the char that was returned by the latest call to myGetc().
 * Restriction 4: only a single thread can make myGetc() or myUngetc() calls.
 *
 * Note: Restrictions 1 & 2 &3 are 'protected' by assert(), but Restriction 4 is not!
 *
 * Double buffering is used to allow a buffer of data to be processed while the other buffer is being loaded.
 *
 * TO-DO:
 *  1. Fix timing problem that can result in premature EOFs.
 *  2. Add line-oriented API to replace getline(3) calls in TSV and CSV loaders.
 *  3. Move initial I/O out of constructor... defer until first read.
 */
class BufferedFileInput
{
public: // public data structure.
    /**
     * The state of the BufferedFileInput object.
     */
    enum State
    {
        UNINITIALIZED,  // Right after construction, the object is UNINITIALIZED.
        SERVING,        // When the first call to myGetc() is made, the object is SERVING.
        TERMINATED      // When the object is TERMINATED, it cannot serve any further calls.
    };

private: // private structures.
    /*
     * A single buffer.
     */
    class Buffer
    {
    public:
        // whether data have been loaded to the buffer
        // The other fields are meaningless if loaded==false.
        bool _loaded;

        // the buffer
        boost::scoped_array<char> _buffer;

        // the char from an unget()
        // Note: we assume unget() cannot be followed immediately by another unget()
        char _fromUnget;

        // how many bytes (not including 'fromUnget' in total are in use?
        int64_t _sizeTotal;

        // the address of the char to return next;
        // index==-1 means to return 'fromUnget'
        int64_t _index;

        // Mutex
        Mutex _mutex;

        // event that the loader thread waits for
        Event _eventBlockingLoader;

        // event that the processing thread waits for
        Event _eventBlockingScanner;

        // whether the loader is waiting
        bool _loaderIsWaiting;

        // whether the scanner is waiting
        bool _scannerIsWaiting;

        // Whether error has occurred when the loader reads the file.
        // Values are 0: no error, > 0: errno.
        // This is set in the loader thread, so that the scanner can pick it up and throw.
        int _readFileError;

        Buffer();
    };

    /*
     * The thread to keep filling the next buffer until eof is reached.
     * The thread will wait if both buffers are loaded.
     */
    class FillBufferJob : public Job
    {
    private:
        Buffer* _buffers;
        FILE* _f;
        int64_t _bufferSize;
        BufferedFileInput::State* _pState;

    public:
        FillBufferJob(
                boost::shared_ptr<Query> const& query,
                Buffer* buffers,
                FILE* f,
                int64_t bufferSize,
                BufferedFileInput::State* pState);
        virtual void run();
    };

private: // private members.
    // the state.
    State _state;

    // the file pointer
    FILE* _f;

    // which of the two buffers can be used to support the next call to myGetc()?
    short _which;

    // buffer size in bytes
    int64_t _bufferSize;

    // the query
    boost::weak_ptr<Query> _query;

#ifndef NDEBUG
    // _debugOnlyNoMoreCalls is used only in debug mode, to assert that, once myGetc() returns EOF,
    // no more calls to either myGetc() or myUngetc() can be made.
    bool _debugOnlyNoMoreCalls;
#endif

    // the two buffers
    Buffer _buffers[2];

    // the fill-buffer job
    // It is stored so that the destructor will wait for the job to finishes
    boost::shared_ptr<FillBufferJob> _fillBufferJob;

    /*
     * The non-inlined part of myGetc().
     * The reason to separate the inlined part and non-inlined part is to ensure fast execution in normal case,
     * without enlengthening the executable size unnecessarily.
     *
     * @return  the next character; or EOF if end of file has reached.
     */
    char myGetcNonInlinedPart();

    /**
     * The initialize routine which must be called before myGetc() or myUngetc() may be used.
     * @return whether the initialization is successful.
     */
    bool initialize();

    /*
     * The method to return the size (in bytes) of one buffer
     */
    static int64_t bufferSize() {
        int64_t size = Config::getInstance()->getOption<int>(CONFIG_LOAD_SCAN_BUFFER) * MiB;
        if (size <= 0) {
            size = KiB;
        }
        return size;
    }

public: // public members.
    /*
     * Constructor.
     */
    BufferedFileInput(FILE* f, boost::shared_ptr<Query>& query);

    /*
     * Destructor.
     * The scanner thread needs to wait till the fillBufferJob finishes.
     * But before waiting, it should tell the fillBufferJob to quit, and wake up the fillBufferJob, if apply.
     */
    ~BufferedFileInput();

    /*
     * Extract a single character.
     * @return  the next character; or EOF if end of file has reached.
     *
     */
    inline char myGetc()
    {
        assert(!_debugOnlyNoMoreCalls);
        if (_state==UNINITIALIZED) {
            if (! initialize()) {
                return EOF;
            }
        }

        Buffer& theBuffer = _buffers[_which];
        int64_t index = theBuffer._index;

        // the normal case
        if (index>=0 && index<theBuffer._sizeTotal) {
            ++ theBuffer._index;
            return theBuffer._buffer[index];
        }

        // the other cases
        return myGetcNonInlinedPart();
    }

    /*
     * myUngetc: return a char back to the stream.
     */
    inline void myUngetc(char c)
    {
        assert(!_debugOnlyNoMoreCalls);
        ASSERT_EXCEPTION(_state!=UNINITIALIZED, "No one should call myUngetc() before myGetc() in BufferedFileInput.");

        Buffer& theBuffer = _buffers[_which];
        int64_t index = theBuffer._index;
        assert(index>=0);
        if (index>0) {
            assert(theBuffer._buffer[index-1] == c);
        }
        else {
            theBuffer._fromUnget = c;
        }
        -- theBuffer._index;
    }
};

} //namespace scidb

#endif /* BUFFERED_FILE_INPUT_H */

