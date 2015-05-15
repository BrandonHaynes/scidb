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
 * BufferedFileInput.cpp
 *
 * Description: Buffered scan of an input file.
 * @author: donghui
 * Created on: May 7, 2012
 */

#include "BufferedFileInput.h"

using namespace std;
using namespace boost;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.util.BufferedFileInput"));

/**
 * The ErrorChecker.
 * @param query       the query context.
 * @param pState      a pointer to a BuffereFileInput::State object.
 * @throws scidb::SystemExeption via validateQueryPtr if the query is not valid.
 * @return true if query is valid AND if the state is not TERMINATED.
 */
bool queryAndScannerValid(shared_ptr<Query>& query, BufferedFileInput::State* pState)
{
    Query::validateQueryPtr(query);
    return *pState != BufferedFileInput::TERMINATED;
}

BufferedFileInput::Buffer::Buffer()
: _loaded(false),
  _buffer(new char[BufferedFileInput::bufferSize()]),
  _fromUnget(0),
  _sizeTotal(0),
  _index(0),
  _loaderIsWaiting(false),
  _scannerIsWaiting(false),
  _readFileError(0)
{
}

BufferedFileInput::FillBufferJob::FillBufferJob(
        shared_ptr<Query> const& query,
        Buffer* buffers,
        FILE* f,
        int64_t bufferSize,
        BufferedFileInput::State* pState)
: Job(query),
  _buffers(buffers),
  _f(f),
  _bufferSize(bufferSize),
  _pState(pState)
{
}

BufferedFileInput::BufferedFileInput(FILE* f, boost::shared_ptr<Query>& query)
: _state(UNINITIALIZED),
  _f(f),
  _which(0),
  _bufferSize(bufferSize()),
  _query(query)
  #ifndef NDEBUG
  ,_debugOnlyNoMoreCalls(false)
  #endif
{
}

bool BufferedFileInput::initialize()
{
    assert(_state == UNINITIALIZED);
    _state = SERVING;

    // Disable fread file buffering.
    // We are doing manual buffering (for lookahead). So it does not make sense to have another layer of buffering.
    setbuf(_f, NULL);

    // start the prefetching thread
    PhysicalOperator::getGlobalQueueForOperators()->pushJob( _fillBufferJob =
            shared_ptr<FillBufferJob>(new FillBufferJob(Query::getValidQueryPtr(_query), _buffers, _f, _bufferSize, &_state)) );

    // lock buffer[0]
    Buffer& theBuffer = _buffers[0];
    ScopedMutexLock cs(theBuffer._mutex);

    // wait till the loader thread sets buffers[0]._loaded to be true and signal us
    if (!theBuffer._loaded) {
        try {
            theBuffer._scannerIsWaiting = true;
            Event::ErrorChecker ec = bind(&queryAndScannerValid, Query::getValidQueryPtr(_query), &_state);
            bool signalled = theBuffer._eventBlockingScanner.wait(theBuffer._mutex, ec);
            theBuffer._scannerIsWaiting = false;

            if (!signalled) {
                LOG4CXX_DEBUG(logger, "BufferedFileInput::BufferedFileInput eventBlockingScanner.wait() returned false!");
                return false;
            }
        }
        catch (Exception& e) {
            LOG4CXX_DEBUG(logger, "BufferedFileInput::BufferedFileInput exception in eventBlockingScanner.wait().");
            return false;
        }
    }

    // whether error has occurred in the loader
    if (theBuffer._readFileError) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_PREAD_ERROR)
            << _bufferSize << "(offset)" << "(rc)"
            << ::strerror(theBuffer._readFileError) << theBuffer._readFileError;
    }

    return true;
}

/*
 * Load one buffer.
 */
void BufferedFileInput::FillBufferJob::run()
{
    // i is the buffer ID to fill
    short i = 0;

    // prepare ErrorChecker for Event.wait() in the while loop
    Event::ErrorChecker ec = bind(&queryAndScannerValid, _query, _pState);

    // keep loading the next buffer, till the end of file is reached
    while (true) {
        // get mutex for buffers[i]
        Buffer& theBuffer = _buffers[i];
        ScopedMutexLock cs(theBuffer._mutex);

        // If the buffer was loaded already, wait on eventBlockingLoader.
        if (theBuffer._loaded) {
            try {
                theBuffer._loaderIsWaiting = true;
                bool signalled = theBuffer._eventBlockingLoader.wait(theBuffer._mutex, ec);
                theBuffer._loaderIsWaiting = false;

                if (!signalled) {
                    LOG4CXX_DEBUG(logger, "BufferedFileInput::FillBufferJob::run() eventBlockingLoader.wait() returned false!");
                    return;
                }
            }
            catch (Exception& e) {
                LOG4CXX_DEBUG(logger, "BufferedFileInput::FillBufferJob exception in eventBlockingLoader.wait().");
                return;
            }
        }

        // fill the buffer
        theBuffer._loaded = true;
        theBuffer._sizeTotal = ::fread(theBuffer._buffer.get(), 1, _bufferSize, _f);
        theBuffer._index = 0;
        if (theBuffer._sizeTotal < _bufferSize) {
            // EOF or error.
            int err = errno;    // paranoid
            if (!feof(_f)) {
                assert(err);
                theBuffer._readFileError = err;
            }

            if (theBuffer._scannerIsWaiting) {
                theBuffer._eventBlockingScanner.signal();
            }
            return;
        }

        // wake up the processing thread
        if (theBuffer._scannerIsWaiting) {
            theBuffer._eventBlockingScanner.signal();
        }

        // start loading the next buffer
        i = 1-i;
    }
}

/*
 * Non-inlined part of myGetc().
 */
char BufferedFileInput::myGetcNonInlinedPart() {
    Buffer& theBuffer = _buffers[_which];
    int64_t index = theBuffer._index;

    // if there was an unget
    if (index < 0) {
        assert(index == -1);
        index = 0;
        return theBuffer._fromUnget;
    }

    // assert that the scanner has indeed finished the current buffer
    assert(index == theBuffer._sizeTotal);

    // if eofReached, return;
    if ( index < _bufferSize) {
#ifndef NDEBUG
        _debugOnlyNoMoreCalls = true;
#endif
        return EOF;
    }

    // signal the loader
    // Put in a scope so that the mutex is not held too long.
    {
        // lock
        ScopedMutexLock cs(theBuffer._mutex);

        // signal
        theBuffer._loaded = false;
        if (theBuffer._loaderIsWaiting) {
            theBuffer._eventBlockingLoader.signal();
        }
    }

    // switch to the next buffer
    _which = 1 - _which;
    Buffer& nextBuffer = _buffers[_which];

    // lock the next buffer
    ScopedMutexLock cs2(nextBuffer._mutex);

    // wait till the buffer is LOADED
    if (!nextBuffer._loaded) {
        try {
            nextBuffer._scannerIsWaiting = true;
            Event::ErrorChecker ec = bind(&queryAndScannerValid, Query::getValidQueryPtr(_query), &_state);
            bool signalled = nextBuffer._eventBlockingScanner.wait(nextBuffer._mutex, ec);
            nextBuffer._scannerIsWaiting = false;

            if (!signalled) {
                LOG4CXX_DEBUG(logger, "BufferedFileInput::myGetc() eventBlockingScanner.wait() returned false!");
                return EOF;
            }
        }
        catch (Exception& e) {
            LOG4CXX_DEBUG(logger, "BufferedFileInput::myGetc() exception in eventBlockingScanner.wait().");
            return EOF;
        }
    }

    // whether error has occurred in the loader
    if (theBuffer._readFileError) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_PREAD_ERROR)
            << _bufferSize << "(offset)" << "(rc)"
            << ::strerror(theBuffer._readFileError) << theBuffer._readFileError;
    }

    // call myGetc() on the new buffer
    return myGetc();
}

/*
 * Destructor.
 * The scanner thread needs to wait till the fillBufferJob finishes.
 * But before waiting, it should tell the fillBufferJob to quit, and wake up the fillBufferJob, if apply.
 */
BufferedFileInput::~BufferedFileInput()
{
    if (_state != SERVING) {
        return;
    }
    _state = TERMINATED;

    if (_fillBufferJob) {
        // mark the first buffer
        {
            Buffer& theBuffer = _buffers[0];
            ScopedMutexLock cs(theBuffer._mutex);
            if (theBuffer._loaderIsWaiting) {
                theBuffer._eventBlockingLoader.signal();
            }
        }

        // mark the second buffer
        {
            Buffer& theBuffer = _buffers[1];
            ScopedMutexLock cs(theBuffer._mutex);
            if (theBuffer._loaderIsWaiting) {
                theBuffer._eventBlockingLoader.signal();
            }
        }

        // wait till the fillBufferJob finishes
        _fillBufferJob->wait();
    }
}

} //namespace scidb
