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
 * @file
 *
 * @brief Wrapper for file I/O operations.
 * 
 * @details FileIO is a pass-through layer that gives scidb
 * a chance to gather statistics, handle errors, and manage
 * resources for file i/o operations. There are two choices
 * for using this interface:
 * 
 * 1) Low-level static functions.  These functions wrap
 *    the standard posix fs system-call interface.  They
 *    function in largely the same way, but add error handling
 *    and statistics.  Standard file descriptors are passed
 *    to and returned by this interface.  This is good for
 *    legacy code that manipulate file descriptors on non-
 *    standard file objects such as pipes, stdin/stdout, or
 *    shared memory objects.
 *
 * 2) "File" abstraction.  This class wraps the traditional
 *    file descriptor, and adds an LRU list. The implementation
 *    ensures that a hard configured limit on the number of
 *    open fds is respected.  When the limit is reached files
 *    are closed based on the LRU list, and re-opened when
 *    necessary.  (This class assumes that the path to the
 *    file does not change during the time that it is being
 *    referenced by the File class)  This resource management
 *    is transparent to users of the class.  This class should
 *    be used by code that needs to open and manage a potentially
 *    unlimited number of files.  To use this abstraction
 *    users grab a reference to the singleton clas FileManager
 *    and use it to open individual File objects.
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 * @author sfridella@paradigm4.com
 */

#ifndef __FILE_IO__
#define __FILE_IO__

//
// The macro defintions below are used to switch on 64-bit IO mode
//
#define __EXTENSIONS__
#define _EXTENSIONS
#define _FILE_OFFSET_BITS 64
#if ! defined(HPUX11_NOT_ITANIUM) && ! defined(L64)
#define _LARGEFILE64_SOURCE 1 // access to files greater than 2Gb in Solaris
#define _LARGE_FILE_API     1 // access to files greater than 2Gb in AIX
#endif

#if !defined(O_LARGEFILE) && !defined(aix64) && (!defined(SOLARIS) || defined(SOLARIS64))
#define O_LARGEFILE 0
#endif

#include <boost/shared_ptr.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/tuple/tuple_comparison.hpp>
#include <boost/atomic.hpp>
#include <util/Mutex.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <set>
#include <list>
#include <sys/types.h>
#include <dirent.h>

#include "query/Statistics.h"
#include "util/Singleton.h"

#ifndef SCIDB_CLIENT
#include "util/JobQueue.h"
#include "util/ThreadPool.h"
#endif

namespace scidb
{
    class File;
    class FileManager;

    typedef std::list<File*> FileList;

    class File
    {
    public:

        /* Low-level interface --- static functions
         */

        /**
         * Remove (unlink) file
         * @param file to remove
         * @param raise exception on error (true by default)
         * @return errno on error, 0 otherwise
         * @throws SystemException if the underlying system call fails
         */
        static int remove(char const* filePath, bool raise=true);

        /**
         * Read the contents of a directory
         * @param dirName directory name
         * @param entries directory entries
         * @return errno on error, 0 otherwise
         * @throws SystemException if the underlying system call fails
         */
        static void readDir(const char* dirName, std::list<std::string>& entries);

        /**
         * Create a directory
         * @return true if directory is created or has already existed
         * @param dirPath directory path
         */
        static bool createDir(const std::string& dirPath);

        /**
         * Close file descriptor (restarting after signal interrupt if necessary)
         * @return 0 on success, or -1 otherwise
         */
        static int closeFd(int fd);

        /**
         * Open a file (restarting after signal interrupt if necessary)
         * @return fd or -1
         * @param filePath file path
         * @param flags open flags to pass to ::open()
         *        such as O_APPEND, O_CREAT, O_EXCL, etc.
         */
        static int openFile(const std::string& fileName, int flags);

        /**
         * Close directory
         * @param dirName directory name
         * @param dirp open directory handle
         * @param raise exception on error (true by default)
         * @return errno on error, 0 otherwise
         * @throws SystemException if the underlying system call fails
         */
        static int closeDir(const char* dirName, DIR *dirp, bool raise=true);


    public:

        /* Object interface
         */

        typedef boost::shared_ptr<File> FilePtr;

        /**
         * Write data to the file
         * @param data buffer to write from
         * @param size bytes to write
         * @param file offset at which to write
         * @throws SystemException if < size bytes written
         */
        void writeAll(const void* data, size_t size, uint64_t offs);

        /**
         * Write vector of data to the file (gather write)
         * @param iovs pointer to iovec structures describing data
         * @param niovs size of vector
         * @param offs offset at which to write
         * @throws SystemException if can't write all data
         */
        void writeAllv(const struct iovec* iovs, int niovs, uint64_t offs);

        /**
         * Read data from the file
         * @param data buffer to read into
         * @param size bytes to read
         * @param file offset at which to read
         * @throws SystemException if < size bytes read
         */
        void readAll(void* data, size_t size, uint64_t offs);

        /**
         * Read vector of data to the file (scatter read)
         * @param iovs pointer to iovec structures describing buffers
         * @param niovs size of vector
         * @param offs offset from which to read
         * @throws SystemException if can't read all data
         */
        void readAllv(const struct iovec* iovs, int niovs, uint64_t offs);

        /**
         * Try to read from the file
         * @param data buffer to read into
         * @param size bytes to read
         * @param file offset at which to read
         * @returns number of bytes read
         */
        size_t read(void* data, size_t size, uint64_t offs);

        /**
         * fsync a file (restarting after signal interrupt if necessary)
         * @return 0 on success or -1
         */
        int fsync();

        /**
         * fdatasync a file (restarting after signal interrupt if necessary)
         * @return 0 on success or -1
         */
        int fdatasync();

        /**
         * ftruncate a file (restarting after signal interrupt in necessary)
         * @param len requested len of file
         * @return 0 on success or -1
         */
        int ftruncate(off_t len);

        /**
         * Set an advisory lock on the file (restarting after signal intr)
         * @param flc file lock structure pointer
         * @return 0 on success or -1
         */
        int fsetlock(struct flock* flc);

        /**
         * Stat the file
         * @param st pointer to stat structure to fill in
         * @return 0 on success or -1
         */
        int fstat(struct stat* st);

        /**
         * Mark file to be removed on last close
         */
        void removeOnClose();

        /**
         * Return a const ref to the path
         */
        std::string const& getPath()
            { return _path; }

        /**
         * Close the file immediately
         * @post file object cannot be used again
         * @returns 0 on success or -1
         */
        int close();

        /**
         * Destructor
         * @post file is closed
         */
        ~File();

    private:

        /**
         * Constructor
         * Private constructor.
         */
        File(int fd, const std::string path, int flags, bool temp);

        /**
         * Check if the file had been explicitly close and throw if so
         */
        void checkClosedByUser();

        /* Stack allocated helper that ensures the file is open on construction,
           and unpins the file on destruction
         */
        class FileMonitor
        {
        public:
            FileMonitor(FileManager* fm, File& f);
            ~FileMonitor();
        private:
            FileManager* _fm;
            File&        _f;
        };
            
        /* State for object interface
         */

        /* Data members
         */
        int                      _fd;      // current fd
        const std::string        _path;    // path used to open
        int                      _flags;   // flags passed to open
        bool                     _remove;  // true if the file should be removed on close
        bool                     _closed;  // true if fd explicitly closed by user 
        boost::atomic<uint64_t>  _pin;     // number of current users of this file
        FileList::iterator       _listPos; // location of this entry in _lru or _closed list
        FileManager*             _fm;      // pointer to singleton FileManager instance

        friend class FileManager;
    };


    class FileManager : public Singleton<FileManager>
    {
    public:

        /**
         * Create a (temp) File object (file is removed on close)
         * @param arrName string to use as base for the temp name
         * @param filePath if specified then this path is used for the temp file
         * @return shared pointer to file object or NULL on error
         */
        File::FilePtr createTemporary(std::string const& arrName, 
                                      char const* filePath = NULL);

        /**
         * Open a file (restarting after signal interrupt if necessary)
         * @return shared pointer to file object or NULL on error
         * @param filePath file path
         * @param flags open flags to pass to ::open()
         *        such as O_APPEND, O_CREAT, O_EXCL, etc.
         */
        File::FilePtr openFileObj(const std::string& fileName, 
                                  int flags);

        /**
         * @return full path of the temp directory
         */
        std::string getTempDir();

        /**
         * Constructor -- need to ensure that everything in the temp dir
         * is wiped out
         */
        FileManager();

    private:

        /**
         * Add a new open entry to the lru
         * @pre _filelock is NOT held
         * @param file entry to add
         * @throws SystemException if the lru limit is reached and the whole
         *         lru list is pinned
         */
        void addFd(File& file);

        /**
         * Remove an open entry from the lru or closed list
         * @pre _filelock is NOT held
         * @param file entry to remove
         */
        void forgetFd(File& file);

        /**
         * Check if the entry is open and on the lru list---
         * if not re-open it
         * @pre _filelock is NOT held
         * @throws SystemException if the lru limit is reached and the whole
         *         lru list is pinned.
         */
        void checkActive(File& file);

        /**
         * Check if we have reached the limit of the lru list---
         * if so close the lru element
         * @pre _fileLock is locked
         * @throws SystemException if the lru limit is reached and the whole
         *         lru list is pinned
         */
        void checkLimit();

        /**
         * LRU list of open File objects
         * List of closed File objects
         */
        FileList _lru;
        FileList _closed;

        /**
         * Upper limit on size of LRU list (# of open descriptors)
         */
        uint32_t _maxLru;

        /**
         * Mutex which protects LRU list and file table
         */
        Mutex _fileLock;

        friend class File;
    };

    struct FdCleaner
    {
        FdCleaner(int fd) : _fd(fd) {}
        ~FdCleaner() {
            if (_fd>-1) { File::closeFd(_fd); }
        }
        int _fd;
    };

}

#endif
