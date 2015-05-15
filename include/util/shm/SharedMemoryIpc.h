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
 * @file SharedMemoryIpc.h
 *      SharedMemoryIpc class provides an interface to shared memory IPC
 */

#ifndef SHAREDMEMORYIPC_H_
#define SHAREDMEMORYIPC_H_

#include <stdint.h>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <util/StringUtil.h>

namespace scidb
{
/**
 * SharedMemoryIpc is an abstraction for using OS shared memory functionality such as shm_open.
 * (An identical interface can be used for memory mapped files).
 */
class SharedMemoryIpc
{
 public:
    class InvalidStateException : public std::exception
    {
    public:
        InvalidStateException(const char* file, const char* function, int32_t line)
        : _file(file), _function(function), _line(line)
        {
        }
        ~InvalidStateException() throw () {}
        void raise() const { throw *this; }
        const std::string& getFile() const { return _file; }
        const std::string& getFunction() const { return _function; }
        int32_t getLine() const { return _line; }
        virtual const char* what() const throw()
        {
            return "SharedMemoryIpc::InvalidStateException";
        }
    private:
        std::string _file;
        std::string _function;
        int32_t _line;
    };

    class SystemErrorException : public std::exception
    {
    public:
        SystemErrorException(int err, const char* file, const char* function, int32_t line)
        : _err(err), _file(file), _function(function), _line(line)
        {
        }
        virtual ~SystemErrorException() throw () {}
        virtual void raise() const { throw *this; }
        virtual const std::string& getFile() const { return _file; }
        virtual const std::string& getFunction() const { return _function; }
        virtual int32_t getLine() const { return _line; }
        virtual int getErrorCode() const {return _err; }
        virtual const char* what() const throw()
        {
            return "SharedMemoryIpc::SystemErrorException";
        }
    private:
        int _err;
        std::string _file;
        std::string _function;
        int32_t _line;
    };

    class AlreadyExistsException : public SystemErrorException
    {
    public:
        AlreadyExistsException(int err, const char* file, const char* function, int32_t line)
        : SystemErrorException(err, file, function, line)
        {
        }
        virtual ~AlreadyExistsException() throw () {}
        virtual void raise() const { throw *this; }
        virtual const char* what() const throw()
        {
            return "SharedMemoryIpc::AlreadyExistsException";
        }
    };

    class NotFoundException : public SystemErrorException
    {
    public:
        NotFoundException(int err, const char* file, const char* function, int32_t line)
        : SystemErrorException(err, file, function, line)
        {
        }
        virtual ~NotFoundException() throw () {}
        virtual void raise() const { throw *this; }
        virtual const char* what() const throw()
        {
            return "SharedMemoryIpc::NotFoundException";
        }
    };

    class NoShmMemoryException : public SystemErrorException, std::bad_alloc
    {
    public:
        NoShmMemoryException(int err, const char* file, const char* function, int32_t line)
        : SystemErrorException(err, file, function, line)
        {
        }
        virtual ~NoShmMemoryException() throw () {}
        virtual void raise() const { throw *this; }
        virtual const char* what() const throw()
        {
            return "SharedMemoryIpc::NoShmMemoryException: unable to allocate shared memory."
            " Try increasing the size of the partition backing the shared memory to accomodate your data,"
            " e.g. 'mount -oremount,size=<#GB_per_host>G /dev/shm'."
            " If /dev/shm overcommits memory, make sure to add swap space as well (see 'man swapon' on Linux).";
        }
    };

    class ShmMapErrorException : public SystemErrorException, std::bad_alloc
    {
    public:
        ShmMapErrorException(int err, const char* file, const char* function, int32_t line)
        : SystemErrorException(err, file, function, line)
        {
        }
        virtual ~ShmMapErrorException() throw () {}
        virtual void raise() const { throw *this; }
        virtual const char* what() const throw()
        {
            return "SharedMemoryIpc::ShmMapErrorException: unable to map shared memory."
            " Try increasing the size of the ulimit of the shell from which SciDB was started"
            " or check your config.ini for a 'max-memory-limit=' that is too small.";
        }
    };

    /// Access modes
    typedef enum { RDONLY=boost::interprocess::read_only,
                   RDWR=boost::interprocess::read_write } AccessMode;

    /// Types of shared memory
    enum { SHM_TYPE=0, FILE_TYPE };
    typedef uint32_t SharedMemoryIpcType_t;
public:
    /**
     * Constructor
     * @param name of the form /somename; that  is,  a  null-terminated  string  of  up  to
     *  NAME_MAX (i.e., 255) characters consisting of an initial slash, followed by one or more characters, none of which are slashes.
     */
    SharedMemoryIpc(const std::string& name);
    virtual ~SharedMemoryIpc() {}

    /**
     * Create a shared memory IPC object in a given access mode
     * @param amode read_only or read/write
     * @throw InvalidStateException if this object has already been created/opened
     * @throw AlreadyExistsException if the system shared memory resource with this object's name already exists
     * @throw SystemErrorException if other OS related error occurs
     */
    virtual void create(AccessMode amode) = 0;

    /**
     * Open a shared memory IPC object in a given access mode
     * @param amode read_only or read/write
     * @throw InvalidStateException if this object has already been created/opened
     * @throw NotFoundException if the system shared memory resource with this object's name cannot be found
     * @throw SystemErrorException if an OS related error occurs
     */
    virtual void open(AccessMode amode) = 0;

    /**
     * Clean up system resources related to this object
     * but keep the memory valid if already in use
     * @note In POSIX terms close the file descriptor, but keep mmaped memory intact
     */
    virtual void close() = 0;

    /**
     * Release/unmap the shared memory region created by this object
     * @note It invalidates the memory region previously returned by get().
     *       Thus, any  memory accesses to that region will cause a crash.
     * @note In POSIX terms close the file descriptor, but keep mmaped memory intact
     */
    virtual void unmap() = 0;

    /**
     * Set the size of this memory object.
     * @param size of this memory object
     * @param force if set to true the object is truncated even if the memory has been mapped
     *        As a result, the memory address previously returned by get() may no longer be valid after this call.
     *        The behavior of this object is undefined and the memory returned by this object is invalid if force==true.
     *        size=0 and force=true can be used to force the other processes using the shared memory die upon next memory access.
     * @throw InvalidStateException if this object has already been closed
     * @throw InvalidStateException if the memory has already been mapped and changing the size is no longer allowed
     * @throw SystemErrorException if an OS related error occurs
     * @note In POSIX terms, truncate is called on the file descriptor
     */
    virtual void truncate(uint64_t size, bool force=false) = 0;

    /// Get the name of this object
    virtual const std::string& getName() const
    {
        return _name;
    }

    /**
     * Get the size of this object if already openned/created
     * @throw InvalidStateException if this object has not been created/opened
     * @note truncate() can change this value
     * @return size of the shared memory
     */
    virtual uint64_t getSize() const = 0;

    /**
     * Get the access mode used in open() or create()
     * @throw InvalidStateException if this object has not been created/opened
     * @return access mode
     */
    virtual AccessMode getAccessMode() const = 0;

    /**
     * Get the memory address of the first byte of this shared memory
     * @return memory address of the shared memory
     * @throw InvalidStateException if this object has not been created/opened
     * or has been close()'d before the first call to get()
     * @throw SystemErrorException if an OS related error occurs
     * @note In POSIX terms, mmap the whole region (file)
     */
    virtual void* get() = 0;

    /**
     * Flush the memory contents (if backed by files)
     * @return true if flush was successfuly initiated
     */
    virtual bool flush() = 0;

    /**
     * Remove the shared memory object from the namespace
     * @note Neither close() nor ~SharedMemoryIpc will remove the object from the OS
     * @return true if successfully removed
     */
    virtual bool remove() = 0;

 private:
    SharedMemoryIpc();
    SharedMemoryIpc(const SharedMemoryIpc&);
    SharedMemoryIpc& operator=(const SharedMemoryIpc&);

    std::string _name;
};

/**
 * Implementation of SharedMemoryIpc based on shared memory objects (created in /dev/shm, with shm_open)
 */
class SharedMemory : public SharedMemoryIpc
{
public:
    explicit SharedMemory(const std::string& name, bool prealloc=false)
    : SharedMemoryIpc(name), _isPreallocate(prealloc)
    { }
    virtual ~SharedMemory();
    virtual void create(AccessMode amode);
    virtual void open(AccessMode amode);
    virtual void close();
    virtual void unmap();
    virtual void truncate(uint64_t size, bool force=false);
    virtual uint64_t getSize() const ;
    virtual AccessMode getAccessMode() const ;
    virtual void* get();
    virtual bool flush();
    virtual bool remove();
    /**
     * Remove the named shared memory object from the namespace
     * @param name
     * @return true if successfully removed
     */
    static bool remove(const std::string& name)
    {
        // erase previous shared memory
        return boost::interprocess::shared_memory_object::remove(name.c_str());
    }

 private:
    SharedMemory();
    SharedMemory(const SharedMemory&);
    SharedMemory& operator=(const SharedMemory&);

    /**
     * Pre-allocate the space in the /dev/shm partition
     *  to avoid running out of space later and being killed by SIGBUS
     */
    void preallocateShmMemory();

    boost::scoped_ptr<boost::interprocess::shared_memory_object> _shm;
    boost::scoped_ptr<boost::interprocess::mapped_region> _region;
    bool _isPreallocate;
};

///
/// Class for interfaces that require {std,boost}::shared_ptr get() method
///
template<class MemoryType_tt>
class SharedMemoryPtr {
public:
    SharedMemoryPtr(const boost::shared_ptr<SharedMemoryIpc>& shm)
    : _shm(shm),_ptr(0)
    {
        _ptr = reinterpret_cast<MemoryType_tt*>(_shm->get());
        assert(_ptr);
    }
    ~SharedMemoryPtr() { }
    MemoryType_tt* get() const  { return _ptr; }
private:
    boost::shared_ptr<SharedMemoryIpc> _shm;
    MemoryType_tt* _ptr;
};


/**
 * Implementation of SharedMemoryIpc based on regular files (VFAT filesystem is not supported).
 */
class SharedFile : public SharedMemoryIpc
{
public:

    explicit SharedFile(const std::string& name, bool prealloc=false)
    : SharedMemoryIpc(name), _isPreallocate(prealloc)
    { }
    virtual ~SharedFile();
    virtual void create(AccessMode amode);
    virtual void open(AccessMode amode);
    virtual void close();
    virtual void unmap();
    virtual void truncate(uint64_t size, bool force=false);
    virtual uint64_t getSize() const ;
    virtual AccessMode getAccessMode() const ;
    virtual void* get();
    virtual bool flush();
    virtual bool remove();
    /**
     * Remove the named file
     * @param name
     * @return true if successfully removed
     */
    static bool remove(const std::string& name)
    {
        return boost::interprocess::file_mapping::remove(name.c_str());
    }

 private:
    void createFile();

 private:
    SharedFile();
    SharedFile(const SharedFile&);
    SharedFile& operator=(const SharedFile&);

    /**
     * Pre-allocate the space in the /dev/shm partition
     *  to avoid running out of space later and being killed by SIGBUS
     */
    void preallocateShmMemory();

    boost::scoped_ptr<boost::interprocess::file_mapping> _file;
    boost::scoped_ptr<boost::interprocess::mapped_region> _region;
    bool _isPreallocate;
};

}
#endif
