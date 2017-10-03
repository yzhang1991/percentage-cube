/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- C++ -*-
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of the Hewlett Packard Enterprise Company nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ****************************/

#ifndef VERTICA_UDFS_H
#define VERTICA_UDFS_H

#include "UdfException.h"
#include <errno.h>
#include <sys/stat.h> // for struct ::stat
#include <sys/statvfs.h> // for struct ::statvfs

#define UNSUPPORTED_ERROR 0 // different from any real definition in errno.h

namespace Vertica
{
class UDFileOperator
{
public:
    virtual size_t read(void *buf, size_t count) = 0;
    virtual size_t append(const void *buf, size_t count) = 0;
    void appendWithRetry(const void *buffer, size_t size);
    virtual off_t seek(off_t offset, int whence) = 0;
    virtual off_t getOffset() const = 0;
    // remaining operations are optional and not required.
    virtual void *mmap(void *addr, size_t length, int prot, int flags, off_t offset);
    virtual void munmap(void *addr, size_t length);
    virtual void fsync();
    virtual void doHurryUp();
    virtual bool eof();

    UDFileOperator() {}
    virtual ~UDFileOperator() {}
};

class UDFileSystem
{
public:
    // override to get better representation in error message/log/system table
    virtual std::string getDescription() const
    {
        return "File System";
    }

    virtual std::string getDisplayName() const
    {
        return "UDFileSystem";
    }

    /* operations with state, go through UDFileOperator layer*/
    // creates a dummy UDFileOperator, used for verification.
    virtual UDFileOperator *open() const = 0;
    // creates a valid UDFileOperator with open semantics.
    virtual UDFileOperator *open(const char *path, int flags, mode_t mode, bool removeOnClose = false) const = 0;
    // closes and destroys the UDFileOperator opened; should flush state and release all resources.
    virtual void close(UDFileOperator *udfo) const = 0;
    virtual void copy(const char *srcpath, const char *dstpath) const = 0;
    /* operations w/o state*/
    virtual void statvfs(const char *path, struct ::statvfs *buf) const = 0;
    // return value & error code should be compliant with POSIX. If a file does not exist, set ENOENT.
    virtual void stat(const char *path, struct ::stat *buf) const = 0;
    virtual void mkdirs(const char *path, mode_t mode) const = 0;
    virtual void rmdir(const char *path) const = 0;
    virtual void rmdirRecursive(const char *path) const = 0;
    virtual void listFiles(const char *path, std::vector<std::string> &result) const = 0; // return vector of relative names of children
    virtual void rename(const char *oldpath, const char *newpath) const = 0;
    virtual void remove(const char *path) const = 0;
    // remaining operations are optional and not required.
    virtual void link(const char *oldpath, const char *newpath) const;
    virtual void symlink(const char *oldpath, const char *newpath) const;

    //snapshot releated operations
    virtual bool supportsDirectorySnapshots() const;
    virtual bool snapshotDirectory(const std::string &path, std::string &snapshot_handle) const ;
    virtual bool restoreSnapshot(const std::string &snapshot_handle, const std::string &path) const ;
    virtual bool deleteSnapshot(const std::string &snapshot_handle) const ;

    // features supported
    // Does this FS have actual directories (AKA S3 does not).
    virtual bool supportsDirectories() const { return true; }
    // return an URI-encoded string, path must be in ligit URI format
    virtual std::string getEncodedURI(const char* path) const
    {
        return path;
    }
    // return an URI-encoded string
    virtual std::string getURIEncodedStr(const char* str) const
    {
        return str;
    }

    // Implement file system specific restrictions on path format
    virtual bool validatePath(const std::string& path) const { return true; }

    // Do canonicalization specific actions to a path (no op usually)
    virtual std::string canonicalizePath(const std::string& path) const { return path; }

    // cstr/dstr
    UDFileSystem() {}
    virtual ~UDFileSystem() {}
};

class UDFileSystemFactory : public UDXFactory
{
public:
    virtual ~UDFileSystemFactory() {}

    virtual UDFileSystem *createUDFileSystem(ServerInterface &srvInterface) = 0;

protected:
    /**
     * @return the object type internally used by Vertica
     */
    virtual UDXType getUDXFactoryType() { return FILESYSTEM; }

public:
    /**
     * UDFileSystems take no input tuples; as such, their prototype must be empty.
     */
    void getPrototype(ServerInterface &srvInterface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType) {}

    /**
     * UDFileSystems return no output; as such, their prototype must be empty.
     */
    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnType) {}
};

} // namespace Vertica

#endif
