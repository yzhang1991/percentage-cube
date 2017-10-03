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

/* 
 * Default implementations of UDFS optional API.
 * Compiled together with Vertica.cpp and linked with both server and shared library binary.
 */
#ifndef VERTICA_UDFS_IMPL_H
#define VERTICA_UDFS_IMPL_H

#include <sys/mman.h>

namespace Vertica
{

/**
 * A wrapper around the append(). Attempt to write the specified data to
 * disk. Retry in cases that didn't succeed in writing all data, but that
 * stand a decent chance of succeeding on a retry.
 * Throws on error.
 */
void UDFileOperator::appendWithRetry(const void *buffer, size_t size)
{
    size_t r = 0;
    uint8_t const *buf = (const uint8_t *)buffer;  // Waste a variable to let us keep the 'const' in the type signature
    while (size > 0)
    {
        try {
            r = append(buf, size);
        }
        catch (udf_exception &e) {
            // There are probably more cases here, based on the manpage,
            // but hopefully this is a start
            
            // It's possible for an interrupt to cause a system call to
            // abort for no reason other than "there was an interrupt,
            // we can't deal with a syscall right now".  So try again.
            if (e.errorcode == EINTR) continue;
            // Otherwise... a real failure
            throw e;
        }

        VIAssert(r > 0);
        // If we're here, we did a partial write.
        // Try again, to write anything that's left.
        buf += r;
        size -= r;
    }
}

void *UDFileOperator::mmap(void *addr, size_t length, int prot, int flags, off_t offset)
{
    vt_throw_exception(UNSUPPORTED_ERROR, __FUNCTION__ + std::string(" not supported"), __FILE__, __LINE__);
    return MAP_FAILED;
}

void UDFileOperator::munmap(void *addr, size_t length)
{
    vt_throw_exception(UNSUPPORTED_ERROR, __FUNCTION__ + std::string(" not supported"), __FILE__, __LINE__);
}

void UDFileOperator::fsync()
{
    vt_throw_exception(UNSUPPORTED_ERROR, __FUNCTION__ + std::string(" not supported"), __FILE__, __LINE__);
}

void UDFileOperator::doHurryUp()
{
    // no-op
}

bool UDFileOperator::eof()
{
    vt_throw_exception(UNSUPPORTED_ERROR, __FUNCTION__ + std::string(" not supported"), __FILE__, __LINE__);
    return false;
}

void UDFileSystem::link(const char *oldpath, const char *newpath) const
{
    vt_throw_exception(UNSUPPORTED_ERROR, __FUNCTION__ + std::string(" not supported"), __FILE__, __LINE__);
}

void UDFileSystem::symlink(const char *oldpath, const char *newpath) const
{
    vt_throw_exception(UNSUPPORTED_ERROR, __FUNCTION__ + std::string(" not supported"), __FILE__, __LINE__);
}

bool UDFileSystem::supportsDirectorySnapshots() const
{
    return false;
}
bool UDFileSystem::snapshotDirectory(const std::string &path, std::string &snapshot_handle) const
{
    vt_throw_exception(UNSUPPORTED_ERROR, __FUNCTION__ + std::string(" not supported"), __FILE__, __LINE__);
    return false;
}
bool UDFileSystem::restoreSnapshot(const std::string &snapshot_handle, const std::string &path) const
{
    vt_throw_exception(UNSUPPORTED_ERROR, __FUNCTION__ + std::string(" not supported"), __FILE__, __LINE__);
    return false;
}
bool UDFileSystem::deleteSnapshot(const std::string &snapshot_path) const
{
    vt_throw_exception(UNSUPPORTED_ERROR, __FUNCTION__ + std::string(" not supported"), __FILE__, __LINE__);
    return false;
}

}
#endif
