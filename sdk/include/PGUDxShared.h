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
 *
 * Description: Support code for UDx subsystem
 *
 * Create Date: Feb 10, 2011
 */
#ifndef PG_UDX_SHARED_H
#define PG_UDX_SHARED_H

/*
 * Object ID is a fundamental type in Postgres.
 */
typedef unsigned long long int Oid;

#define VARHDRSZ		4               // ((int32) sizeof(int32))

/* Numeric SQL type support */
#define NUMERIC_MAX_PRECISION 1024
/* construct a typmod from precision and scale*/
#define TYPMODFROMPRECSCALE(p, s) ((((p) << 16) | (s)) + VARHDRSZ)
/* derive a precision from a typmod */
#define PRECISIONFROMTYPMOD(t) (((t) < VARHDRSZ) ? 38 : ((t) >> 16) & 0xffff)
/* derive a scale from a typmod */
#define SCALEFROMTYPMOD(t) (((t) < VARHDRSZ) ? 15 : ((t) - VARHDRSZ) & 0xffff)

#ifdef __cplusplus
#define STATIC_CAST(type, thing) static_cast<type>(thing)
#define REINTERPRET_CAST(type, thing) reinterpret_cast<type>(thing)
#else
#define STATIC_CAST(type, thing) ((type)(thing))
#define REINTERPRET_CAST(type, thing) ((type)(thing))
#endif

/* Decorate 64-bit constants */
#define INT64CONST(x)  STATIC_CAST(int64, x##LL)
#define UINT64CONST(x) STATIC_CAST(uint64, x##ULL)

#endif
