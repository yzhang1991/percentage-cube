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
 * Description: Support code for UDx subsystem
 *
 * Create Date: Feb 10, 2011 
 */

/**
* \internal
* \file   BasicsUDxShared.h
* \brief  Support code for UDx subsystem. 
*/


#ifndef BASICS_UDX_SHARED_H
#define BASICS_UDX_SHARED_H

#include "Oids.h"
#include "PGUDxShared.h"
#include <math.h>
#include <sstream>

#ifndef VERTICA_INTERNAL
namespace Vertica
{
#endif

/**
* \typedef signed char           int8;
* \brief 8-bit integer
*/
typedef signed char           int8;
/**
* \typedef unsigned char         uint8;
* \brief unsigned 8-bit integer
*/
typedef unsigned char         uint8;
/**
* \typedef signed short          int16;
* \brief signed 16-bit integer
*/
typedef signed short          int16;
/**
* \brief unsigned 16-bit integer
*/
typedef unsigned short        uint16;
/**
* \brief signed 32-bit integer
*/
typedef signed int            int32;
/**
* \brief unsigned 32-bit integer
*/
typedef unsigned int          uint32;
/**
* \brief signed 64-bit integer
*/
typedef signed long long      int64;
/**
* \brief unsigned 64-bit integer
*/
typedef unsigned long long    uint64;

/**
 * \brief unsigned 8-bit integer
 */
typedef uint8                 byte;    
/**
 * \brief  64-bit vertica position
*/
typedef unsigned long long    vpos;    
/**
 * \brief  vertica 8-bit boolean (t,f,null)
*/
typedef uint8                 vbool;   
/**
 * \brief vertica 64-bit integer (not int64)
*/
typedef signed long long      vint;    
#define MAX_VINT (INT64CONST(0x7fffffffffffffff))
#define MIN_VINT (-INT64CONST(0x7fffffffffffffff))

/**
 * \brief Maximum length in bytes for data types: Char, Binary, Varchar, Varbinary
*/
#define MAX_STRING_LENGTH 65000
/**
 * \brief Maximum length in bytes for data types: Long Varchar, Long Varbinary
*/
#define MAX_LONG_STRING_LENGTH 32000000

/**
 * \brief vertica 32-bit block size
*/ 
typedef uint32                vsize;    

/**
 * \brief Represents Vertica 64-bit floating-point type
 * for NULL checking use vfloatIsNull(), assignment to NULL use vfloat_null
 * for NaN checking use vfloatIsNaN(), assignment to NaN use vfloat_NaN
 */
typedef double                vfloat;   // vertica 64-bit float
/**
 * \brief vertica 80-bit intermediate result
*/
typedef long double           ifloat;   // vertica 80-bit intermediate result

/**
* \brief Vertica timestamp data type. 
*/
typedef int64 TimestampTz;


// SQL defines VUnknown to be the same value as vbool_null
// "I kept seeing ones and zeroes it was horrible. . .
//     I think I saw a two *shivers*"
//                            -- Bender
/// Enumeration for Boolean data values
enum VBool {VFalse = 0, VTrue = 1, VUnknown = 2}; // Be careful changing!

/// Value for Integer NULLs.
const vint vint_null  =   0x8000000000000000LL;
/// Value for Boolean NULL
const vbool vbool_null =  VUnknown;
/// Value for Boolean TRUE
const vbool vbool_true =  VTrue;
/// Value for Boolean FALSE
const vbool vbool_false =  VFalse;

/**
* \brief Indicates NULL strings
*/
#define StringNull vsize(-1)            // Used to indicate NULL strings
// isNullSV is defined in EEUDxShared.h
#if 1 // being changed
/**
* \brief Indicates NULL char
*/
const char char_null  =   0xff;
/**
* \brief Indicates NULL byte
*/
const byte byte_null  =   0xff;
#endif


/**
*  \INTERNAL Version of memcpy optimized for vertica's small data types.
*  Inline function for copying small things of arbitrary length.
*/
inline void vsmemcpy(void *dst, const void *src, size_t len){
    uint8 *d = static_cast<uint8 *>(dst);
    const uint8 *s = static_cast<const uint8 *>(src);

    while (len >= 8){
       *reinterpret_cast<vint *>(d) = *reinterpret_cast<const vint *>(s);
       d+=8; s+=8; len-=8;
    }
    if (len >= 4){ // eliminate on machines with loop detectors?
       *reinterpret_cast<uint32 *>(d) = *reinterpret_cast<const uint32 *>(s);
       d+=4; s+=4; len-=4;
    }
    while (len){
       *(d) = *(s);
       ++d; ++s; --len;
    }
}

/**
 * \Brief Vertica memcpy 
*/
#define VMEMCPY(x,y,z) do {                                         \
    if ((z) == 8)                                                   \
        *reinterpret_cast<vint *>(x) = *reinterpret_cast<const vint *>(y); \
    else vsmemcpy(x, y, z);                                         \
    } while (0)
/**
 * \brief Version of memcmp for detecting not equal only.
 *   Inline function for comparing small things of arbitrary length. 
 */
inline bool vsmemne(const void *p1, const void *p2, size_t len){
    const uint8 *v1 = static_cast<const uint8 *>(p1);
    const uint8 *v2 = static_cast<const uint8 *>(p2);

    if (len >= 8) {
        if (*reinterpret_cast<const vint *>(v1) !=
            *reinterpret_cast<const vint *>(v2))
            return true;
        v1+=8; v2+=8; len-=8;
        if (len >= 8) {
            if (*reinterpret_cast<const vint *>(v1) !=
                *reinterpret_cast<const vint *>(v2))
                return true;
            v1+=8; v2+=8; len-=8;
        }
    }

    bool diff = false;

    while (len >= 8){
       diff |= *reinterpret_cast<const vint *>(v1) 
                 != *reinterpret_cast<const vint *>(v2);
       v1+=8; v2+=8; len-=8;
    }
    if (len >= 4){ // eliminate on machines with loop detectors?
       diff |= *reinterpret_cast<const uint32 *>(v1) 
                 != *reinterpret_cast<const uint32 *>(v2);
       v1+=4; v2+=4; len-=4;
    }
    while (len){
       diff |= *reinterpret_cast<const uint8 *>(v1) 
                 != *reinterpret_cast<const uint8 *>(v2);
       ++v1; ++v2; --len;
    }

    return diff;
}

/*
 * \brief  Version of memset for setting to 0 optimized for vertica's small data types.
 *  Inline function for zeroing small things of arbitrary length.
 */
inline void vsmemclr(void *dst, size_t len){
    uint8 *d = static_cast<uint8 *>(dst);
    while (len >= 8){
        *reinterpret_cast<uint64 *>(d) = 0;
        d+=8; len-=8;
    }
    if (len >= 4){
       *reinterpret_cast<uint32 *>(d) = 0;
       d+=4; len-=4;
    }
    while (len){
       *(d) = 0;
       ++d; --len;
    }
}

/*
 * \brief non-signalling NAN
 *
 */
const union FIunion {
    vint vi;
    vfloat vf;
} vfn = {0x7ffffffffffffffeLL}; // our non-signalling NAN (fe helps valgrind)
/*
 * \brief Indicates NULL floats
 */
const vfloat vfloat_null = vfn.vf;
/*
 * \brief Checks if a float is NULL
 *
 */
inline bool vfloatIsNull(const vfloat* vf) {
    return !vsmemne(vf, &vfloat_null, sizeof(vfloat)); }
/*
 * \brief  Checks if a float is NULL
 *
 */
inline bool vfloatIsNull(const vfloat vf) { return vfloatIsNull(&vf); }

/*
 * \brief  The x86 generated version of NaN is not the c99 version (math.h NAN)
 */
const union FIunion vfNaN = { static_cast<vint>(0xFFF8000000000000LL) };
/*
 * \brief  Indicates float NAN
q */
#define vfloat_NaN vfNaN.vf
/*
 * \brief  Check if a float is NAN
 */
inline bool vfloatIsNaN(const vfloat *f) {
   vint v = *reinterpret_cast<const vint *>(f);
   return (((v & 0x7FF0000000000000LL) == 0x7FF0000000000000LL) 
           && ((v & 0x000FFFFFFFFFFFFFLL) != 0)); // big exp but not Infinity
}
/*
 * \brief Check if a float is NAN
 *
 */
inline bool vfloatIsNaN(const vfloat f) { return vfloatIsNaN(&f); }

#ifndef VERTICA_INTERNAL
}
#endif

/// basic utilities mainly for data types.
namespace Basics
{
using namespace Vertica;

// fwd declaration
bool Divide32(uint64 hi, uint64 lo, uint64 d, uint64 &_q, uint64 &_r);

// length is the Vertica data length in bytes; for CHARs it is typmod-4
// For VARCHARs it is the current or desired data length if known, otherwise -1.

// typmod is the 'Type Modifier' -- used for precision in time/timestamp, etc.
// For CHAR/VARCHAR fields PG adds 4 to the user-specified field width to
// allow for the four-byte string length field it prepends to the string; if
// the maximum length is unspecified, typmod is set to -1.

/* From pg_attribute.h: "atttypmod records type-specific data supplied at table
 * creation time (for example, the max length of a varchar field).  It is
 * passed to type-specific input and output functions as the third
 * argument.  The value will generally be -1 for types that do not need
 * typmod." */

// Special code for interpreting numeric typemod
#define NUMERIC_DSCALE_MASK 0x1FFF      /* allow an extra sign/type bit */

/// Get Numeric precision from typmod
inline int32 getNumericPrecision(int32 typmod)
{
    return ((typmod - VARHDRSZ) >> 16) & 0xffff; // unsigned
}
/// Get Numeric scale from typmod
inline int32 getNumericScale(int32 typmod)
{
    return (typmod - VARHDRSZ) & NUMERIC_DSCALE_MASK; // unsigned
}
/// Get Numeric word count from precision
inline int32 getNumericWordCount(int32 precision)
{
    // TODO: Replace with a formula that is not as conservative?
    //   (a word holds a little over 19 digits,
    //    except first holds slightly less 19 due to sign bit)
    return precision / 19 + 1;
}
/// Get Numeric data length from typmod
static int32 getNumericLength(int32 typmod)
{
    // Eliminate all -1 usage
    VIAssert(typmod != -1);
    int32 precision = getNumericPrecision(typmod);
    assert (precision > 0);
    //assert (precision >= 0);
    assert (getNumericScale(typmod) >= 0);
    return 8 * getNumericWordCount(precision);
}

/// Return true if these have the same EE representation
inline bool isSimilarNumericTypmod(int32 a, int32 b)
{
    return (Basics::getNumericScale(a) == Basics::getNumericScale(b))
       && (Basics::getNumericLength(a) == Basics::getNumericLength(b));
}



struct FiveToScale
{
    unsigned scale;
    unsigned wordCount;
    const uint64 *value;
};

// Multiply u * v; results in p1, p2; 
#if defined(__Linux64__)
#define Multiply(u, v, p1, p2)                          \
    asm ("\tmul %%rdx\n"                                \
         : "=d"(p1), "=a"(p2) /* 128-bit product */     \
         : "d"(u), "a"(v)     /* Input operands */      \
         : "cc"               /* flags clobbered */ )
#else
#define Multiply(u, v, p1, p2)                           \
do {                                                       \
    uint64 _u = u, _v = v;                                 \
    uint64 lw = (_u & 0xFFFFFFFFLLU) * (_v & 0xFFFFFFFFLLU); \
    uint64 mw1 = (_u & 0xFFFFFFFFLLU) * (_v >> 32);          \
    uint64 mw2 = (_u >> 32) * (_v & 0xFFFFFFFFLLU);          \
    uint64 hw = (_u >> 32) * (_v >> 32);                     \
    p1 = hw + (mw1 >> 32) + (mw2 >> 32);                   \
    uint64 rla = lw + (mw1 << 32);                         \
    if (rla < lw) ++p1;                                    \
    p2 = rla + (mw2 << 32);                                \
    if (p2 < rla) ++p1;                                    \
} while (0)
#endif

// Divide u1u2 / v1, where u1 < v1; results in q and r
#if defined(__Linux64__)
#define Divide(u1,u2, v1, q, r)                         \
    asm ("\tdiv %%rcx\n"                                \
         : "=a"(q), "=d"(r) /* Out operands */          \
         : "d"(u1), "a"(u2), "c"(v1) /* In operands */  \
         : "cc" /* flags clobbered */ )
#else
// replace this with x86 asm using real div, etc., if anyone cares
#define Divide(u1,u2, v1, q, r)                         \
    Basics::Divide32(u1,u2, v1, q, r)
#endif

#define Min(x, y)               ((x) < (y) ? (x) : (y))

// This is just for a namespace kind of thing
/**
 * Holds integer utilities
 * A few are inlined for performance reasons.  Most are in vertica.cpp.
 */
struct BigInt
{
/** 
* \brief  cpoy nWords from src to buf 
*/
static void copy(void *buf, const void *src, int nWords)
{
    const uint64 *ubuf = static_cast<const uint64 *>(src);
    uint64 *ubufo = static_cast<uint64 *>(buf);

    for (int i=0; i<nWords; ++i) ubufo[i] = ubuf[i];
}

/** \fn setZero(void *buf, int nWords)
* \brief Set an integer to 0 
*/
static void setZero(void *buf, int nWords);

/** 
 * \brief Check an integer for 0
* 
 * \note Optimized for small nWords; scan from end and break out of loop
 */
static bool isZero(const void *buf, int nWords);

/** 
* \brief Set an integer to NULL 
*/
static void setNull(void *buf, int nWords);

/** 
 * \brief Check an integer for NULL
 */
static bool isNull(const void *buf, int nWords);

/** 
 * \brief Check if integer is less than zero
 */
static bool isNeg(const void *buf, int nWords){
   const int64 *ibuf = static_cast<const int64 *>(buf);
   Assert (nWords > 0);
   return ibuf[0] < 0;
}


/** 
 * \brief Check integers for equality
 * 
 * \note Optimized for small nWords; scan from end and break out of loop
 */
static bool isEqualNN(const void *buf1, const void *buf2, int nWords);

/** 
 * \brief Compare integers, return -1, 0, 1
 */
static int compareNN(const void *buf1, const void *buf2, int nWords);

/** 
 * Compare integers, return -1, 0, 1
 */
static int ucompareNN(const void *buf1, const void *buf2, int nWords){
   Assert (nWords > 0);
   const uint64 *ubuf1 = static_cast<const uint64 *>(buf1);
   const uint64 *ubuf2 = static_cast<const uint64 *>(buf2);

   for (int i=0; i<nWords; ++i) {
       if (ubuf1[i] != ubuf2[i]) return ubuf1[i] < ubuf2[i] ? -1 : 1;
   }
   return 0; // ==
}



/** 
 * \brief Increment by 1
 */
static void incrementNN(void *buf, int nWords);

/** 
 * \brief Invert the sign of a number, performed in place, using the invert and +1
 *   method, turns out NULLs are OK in this sense
 */
static void invertSign(void *buf, int nWords);

/** 
 * \brief Add together 2 numbers w/ same number of digits
 *  No null handling
 * Returns carry
 */
static uint64 addNN(void *outBuf, const void *buf1, const void *buf2, int nWords);

/** 
 * \brief Add number on to a temporary
 *  No null handling
 * Returns carry
 */
static uint64 accumulateNN(void *outBuf, const void *buf1, int nWords);

/** 
 * \brief Subtract 2 numbers w/ same number of digits
 *  No null handling
 */
static void subNN(void *outBuf, const void *buf1, const void *buf2, int nWords);

/** 
 * 
 * \brief Shift a BigInt to the right (>>) by the given number of bits.
 * The given BigInt must be positive and non-null.
 */
static void shiftRightNN(void *buf, unsigned nw, unsigned bitsToShift);

/**
 * 
 * \brief Shift a BigInt to the left (<<) by the given number of bits.
 * The given BigInt must be positive and non-null.
 */
static void shiftLeftNN(void *buf, unsigned nw, unsigned bitsToShift);

/**
 * \brief Load from 64-bit signed int (does not handle NULL inside)
 */
static void fromIntNN(void *buf, int nWords, int64 val);

/**
 * \brief Convert to int (return false if there was an overflow)
 */
static bool toIntNN(int64 &out, const void *buf, int nWords);

/**
 * \brief Calculate number of words that are actually used
 *  to represent the value (amount left after stripping leading 0's)
 */
static int usedWordsUnsigned(const void *buf, int nWords);

static void setNumericBoundsFromType(uint64 *numUpperBound,
                                     uint64 *numLowerBound, 
                                     int nwdso, int32 typmod);

static bool checkOverflowNN(const void *po, int nwdso, int32 typmodo);
static bool checkOverflowNN(const void *po, int nwo, int nwdso, int32 typmodo);

// From VEval::NumericRescaleDown
static void NumericRescaleDown(uint64 *wordso, int nwdso, int32 typmodo, uint64 *wordsi, int nwdsi, int32 typmodi);

// From VEval::NumericRescaleUp
static void NumericRescaleUp(uint64 *wordso, int nwdso, int32 typmodo, uint64 *wordsi, int nwdsi, int32 typmodi);

// From VEval::NumericRescaleSameScaleSmallerPrec
static void NumericRescaleSameScaleSmallerPrec(uint64 *wordso, int nwdso, int32 typmodo, uint64 *wordsi, int nwdsi, int32 typmodi);

/*
 * \brief Cast a numeric from one typmod to another. The params wordsi, nwdsi and
 * typmodi are the words, number of words and typmod of the input numeric,
 * while wordso, nwdso and typmodo are the words, number of words and typmod
 * of output numeric. It is the caller's responsibility to ensure that wordso
 * has space to store 'nwdso' words.
 */
static void castNumeric(uint64 *wordso, int nwdso, int32 typmodo, uint64 *wordsi, int nwdsi, int32 typmodi);

/**
 * x86 only
 * 80-bit extended precision float
 * 64 bit mantissa, stored little endian (just like a 64-bit int)
 *    Note that the mantissa does not have an implicit "1" bit
 *      thus eliminating the concept of denormal numbers.
 * 16 bit exponent, also little endian, within the next 64 bits.
 *    High bit is sign
 *    Lower 15 bits are "excess 16383"
 * Example:
 * 1.0 is stored as, in binary:
 *  mantissa: 1000000000000000000000000000000000000000000000000000000000000000,
 *  exponent: 0011111111111111
 */
union long_double_parts
{
    long_double_parts(long double v = 0.0) : value(v) {}
    long double value;
    struct
    {
        unsigned long long int mantissa;
        unsigned short int exponent; // Includes the sign bit.
        unsigned short int remainder[3];
    };
};

/**
 * \brief Multiply, unsigned only.  uw words by 1 word -> uw+1 words
 *  Output array must be uw+1 words long; may not overlap inputs
 */
static void mulUnsignedN1(void *obuf, const void *ubuf, int uw, uint64 v);

/**
 * \brief Multiply, unsigned only.
 *  Output array must be nw1+nw2 long; may not overlap inputs
 * PERF NOTE: Operates like a school boy, could do Karatsuba (or better)
 */
static void mulUnsignedNN(void *obuf, const void *buf1, int nw1,
                          const void *buf2, int nw2);

// qbuf = ubuf / v; r = ubuf % v
//   qbuf is qw words, and ubuf is uw; returns r
// qbuf may be the same as ubuf if qw == uw
// qw may be < uw if the missing result words are zero
static uint64 divUnsignedN1(void *qbuf, int qw, int round,
                            const void *ubuf, int uw, uint64 v);

// qbuf = ubuf / vbuf; rbuf = ubuf % vbuf
//   qbuf must be qw words; rbuf must be rw words or NULL
//   round: trunc=0, round-up=1
// qbuf may overlay ubuf
// Knuth Vol 2, 4.3.1, Algorithm D, pg 257
static void divUnsignedNN(void *qbuf, int qw, int round,
                          uint64 *rbuf, int rw, const void *ubuf,
                          int uw, const void *vbuf, int vw);

/**
 * \brief Rescale a given numeric to a specific prec/scale/nwds
 * The input should have minimal precision to avoid unnecessary overflow;
 * for example, "0" should have precision 0 (as generated by charToNumeric).
 * Accepts signed numerics.  Will set its "in" argument to abs(in).
 */
static bool rescaleNumeric(void *out, int ow, int pout, int sout,
                           void *in, int iw, int pin, int sin);

/**
 * \brief Convert floating point multiplied by 10^scale to an integer
 *  This truncates if round is false; otherwise the numeric result is rounded
 *  @return false if high bits were lost, true if reasonable fidelity was achieved
 */
static bool setFromFloat(void *bbuf, int bwords, int typmod,
                         long double value, bool round);

/**
 * \brief Convert Numeric to a float helper function
 * Input should not be negative / null
 */
static long double convertPosToFloatNN(const void *bbuf, int bwords);

/**
 * \brief Convert Numeric to a float
 * Handles negative / null input
 *
 * @param tenthtoscale 10^-scale of the Numeric
 */
static ifloat numericToFloat(const void *buf, int nwds, ifloat tenthtoscale);

/**
 * \brief Multiply Numerics , result in outNum
 * Handles negative / null input
 */
static void numericMultiply(const uint64 *pa, int nwdsa,
                            const uint64 *pb, int nwdsb,
                            uint64 *outNum, int nwdso);

/**
 * \brief Divide Numerics
 * Handles negative / null input
 */
static void numericDivide(const uint64 *pa, int nwdsa, int32 typmoda,
                          const uint64 *pb, int nwdsb, int32 typmodb,
                          uint64 *outNum, int nwdso, int32 typmodo);

/*
 * @cond INTERNAL Declaration of rest of BigInt member functions,
 * used internally in Vertica, definition not exposed to UDx writers
 */
#define InlineNNSubs 0 // 0 for debugging, 1 for inlining

#if InlineNNSubs

#define STATIC static
#define MEMBER
#include "Basics/BigIntSubs.h"
#undef MEMBER
#undef STATIC

#else

static void accumulateN1(void *obuf, int ow, uint64 v);

static bool charToNumeric(const char *c, int clen, bool allowE,
                          int64 *&pout, int &outWords,
                          int &precis, int &scale, bool packInteger = false);

static void binaryToDec(const void *bbuf, int bwords, char *outBuf, int olen);

static void binaryToDecScale(const void *bbuf, int bwords, char *outBuf,
                             int olen, int scale);

#endif
/* @endcond */

};

}

/** Allocates a block of memory to fit a specific data type (vint, struct, etc.).
 *  
 */
#define vt_alloc(_allocator, _type) \
  (static_cast<_type *>((_allocator)->alloc(sizeof(_type))))

/** Allocates a block of memory to hold an array of a specific data type.
 *  
 */
#define vt_allocSize(_allocator, _type, _size) \
  (static_cast<_type *>((_allocator)->alloc(_size)))

/** Allocates an arbitrarilty-sized block of memory.
 *  
 */

#define vt_allocArray(_allocator, _type, _len) \
  (static_cast<_type *>((_allocator)->alloc(sizeof(_type) * (_len))))

// Only supposed to be used to create UDScalarFuncObj in UDScalarFuncInfo::createFuncObj()
// Note: some versions of gcc have problems compiling this macro ("Internal compiler error..").
//   The workaround is to subtitute one of the vt_createFuncObject function templates defined
//   in VerticaUDx.h.
#define vt_createFuncObj(_allocator, _type, _args...)             \
    (new ((_allocator)->alloc(sizeof(_type))) _type(_args))

#endif // BASICS_UDX_SHARED_H
