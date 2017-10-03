#define _FAST_NEXT

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
#ifndef VERTICA_UDX_H
#define VERTICA_UDX_H
#include <list>
#include <vector>
#include <set>
#include <string>
#include <sstream>
#include <algorithm>
#include <stdexcept>
#include <typeinfo>


namespace Vertica {
class VerticaBlock;
class VerticaBlockSerializer;
}

struct CPPExecContext;

class AnalyticPartitionReaderHelper;

namespace Vertica
{

/*
 * Include definitions shared between UDx and Vertica server
 */

/** \internal @file
 * \brief Contains the classes needed to write User-Defined things in Vertica
 *
 * This file should not be directly #include'd in user code. Include 'Vertica.h' instead
 */

typedef void (*VT_THROW_EXCEPTION)(int errcode, const std::string &message, const char *filename, int lineno);
extern "C" VT_THROW_EXCEPTION vt_throw_exception;

typedef void (*VT_THROW_INTERNAL_EXCEPTION)(int errcode, const std::string &message, const std::string &info, const char *filename, int lineno, const char *funcname);
extern "C" VT_THROW_INTERNAL_EXCEPTION vt_throw_internal_exception;
typedef int (*VT_ERRMSG)(const char *fmt, ...);
extern "C" VT_ERRMSG vt_server_errmsg;

extern int makeErrMsg(std::basic_ostream<char, std::char_traits<char> > &err_msg, const char *fmt, ...);
extern void dummy();
}


#include "PGUDxShared.h"
#include "BasicsUDxShared.h"
#include "EEUDxShared.h"
#include "TimestampUDxShared.h"
#include "IntervalUDx.h"

class UdfSupport; // fwd declarations for Vertica internal classes
class UDxSideProcessInfo;

namespace Vertica
{

extern Oid getUDTypeOid(const char *typeName);
extern Oid getUDTypeUnderlyingOid(Oid typeOid);
extern bool isUDType(Oid typeOid);
extern vint getUDxDebugLogLevel();

class ServerInterface;
class FileManager;
class VerticaBlockSerializer;
class BlockReader;
class BlockWriter;
class ValueRangeReader;
class ValueRangeWriter;
class PartitionReader;
class PartitionWriter;
class AnalyticPartitionReader;
class AnalyticPartitionWriter;
class VerticaRInterface;
struct FullPartition;
class IntermediateAggs;
class MultipleIntermediateAggs;
class ParamReader;
class ParamWriter;
class VerticaType;
class SessionParamReaderMap;
class SessionParamWriterMap;

class UDFileSystem;

struct VerticaBuildInfo
{
    const char *brand_name;
    const char *brand_version;
    const char *codename;
    const char *build_date;
    const char *build_machine;
    const char *branch;
    const char *revision;
    const char *checksum;
    const char *sdk_version;

    VerticaBuildInfo():
        brand_name(NULL), brand_version(NULL),
        codename(NULL), build_date(NULL),
        build_machine(NULL), branch(NULL),
        revision(NULL), checksum(NULL), sdk_version(NULL) {}
};

/// @cond INTERNAL
class UserLibraryManifest
{
public:
    UserLibraryManifest() {}

    void registerObject(const std::string obj_name)
    { objNames.push_back(obj_name); }

    void setMetadata(std::string key, std::string value)
    { license_metadata[key] = value; }

    std::list<std::string> objNames;
    std::map<std::string, std::string> license_metadata;
};
/// @endcond

/**
 * Representation of the resources user code can ask Vertica for
 */
struct VResources
{
    VResources()
    : scratchMemory(0)
    , nFileHandles(100) // default to 100 to set a minimum bar
    { }

    /** Amount of RAM in bytes used by User defined function */
    vint scratchMemory;
    /** Number of file handles / sockets required */
    int nFileHandles;
};


/**
 * \brief Representation of a String in Vertica. All character data is
 *        internally encoded as UTF-8 characters and is not NULL terminated.
 */
class VString
{
private:
    VString();

public:
    /// @cond INTERNAL
    VString(EE::StringValue *sv, EE::DataArea *da=NULL, vsize max_size = StringNull)
        : sv(sv), da(da), max_size(max_size) {}
    EE::StringValue *sv; // Opaque pointer to internal representation. Do not modify
    EE::DataArea     *da; // out of line area for variable length strings. May be null
    vsize max_size;
    /// @endcond

    /**
     * @brief Returns the length of this VString
     *
     * @return the length of the string, in bytes. Does not include any extra
     * space for null characters.
     */
    vsize length() const { return lenSV(sv); }

    /**
     * @brief Indicates if this VString contains the SQL NULL value
     *
     * @return true if this string contains the SQL NULL value, false otherwise
     */
    bool isNull() const { return isNullSV(sv); }

    /**
     * @brief Sets this VString to the SQL NULL value.
     */
    void setNull() { setNullSV(sv); }

    /**
     * @brief Provides a read-only pointer to this VString's internal data.
     *
     * @return the read only character data for this string, as a pointer.
     *
     * @note The returned string is \b not null terminated
     */
    const char *data() const { return cvalueSV(sv, da); }

    /**
     * @brief Provides a copy of this VString's data as a std::string
     *
     * @return a std::string copy of the data in this VString
     */
    std::string str() const {
        if (isNull() || length() == 0) return std::string("", 0);
        else                           return std::string(data(), length());
    }

    /**
     * @brief Provides a writeable pointer to this VString's internal data.
     *
     * @return the writeable character data for this string, as a pointer.
     *
     * @note The returned string is \b not null terminated
     */
    char *data() { return strSV(sv, da); }

    /**
    * @brief Allocate the VString's internal buffer and initialize to 'len' 0 bytes.
    *
    * The VString is allocated and set to 'len' zero bytes. It is the caller's
    * responsibility to not provide a value of 'len' that is larger than the
    * maximum size of this VString
    *
    * @param len  length in bytes
    */
    void alloc(vsize len) {
        if (max_size != StringNull && len > max_size) {
            ereport(ERROR,
                    (errmsg("Tried to allocate and initialize a %d-byte string with %d zero bytes; VString is too small",
                            (int)len, (int)max_size),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }
        allocSV(sv, myDA(), len);
        memset(data(), 0, len);
    }

    /**
     * @brief Copy character data from C string to the VString's internal buffer.
     *
     * Data is copied from s into this VString. It is the caller's
     * responsibility to not provide a value of 'len' that is larger than the
     * maximum size of this VString
     *
     * @param s   character input data
     *
     * @param len  length in bytes, \b not including the terminating null character
     */
    void copy(const char *s, vsize len) {
        if (max_size != StringNull && len > max_size) {
            ereport(ERROR,
                    (errmsg("Tried to copy a %d-byte string to %d-byte VString object; VString is too small",
                            (int)len, (int)max_size),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }
        setSV(sv, myDA(), s, len);
    }

    /**
     * @brief Copy character data from null terminated C string to the VString's internal buffer.
     *
     * @param s   null-terminated character input data
     */
    void copy(const char *s)            { copy(s, strlen(s)); }

    /**
     * @brief Copy character data from std::string
     *
     * @param s   null-terminated character input data
     */
    void copy(const std::string &s)      { copy(s.c_str(), s.size()); }

    /**
     * @brief Copy data from another VString.
     *
     * @param from  The source VString
     *
     **/
    void copy(const VString *from) {
        if(from->isNull()){
            setNull();
            return;
        }
        if (max_size != StringNull && lenSV(from->sv) > max_size) {
            ereport(ERROR,
                    (errmsg("Tried to copy a %d-byte string to %d-byte VString object; VString is too small",
                            (int)lenSV(from->sv), (int)max_size),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }
        copySV(sv, myDA(), from->sv, from->da);
    }

    /**
     * @brief Copy data from another VString.
     *
     * @param from  The source VString
     *
     **/
    void copy(const VString &from) { copy(&from); }

    /**
     * @brief Indicates whether some other VString is equal to this one
     *
     * @return -1 if both are SQL NULL, 0 if not equal, 1 if equal
     * so you can easily consider two NULL values to be equal to each other, or not
     */
    int equal(const VString *other) const { return eqSV(sv, da, other->sv, other->da); }

    /**
     * @brief Compares this VString to another
     *
     * @return -1 if this < other, 0 if equal, 1 if this > other (just like memcmp)
     *
     * @note SQL NULL compares greater than anything else; two SQL NULLs are considered equal
     */
    int compare(const VString *other) const { return cmpSV(sv, da, other->sv, other->da); }

private:
    // get the DataArea to use for the SV API
    EE::DataArea **myDA() { return da ? &da : OutDA; }
};

/**
 * @brief Representation of NUMERIC, fixed point data types in Vertica.
 */
class VNumeric
{
private:
    VNumeric();

public:
    /// @cond INTERNAL
    uint64 *words;
    int nwds;
    int typmod;

    /**
     * Convert a human-readable string (stored in a char*) to a VNumeric
     *
     * @param str A C-style string to parse.  Must be null-terminated.
     * @param type Type and scale for the target VNumeric
     * @param target VNumeric to write the value into
     * @return true if the value could be stored into the specified VNumeric/scale; false if not
     */
    static bool charToNumeric(const char *str, const VerticaType &type, VNumeric &target);


    VNumeric(uint64 *words, int32 t)
        : words(words), typmod(t)
    {
        nwds = Basics::getNumericLength(typmod) >> 3;
    }
    /// @endcond

    /**
     * @brief Create a VNumeric with the provided storage location, precision and scale
     *
     * @note It is the callers responsibility to allocate enough memory for the \c words parameter
     */
    VNumeric(uint64 *words, int32 precision, int32 scale)
        : words(words)
    {
        nwds = Basics::getNumericLength(TYPMODFROMPRECSCALE(precision, scale)) >> 3;
        typmod = TYPMODFROMPRECSCALE(precision, scale);
    }

    /**
     * @brief Indicates if this VNumeric is the SQL NULL value
     */
    bool isNull() const
    { return Basics::BigInt::isNull(words, nwds); }

    /**
     * @brief Sets this VNumeric to the SQL NULL value.
     */
    void setNull()
    { Basics::BigInt::setNull(words, nwds); }

    /**
     * @brief Indicates if this VNumeric is zero
     */
    bool isZero() const
    { return Basics::BigInt::isZero(words, nwds); }

    /**
     * @brief Sets this VNumeric to zero
     */
    void setZero()
    { Basics::BigInt::setZero(words, nwds); }

    /**
     * @brief Copy data from another VNumeric.
     *
     * @param from  The source VNumeric
     *
     **/
    void copy(const VNumeric *from)
    {
        if (Basics::isSimilarNumericTypmod(from->typmod, this->typmod))
            Basics::BigInt::copy(words, from->words, nwds);
        else
            Basics::BigInt::castNumeric(words, nwds, typmod, from->words, from->nwds,from->typmod);
    }

    /**
     * @brief Copy data from a floating-point number
     *
     * @param value  The source floating-point number
     * @param round  Truncates if false; otherwise the numeric result is rounded
     *
     * @return false if conversion failed (precision lost or overflow, etc); true otherwise
     **/
    bool copy(ifloat value, bool round = true)
    {
        return Basics::BigInt::setFromFloat(words, nwds, typmod, value, round);
    }

    /**
     * @brief Convert the VNumeric value into floating-point
     *
     * @return the value in 80-bit floating-point
     **/
    ifloat toFloat() const
    {
        ifloat tenthtoscale = powl(10.0L, -SCALEFROMTYPMOD(typmod));
        return Basics::BigInt::numericToFloat(words, nwds, tenthtoscale);
    }

    /**
     * @brief Indicates if this VNumeric is negative
     */
    bool isNeg() const
    { return Basics::BigInt::isNeg(words, nwds); }

    /**
     * @brief Inverts the sign of this VNumeric (equivalent to multiplying
     * this VNumeric by -1)
     */
    void invertSign()
    { Basics::BigInt::invertSign(words, nwds); }

    /**
     * @brief Indicates whether some other VNumeric is equal to this one
     *
     */
    bool equal(const VNumeric *from) const
    {
        if (Basics::isSimilarNumericTypmod(from->typmod, this->typmod))
            return Basics::BigInt::isEqualNN(words, from->words, nwds);
        else
        {
            int tempnwds = (nwds >= from->nwds)? nwds : from->nwds;
            uint64 tempwords[tempnwds];
            if (Basics::getNumericPrecision(typmod) >= Basics::getNumericPrecision(from->typmod))
            {
                Basics::BigInt::castNumeric(tempwords, nwds, typmod, from->words, from->nwds,from->typmod);
                return Basics::BigInt::isEqualNN(words, tempwords, nwds);
            }
            else
            {
                Basics::BigInt::castNumeric(tempwords, from->nwds, from->typmod, words, nwds, typmod);
                return Basics::BigInt::isEqualNN(from->words, tempwords, from->nwds);
            }
        }
    }

    /**
     * @brief Compares this (signed) VNumeric to another
     *
     * @return -1 if this < other, 0 if equal, 1 if this > other
     *
     * @note SQL NULL compares less than anything else; two SQL NULLs are considered equal
     */
    int compare(const VNumeric *from) const
    {
        if (Basics::isSimilarNumericTypmod(from->typmod, this->typmod))
            return Basics::BigInt::compareNN(words, from->words, nwds);
        else
        {
            int tempnwds = (nwds > from->nwds)? nwds : from->nwds;
            uint64 tempwords[tempnwds];
            if (Basics::getNumericPrecision(typmod) >= Basics::getNumericPrecision(from->typmod))
            {
                Basics::BigInt::castNumeric(tempwords, nwds, typmod, from->words, from->nwds,from->typmod);
                return Basics::BigInt::compareNN(words, tempwords, nwds);
            }
            else
            {
                Basics::BigInt::castNumeric(tempwords, from->nwds, from->typmod, words, nwds, typmod);
                return Basics::BigInt::compareNN(tempwords, from->words, from->nwds);
            }
        }
    }

    /**
     * @brief Compares this (unsigned) VNumeric to another
     *
     * @return -1 if this < other, 0 if equal, 1 if this > other
     *
     * @note SQL NULL compares less than anything else; two SQL NULLs are considered equal
     */
    int compareUnsigned(const VNumeric *from) const
    {
        if (Basics::isSimilarNumericTypmod(from->typmod, this->typmod))
            return Basics::BigInt::ucompareNN(words, from->words, nwds);
        else
        {
            int tempnwds = (nwds > from->nwds)? nwds : from->nwds;
            uint64 tempwords[tempnwds];
            if (Basics::getNumericPrecision(typmod) >= Basics::getNumericPrecision(from->typmod))
            {
                Basics::BigInt::castNumeric(tempwords, nwds, typmod, from->words, from->nwds,from->typmod);
                return Basics::BigInt::ucompareNN(words, tempwords, nwds);
            }
            else
            {
                Basics::BigInt::castNumeric(tempwords, from->nwds, from->typmod, words, nwds, typmod);
                return Basics::BigInt::ucompareNN(tempwords, from->words, from->nwds);
            }
        }
    }

    /* @brief Adds two VNumerics and stores the result in this VNumeric
    */
    void add(const VNumeric *a, const VNumeric *b)
    {
        if (Basics::isSimilarNumericTypmod(a->typmod, this->typmod) && Basics::isSimilarNumericTypmod(b->typmod, this->typmod))
            Basics::BigInt::addNN(words, a->words, b->words, nwds);
        else
        {
            uint64 tempa[nwds];
            uint64 tempb[nwds];
            Basics::BigInt::castNumeric(tempa, nwds, typmod, a->words, a->nwds, a->typmod);
            Basics::BigInt::castNumeric(tempb, nwds, typmod, b->words, b->nwds, b->typmod);
            Basics::BigInt::addNN(words, tempa, tempb, nwds);
        }
    }

    /// @brief Adds another VNumeric to this VNumeric
    void accumulate(const VNumeric *from)
    {
        if (Basics::isSimilarNumericTypmod(from->typmod, this->typmod))
            Basics::BigInt::accumulateNN(words, from->words, nwds);
        else
        {
            uint64 tempwords[nwds];
            Basics::BigInt::castNumeric(tempwords, nwds, typmod, from->words, from->nwds,from->typmod);
            Basics::BigInt::accumulateNN(words, tempwords, nwds);
        }
    }

    /* @brief Subtract one VNumeric from another VNumeric and stores the result in this VNumeric
    */
    void sub(const VNumeric *a, const VNumeric *b)
    {
        if (Basics::isSimilarNumericTypmod(a->typmod, this->typmod) && Basics::isSimilarNumericTypmod(b->typmod, this->typmod))
            Basics::BigInt::subNN(words, a->words, b->words, nwds);
        else
        {
            uint64 tempa[nwds];
            uint64 tempb[nwds];
            Basics::BigInt::castNumeric(tempa, nwds, typmod, a->words, a->nwds, a->typmod);
            Basics::BigInt::castNumeric(tempb, nwds, typmod, b->words, b->nwds, b->typmod);
            Basics::BigInt::subNN(words, tempa, tempb, nwds);
        }
    }

    /* @brief Increment by 1, no NULL checking
    */
    void incr()
    { Basics::BigInt::incrementNN(words, nwds); }

    /* @brief Shift a VNumeric to the left (<<) by the given number of bits.
     * The input must be positive and non-NULL.
     */
    void shiftLeft(unsigned bitsToShift)
    { Basics::BigInt::shiftLeftNN(words, nwds, bitsToShift); }

    /* @brief Shift a VNumeric to the right (>>) by the given number of bits.
     * The input must be positive and non-NULL.
     */
    void shiftRight(unsigned bitsToShift)
    { Basics::BigInt::shiftRightNN(words, nwds, bitsToShift); }

    /* @brief Multiply two VNumerics and stores the result in this VNumeric
     * Caller need to make sure there is enough scale in this VNumeric to hold the result
     */
    void mul(const VNumeric *a, const VNumeric *b)
    {
        Basics::BigInt::numericMultiply(a->words, a->nwds,
                                        b->words, b->nwds,
                                        words, nwds);
    }

    /* @brief Divide a VNumeric by another VNumeric and stores the result in this VNumeric
     * Caller need to make sure there is enough scale in this VNumeric to hold the result
     */
    void div(const VNumeric *a, const VNumeric *b)
    {
        Basics::BigInt::numericDivide(a->words, a->nwds, a->typmod,
                                      b->words, b->nwds, b->typmod,
                                      words, nwds, typmod);
    }

    /*
     * @brief Convert a VNumeric to a string. The character array should be allocated by the user.
     * @param[outBuf] A character array where the output is stored.
     * @param[olem]   Length of the character array. As a guideline, the array should be at least 64 bytes long.
     */
    void toString(char* outBuf, int olen) const {
        int scale = SCALEFROMTYPMOD(typmod);
        Basics::BigInt::binaryToDecScale(words, nwds,
                outBuf, olen, scale);
    }
};

/**
 * @brief Represents types of data that are passed into and returned back from
 *        user's code
 */
class VerticaType
{
public:
    /// @cond INTERNAL
    VerticaType(Oid oid, int32 typmod)
    : typeOid(oid)
    , typeMod(typmod)
    { }

    BaseDataOID getTypeOid() const { return typeOid; }
    int32 getTypeMod() const { return typeMod; }

    void setTypeOid(BaseDataOID oid) { typeOid = oid; }
    void setTypeMod(int typmod) { typeMod = typmod; }
    /// @endcond

    /// @brief Return true for VARCHAR/CHAR/VARBINARY/BINARY/LONG VARCHAR/LONG VARBINARY data types
    bool isStringType() const
    {
        return isStringOid(getUnderlyingType());
    }

    bool isStringOid(Oid typeOid) const
    {
        return(typeOid == VarcharOID   || typeOid == CharOID ||
               typeOid == VarbinaryOID || typeOid == BinaryOID ||
               typeOid == LongVarcharOID || typeOid == LongVarbinaryOID);
    }

    /// @brief For VARCHAR/CHAR/VARBINARY/BINARY data types, returns the length of the string
    int32 getStringLength(bool checkType = true) const
    {
        VIAssert(!checkType || isStringType());
        if (typeMod == -1) //If -1, then the length is not defined
            return typeMod;
        return typeMod - VARHDRSZ;
    }

    /// @brief For NUMERIC data types, returns the precision
    int32 getNumericPrecision() const
    { return Basics::getNumericPrecision(typeMod); }

    int32 getNumericWordCount() const
    { return Basics::getNumericWordCount(getNumericPrecision()); }

    /// @brief For NUMERIC data types, returns the scale
    int32 getNumericScale() const
    { return Basics::getNumericScale(typeMod); }

    /// @brief For NUMERIC data types, returns the number of fractional digits (i.e., digits right of the decimal point)
    int32 getNumericFractional() const
    { return getNumericScale(); }

    /// @brief For NUMERIC data types, returns the number of integral digits (i.e., digits left of the decimal point)
    int32 getNumericIntegral() const
    { return getNumericPrecision() - getNumericScale(); }

    /// @brief For NUMERIC data types, sets the precision
    void setNumericPrecision(int32 precision)
    { typeMod = makeNumericTypeMod(precision, getNumericScale()); }

    /// @brief For NUMERIC data types, sets the scale
    void setNumericScale(int32 scale)
    { typeMod = makeNumericTypeMod(getNumericPrecision(), scale); }

    /// @brief For NUMERIC data types, returns the number of bytes required to store an element. Calling this with a non-numeric data type can cause a crash
    int32 getNumericLength() const
    { return Basics::getNumericLength(typeMod); }

    /*
     * Interval helpers
     */
    /// @brief For INTERVAL data types, returns the precision
    int32 getIntervalPrecision() const
    { return INTERVAL_PRECISION(typeMod); }

    /// @brief For INTERVAL data types, returns the range
    int32 getIntervalRange() const
    { return INTERVAL_RANGE(typeMod); }

    /// @brief For INTERVAL data types, sets the precision
    void setIntervalPrecision(int32 precision)
    { typeMod = makeIntervalTypeMod(precision, getIntervalRange()); }

    /// @brief For INTERVAL data types, sets the range
    void setIntervalRange(int32 range)
    { typeMod = makeIntervalTypeMod(getIntervalPrecision(), range); }

    /*
     * Timestamp helpers
     */
    /// @brief For TIMESTAMP data types, returns the precision
    int32 getTimestampPrecision() const
    { return typeMod; }

    /// @brief For TIMESTAMP data types, sets the precision
    void setTimestampPrecision(int32 precision)
    { typeMod = precision; }

    /*
     * Time helpers
     */
    /// @brief For TIMESTAMP data types, returns the precision
    int32 getTimePrecision() const
    { return typeMod; }

    /// @brief For TIMESTAMP data types, sets the precision
    void setTimePrecision(int32 precision)
    { typeMod = precision; }

    /*
     * typmod makers
     */
    /// @cond INTERNAL
    static int32 makeStringTypeMod(int32 len)
    { return len + VARHDRSZ; }

    static int32 makeNumericTypeMod(int32 precision, int32 scale)
    { return TYPMODFROMPRECSCALE(precision, scale); }

    static int32 makeIntervalYMTypeMod(int32 range)
    { return INTERVAL_TYPMOD(0, range); }

    static int32 makeIntervalTypeMod(int32 precision, int32 range)
    { return INTERVAL_TYPMOD(precision, range); }

    static int32 makeTimestampTypeMod(int32 precision)
    { return precision; }

    static int32 makeTimeTypeMod(int32 precision)
    { return precision; }
    /// @endcond

    /// @brief Return human readable type string.
    std::string getPrettyPrintStr() const {
        std::ostringstream oss;
        oss << getTypeStr();
        switch (typeOid) {
        case DateOID:
        case TimeOID:
        case TimestampOID:
        case TimestampTzOID:
        case TimeTzOID:
            if(typeMod >=0){
                oss << "(" << typeMod << ")";
            }
            break;
        case IntervalOID:
        case IntervalYMOID:
            char buf[64];
            describeIntervalTypeMod(buf, typeMod);
            oss << buf;
            break;
        case NumericOID:
            oss << "(" << getNumericPrecision() << ","
                << getNumericScale() << ")";
            break;
        case CharOID:
        case VarcharOID:
        case LongVarcharOID:
        case BinaryOID:
        case VarbinaryOID:
        case LongVarbinaryOID:
                oss << "(" << getStringLength(false) << ")";
                break;
        default:
            break;
        };
        return oss.str();
    }

    const char *getTypeStr() const {
        switch (typeOid) {
#define T(x) case x##OID: return #x
            T(Int8);
            T(Time);
            T(TimeTz);
            T(Date);
            T(Float8);
            T(Bool);
            T(Interval);
            T(IntervalYM);
            T(Timestamp);
            T(TimestampTz);
            T(Numeric);
            T(Char);
            T(Varchar);
            T(Binary);
            T(Varbinary);
            T(LongVarchar);
            T(LongVarbinary);
            default: return "UNKNOWN";
#undef T
        }
    }

    /**
     * @brief Returns the maximum size, in bytes, of a data element of this type.
     */
    int32 getMaxSize() const {
        switch(getUnderlyingType())
        {
          case Int8OID:
          case TimeOID:
          case TimeTzOID:
          case DateOID:        return sizeof(vint);
          case Float8OID:      return sizeof(vfloat);
          case BoolOID:        return sizeof(vbool);
          case IntervalOID:
          case IntervalYMOID:
          case TimestampOID:
          case TimestampTzOID: return sizeof(vint);
          case NumericOID:     return getNumericLength();
          case CharOID:
          case VarcharOID:
          case LongVarcharOID:
          case BinaryOID:
          case VarbinaryOID:
          case LongVarbinaryOID: return getStringLength(false) + sizeof(struct EE::StringValue);
          default:             return 0;
        }
    }

    inline bool operator==(const VerticaType &rhs) const
    { return typeOid == rhs.typeOid && typeMod == rhs.typeMod; }

    inline bool operator!=(const VerticaType &rhs) const
    { return !(*this == rhs); }

    /**
     * @brief Returns true if this type is INTEGER, false otherwise.
     */
    bool isInt() const { return (typeOid == Int8OID); }

    /**
     * @brief Returns true if this type is FLOAT, false otherwise.
     */
    bool isFloat() const { return (typeOid == Float8OID); }

    /**
     * @brief Returns true if this type is BOOLEAN, false otherwise.
     */
    bool isBool() const { return (typeOid == BoolOID); }

    /**
     * @brief Returns true if this type is DATE, false otherwise.
     */
    bool isDate() const { return (typeOid == DateOID); }

    /**
     * @brief Returns true if this type is INTERVAL, false otherwise.
     */
    bool isInterval() const { return (typeOid == IntervalOID); }

    /**
     * @brief Returns true if this type is INTERVAL YEAR TO MONTH, false otherwise.
     */
    bool isIntervalYM() const { return (typeOid == IntervalYMOID); }

    /**
     * @brief Returns true if this type is TIMESTAMP, false otherwise.
     */
    bool isTimestamp() const { return (typeOid == TimestampOID); }

    /**
     * @brief Returns true if this type is TIMESTAMP WITH TIMEZONE, false otherwise.
     */
    bool isTimestampTz() const { return (typeOid == TimestampTzOID); }

    /**
     * @brief Returns true if this type is TIME, false otherwise.
     */
    bool isTime() const { return (typeOid == TimeOID); }

    /**
     * @brief Returns true if this type is TIME WITH TIMEZONE, false otherwise.
     */
    bool isTimeTz() const { return (typeOid == TimeTzOID); }

    /**
     * @brief Returns true if this type is NUMERIC, false otherwise.
     */
    bool isNumeric() const { return (typeOid == NumericOID); }

    /**
     * @brief Returns true if this type is CHAR, false otherwise.
     */
    bool isChar() const { return (typeOid == CharOID); }

    /**
     * @brief Returns true if this type is VARCHAR, false otherwise.
     */
    bool isVarchar() const { return (typeOid == VarcharOID); }

    /**
     * @brief Returns true if this type is LONG VARCHAR, false otherwise.
     */
    bool isLongVarchar() const { return (typeOid == LongVarcharOID); }

    /**
     * @brief Returns true if this type is BINARY, false otherwise.
     */
    bool isBinary() const { return (typeOid == BinaryOID); }

    /**
     * @brief Returns true if this type is VARBINARY, false otherwise.
     */
    bool isVarbinary() const { return (typeOid == VarbinaryOID); }

    /**
     * @brief Returns true if this type is LONG VARCHAR, false otherwise.
     */
    bool isLongVarbinary() const { return (typeOid == LongVarbinaryOID); }


    /**
     * @return If this is a built in type, returns the typeOid. Otherwise, if a
     * user defined type returns the base type oid on which the user type is
     * based.
     *
     * Note: This function is designed so that the common case (aka a built in,
     * non user defined type) is fast -- calling isUDType() is a heavyweight
     * operation. -- See VER-24673 for an example of when it matters.
     */
    Oid getUnderlyingType() const
    {
        if (isBuiltInType(typeOid))
        {
            return typeOid;
        }
        else
        {
            Oid aliasedTypeOid = getUDTypeUnderlyingOid(typeOid);
            if (aliasedTypeOid == VUnspecOID)
                VIAssert("Unknown data type" == 0);
            return aliasedTypeOid;
        }
    }

    static Oid isBuiltInType(const Oid typeOid)
    {
        switch (typeOid)
        {
          case VUnspecOID:       /* fallthrough */
          case VTuple:           /* fallthrough */
          case VPosOID:          /* fallthrough */
          case RecordOID:        /* fallthrough */
          case UnknownOID:       /* fallthrough */
          case BoolOID:          /* fallthrough */
          case Int8OID:          /* fallthrough */
          case Float8OID:        /* fallthrough */
          case CharOID:          /* fallthrough */
          case VarcharOID:       /* fallthrough */
          case DateOID:          /* fallthrough */
          case TimeOID:          /* fallthrough */
          case TimestampOID:     /* fallthrough */
          case TimestampTzOID:   /* fallthrough */
          case IntervalOID:      /* fallthrough */
          case IntervalYMOID:    /* fallthrough */
          case TimeTzOID:        /* fallthrough */
          case NumericOID:       /* fallthrough */
          case VarbinaryOID:     /* fallthrough */
          case RLETuple:         /* fallthrough */
          case BinaryOID:        /* fallthrough */
          case LongVarbinaryOID: /* fallthrough */
          case LongVarcharOID:   /* fallthrough */
            return true;
        default:
            return false;
        }
    }

private:
    Oid typeOid;
    int32 typeMod;
};

/**
 * @brief Represents (unsized) types of the columns used as input/output of a
 * User Defined Function/Transform Function.
 *
 * This class is used only for generating the function or transform function
 * prototype, where the sizes and/or precisions of the data types are not
 * known.
 */
class ColumnTypes
{
public:

    /// @brief Adds a column of type INTEGER
    void addInt() { colTypes.push_back(Int8OID); }

    /// @brief Adds a column of type FLOAT
    void addFloat() { colTypes.push_back(Float8OID); }

    /// @brief Adds a column of type BOOLEAN
    void addBool() { colTypes.push_back(BoolOID); }

    /// @brief Adds a column of type DATE
    void addDate() { colTypes.push_back(DateOID); }

    /// @brief Adds a column of type INTERVAL/INTERVAL DAY TO SECOND
    void addInterval() { colTypes.push_back(IntervalOID); }

    /// @brief Adds a column of type INTERVAL YEAR TO MONTH
    void addIntervalYM() { colTypes.push_back(IntervalYMOID); }

    /// @brief Adds a column of type TIMESTAMP
    void addTimestamp() { colTypes.push_back(TimestampOID); }

    /// @brief Adds a column of type TIMESTAMP WITH TIMEZONE
    void addTimestampTz() { colTypes.push_back(TimestampTzOID); }

    /// @brief Adds a column of type TIME
    void addTime() { colTypes.push_back(TimeOID); }

    /// @brief Adds a column of type TIME WITH TIMEZONE
    void addTimeTz() { colTypes.push_back(TimeTzOID); }
    /// @brief Adds a column of type NUMERIC
    void addNumeric() { colTypes.push_back(NumericOID); }

    /// @brief Adds a column of type CHAR
    void addChar() { colTypes.push_back(CharOID); }

    /// @brief Adds a column of type VARCHAR
    void addVarchar() { colTypes.push_back(VarcharOID); }

    /// @brief Adds a column of type BINARY
    void addBinary() { colTypes.push_back(BinaryOID); }

    /// @brief Adds a column of type VARBINARY
    void addVarbinary() { colTypes.push_back(VarbinaryOID); }

    /// @brief Adds a column of type VARBINARY
    void addLongVarchar() { colTypes.push_back(LongVarcharOID); }

    /// @brief Adds a column of type VARBINARY
    void addLongVarbinary() { colTypes.push_back(LongVarbinaryOID); }

    /// @brief Indicates that function can take any number and type of arguments
    void addAny() { varArgs = true; }

    void addUserDefinedType(const char *typeName) {
        colTypes.push_back(getUDTypeOid(typeName));
    }

    /// @cond INTERNAL
    size_t getColumnCount() const { return colTypes.size(); }

    // Returns the type of the field at the specified index.
    BaseDataOID getColumnType(size_t idx) const { return colTypes.at(idx); }

    // Update the type of the specified column
    void setColumnType(size_t idx, BaseDataOID type) {
        colTypes.at(idx) = type; // bounds check
    }

    // Default constructor - no varArgs (yet)
    ColumnTypes() : varArgs(false) {}

    bool isVarArgs() const { return varArgs; }

    std::vector<BaseDataOID> colTypes;
    /// @endcond

private:
    bool varArgs;
};

/**
 * @brief Represents the partition by and order by column information for
 * each phase in a multi-phase transform function
 *
 */
struct PartitionOrderColumnInfo
{
    PartitionOrderColumnInfo() : last_partition_col(-1), last_order_col(-1) {}

    int last_partition_col;
    int last_order_col;
};

/**
 * @brief Represents types and information to determine the size of the columns
 *        as input/output of a User Defined Function/Transform
 *
 * This class is used to exchange size and precision information between
 * Vertica and the user defined function/transform function. Vertica provides
 * the user code with size/precision information about the particular data
 * types that it has been called with, and expects the user code to provide
 * size/precision information about what it will return.
 */
class SizedColumnTypes
{
public:
    /// @brief Sets column properties. Properties have specific meaning in a given contexts
    ///        (e.g., in UDx parameter declaration) and may not apply in every context.
    struct Properties {
    public:
        Properties() : visible(false), required(false), canBeNull(true), comment() { }
        Properties(const Properties &props) :
            visible(props.visible), required(props.required), canBeNull(props.canBeNull), comment(props.comment) { }
        Properties(bool visible, bool required, bool canBeNull, const std::string comment):
            visible(visible), required(required), canBeNull(canBeNull), comment(comment) { }


        // Used in UDx parameter declaration
        bool visible; // If TRUE, the parameter will be displayed in system tables
        bool required; // If TRUE and not include in 'USING PARAMETERS' list, execution fails
        bool canBeNull; // If TRUE and parameter value is NULL, execution fails

        std::string comment; // Brief description
        static const uint32 MAX_COMMENT_LEN = 128;

        bool operator==(const Properties &right) const {
            return this->visible == right.visible &&
                this->required == right.required &&
                this->canBeNull == right.canBeNull &&
                this->comment == right.comment;
        }
        bool operator!=(const Properties &right) const {
            return !(*this == right);
        }
    };

    /// @cond INTERNAL
    /* Add a new element to the tuple at the end */
    void addArg(const VerticaType &dt, const std::string &fieldName = "")
    { argTypes.push_back(Field(dt, fieldName)); }

    void addArg(const VerticaType &dt, const std::string &fieldName, const Properties& props)
    { argTypes.push_back(Field(dt, fieldName, props)); }

    /* Add a new element to the tuple at the end */
    void addArg(Oid typeOid, int32 typeMod, const std::string &fieldName = "")
    { addArg(VerticaType(typeOid, typeMod), fieldName); }

    /* Add a new element to the tuple at the end */
    void addArg(Oid typeOid, int32 typeMod, const std::string &fieldName, const Properties& props)
    { addArg(VerticaType(typeOid, typeMod), fieldName, props); }

    /* Add a new partition element to the tuple at the end of the Pby columns */
    void addPartitionColumn(Oid typeOid, int32 typeMod, const std::string &fieldName = "")
    { addPartitionColumn(VerticaType(typeOid, typeMod), fieldName); }

    /* Add a new order element to the tuple at the end of the Oby columns */
    void addOrderColumn(Oid typeOid, int32 typeMod, const std::string &fieldName = "")
    { addOrderColumn(VerticaType(typeOid, typeMod), fieldName); }

    void validateStringLengthOrThrow(int32 len, const char* typeStr, int32 maxLength) {
        if (len > maxLength) {
            ereport(ERROR,
                    (errmsg("Length for type %s cannot exceed %d", typeStr, maxLength),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }
    }

    void validateUDTypeLengthOrThrow(Oid udTypeOid, const char *typeName, int32 len) {
        VerticaType vt(udTypeOid, -1);
        if (vt.isStringType()) {
            // Check that length be within data type bounds
            int32 maxStringLen = MAX_STRING_LENGTH;
            const Oid baseTypeOid = vt.getUnderlyingType();
            if (baseTypeOid == LongVarcharOID ||
                baseTypeOid == LongVarbinaryOID) {
                maxStringLen = MAX_LONG_STRING_LENGTH;
            }
            validateStringLengthOrThrow(len, typeName, maxStringLen);
        }
    }
    /// @endcond

    /// @brief Adds a partition column of the specified type
    ///        (only relevant to multiphase UDTs.)
    void addPartitionColumn(const VerticaType &dt, const std::string &fieldName = "")
    {
        int newLastPbyIdx = partitionOrderInfo.last_partition_col + 1;
        std::vector<Field>::iterator it = argTypes.begin();

        for (int i = 0; i < newLastPbyIdx && it < argTypes.end(); i++)
            ++it;

        argTypes.insert(it, Field(dt, fieldName));
        partitionOrderInfo.last_partition_col = newLastPbyIdx;
        partitionOrderInfo.last_order_col++;
    }

    /// @brief Adds an order column of the specified type.
    ///        (only relevant to multiphase UDTs.)
    void addOrderColumn(const VerticaType &dt, const std::string &fieldName = "")
    {
        int newLastObyIdx = partitionOrderInfo.last_order_col + 1;
        std::vector<Field>::iterator it = argTypes.begin();

        for (int i = 0; i < newLastObyIdx && it < argTypes.end(); i++)
            ++it;

        argTypes.insert(it, Field(dt, fieldName));
        partitionOrderInfo.last_order_col = newLastObyIdx;
    }

    /// @brief Adds a column of type INTEGER
    ///
    /// @param fieldName The name for the output column.
    void addInt(const std::string &fieldName = "")
    { addArg(Int8OID, -1, fieldName); }

    void addInt(const std::string &fieldName, const Properties& props)
    { addArg(VerticaType(Int8OID, -1), fieldName, props); }

    /// @brief Adds a column of type FLOAT
    ///
    /// @param fieldName The name for the output column.
    void addFloat(const std::string &fieldName = "")
    { addArg(Float8OID, -1, fieldName); }

    void addFloat(const std::string &fieldName, const Properties& props)
    { addArg(VerticaType(Float8OID, -1), fieldName, props); }

    /// @brief Adds a column of type BOOLEAN
    ///
    /// @param fieldName The name for the output column.
    void addBool(const std::string &fieldName = "")
    { addArg(BoolOID, -1, fieldName); }

    void addBool(const std::string &fieldName, const Properties& props)
    { addArg(VerticaType(BoolOID, -1), fieldName, props); }

    /// @brief Adds a column of type DATE
    ///
    /// @param fieldName The name for the output column.
    void addDate(const std::string &fieldName = "")
    { addArg(DateOID, -1, fieldName); }

    void addDate(const std::string &fieldName, const Properties& props)
    { addArg(DateOID, -1, fieldName, props); }

    /// @brief Adds a column of type INTERVAL/INTERVAL DAY TO SECOND
    ///
    /// @param precision The precision for the interval.
    ///
    /// @param range The range for the interval.
    ///
    /// @param fieldName The name for the output column.
    void addInterval(int32 precision, int32 range, const std::string &fieldName = "")
    { addArg(IntervalOID, VerticaType::makeIntervalTypeMod(precision, range), fieldName); }

    void addInterval(int32 precision, int32 range, const std::string &fieldName, const Properties& props)
    { addArg(IntervalOID, VerticaType::makeIntervalTypeMod(precision, range), fieldName, props); }

    /// @brief Adds a column of type INTERVAL YEAR TO MONTH
    ///
    ///
    /// @param range The range for the interval.
    ///
    /// @param fieldName The name for the output column.
    void addIntervalYM(int32 range, const std::string &fieldName = "")
    { addArg(IntervalYMOID, VerticaType::makeIntervalYMTypeMod(range), fieldName); }

    void addIntervalYM(int32 range, const std::string &fieldName, const Properties& props)
    { addArg(IntervalYMOID, VerticaType::makeIntervalYMTypeMod(range), fieldName, props); }

    /// @brief Adds a column of type TIMESTAMP
    ///
    /// @param precision The precision for the timestamp.
    ///
    /// @param fieldName The name for the output column.
    void addTimestamp(int32 precision, const std::string &fieldName = "")
    { addArg(TimestampOID, VerticaType::makeTimestampTypeMod(precision), fieldName); }

    void addTimestamp(int32 precision, const std::string &fieldName, const Properties& props)
    { addArg(TimestampOID, VerticaType::makeTimestampTypeMod(precision), fieldName, props); }

    /// @brief Adds a column of type TIMESTAMP WITH TIMEZONE
    ///
    /// @param precision The precision for the timestamp.
    ///
    /// @param fieldName The name for the output column.
    void addTimestampTz(int32 precision, const std::string &fieldName = "")
    { addArg(TimestampTzOID, VerticaType::makeTimestampTypeMod(precision), fieldName); }

    void addTimestampTz(int32 precision, const std::string &fieldName, const Properties& props)
    { addArg(TimestampTzOID, VerticaType::makeTimestampTypeMod(precision), fieldName, props); }

    /// @brief Adds a column of type TIME
    ///
    /// @param precision The precision for the time.
    ///
    /// @param fieldName The name for the output column.
    void addTime(int32 precision, const std::string &fieldName = "")
    { addArg(TimeOID, VerticaType::makeTimeTypeMod(precision), fieldName); }

    void addTime(int32 precision, const std::string &fieldName, const Properties& props)
    { addArg(TimeOID, VerticaType::makeTimeTypeMod(precision), fieldName, props); }

    /// @brief Adds a column of type TIME WITH TIMEZONE
    ///
    /// @param precision The precision for the time.
    ///
    /// @param fieldName The name for the output column.
    void addTimeTz(int32 precision, const std::string &fieldName = "")
    { addArg(TimeTzOID, VerticaType::makeTimeTypeMod(precision), fieldName); }

    void addTimeTz(int32 precision, const std::string &fieldName, const Properties& props)
    { addArg(TimeTzOID, VerticaType::makeTimeTypeMod(precision), fieldName, props); }

    /// @brief Adds a column of type NUMERIC
    ///
    /// @param precision The precision for the numeric value.
    ///
    /// @param scale The scale for the numeric value.
    ///
    /// @param fieldName The name for the output column.
    void addNumeric(int32 precision, int32 scale, const std::string &fieldName = "")
    { addArg(NumericOID, VerticaType::makeNumericTypeMod(precision, scale), fieldName); }

    void addNumeric(int32 precision, int32 scale, const std::string &fieldName, const Properties& props)
    { addArg(NumericOID, VerticaType::makeNumericTypeMod(precision, scale), fieldName, props); }

    /// @brief Adds a column of type CHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.
    void addChar(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Char", MAX_STRING_LENGTH);
        addArg(CharOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    void addChar(int32 len, const std::string &fieldName, const Properties& props) {
        validateStringLengthOrThrow(len, "Char", MAX_STRING_LENGTH);
        addArg(CharOID, VerticaType::makeStringTypeMod(len), fieldName, props);
    }

    /// @brief Adds a column of type VARCHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.
    void addVarchar(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Varchar", MAX_STRING_LENGTH);
        addArg(VarcharOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    void addVarchar(int32 len, const std::string &fieldName, const Properties& props) {
        validateStringLengthOrThrow(len, "Varchar", MAX_STRING_LENGTH);
        addArg(VerticaType(VarcharOID, VerticaType::makeStringTypeMod(len)), fieldName, props);
    }

    /// @brief Adds a column of type LONG VARCHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.
    void addLongVarchar(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Long Varchar", MAX_LONG_STRING_LENGTH);
        addArg(LongVarcharOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    void addLongVarchar(int32 len, const std::string &fieldName, const Properties& props) {
        validateStringLengthOrThrow(len, "Long Varchar", MAX_LONG_STRING_LENGTH);
        addArg(LongVarcharOID, VerticaType::makeStringTypeMod(len), fieldName, props);
    }

    /// @brief Adds a column of type BINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.
    void addBinary(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Binary", MAX_STRING_LENGTH);
        addArg(BinaryOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    void addBinary(int32 len, const std::string &fieldName, const Properties& props) {
        validateStringLengthOrThrow(len, "Binary", MAX_STRING_LENGTH);
        addArg(BinaryOID, VerticaType::makeStringTypeMod(len), fieldName, props);
    }

    /// @brief Adds a column of type VARBINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.
    void addVarbinary(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Varbinary", MAX_STRING_LENGTH);
        addArg(VarbinaryOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    void addVarbinary(int32 len, const std::string &fieldName, const Properties& props) {
        validateStringLengthOrThrow(len, "Varbinary", MAX_STRING_LENGTH);
        addArg(VarbinaryOID, VerticaType::makeStringTypeMod(len), fieldName, props);
    }

    /// @brief Adds a column of type LONG VARBINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.
    void addLongVarbinary(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Long Varbinary", MAX_LONG_STRING_LENGTH);
        addArg(LongVarbinaryOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    void addLongVarbinary(int32 len, const std::string &fieldName, const Properties& props) {
        validateStringLengthOrThrow(len, "Long Varbinary", MAX_LONG_STRING_LENGTH);
        addArg(LongVarbinaryOID, VerticaType::makeStringTypeMod(len), fieldName, props);
    }

    /// @brief Adds a column of a user-defined type
    ///
    /// @param typeName the name of the type
    /// @param len the length of the type field, in bytes
    /// @param fieldName the name of the field
    ///
    /// @param fieldName The name for the output column.
    void addUserDefinedType(const char *typeName, int32 len, const std::string &fieldName = "") {
        const Oid typeOid = getUDTypeOid(typeName);
        validateUDTypeLengthOrThrow(typeOid, typeName, len);
        addArg(typeOid, VerticaType::makeStringTypeMod(len), fieldName);
    }

    void addUserDefinedType(const char *typeName, int32 len, const std::string &fieldName, const Properties& props) {
        const Oid typeOid = getUDTypeOid(typeName);
        validateUDTypeLengthOrThrow(typeOid, typeName, len);
        addArg(getUDTypeOid(typeName), VerticaType::makeStringTypeMod(len), fieldName, props);
    }

    /// @brief Adds a partition column of type INTEGER
    ///
    /// @param fieldName The name for the output column.
    void addIntPartitionColumn(const std::string &fieldName = "")
    { addPartitionColumn(Int8OID, -1, fieldName); }

    /// @brief Adds a partition column of type FLOAT
    ///
    /// @param fieldName The name for the output column.
    void addFloatPartitionColumn(const std::string &fieldName = "")
    { addPartitionColumn(Float8OID, -1, fieldName); }

    /// @brief Adds a partition column of type BOOLEAN
    ///
    /// @param fieldName The name for the output column.
    void addBoolPartitionColumn(const std::string &fieldName = "")
    { addPartitionColumn(BoolOID, -1, fieldName); }

    /// @brief Adds a partition column of type DATE
    ///
    /// @param fieldName The name for the output column.
    void addDatePartitionColumn(const std::string &fieldName = "")
    { addPartitionColumn(DateOID, -1, fieldName); }

    /// @brief Adds a partition column of type INTERVAL/INTERVAL DAY TO SECOND
    ///
    /// @param precision The precision for the interval.
    ///
    /// @param range The range for the interval.
    ///
    /// @param fieldName The name for the output column.
    void addIntervalPartitionColumn(int32 precision, int32 range, const std::string &fieldName = "")
    { addPartitionColumn(IntervalOID, VerticaType::makeIntervalTypeMod(precision, range), fieldName); }

    /// @brief Adds a partition column of type INTERVAL YEAR TO MONTH
    ///
    /// @param range The range for the interval.
    ///
    /// @param fieldName The name for the output column.
    void addIntervalYMPartitionColumn(int32 range, const std::string &fieldName = "")
    { addPartitionColumn(IntervalYMOID, VerticaType::makeIntervalYMTypeMod(range), fieldName); }

    /// @brief Adds a partition column of type TIMESTAMP
    ///
    /// @param precision The precision for the timestamp.
    ///
    /// @param fieldName The name for the output column.
    void addTimestampPartitionColumn(int32 precision, const std::string &fieldName = "")
    { addPartitionColumn(TimestampOID, VerticaType::makeTimestampTypeMod(precision), fieldName); }

    /// @brief Adds a partition column of type TIMESTAMP WITH TIMEZONE
    ///
    /// @param precision The precision for the timestamp.
    ///
    /// @param fieldName The name for the output column.
    void addTimestampTzPartitionColumn(int32 precision, const std::string &fieldName = "")
    { addPartitionColumn(TimestampTzOID, VerticaType::makeTimestampTypeMod(precision), fieldName); }

    /// @brief Adds a partition column of type TIME
    ///
    /// @param precision The precision for the time.
    ///
    /// @param fieldName The name for the output column.
    void addTimePartitionColumn(int32 precision, const std::string &fieldName = "")
    { addPartitionColumn(TimeOID, VerticaType::makeTimeTypeMod(precision), fieldName); }

    /// @brief Adds a partition column of type TIME WITH TIMEZONE
    ///
    /// @param precision The precision for the time.
    ///
    /// @param fieldName The name for the output column.
    void addTimeTzPartitionColumn(int32 precision, const std::string &fieldName = "")
    { addPartitionColumn(TimeTzOID, VerticaType::makeTimeTypeMod(precision), fieldName); }

    /// @brief Adds a partition column of type NUMERIC
    ///
    /// @param precision The precision for the numeric value.
    ///
    /// @param scale The scale for the numeric value.
    ///
    /// @param fieldName The name for the output column.
    void addNumericPartitionColumn(int32 precision, int32 scale, const std::string &fieldName = "")
    { addPartitionColumn(NumericOID, VerticaType::makeNumericTypeMod(precision, scale), fieldName); }

    /// @brief Adds a partition column of type CHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.
    void addCharPartitionColumn(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Char", MAX_STRING_LENGTH);
        addPartitionColumn(CharOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    /// @brief Adds a partition column of type VARCHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.
    void addVarcharPartitionColumn(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Varchar", MAX_STRING_LENGTH);
        addPartitionColumn(VarcharOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    /// @brief Adds a partition column of type VARCHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.
    void addLongVarcharPartitionColumn(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Long Varchar", MAX_LONG_STRING_LENGTH);
        addPartitionColumn(LongVarcharOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    /// @brief Adds a partition column of type BINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.
    void addBinaryPartitionColumn(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Binary", MAX_STRING_LENGTH);
        addPartitionColumn(BinaryOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    /// @brief Adds a partition column of type VARBINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.
    void addVarbinaryPartitionColumn(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Varbinary", MAX_STRING_LENGTH);
        addPartitionColumn(VarbinaryOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    /// @brief Adds a partition column of type VARBINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.
    void addLongVarbinaryPartitionColumn(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Long Varbinary", MAX_LONG_STRING_LENGTH);
        addPartitionColumn(LongVarbinaryOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    /// @brief Adds an order column of type INTEGER
    ///
    /// @param fieldName The name for the output column.
    void addIntOrderColumn(const std::string &fieldName = "")
    { addOrderColumn(Int8OID, -1, fieldName); }

    /// @brief Adds an order column of type FLOAT
    ///
    /// @param fieldName The name for the output column.
    void addFloatOrderColumn(const std::string &fieldName = "")
    { addOrderColumn(Float8OID, -1, fieldName); }

    /// @brief Adds an order column of type BOOLEAN
    ///
    /// @param fieldName The name for the output column.
    void addBoolOrderColumn(const std::string &fieldName = "")
    { addOrderColumn(BoolOID, -1, fieldName); }

    /// @brief Adds an order column of type DATE
    ///
    /// @param fieldName The name for the output column.
    void addDateOrderColumn(const std::string &fieldName = "")
    { addOrderColumn(DateOID, -1, fieldName); }

    /// @brief Adds an order column of type INTERVAL/INTERVAL DAY TO SECOND
    ///
    /// @param precision The precision for the interval.
    ///
    /// @param range The range for the interval.
    ///
    /// @param fieldName The name for the output column.
    void addIntervalOrderColumn(int32 precision, int32 range, const std::string &fieldName = "")
    { addOrderColumn(IntervalOID, VerticaType::makeIntervalTypeMod(precision, range), fieldName); }

    /// @brief Adds an order column of type INTERVAL YEAR TO MONTH
    ///
    ///
    /// @param range The range for the interval.
    ///
    /// @param fieldName The name for the output column.
    void addIntervalYMOrderColumn(int32 range, const std::string &fieldName = "")
    { addOrderColumn(IntervalYMOID, VerticaType::makeIntervalYMTypeMod(range), fieldName); }

    /// @brief Adds an order column of type TIMESTAMP
    ///
    /// @param precision The precision for the timestamp.
    ///
    /// @param fieldName The name for the output column.
    void addTimestampOrderColumn(int32 precision, const std::string &fieldName = "")
    { addOrderColumn(TimestampOID, VerticaType::makeTimestampTypeMod(precision), fieldName); }

    /// @brief Adds an order column of type TIMESTAMP WITH TIMEZONE
    ///
    /// @param precision The precision for the timestamp.
    ///
    /// @param fieldName The name for the output column.
    void addTimestampTzOrderColumn(int32 precision, const std::string &fieldName = "")
    { addOrderColumn(TimestampTzOID, VerticaType::makeTimestampTypeMod(precision), fieldName); }

    /// @brief Adds an order column of type TIME
    ///
    /// @param precision The precision for the time.
    ///
    /// @param fieldName The name for the output column.
    void addTimeOrderColumn(int32 precision, const std::string &fieldName = "")
    { addOrderColumn(TimeOID, VerticaType::makeTimeTypeMod(precision), fieldName); }

    /// @brief Adds an order column of type TIME WITH TIMEZONE
    ///
    /// @param precision The precision for the time.
    ///
    /// @param fieldName The name for the output column.
    void addTimeTzOrderColumn(int32 precision, const std::string &fieldName = "")
    { addOrderColumn(TimeTzOID, VerticaType::makeTimeTypeMod(precision), fieldName); }

    /// @brief Adds an order column of type NUMERIC
    ///
    /// @param precision The precision for the numeric value.
    ///
    /// @param scale The scale for the numeric value.
    ///
    /// @param fieldName The name for the output column.
    void addNumericOrderColumn(int32 precision, int32 scale, const std::string &fieldName = "")
    { addOrderColumn(NumericOID, VerticaType::makeNumericTypeMod(precision, scale), fieldName); }

    /// @brief Adds an order column of type CHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.
    void addCharOrderColumn(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Char", MAX_STRING_LENGTH);
        addOrderColumn(CharOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    /// @brief Adds an order column of type VARCHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.
    void addVarcharOrderColumn(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Varchar", MAX_STRING_LENGTH);
        addOrderColumn(VarcharOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    /// @brief Adds an order column of type VARCHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.
    void addLongVarcharOrderColumn(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Long Varchar", MAX_LONG_STRING_LENGTH);
        addOrderColumn(LongVarcharOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    /// @brief Adds an order column of type BINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.
    void addBinaryOrderColumn(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Binary", MAX_STRING_LENGTH);
        addOrderColumn(BinaryOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    /// @brief Adds an order column of type VARBINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.
    void addVarbinaryOrderColumn(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Varbinary", MAX_STRING_LENGTH);
        addOrderColumn(VarbinaryOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    /// @brief Adds an order column of type VARBINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.
    void addLongVarbinaryOrderColumn(int32 len, const std::string &fieldName = "") {
        validateStringLengthOrThrow(len, "Long Varbinary", MAX_LONG_STRING_LENGTH);
        addOrderColumn(LongVarbinaryOID, VerticaType::makeStringTypeMod(len), fieldName);
    }

    /// @brief Returns the number of columns
    size_t getColumnCount() const { return argTypes.size(); }

    /// @brief Returns the type of the column at the specified index.
    /// @param idx The index of the column
    /// @return a VerticaType object describing the column's data type.
    const VerticaType &getColumnType(size_t idx) const {
        return argTypes.at(idx)._type; // bounds check
    }

    /// @brief Returns the type of the column at the specified index.
    /// @param idx The index of the column
    VerticaType &getColumnType(size_t idx) {
        return argTypes.at(idx)._type; // bounds check
    }

    /*
     * Update the type of the specified column
     */
    /// @cond INTERNAL
    void setColumnType(size_t idx, const VerticaType &t,
                       const std::string &fieldName = "") {
        argTypes.at(idx) = Field(t, fieldName); // bounds check
    }
    /// @endcond

    /// @brief Returns the name of the column at the specified index
    /// @param idx The index of the column
    const std::string &getColumnName(size_t idx) const {
        return argTypes.at(idx)._name; // bounds check
    }

    /// @brief Returns column properties
    /// @param idx The index of the column
    const Properties& getColumnProperties(size_t idx) const {
        return argTypes.at(idx).props;
    }

    /// @brief Sets column properties
    /// @param idx The index of the column
    void setColumnProperties(size_t idx, const Properties &props) {
        argTypes.at(idx).props = props;
    }

    /// @brief Indicates whether the column at the specified index is a PARTITION BY column
    /// @param idx The index of the column
    bool isPartitionByColumn(int idx) const {
        return idx <= partitionOrderInfo.last_partition_col;
    }

    /// @brief Indicates whether the column at the specified index is an ORDER BY column
    /// @param idx The index of the column
    bool isOrderByColumn(int idx) const {
        return idx > partitionOrderInfo.last_partition_col &&
            idx <= partitionOrderInfo.last_order_col;
    }

    /// @brief Sets the PARTITION BY and ORDER BY column indexes
    /// @param partition_idx Index of the last partition-by column
    /// @param order_idx Index of the last order-by column
    void setPartitionOrderColumnIdx(int partition_idx, int order_idx)
    {
        partitionOrderInfo.last_partition_col = partition_idx;
        partitionOrderInfo.last_order_col = order_idx;
    }

    /// @brief Sets the PARTITION BY and ORDER BY column indexes from another SizedColumnTypes object
    /// @param other The SizedColumnTypes object to set the indexes from
    void setPartitionOrderColumnIdx(const SizedColumnTypes &other) {
        partitionOrderInfo = other.partitionOrderInfo;
    }

    /// @brief Gets the last PARTITION BY column index
    int getLastPartitionColumnIdx() const {
        return partitionOrderInfo.last_partition_col;
    }

    /// @brief Gets the last ORDER BY column index
    int getLastOrderColumnIdx() const {
        return partitionOrderInfo.last_order_col;
    }

    /// @brief Retrieves indexes of PARTITION BY columns in the OVER() clause.
    ///        Indexes in cols can be used in conjunction with other functions,
    ///        e.g. getColumnType(size_t) and getColumnName(size_t).
    /// @param cols A vector to store the retrieved column indexes.
    void getPartitionByColumns(std::vector<size_t>& cols) const {
        cols.clear();
        int lastPbyIdx = getLastPartitionColumnIdx();

        if (lastPbyIdx > -1) {
            // Append to cols the sequence: [0, 1, ..., lastPbyIdx]
            for (size_t i = 0; i <= (size_t) lastPbyIdx; i++)
                cols.push_back(i);
        }
    }

    /// @brief Retrieves indexes of ORDER BY columns in the OVER() clause.
    ///        Indexes in cols can be used in conjunction with other functions,
    ///        e.g. getColumnType(size_t) and getColumnName(size_t).
    /// @param cols A vector to store the retrieved column indexes.
    void getOrderByColumns(std::vector<size_t>& cols) const {
        cols.clear();
        int lastObyIdx = getLastOrderColumnIdx();
        int lastPbyIdx = getLastPartitionColumnIdx();

        if (lastObyIdx > lastPbyIdx) {
            // Append to cols the sequence: [i, i+1, ..., lastObyIdx]
            for (size_t i = lastPbyIdx + 1; i <= (size_t) lastObyIdx; i++)
                cols.push_back(i);
        }
    }

    /// @brief Retrieves indexes of argument columns.
    ///        Indexes in cols can be used in conjunction with other functions,
    ///        e.g. getColumnType(size_t) and getColumnName(size_t).
    /// @param cols A vector to store the retrieved column indexes.
    void getArgumentColumns(std::vector<size_t>& cols) const {
        cols.clear();
        size_t nOverCl = getOverClauseCount();
        size_t nCols = getColumnCount();

        // Arguments follow the Over() clause columns.
        for (size_t i = nOverCl; i < nCols; i++) {
            cols.push_back(i);
        }
    }

    /// @cond INTERNAL
    void reset(){
        argTypes.clear();
        partitionOrderInfo.last_order_col=-1;
        partitionOrderInfo.last_partition_col=-1;
    }

private:
    struct Field
    {
        Field(const VerticaType &t, const std::string name) :
            _type(t), _name(name), props() {}

        Field(const VerticaType &t, const std::string name, const Properties &props) :
            _type(t), _name(name), props(props) {}

        VerticaType      _type;
        std::string      _name;
        Properties props;
    };

    std::vector<Field> argTypes;     // list of argument type
    PartitionOrderColumnInfo partitionOrderInfo;

    /// @cond INTERNAL
    /// Returns the number of columns in the OVER() clause.
    size_t getOverClauseCount() const {
        int nPbyOby = getLastOrderColumnIdx();
        int lastPbyIdx = getLastPartitionColumnIdx();

        if (nPbyOby < lastPbyIdx)
            nPbyOby = lastPbyIdx;

        return ++nPbyOby;
    }
    /// @endcond
};

/**
 * @brief A pool-based allocator that is provided to simplify memory
 * management for UDF implementors.
 */
class VTAllocator
{
public:
    virtual ~VTAllocator() {}

    /**
     * Allocate size_t bytes of memory on a pool. This memory is guaranteed to
     * persist beyond the destroy call but might have been destroyed when the
     * dtor is run.
     */
    virtual void *alloc(size_t size) = 0;

    // construct an object/objects in Vertica managed memory area
    // not exposed for this release
//    virtual void *constr(size_t obj_size, void (*destr)(void *), size_t array_size = 1) = 0;
};

/**
 * Bit mast for communicatnig the available UDx debug logging levels.
**/
enum UDXDebugLevels {
    UDXDebugging_WARNING = 0x0001,    // Basics::DlogManager::DLOGWARNING
    UDXDebugging_INFO = 0x0002,       // Basics::DlogManager::DLOGINFO
    UDXDebugging_BASIC = 0x0008,      // Basics::DlogManager::DLOGBASIC
    UDXDebugging_ALL = 0xFFFF,        // Basics::DlogManager::DLOGALL_M
};


/**
 * @brief Parent class for UDx factories; not intended for direct use by
 * applications.
 */
class UDXFactory
{
public:
    virtual ~UDXFactory() {}

    /**
     * Provides the argument and return types of the UDX
     */
    virtual void getPrototype(ServerInterface &srvInterface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType) = 0;

    /**
     * Function to tell Vertica what the return types (and length/precision if
     * necessary) of this UDX are.
     *
     * For CHAR/VARCHAR types, specify the max length,
     *
     * For NUMERIC types, specify the precision and scale.
     *
     * For Time/Timestamp types (with or without time zone),
     * specify the precision, -1 means unspecified/don't care
     *
     * For IntervalYM/IntervalDS types, specify the precision and range
     *
     * For all other types, no length/precision specification needed
     *
     * @param argTypes Provides the data types of arguments that
     *                       this UDT was called with. This may be used
     *                       to modify the return types accordingly.
     *
     * @param returnType User code must fill in the names and data
     *                       types returned by the UDT.
     */
    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnType) = 0;

    /**
     * Function to tell Vertica the name and types of parameters that this
     * function uses. Vertica will use this to warn function callers that certain
     * parameters they provide are not affecting anything, or that certain
     * parameters that are not being set are reverting to default values.
     */
    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {}

    /** The type of UDX instance this factory produces */
    enum UDXType
    {
        /// @cond INTERNAL
        INVALID_UDX, // Invalid type (used for initialization)
        /// @endcond
        FUNCTION, // User Defined Scalar Function
        TRANSFORM, // User Defined Transform Function
        ANALYTIC, // User Defined Analytic Function
        MULTI_TRANSFORM, // Multi-phase transform function
        AGGREGATE, // User Defined Aggregate Function
        LOAD_SOURCE, // Data source for Load operations
        LOAD_FILTER, // Intermediate processing for raw Load stream data
        LOAD_PARSER, // User Defined Parser
        FILESYSTEM, // UD Filesystem
        TYPE,
        CURSOR_TRANSFORM, // Transform with direct access to projection data via cursors
    };

    /**
     * @return the type of UDX Object instance this factory returns.
     *
     * @note User subclasses should use the appropriate subclass of
     * UDXFactory and not override this method on their own.
     */
    virtual UDXType getUDXFactoryType() = 0;

    /**
     * Set the resource required for each instance of the UDX Object subclass.
     * Users should override only one of the following methods.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     * @param res a VResources object used to tell Vertica what resources are needed by the UDX
     */
    virtual void getPerInstanceResources(ServerInterface &srvInterface, VResources &res) {
        // We certainly want a nonzero memory allocation.
        // User can always override this.
        res.scratchMemory = 10LL * 1024LL * 1024LL;
    }
     /**
     * Set the resource required for each instance of the UDX Object subclass.
     * Users should override only one of the following methods.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     * @param res a VResources object used to tell Vertica what resources are needed by the UDX
     * @param argTypes Provides the data types of arguments that this UDT was called with
     */

    virtual void getPerInstanceResources(ServerInterface &srvInterface, VResources &res,
        const SizedColumnTypes &inputTypes) {

        getPerInstanceResources(srvInterface, res);
    }

protected:
    // set by Vertica to mirror the object name created in DDL
    // used for providing sensible error message
    std::string sqlName;

    // used by Vertica internally
    Oid libOid;

    friend class ::UdfSupport;
};

/**
 * @brief Base class for Vertica User-Defined extensions (the function
 * classes themselves). Not intended for direct use by applications.
 */
class UDXObject
{
public:
    /**
     * Destructors MAY NOT throw errors / exceptions. Exceptions thrown during
     * the destructor will be ignored.
     */
#if __cplusplus >= 201103L
    virtual ~UDXObject() noexcept(false) {}
#else
    virtual ~UDXObject() {}
#endif

    /**
     * Perform per instance initialization. This function may throw errors.
     */
    virtual void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes) {}

    /**
     * Perform per instance destruction.  This function may throw errors
     */
    virtual void destroy(ServerInterface &srvInterface, const SizedColumnTypes &argTypes) {}
    /**
     * Perform per instance destruction and write session parameters to be used by UDxs that are invoked after this one returns.
     * This function may throw errors
     */
    virtual void destroy(ServerInterface &srvInterface, const SizedColumnTypes &argTypes, SessionParamWriterMap &udSessionParams) {
        destroy(srvInterface, argTypes);
    }
};

class UDXObjectCancelable : public UDXObject
{
public:
    UDXObjectCancelable() : canceled(false) {}

    /**
     * This function is invoked from a different thread when the execution is canceled
     * This baseclass cancel should be called in any override.
     */
    virtual void cancel(ServerInterface &srvInterface)
    {
        canceled = true;
    }

    /**
     * Returns true if execution was canceled.
     */
    bool isCanceled() { return canceled; }

protected:
    volatile bool canceled;
};

/**
 * Enums to allow programmatic specification of volatility and strictness
 */
enum volatility {
    DEFAULT_VOLATILITY,
    VOLATILE,
    IMMUTABLE,
    STABLE
};

enum strictness {
    DEFAULT_STRICTNESS,
    CALLED_ON_NULL_INPUT,
    RETURN_NULL_ON_NULL_INPUT,
    STRICT
};

/**
 * @brief Interface for User-Defined Scalar Function (UDSF).  A UDSF produces
 * a single value from a row of data.
 *
 * A UDSF can be used anywhere a native function can be used, except
 * CREATE TABLE BY PARTITION and SEGMENTED BY expressions.
 *
 * A ScalarFunction must have an associated ScalarFunctionFactory.
 *
 */
class ScalarFunction : public UDXObject
{
public:
    /**
     * Invoke a user defined function on a set of rows. As the name suggests, a
     * batch of rows are passed in for every invocation to amortize performance.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     *
     * @param arg_reader input rows
     *
     * @param res_writer output location
     *
     * @note @li This methods may be invoked by different threads at different
     * times, and by a different thread than the constructor.
     *
     * @li The order in which the function sees rows is not guaranteed.
     *
     * @li C++ exceptions may NOT be thrown out of this method. Use the vertica
     * specific vt_throw_exception() function or vt_report_error() macro instead
     */
    virtual void processBlock(ServerInterface &srvInterface,
                              BlockReader &arg_reader,
                              BlockWriter &res_writer) = 0;

    /**
     * Invoke a user defined function to determine the output value range of this function.
     * Ranges are represented by a minimum/maximum pair of values (inclusive).
     * The developer is responsible to provide an output value range
     * on the basis of the input argument ranges.
     * Minimum/maximum values of ranges are of the same type as defined in the metadata
     * class getPrototype() function.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     *
     * @param inRange input value range
     *
     * @param outRange output value range
     * @remark By default, the ValueRangeWriter object can have NULL values,
     * values in the range are unsorted, and it is unbounded, i.e., the following
     * functions return as follows:
     * @li outRange.canHaveNulls() == true
     * @li outRange.getSortedness() == EE::SORT_UNORDERED
     * @li outRange.isBounded() == false
     *
     * @note @li This methods may be invoked by different threads at different
     * times, and by a different thread than the constructor.
     *
     * @li C++ exceptions may NOT be thrown out of this method. Use the vertica
     * specific vt_throw_exception() function or vt_report_error() macro instead
     *
     * @li Invoking vt_throw_exception() or vt_report_error() from this method
     * will not stop the function execution, which may still complete successfully.
     * Instead, the output range will be discarded, and a WARNING message
     * will be written to the Vertica log
     */
    virtual void getOutputRange(ServerInterface &srvInterface, ValueRangeReader &inRange, ValueRangeWriter &outRange) {
        // Output range is unbounded by default
    }

protected:

    friend class ::UdfSupport;
};

/**
 * @brief Interface for declaring parameters and return types for, and
 * instantiating, an associated ScalarFunction.
 */
class ScalarFunctionFactory : public UDXFactory
{
public:
    ScalarFunctionFactory() : vol(DEFAULT_VOLATILITY), strict(DEFAULT_STRICTNESS) {}
    virtual ~ScalarFunctionFactory() {}

    /**
     * For scalar functions, this function needs to be overridden only if the
     * return type needs length/precision specification.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     *
     * @param argTypes The data type of the return value defined by processBlock()
     *
     * @param returnType The size of the data returned by processBlock()
     */
    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnType)
    {
        ColumnTypes p_argTypes;
        ColumnTypes p_returnType;
        getPrototype(srvInterface, p_argTypes, p_returnType);
        if (p_returnType.getColumnCount() != 1) {
            ereport(ERROR,
                    (errmsg("User Defined Scalar Function can only have 1 return column, but %zu is provided",
                            p_returnType.getColumnCount()),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }
        BaseDataOID typeOid = p_returnType.getColumnType(0);
        switch (p_returnType.getColumnType(0))
        {
        case CharOID:
        case VarcharOID:
        case LongVarcharOID:
        case BinaryOID:
        case VarbinaryOID:
        case LongVarbinaryOID:
        case NumericOID:
        case IntervalOID:
        case IntervalYMOID:
        case TimestampOID:
        case TimestampTzOID:
        case TimeOID:
        case TimeTzOID:
            ereport(ERROR,
                    (errmsg("The data type requires length/precision specification"),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
            break;
        default:
            returnType.addArg(typeOid, -1); // use default typmod
        }
    }

    /**
     * @return an ScalarFunction object which implements the UDx API described
     * by this metadata.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     *
     * @note More than one object may be instantiated per query.
     *
     *
     *
     */
    virtual ScalarFunction *createScalarFunction(ServerInterface &srvInterface) = 0;

    /**
     * Strictness and Volatility settings that the UDSF programmer can set
     * Defaults are VOLATILE and CALLED_ON_NULL_INPUT
     */
    volatility vol;
    strictness strict;

protected:
    /**
     * @return the object type internally used by Vertica
     */
    virtual UDXType getUDXFactoryType() { return FUNCTION; }

};

/**
 * @brief Interface for User-Defined Transform Function (UDTF).  A UDTF
 * operates on a table segment and returns zero or more rows of data.
 *
 * UDTFs can only be used in the SELECT list of a query. UDTFs are cancelable.
 *
 * A TransformFunction must have an associated TransformFunctionFactory.
 */
class TransformFunction : public UDXObjectCancelable
{
public:
    /**
     * Invoke a user defined transform on a set of rows. As the name suggests, a
     * batch of rows are passed in for every invocation to amortize performance.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     *
     * @param input_reader input rows
     *
     * @param output_writer output location
     */
    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &input_reader,
                                  PartitionWriter &output_writer) = 0;
};

/**
 * @brief Interface for declaring parameters and return types for, and
 * instantiating, an associated TransformFunction.
 */
class TransformFunctionFactory : public UDXFactory
{
public:
    virtual ~TransformFunctionFactory() {}

    /** Called when Vertica needs a new TransformFunction object to process a 
     * UDTF function call.
     *
     * @return a TransformFunction object which implements the UDx API described
     * by this metadata.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     *
     * @note More than one object may be instantiated per query.
     *
     */
    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface) = 0;

protected:
    /**
     * @return the object type internally used by Vertica
     */
    virtual UDXType getUDXFactoryType() { return TRANSFORM; }
};

/**
 * @brief Class for reading and writing to a blob. A blob is divided internally
 * into several chunks, this gives a cursor to one of the chunks. To change
 * which chunk the cursor points to, the user may call a function
 * 'switchToChunk(i)'. It also contains metadata about its blob.
 */
class UDxBlobCursor {
public:
    virtual ~UDxBlobCursor() {}

    /**
     * Get the name of the blob
     *
     * @return the name of the blob
     */
    virtual std::string getBlobName() const = 0;

    /**
     * @return the namespace of the blob
     */
    virtual std::string getBlobNamespace() const = 0;

    /**
     * @return the number of chunks in the blob
     */
    virtual size_t getNumberOfChunks() const = 0;

    /**
     * @return the total size of the blob in bytes
     */
    virtual vpos getTotalBlobSize() = 0;

    /**
     * @return the total amount of memory reserved for this blob
     */
    virtual vpos getTotalReservedMemory() = 0;

    /**
     * Change which chunk this cursor points to
     *
     * @param i which chunk to switch to
     */
    virtual void switchToChunk(size_t i) = 0;

    /**
     * @return the index of the current chunk pointed to by the cursor
     */
    virtual size_t getCurrentChunkIndex() const = 0;

    /**
     * @return the current size of the current chunk in bytes
     */
    virtual vpos getSize() const = 0;

    /**
     * @return the maximum size of the current chunk in bytes
     */
    virtual vpos getMaxSize() const = 0;

    /**
     * @return a vector containing the size of each chunk in bytes
     */
    virtual std::vector<vpos> getChunkSizes() = 0;

    /**
     * Gets a read pointer to the chunk at the given offset, with nBytes
     * bytes available for read
     *
     * @note This pointer may be invalidated on subsequent calls to this
     * function
     *
     * @return a pointer to the chunk data at this offset
     */
    virtual const void *getReadPtr(vpos offset, vpos nBytes) = 0;

    /**
     * Writes data from a source buffer into the chunk at a given offset
     *
     * @param src       a buffer of bytes to write
     * @param tgtOffset the position in the chunk where write should begin
     * @param nBytes    the number of bytes to write from the buffer
     */
    virtual void write(const void *src, vpos tgtOffset, vpos nBytes) = 0;

    /**
     * Truncates the current chunk to the current maximum offset that has been
     * written to, and changes the max size of the chunk to be its current size
     * so that it cannot be expanded in the future
     */
    virtual void finalizeWrite() = 0;

    /**
     * Truncates the current chunk to the offset specified by finalSize that has been
     * written to, and changes the max size of the chunk to be its current size
     * so that it cannot be expanded in the future
     *
     * @param finalSize the final desired size of the chunk after write
     */
    virtual void finalizeWrite(vpos finalSize) = 0;
};

/** Direct projection data access API for UDx */
class UDxCursor
{
public:
    virtual ~UDxCursor() {}

    virtual std::string getName() {return "<unknown>";}

    // Structured metadata
    virtual int getNumColumns() = 0;
    virtual VerticaType getColumnType(int col) = 0;
    virtual vpos getRecordCount() = 0;

    // Structured read (generic)
    virtual const void *peekValue(int col) = 0;
    virtual bool endOfData() = 0;
    virtual bool nextRecord() = 0;

    // Structured read (unsorted)
    virtual vpos getPosition() = 0;
    virtual bool goToPosition(vpos pos) = 0;

    // Structured read (sorted / merging)

    /**
     * Implementation may want something more sophisticated than a single
     *  position for handling sorted data.  This structure is for that internal
     *  state.
     */
    struct CursorPos;
    /** Allocate state */
    virtual CursorPos *allocCursorPos() = 0;
    /** Set a position to the beginning of the data */
    virtual void setToStart(CursorPos *pos) = 0;
    /** Set a position to the end of the data */
    virtual void setToEnd(CursorPos *pos) = 0;
    /** Get the current position of sorted read */
    virtual void getCurrentPosition(CursorPos *pos) = 0;
    /** Go to a previously saved position */
    virtual bool goToPosition(CursorPos *pos) = 0;
    /** Count the number of records between two positions */
    virtual vpos countRecordsBetween(CursorPos *s, CursorPos *e) = 0;
    /**
     * Very elaborate function for finding records by value within the data.
     *   This allows initial search within the sort order, or refining a search
     *     between two positions.
     *   Search is based on values, and can include the end value or stop just
     *     short of it.
     *   Search can return
     * @param nCols the number of columns being searched
     * @param cols the columns being searched; if NULL the lead column(s)
     * @param startVal the start value of the range to find; if NULL the start of data
     * @param endVal the end value of the range to find; if NULL the end of the data
     * @param inclusiveEndVal if true, include the ending value, otherwise stop short of it
     * @param start Optional cursor to be filled in with the start of the values found
     * @param end Optional cursor to be filled in with the end of the values found
     * @param withinRangeStart Optional cursor to restrict the range of the search; it will start here
     * @param withinRangeEnd Optional cursor to restrict the range of the search; it will end here
     * @return the number of matching records
     */
    virtual vpos findMatchingRecords(void **startVal, void **endVal, int nCols, int *cols,
                                     bool inclusiveEndVal, CursorPos *start, CursorPos *end,
                                     CursorPos *withinRangeStart, CursorPos *withinRangeEnd) = 0;
    /** Find next matching record, from list of candidates */
    virtual int findNextMatchingRecord(void **findVals, int *findStrides, int nvals, int nCols, int *cols,
                                       CursorPos *hitStart, CursorPos *hitEnd,
                                       CursorPos *findStart, CursorPos *findEnd,
                                       int startFrom = 0) = 0;
    /** Intersect column, within sorted data, from current cursor */
    virtual int countMatchingRecords(void **keys, int *kstrides, int nCols, int *cols, int nkeys,
                                       vpos *hits, CursorPos *start, CursorPos *end) = 0;
    /** Read records between two cursors; do not keep them sorted */
    virtual int readRecordsUnsorted(CursorPos *start, CursorPos *end, void **data, int *strides, int nrec, int ncol, int *cols) = 0;
    /** Read records between two cursors; do not keep them sorted */
    virtual int readRecordsUnsortedInline(CursorPos *start, CursorPos *end, void *data, int rstride, int nrec, int ncol, int *cols) = 0;
    /** Read records between two cursors; keeps them sorted according to cursor request */
    virtual int readRecordsSorted(CursorPos *start, CursorPos *end, void **data, int *strides, int nrec, int ncol, int *cols) = 0;
    /** Read records between two cursors; keeps them sorted according to cursor request */
    virtual int readRecordsSortedInline(CursorPos *start, CursorPos *end, void *data, int rstride, int nrec, int ncol, int *cols) = 0;
    /** Read single RLE column unsorted */
    virtual int readRLEUnsorted(CursorPos *start, CursorPos *end, void *data, int stride, vpos *counts, int nruns, int col) = 0;
    /** Read single RLE column sorted */
    virtual int readRLESorted(CursorPos *start, CursorPos *end, void *data, int stride, vpos *counts, int nruns, int col) = 0;
    /** Filtered records fetch, by key, expecting one match per input key */
    virtual int readMatchingRecords(CursorPos *start, CursorPos *end, void **data, int *dstrides, void *keys, int kstride, int nrecs, int ndcols, int *dcols, int kcol) = 0;

    // Structured write
    virtual void truncateTuples() = 0;
    virtual void beginTupleWrite() = 0;
    virtual void finalizeTupleWrite() = 0;
    virtual vpos getNumTuplesWritten() = 0;
    virtual void writeInlineTuples(const void *ptr, int tstride, vpos n) = 0;
    virtual void writeDATuples(const void *ptr, const void *da, int tstride, vpos n) = 0;
    virtual void writeColumnarTuples(const void **colptrs, int *colstrides, vpos n) = 0;
    virtual vpos reserveTupleWriteSpace(vpos ntuples) = 0;
    virtual vpos tupleWriteAvailable() = 0;
    virtual void writeColumn(int col, const void *src, int stride, vpos n) = 0;
    virtual vpos getTupleWritePointers(void **ptrs, int *strides, vpos n) = 0;
    virtual vpos getColWritePtr(int col, void **dst, int *pstride, vpos n) = 0;
    virtual vpos finalizeTuples(vpos nTuples) = 0;

    // Shard control
    virtual int getNumShards() = 0;
    virtual int getCurrentShard() = 0;
    virtual void switchToShard(int sn) = 0;

    // Cursor control; affects the current shard
    // Indicate that the data is to be kept for read even if we start rewriting
    virtual void snapshotForRead() = 0;
    // Release snapshot
    virtual void closeRead() = 0;
    // Discard all data after the last read cursor is closed
    virtual void markForDiscard() = 0;
};

/** Structure to tell about parallelism situation */
struct ParallelismInfo
{
    virtual ~ParallelismInfo(){}
    // How many instances total
    virtual int getNumPeers() = 0;
    // Which one is us
    virtual int getPeerIndex() = 0;
    // Are we the leader
    bool isLeader() {return getPeerIndex() == 0;}

    // Use as a rendezvous; all show up and wait
    virtual bool awaitRendezvous() = 0;
    // Use as leader/followers, the leader
    virtual bool notifyFollowers() = 0;
    // Use as leader / folloers, wait for leader
    virtual bool waitForLeader() = 0;
    // Disrupt rendezvous, leader/follower with an error
    virtual void markError() = 0;
    // Disrupt rendezvous, leader/follower with a cancel
    virtual void markCanceled() = 0;
    // Was canceled?
    virtual bool isCanceled() = 0;
    // Hit error?
    virtual bool isErrored() = 0;

    // Shared data struct
    virtual int getSharedSize() = 0;
    virtual void *getSharedPtr() = 0;
};

class CounterController;

/**
 * @brief Interface for Cursored UDTF, the actual code to process
 * a partition of data coming in as a stream, or to access projection cursors.
 */
class CursorTransformFunction : public TransformFunction
{
public:
    /**
     * If set to true, processPartition will be called even if there are
     *  no data partitions coming
     */
    bool runProcessPartitionIfEmpty;

    /** Default constructor */
    CursorTransformFunction() : runProcessPartitionIfEmpty(false) {}

    /**
     * Set the cursors or the UDx to use, prior to processPartition.
     * @param srvInterface server interface
     * @param nCursors number of cursors to read
     * @param cursors direct projection access
     */
    virtual void setCursors(ServerInterface &srvInterface,
                            int nCursors,
                            UDxCursor **cursors) {}

    /** Set the blobs the requested in the UDx factory
     * @param srvInterface server interface
     * @param blobs cursors to the blobs requested
     */
    virtual void setBlobs(ServerInterface &srvInterface,
                          const std::vector< UDxBlobCursor * > &blobs) {};

    /**
     * Set parallelism / concurrency info
     */
    virtual void setParallelismInfo(ServerInterface &srvInterface,
                                    ParallelismInfo *parallel) {}

    /**
     * Set CounterController
     */
    virtual void setCounterController(ServerInterface &srvInterface,
                                      CounterController *counterController) {}

    /**
     * Function that will be run during execution just prior to any
     *   processPartition calls; this may write output
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     * @param output_writer output location
     */
    virtual void preProcessPartitions(ServerInterface &srvInterface,
                                      PartitionWriter &output_writer){}

    /**
     * Funtion that will be run during execution just after all
     *   processPartition calls; this may write output
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     * @param output_writer output location
     */
    virtual void postProcessPartitions(ServerInterface &srvInterface,
                                       PartitionWriter &output_writer){}
};

struct BlobCreateOptions {
    static const int ONE_PER_THREAD = -1;
    static const int AUTO_SIZE = -1;

    /**
     * Whether or not the blob will be mutable
     */
    bool isMutable;

    /**
     * Number of chunks to create. If -1, the number of chunks will be set to
     * the number of threads
     */
    int nChunks;

    /**
     * Minimum size of the blob in bytes. If -1, chosen by the resource manager
     */
    vint minSize;

    /**
     * Maximum size of each blob in bytes. If -1, chosen by the resource
     * manager
     */
    vint maxSize;

    /**
     *  How the blob is stored
     */
    enum StorageType {
        ST_IN_MEMORY,
        ST_SPILL_TO_DISK,
        ST_DISK_BACKED,
    } storageType;

    /**
     * How the blob is distributed across the cluster. Segmented implies that
     * the blob is split across the nodes it's created on, whereas unsegmented
     * means that the blob is replicated across the nodes it's created on
     */
    enum DistributionType {
        DT_SEGMENTED,
        DT_UNSEGMENTED,
    } distributionType;

    /**
     * Whether to replace an existing blob if one exists with the same name and
     * namespace. The existing blob is removed at the end of execution and
     * replaced with the new blob.
     */
    bool overwrite;

    /**
     * Default constructor
     */
    BlobCreateOptions() :
        isMutable(true),
        nChunks(ONE_PER_THREAD),
        minSize(AUTO_SIZE),
        maxSize(AUTO_SIZE),
        storageType(ST_IN_MEMORY),
        distributionType(DT_SEGMENTED),
        overwrite(false) {}
};

struct BlobRequest
{
    /**
     * The type of request
     */
    enum RequestType
    {
        // To create a new blob and open it for write
        RT_CREATE,

        // To retrieve an existing blob
        RT_GET,

        // To remove an existing blob. This occurs after execution of the query
        // has completed
        RT_REMOVE,
    } requestType;

    /**
     * Name of this blob
     */
    std::string name;

    /**
     * Namespace of this blob
     */
    enum Namespace
    {
        // This blob will be available to all UDx's regardless of library
        NSP_PUBLIC,
        // This blob will only be available to UDx's from this library
        NSP_LIBRARY
    } nsp;

    /**
     * Whether to remove this blob after the execution of this query
     *
     * Default is false
     */
    bool removeAfterExecution;

    /**
     * Options for creating a blob, with sensible defaults. Only needs to be
     * specified if custom (non-default) options are desired on a create
     * request
     */
    BlobCreateOptions createOptions;

    /**
     * Default constructor
     */
    BlobRequest() :
           requestType(RT_GET),
           name("<unknown>"),
           nsp(NSP_LIBRARY),
           removeAfterExecution(false),
           createOptions() {}

    /**
     * Constructor with request type and name, the only two required attributes
     * for the request
     */
    BlobRequest(RequestType requestType_,
                std::string name_) :
        requestType(requestType_),
        name(name_),
        nsp(NSP_LIBRARY),
        removeAfterExecution(false),
        createOptions() {}
};

/**
 * Cursor request structure
 *  Asks for what is going to be accessed, and how (such that it can be optimized)
 * A cursor can be used for multiple things
 *  Table/projection access
 */
class CursorRequest
{
public:
    enum CursorSource
    {
        // This will be a cursor to an underlying table projection
        //  Tell us how to find that, and the types it will have
        CS_PERSISTENT_TABLE,

        // This will be a cursor to an underlying temp table with structure
        //  It will live as long as the session
        //  (Currently Unused)
        CS_SESSION_TEMP_TABLE,

        // Unused
        CS_STATEMENT_TEMP_TABLE,

        // This is an underlying temp table that will vaporize at the end of
        //  the operation and is not shared
        CS_OPERATION_TEMP_TABLE,

    } source;

    enum CreateType
    {
        // Has to have already existed
        CT_OPEN_EXISTING,

        // Error if it already exists
        CT_OPEN_NEW,

        // Open if exists, otherwise create new
        CT_OPEN_OR_CREATE,

        // Create new, if existing drop the old one
        CT_CREATE_OR_REPLACE,

        // Old one must exist, but will be replaced
        CT_REPLACE,
    } createType;

    enum ReadType
    {
        // Do not read
        R_NONE,

        // Sequential read
        R_SEQUENTIAL,

        // Random access read
        R_RANDOM,
    } readType;

    enum WriteType
    {
        // Do not write
        W_NONE,

        // Sequential write
        W_SEQUENTIAL,

        // Append to existing
        W_APPEND,

        // Random access write
        W_RANDOM,
    } writeType;

    /** This is just a hint */
    enum Orientation
    {
        OH_DEFAULT,
        OH_ROW_MAJOR,
        OH_COLUMN_MAJOR,
        OH_PAX,
    } orientationHint;

    /** Hints about sizing */
    struct SizeHint
    {
        vint minSize;
        vint maxSize;
        bool diskBacked;

        SizeHint () : minSize(-1), maxSize(-1), diskBacked(false) {}
    } sizeHint;

    enum Sharded
    {
        // Singleton for tables, one-to-one otherwise
        S_AUTOMATIC,
        // Not sharded
        S_UNSHARDED,
        // The number of UDTs must match the number of shards
        S_ONE_TO_ONE,
        // The UDT must manage shards
        S_N_TO_M,
    } sharding;
    int nShards;

    // By table name, fill in OID or names (we can search the path etc.)
    Oid tabOid;
    std::string tableDatabase;
    std::string tableSchema;
    std::string tableName;

    // By projection, fill in OID or name parts
    std::string projDatabase;
    std::string projSchema;
    std::string projName;
    Oid projOid;

    // For local scoped objects, the name
    std::string localName;

    // Name, for resource management or display purpose
    std::string cursorName;

    std::vector<std::string> requestColumns;
    // Column data types
    //  For existing data type, this is the (optional) expectations
    //  For new structured data, this has to be filled in
    std::vector<VerticaType> columnTypes;

    // Distinctify data coming out of cursor?
    bool applyDistinct;

    // Sort requested prior to read?
    int sortReadByColumns;
    // UDT promises to write data sorted
    int writesSortedByColumns;

    // How to segment and partition?
    std::vector<int> partitionInputByColumns;
    std::vector<int> segmentInputByColumns;
    std::vector<int> outputIsPartitionedByColumns;
    std::vector<int> outputIsSegmentedByColumns;

    /**
     *  Predicates to push down, as ranges
     *  These are treated as a hint, to eliminate excess data.
     *  Rows outside the range may still be scanned.
     */
    struct ColumnRangeHint
    {
        /** If columnAttr is not specified, it will be looked up */
        std::string columnName;
        /** Column number */
        int columnAttr;

        /** Kind of literal value used to represent the range */
        enum ValueRepresentation
        {
            VR_INT,
            VR_FLOAT,
            VR_STRING
        } valueRepresentation;

        // Fill in one of the following pairs, as indicated by valueRepresentation
        vint intMinValue;
        vint intMaxValue;
        vfloat floatMaxValue;
        vfloat floatMinValue;
        std::string stringMinValue;
        std::string stringMaxValue;
    };
    std::vector<ColumnRangeHint> pruningHints;

    CursorRequest() : source(CS_PERSISTENT_TABLE),
        createType(CT_OPEN_EXISTING),
        readType(R_SEQUENTIAL), writeType(W_NONE),
        orientationHint(OH_DEFAULT),
        sharding(S_AUTOMATIC), nShards(-1),
        tabOid(0), projOid(0),
        cursorName("<unknown>"),
        applyDistinct(false),
        sortReadByColumns(0), writesSortedByColumns(0)
    {}
};

typedef std::vector<BlobRequest> RequestedBlobs;


typedef std::map<std::string, int> RequestedNodes;

/**
 * ConcurrencyModel structure
 *  Request how many nodes, how many threads to be used for the UDT
 *  or specifically which nodes
 */
struct ConcurrencyModel
{
    enum GlobalConcurrencyType
    {
        // Use OVER clause, cursors, etc., to determine node set
        GC_CONTEXTUAL,

        // Use one node only
        GC_SINGLE_NODE,

        // Specify which nodes
        GC_MULTI_NODES,

        // Use all relevant threads
        GC_ALL_NODES,
    } globalConc;

    enum LocalConcurrencyType
    {
        // Use OVER clause, rest of the plan, etc., to determine thread count
        LC_CONTEXTUAL,

        // Suggest a number of threads
        LC_SUGGESTED_THREADS,

        // Require a fixed number of threads
        LC_REQUIRED_THREADS,
    } localConc;

    // Number of threads; or -1 for a reasonable number
    int nThreads;

    // Number of nodes; or -1 for a reasonable number
    int nNodes;

    // Shared region request - this is not intended to hold data, just shared.
    // Shared state
    int sharedStateSize;

    // Which specific nodes we want to use, and how many threads on each of them
    // This is a map from node name to number of threads, or -1 for auto
    RequestedNodes requestedNodes;

    ConcurrencyModel() :
        globalConc(GC_CONTEXTUAL),
        localConc(LC_CONTEXTUAL),
        nThreads(-1),
        nNodes(-1),
        sharedStateSize(0),
        requestedNodes()
        {}
};

/**
 * Request for a counter
 */
struct UDxCounterRequest
{
    /** Type of value held by the counter */
    enum CounterType
    {
        CT_START_TIME,
        CT_END_TIME,
        CT_EXECUTION_TIME,
        CT_INPUT_SIZE_ROWS,
        CT_INPUT_SIZE_BYTES,
        CT_PROCESSED_ROWS,
        CT_PROCESSED_BYTES,
        CT_RESOURCES,
        CT_MISC_SCALAR,
    } type;

    /** Name of the counter, describing what is counted */
    std::string name;
    /** Tag, for cases where the counter name has multiple instances per UDx */
    std::string tag;

    bool isTimer() {
        return (type == CT_EXECUTION_TIME);
    }

    bool isTimeStamper() {
        return (type == CT_START_TIME || type == CT_END_TIME);
    }

    UDxCounterRequest() : type(CT_MISC_SCALAR) {}

    UDxCounterRequest(std::string _name, std::string _tag, CounterType _type) :
        type(_type), name(_name), tag(_tag) {}
};


typedef std::vector<CursorRequest> RequestedCursors;
typedef std::vector<UDxCounterRequest> RequestedCounters;

/**
 * @brief Class to interact with counters
 */
class CounterController
{
public:
    virtual ~CounterController() {}

    virtual RequestedCounters* getCurrentCounters() = 0;

    /** Get the current value of the counter */
    virtual bool getCounterValue(int counterIndex, vint &value) = 0;

    // The next three functions are used for counters that measure duration
    
    /** Start a time counter */
    virtual bool startTimer(int counterIndex) = 0;
    /** Stop a time counter */
    virtual bool stopTimer(int counterIndex) = 0;
    /** Clear a time counter */
    virtual bool resetTimer(int counterIndex) = 0;

    // The next function is used for counters that capture timestamps

    /** Save the current time to the counter */
    virtual bool recordTimestamp(int counterIndex) = 0;

    // The next 2 functions are used for not-time-related counters

    /** Set counter to a value */
    virtual bool setCounterValue(int counterIndex, vint value) = 0;
    /** Add a value to counter */
    virtual bool addToCounter(int counterIndex, vint value) = 0;
};

/**
 * @brief Interface for declaring parameters and return types for, and
 * instantiating, an associated CursorTransformFunction.
 */
class CursorTransformFunctionFactory : public UDXFactory
{
public:
    virtual ~CursorTransformFunctionFactory() {}

    /**
     * Called when Vertica needs a new CursorTransformFunction object.
     *
     * @return a CursorTransformFunction object which implements the UDx API described
     * by this metadata.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     *
     * @note More than one object may be instantiated per query, or per process.
     */
    virtual CursorTransformFunction *createTransformFunction(ServerInterface &srvInterface) = 0;

    /**
     * This is the time in the planning cycle to ask for access to projections
     */
    virtual RequestedCursors getRequestedCursors(ServerInterface &srvInterface,
                                                 const SizedColumnTypes &argType)
    {
        return RequestedCursors();
    }

    /**
     * Get the counters requested by the UDx
     */
    virtual RequestedCounters getRequestedCounters(ServerInterface &srvInterface,
                                                   const SizedColumnTypes &argType)
    {
        return RequestedCounters();
    }

    /**
     * This is the time in the planning cycle to ask the server to create,
     * read, or remove blobs
     */
    virtual RequestedBlobs getRequestedBlobs (
            ServerInterface &srvInterface,
            const SizedColumnTypes &argType)
    {
        return RequestedBlobs();
    }

    /**
     * Indicate what kind of threading model the UDT wants
     */
    virtual void getConcurrencyModel(ServerInterface &srvInterface,
                                     ConcurrencyModel &concModel) {}

protected:
    /**
     * @return the object type internally used by Vertica
     */
    virtual UDXType getUDXFactoryType() { return CURSOR_TRANSFORM; }
};



/**
 * @brief Interface for User-Defined Analytic Function (UDAnF).  A UDAnF
 * operates on rows of data and returns rows of data, not necessarily 1:1.
 *
 * An AnalyticFunction must have an associated AnalyticFunctionFactory.
 */
class AnalyticFunction : public UDXObjectCancelable
{
public:
    /**
     * Invoke a user defined analytic on a set of rows. As the name suggests, a
     * batch of rows are passed in for every invocation to amortize performance.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     *
     * @param input_reader input rows
     *
     * @param output_writer output location
     */
    virtual void processPartition(ServerInterface &srvInterface,
                                  AnalyticPartitionReader &input_reader,
                                  AnalyticPartitionWriter &output_writer) = 0;
};

/**
 * @brief Interface for declaring parameters and return types for, and
 * instantiating, an associated AnalyticFunction.
 */
class AnalyticFunctionFactory : public UDXFactory
{
public:
    virtual ~AnalyticFunctionFactory() {}

    /** Called when Vertica needs a new AnalyticFunction object to process a function call.
     *
     * @return an AnalyticFunction object which implements the UDx API described
     * by this metadata.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     *
     * @note More than one object may be instantiated per query.
     *
     *
     *
     */
    virtual AnalyticFunction *createAnalyticFunction(ServerInterface &srvInterface) = 0;

protected:
    /**
     * @return the object type internally used by Vertica
     */
    virtual UDXType getUDXFactoryType() { return ANALYTIC; }
};

/**
 * @brief Interface for User-Defined Aggregate Function (UDAF).  A UDAF
 * operates on one column of data and returns one column of data.
 *
 * An AggregateFunction must have an associated AggregateFunctionFactory.
 */
class AggregateFunction : public UDXObject
{
public:

    /**
     * Called by the server to perform aggregation on multiple blocks of data
     *
     * @note User should not explicity implement this function. It is implemented by
     * calling the InlineAggregate() macro. User should follow the convention of implementing
     * void aggregate(ServerInterface &srvInterface,
     *                 BlockReader &arg_reader,
     *                 IntermediateAggs &aggs)
     * along with initAggregate, combine, and terminate. For references on what a fully
     * implemented Aggregate Function looks like, check the examples in the example folder.
     *
     * which the inlined aggregateArrs implemention will invoke
     */
    virtual void aggregateArrs(ServerInterface &srvInterface, void **dstTuples,
                               int doff, const void *arr, int stride, const void *rcounts,
                               int rcstride, int count, IntermediateAggs &intAggs,
                               std::vector<int> &intOffsets, BlockReader &arg_reader) = 0;


    /**
     * Called by the server to set the starting values of the Intermediate aggregates.
     *
     * @note This can be called multiple times on multiple machines, so starting values
     * should be idempotent.
     */
    virtual void initAggregate(ServerInterface &srvInterface,
                               IntermediateAggs &aggs) = 0;

    /**
     * Called when intermediate aggregates need to be combined with each other
     */
    virtual void combine(ServerInterface &srvInterface,
                         IntermediateAggs &aggs_output,
                         MultipleIntermediateAggs &aggs_other) = 0;

    /**
     * Called by the server to get the output to the aggregate function
     */
    virtual void terminate(ServerInterface &srvInterface,
                           BlockWriter &res_writer,
                           IntermediateAggs &aggs) {
        ereport(ERROR,
                (errmsg("terminate() must be overidden for a User Defined Aggregate."),
                 errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
    }

    /**
     * Called by the server to get the output to the aggregate function
     */
    virtual void terminate(ServerInterface &srvInterface,
                           BlockWriter &res_writer,
                           IntermediateAggs &aggs, SessionParamWriterMap &udSessionParams ){
        terminate(srvInterface,res_writer,aggs);
    }

    /**
     * Helper function for aggregateArrs.
     *
     * @note User should not call this function.
     */
    static void updateCols(BlockReader &arg_reader, char *arg, int count,
                           IntermediateAggs &intAggs, char *aggPtr, std::vector<int> &intOffsets);
};

/**
 * @brief Interface for declaring parameters and return types for, and
 * instantiating, an associated AggregateFunction.
 */
class AggregateFunctionFactory : public UDXFactory
{
public:
    virtual ~AggregateFunctionFactory() {}

    /**
     * Returns the intermediate types used for this aggregate. Called by the
     * server to set the types of the Intermediate aggregates.
     */
    virtual void getIntermediateTypes(ServerInterface &srvInterface,
                                      const SizedColumnTypes &inputTypes,
                                      SizedColumnTypes &intermediateTypeMetaData) = 0;

    /** Called when Vertica needs a new AggregateFunction object to process a function call.
     *
     * @return an AggregateFunction object which implements the UDx API described
     * by this metadata.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     *
     * @note More than one object may be instantiated per query.
     */
    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvInterface) = 0;

protected:
    /**
     * @return the object type internally used by Vertica
     */
    virtual UDXType getUDXFactoryType() { return AGGREGATE; }
};

/**
 * @brief: Represents an in-memory block of tuples
 */
class VerticaBlock
{
public:
    VerticaBlock(size_t ncols, int rowcount, const int *indices /* NULL if unused */)
    : ncols(ncols), count(rowcount),
      index(0), indices(indices)
    {
        cols.reserve(ncols);
        colstrides.reserve(ncols);
        vnWrappers.reserve(ncols);
        svWrappers.reserve(ncols);
        nrows=rowcount;
        processBlockUserInfoVector.reserve(ncols);
    }

    void copy(const VerticaBlock &other) {
        *this = other;
    }

    /**
     * @return the number of columns held by this block.
     */
    size_t getNumCols() const { return ncols; }

    /**
     * @return the number of rows held by this block.
     */
    int getNumRows() const { return nrows; }

    /**
     * @return information about the types and numbers of arguments
     */
    const SizedColumnTypes &getTypeMetaData() const { return typeMetaData; }

    /**
     * @return information about the types and numbers of arguments
     */
    SizedColumnTypes &getTypeMetaData() { return typeMetaData; }

    /**
     * @return a pointer to the idx'th argument, cast appropriately.
     *
     * Example:
     *
     * @code
     * const vint *a = arg_reader->getColPtr<vint>(0);
     * @endcode
     */
    template <class T>
    const T *getColPtr(size_t idx) const { return reinterpret_cast<const T *>(cols.at(idx)); }

    template <class T>
    T *getColPtrForWrite(size_t idx) { return reinterpret_cast<T *>(cols.at(idx)); }

    int getColStride(size_t idx) const { return colstrides.at(idx); }

    /**
     * @return a pointer to the idx'th argument, cast appropriately.
     *
     * Example:
     * const vint a = arg_reader->getColRef<vint>(0);
     */
    template <class T>
    const T &getColRef(size_t idx) { return *getColPtr<T>(idx); }

    template <class T>
    T &getColRefForWrite(size_t idx) { return *getColPtrForWrite<T>(idx); }


    // Internal API: get raw pointer
    void *getVoidPtr() { return reinterpret_cast<void *>(cols.at(0)); }

    // Internal: Specify the out-of-line data area used to store the variable
    // length data for column idx
    void setDataArea(size_t idx, void *dataarea) {
        svWrappers[idx].da = reinterpret_cast<EE::DataArea*>(dataarea);
    }

    // Internal
    const EE::DataArea *getDataArea(size_t idx) { return svWrappers[idx].da; }

protected:
    /**
     * Add the location for reading a particular argument.
     *
     * @param arg The base location to find data.
     *
     * @param colstride The stride between data instances.
     *
     * @param dt The type of input.
     *
     * @param fieldname the name of the field
     */
    void addCol(char *arg, int colstride, const VerticaType &dt, const std::string fieldName = "")
    {
        cols.push_back(arg + (indices ? (colstride * indices[0]) : 0));
        colstrides.push_back(colstride);
        int32 typmod = (dt.getTypeOid() == NumericOID
                        ? dt.getTypeMod()
                        : VerticaType::makeNumericTypeMod(2,1));
        vnWrappers.push_back(VNumeric(0, typmod));
        svWrappers.push_back(VString(0, NULL, dt.isStringType() ? dt.getStringLength() : 0));
        typeMetaData.addArg(dt, fieldName);
        // populating the processBlockuserInfoVector with the appropriate column
        processBlockUserInfoVector.push_back(dt.getUnderlyingType());
        if (ncols < cols.size()) ncols = cols.size();
    }

    void addCol(const char *arg, int colstride, const VerticaType &dt, const std::string fieldName = "")
    {
        addCol(const_cast<char*>(arg), colstride, dt, fieldName);
    }

    // Private debugging: ensure generated strings confirm to declared sizes
    // Callers must ensure that the VerticaType argument is actually a string type
    void validateStringColumn(size_t idx, const VString &s, size_t colLength) const
    {
        if (!s.isNull() && s.length() > colLength)
        {
            ereport(ERROR,
                    (errmsg("Returned string value '[%s]' with length [%zu] "
                            "is greater than declared field length of [%zu] "
                            "of field [%s] at output column index [%zu]",
                            s.str().c_str(),
                            size_t(s.length()),
                            colLength,
                            typeMetaData.getColumnName(0).c_str(),
                                idx),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }
    }

    bool checkStringUserBlockInfo(size_t idx) const {
          switch(processBlockUserInfoVector[idx]){
             case CharOID:
             case VarcharOID:
             case LongVarcharOID:
             case BinaryOID:
             case VarbinaryOID:
             case LongVarbinaryOID:    return true;
             default: return false;
         }
    }

    bool checkTimeUserBlockInfo(size_t idx) const {
        switch(processBlockUserInfoVector[idx]){
        case DateOID:
        case TimeOID:
        case TimeTzOID:
        case TimestampOID:
        case TimestampTzOID: return true;
        default: return false;
        }
    }

    void reset(){
        cols.clear();
        colstrides.clear();
        ncols=0;
        count=0;
        nrows=0;
        index=0;
        typeMetaData.reset();
        svWrappers.clear();
        vnWrappers.clear();
        processBlockUserInfoVector.clear();
    }

    void setRowCount( int rowCount){
        count = nrows = rowCount;
    }

    void resetIndex(){
        index = 0;
    }

    std::vector<char *> cols;          // list of pointers that holds the cols
    std::vector<int> colstrides;       // list of stride values for each col
    size_t ncols;                      // number of cols
    int count;
    int nrows;                         // for input: total number of tuples in block, for output: total capacity
    int index;                         // for input: number of rows already read, for output: number of rows already written
    const int *indices;                // For skip-list blocks, list of indices to consider

    SizedColumnTypes typeMetaData;     // what types, etc.

    std::vector<VString> svWrappers;   // pre-create the VString objects to avoid per-call creation
    std::vector<VNumeric> vnWrappers;  // pre-create the VNumeric objects to avoid per-call creation

    std::vector<BaseDataOID> processBlockUserInfoVector;

    friend class VerticaBlockSerializer;
    friend class ::EE::UserDefinedProcess;
    friend class ::EE::UserDefinedAnalytic;
    friend class ::EE::UserDefinedTransform;
    friend class ::EE::UserDefinedAggregate;
    friend class AggregateFunction;
    friend struct CPPExecContext;
};


/**
 * @brief Iterator interface for reading rows in a Vertica block.
 *
 * This class provides the input to the ScalarFunction.processBlock()
 * function. You extract values from the input row using data type
 * specific functions to extract each column value. You can also
 * determine the number of columns and their data types, if your
 * processBlock function does not have hard-coded input expectations.
 */
class BlockReader : public VerticaBlock
{
public:

    BlockReader(size_t narg, int rowcount, const int *indices) :
        VerticaBlock(narg, rowcount, indices) {}

    /**
     * @brief Get a pointer to an INTEGER value from the input row.
     *
     * @return a pointer to the idx'th argument, cast appropriately.
     *
     * @param idx The column number to retrieve from the input row.
     *
     * Example:
     *
     * @code
     * const vint *a = arg_reader->getIntPtr(0);
     * @endcode
     */
    const vint *getIntPtr(size_t idx) const           { return getColPtr<vint>(idx); }
    /// @brief Get a pointer to a FLOAT value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a FLOAT.
    const vfloat *getFloatPtr(size_t idx) const        { return getColPtr<vfloat>(idx); }
    /// @brief Get a pointer to a BOOLEAN value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a BOOLEAN.
    const vbool *getBoolPtr(size_t idx) const           { return getColPtr<vbool>(idx); }
    /// @brief Get a pointer to a DATE value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a DATE.
    const DateADT *getDatePtr(size_t idx) const            { return getColPtr<DateADT>(idx); }
    /// @brief Get a pointer to an INTERVAL value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as an INTERVAL.
    const Interval *getIntervalPtr(size_t idx) const       { return getColPtr<Interval>(idx); }
    /// @brief Get a pointer to a INTERVAL YEAR TO MONTH value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A point to the retrieved value cast as a INTERVAL YEAR TO MONTH.
    const IntervalYM *getIntervalYMPtr(size_t idx) const   { return getColPtr<IntervalYM>(idx); }
    /// @brief Get a pointer to a TIMESTAMP value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a TIMESTAMP.
    const Timestamp *getTimestampPtr(size_t idx) const     { return getColPtr<Timestamp>(idx); }
    /// @brief Get a pointer to a TIMESTAMP WITH TIMEZONE value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a TIMESTAMP WITH TIMEZONE .
    const TimestampTz *getTimestampTzPtr(size_t idx) const  { return getColPtr<TimestampTz>(idx); }
    /// @brief Get a pointer to a TIME value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a TIME.
    const TimeADT *getTimePtr(size_t idx) const            { return getColPtr<TimeADT>(idx); }
    /// @brief Get a pointer to a TIME WITH TIMEZONE value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a TIME WITH TIMEZONE.
    const TimeTzADT *getTimeTzPtr(size_t idx) const        { return getColPtr<TimeTzADT>(idx); }
    /// @brief Get a pointer to a VNumeric value from the input row.
    ///
    /// @param idx The column number to retrieve from the input row.
    ///
    /// @return A pointer to the retrieved value cast as a Numeric.
    const VNumeric *getNumericPtr(size_t idx)
    {
        const uint64 *words = reinterpret_cast<const uint64 *>(getColPtr<vint>(idx));
        vnWrappers[idx].words = const_cast<uint64*>(words);
        return &vnWrappers[idx];
    }
    /// @brief Get a pointer to a VString value from the input row.
    ///
    /// @param idx The column number to retrieve from the input row.
    ///
    /// @return A pointer to the retrieved value cast as a VString.
    const VString *getStringPtr(size_t idx)
    {
        const EE::StringValue *sv = reinterpret_cast<const EE::StringValue *>(getColPtr<vint>(idx));
        svWrappers[idx].sv = const_cast<EE::StringValue*>(sv);
        return &svWrappers[idx];
    }

    /**
     * @brief Get a reference to an INTEGER value from the input row.

     * @param idx The column number to retrieve from the input row.
     *
     * @return a reference to the idx'th argument, cast as an INTEGER.
     *
     * Example:
     *
     * @code
     * const vint a = arg_reader->getIntRef(0);
     * @endcode
     */
    const vint        &getIntRef(size_t idx) const { return *getIntPtr(idx); }
    /**
     * @brief Get a reference to a FLOAT value from the input row.

     * @param idx The column number to retrieve from the input row.
     *
     * @return A reference to the idx'th argument, cast as an FLOAT.
     */
    const vfloat      &getFloatRef(size_t idx) const { return *getFloatPtr(idx); }
    /**
     * @brief Get a reference to a BOOLEAN value from the input row.

     * @param idx The column number to retrieve from the input row.
     *
     * @return a reference to the idx'th argument, cast as an BOOLEAN.
     */
    const vbool       &getBoolRef(size_t idx) const { return *getBoolPtr(idx); }
    /**
     * @brief Get a reference to a DATE value from the input row.

     * @param idx The column number to retrieve from the input row.
     *
     * @return a reference to the idx'th argument, cast as an DATE.
     */
    const DateADT     &getDateRef(size_t idx) const { return *getDatePtr(idx); }
    /**
     * @brief Get a reference to an INTERVAL value from the input row.

     * @param idx The column number to retrieve from the input row.
     *
     * @return a reference to the idx'th argument, cast as an INTERVAL.
     */
    const Interval    &getIntervalRef(size_t idx) const { return *getIntervalPtr(idx); }
    /**
     * @brief Get a reference to an INTERVAL YEAR TO MONTH value from the input row.

     * @param idx The column number to retrieve from the input row.
     *
     * @return a reference to the idx'th argument, cast as an INTERVAL YEAR TO MONTH.
     */
    const IntervalYM  &getIntervalYMRef(size_t idx) const { return *getIntervalYMPtr(idx); }
    /**
     * @brief Get a reference to a TIMESTAMP value from the input row.

     * @param idx The column number to retrieve from the input row.
     *
     * @return a reference to the idx'th argument, cast as a TIMESTAMP.
     */
    const Timestamp   &getTimestampRef(size_t idx) const { return *getTimestampPtr(idx); }
    /**
     * @brief Get a reference to a TIMESTAMP WITH TIMEZONE value from the input row.

     * @param idx The column number to retrieve from the input row.
     *
     * @return a reference to the idx'th argument, cast as a TIMESTAMP WITH TIMEZONE.
     */
    const TimestampTz &getTimestampTzRef(size_t idx) const { return *getTimestampTzPtr(idx); }
    /**
     * @brief Get a reference to a TIME value from the input row.

     * @param idx The column number to retrieve from the input row.
     *
     * @return a reference to the idx'th argument, cast as a TIME.
     */
    const TimeADT     &getTimeRef(size_t idx) const { return *getTimePtr(idx); }
    /**
     * @brief Get a reference to a TIME WITH TIMEZONE value from the input row.

     * @param idx The column number to retrieve from the input row.
     *
     * @return a reference to the idx'th argument, cast as a TIME WITH TIMEZONE.
     */
    const TimeTzADT   &getTimeTzRef(size_t idx) const { return *getTimeTzPtr(idx); }
    /**
     * @brief Get a reference to a VNumeric value from the input row.

     * @param idx The column number to retrieve from the input row.
     *
     * @return a reference to the idx'th argument, cast as an VNumeric.
     */
    const VNumeric    &getNumericRef(size_t idx)      { return *getNumericPtr(idx); }
    /**
     * @brief Get a reference to an VString value from the input row.

     * @param idx The column number to retrieve from the input row.
     *
     * @return a reference to the idx'th argument, cast as an VString.
     */
    const VString     &getStringRef(size_t idx)       { return *getStringPtr(idx); }

    /**
     * Advance to the next record.
     *
     * @return true if there are more rows to read, false otherwise.
     */
    bool next()
    {
        if (++index >= count) return false;

        for (size_t i = 0; i < ncols; ++i)
            cols[i] += colstrides[i] * (indices ? (indices[index] - indices[index-1]) : 1);
        return true;
    }

    /**
     * @brief  Check if the idx'th argument is null

     * @param col The column number in the row to check for null
     * @return true is the col value is null false otherwise
     */
    bool isNull(int col)
    {
        int nCols=getNumCols();
        VIAssert(col < nCols );
        const SizedColumnTypes &inTypes = getTypeMetaData();
        const VerticaType &t = inTypes.getColumnType(col);
        BaseDataOID oid = t.getTypeOid();

        switch (oid)
        {
        case BoolOID:{
            vbool a = getBoolRef(col);
            return(a == vbool_null);
        }
            break;
        case Float8OID:     {
            vfloat a = getFloatRef(col);
            return ( vfloatIsNull (a) ) ;
        }
            break;
        case BinaryOID:
        case VarbinaryOID:
        case LongVarbinaryOID:
        case CharOID:
        case VarcharOID:
        case LongVarcharOID:
        {
            VString a = getStringRef(col);
            return ( a.isNull() );
        }
        break;
        case NumericOID:{
            VNumeric a = getNumericRef(col);
            return ( a.isNull() ) ;
        }
            break;
        case Int8OID:{
            vint a = getIntRef(col);
            return (a == vint_null);
        }
            break;
        case IntervalOID:{
            Interval a = getIntervalRef(col);
            return (a == vint_null);
        }
            break;
        case TimestampOID:{
            Timestamp a = getTimestampRef(col);
            return (a == vint_null) ;
        }
            break;
        case TimestampTzOID:{
            TimestampTz a = getTimestampTzRef(col);
            return (a == vint_null);
        }
            break;
        case TimeOID:{
            TimeADT a = getTimeRef(col);
            return (a == vint_null);
        }
            break;
        case TimeTzOID:{
            TimeTzADT a = getTimeTzRef(col);
            return (a == vint_null);
        }
            break;
        case DateOID:{
            DateADT a = getDateRef(col);
            return (a == vint_null);
        }
            break;
        case IntervalYMOID:{
            IntervalYM a = getIntervalYMRef(col);
            return (a == vint_null);
        }
            break;
        default:
            ereport(ERROR,
                    (errmsg("Unknown data type"),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }
        //should never get here, prevents compiler warning
        return false;
    }

    friend class EE::VEval;
    friend class VerticaRInterface;
};

/**
 * @brief Iterator interface for writing rows to a Vertica block
 *
 * This class provides the output rows that ScalarFunction.processBlock()
 * writes to.
 */
class BlockWriter : public VerticaBlock
{
public:
    BlockWriter(int rowcount)
        : VerticaBlock(1, rowcount, NULL) {}

    BlockWriter(char *outArr, int stride, int rowcount, const int *indices, const VerticaType &dt)
        : VerticaBlock(1, rowcount, indices)
    {
        addCol(outArr, stride, dt);
    }

    /**
     * Setter methods
     */

    /// @brief Adds an INTEGER value to the output row.
    /// @param r The INTEGER value to insert into the output row.
    void setInt(vint r)                {
        if(Int8OID != processBlockUserInfoVector[0]){
            throwInCorrectUsageError();
        }
        getColRefForWrite<vint>(0) = r;
    }

    /// @brief Adds a FLOAT value to the output row.
    /// @param r The FLOAT value to insert into the output row.
    void setFloat(vfloat r)            {
        if(Float8OID !=  processBlockUserInfoVector[0]){
            throwInCorrectUsageError();
        }
        getColRefForWrite<vfloat>(0) = r;
    }

    /// @brief Adds a BOOLEAN value to the output row.
    /// @param r The BOOLEAN value to insert into the output row.
    void setBool(vbool r)              {
        if(BoolOID != processBlockUserInfoVector[0]){
            throwInCorrectUsageError();
        }
        getColRefForWrite<vbool>(0) = r;
    }

    /// @brief Adds a BOOLEAN value to the output row.
    /// @param r The BOOLEAN value to insert into the output row.
    void setDate(DateADT r)            {
        if(!checkTimeUserBlockInfo(0)){
            throwInCorrectUsageError();
        }
        getColRefForWrite<DateADT>(0) = r;
    }

    /// @brief Adds an INTERVAL value to the output row.
    /// @param r The INTERVAL value to insert into the output row.
    void setInterval(Interval r)       {
        if(!(IntervalOID == processBlockUserInfoVector[0] || IntervalYMOID == processBlockUserInfoVector[0])){
            throwInCorrectUsageError();
        }
        getColRefForWrite<Interval>(0) = r;
    }
    /// @brief Adds an INTERVAL YEAR TO MONTH value to the output row.
    /// @param r The INTERVAL YEAR TO MONTH value to insert into the output row.
    void setIntervalYM(IntervalYM r)   {
        if(!(IntervalOID == processBlockUserInfoVector[0] || IntervalYMOID == processBlockUserInfoVector[0])){
            throwInCorrectUsageError();
        }
        getColRefForWrite<IntervalYM>(0) = r;
    }

    /// @brief Adds a TIMESTAMP value to the output row.
    /// @param r The TIMESTAMP value to insert into the output row.
    void setTimestamp(Timestamp r)     {
        if(!checkTimeUserBlockInfo(0)){
            throwInCorrectUsageError();
        }
        getColRefForWrite<Timestamp>(0) = r;
    }

    /// @brief Adds a TIMESTAMP WITH TIMEZONE value to the output row.
    /// @param r The TIMESTAMP WITH TIMEZONE value to insert into the output row.
    void setTimestampTz(TimestampTz r) {
        if(!checkTimeUserBlockInfo(0)) {
            throwInCorrectUsageError();
        }
        getColRefForWrite<TimestampTz>(0) = r;
    }

    /// @brief Adds a TIMESTAMP value to the output row.
    /// @param r The TIMESTAMP value to insert into the output row.
    void setTime(TimeADT r)            {
        if(!checkTimeUserBlockInfo(0)){
            throwInCorrectUsageError();
        }
        getColRefForWrite<TimeADT>(0) = r;
    }

    /// @brief Adds a TIMESTAMP WITH TIMEZONE value to the output row.
    /// @param r The TIMESTAMP WITH TIMEZONE value to insert into the output row.
    void setTimeTz(TimeTzADT r)        {
        if(!checkTimeUserBlockInfo(0)){
            throwInCorrectUsageError();
        }
        getColRefForWrite<TimeTzADT>(0) = r;
    }

    /// @brief Allocate a new VNumeric object to use as output.
    ///
    /// @return A new VNumeric object to hold output. This object
    ///         automatically added to the output row.
    VNumeric &getNumericRef()
    {
        if(NumericOID != processBlockUserInfoVector[0]){
            throwInCorrectUsageError();
        }
        uint64 *words = getColPtrForWrite<uint64>(0);
        vnWrappers[0].words = words;
        return vnWrappers[0];
    }
    /// @brief Allocates a new VString object to use as output.
    ///
    /// @return A new VString object to hold output. This object
    ///         automatically added to the output row.
    VString &getStringRef()
    {
        if(!checkStringUserBlockInfo(0)){
            throwInCorrectUsageError();
        }
        EE::StringValue *sv = getColPtrForWrite<EE::StringValue>(0);
        svWrappers[0].sv = sv;
        return svWrappers[0];
    }
    /// @brief Get a pointer for writing output.
    ///
    /// @return A new VString object to hold output. This object
    ///         automatically added to the output row.
    VString * getStringPtr()
    {
        return & (getStringRef());
    }

    void throwInCorrectUsageError(){
           ereport(ERROR,
                    (errmsg("Incorrect use of setter in processBlock"),
                    errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
    }

    /**
     * @brief  Set the column to null
     *
     * @param idx The column number in the row to set to null
     */
    void setNull()
    {
        const SizedColumnTypes &inTypes = getTypeMetaData();
        const VerticaType &t = inTypes.getColumnType(0);
        BaseDataOID oid = t.getTypeOid();
        switch (oid)
        {
        case Int8OID:
            setInt(vint_null);
            break;
        case Float8OID:
            setFloat(vfloat_null);
            break;
        case BoolOID:
            setBool(vbool_null);
            break;
        case DateOID:
            setDate(vint_null);
            break;
        case IntervalYMOID:
        case IntervalOID:
            setInterval(vint_null);
            break;
        case TimestampOID:
            setTimestamp(vint_null);
            break;
        case TimestampTzOID:
            setTimestampTz(vint_null);
            break;
        case TimeOID:
            setTime(vint_null);
            break;
        case TimeTzOID:
            setTimeTz(vint_null);
            break;
        case BinaryOID:
        case VarbinaryOID:
        case LongVarbinaryOID:
        case CharOID:
        case VarcharOID:
        case LongVarcharOID:
            getStringRef().setNull();
            break;
        case NumericOID:
            getNumericRef().setNull();
            break;
        default:
            ereport(ERROR,
                    (errmsg("Unknown data type"),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }
    }

    /// @brief Complete writing this row of output and move to the next row.
    ///
    ///
    void next()
    {
#ifndef FAST_NEXT
        const VerticaType &t = typeMetaData.getColumnType(0);
        if (t.isStringOid(processBlockUserInfoVector[0])) {
            validateStringColumn(0, getStringRef(), t.getStringLength(false));
        }
#endif
        if (++index >= count)
            return;

        cols[0] += colstrides[0] * (indices ? (indices[index] - indices[index-1]) : 1);
    }

private:

    friend class EE::VEval;
};

/**
 * \brief This class represents value ranges used in analyzing the output
 * of UDSFs. A range is expressed as a minimum/maximum value (inclusive) pair.
 *
 * Instances of this class are used to allow UDSF developers to specify what the
 * output range of of the function is.
 * The UDSF developer is responsible to override ScalarFunction::getOutputRange()
 * and set the function's output range using the knowledge of the input argument
 * ranges passed as references to ScalarFunction::getOutputRange().
 */
class VerticaValueRange {
protected:
    struct ValueRange {
        ValueRange(char* loBound, char* upBound, VerticaType dt,
                   EE::ValueSort sortedness, bool canHaveNulls) :
            dt(dt), sortedness(sortedness), loBound(loBound), upBound(upBound),
            canHaveNulls(canHaveNulls) { }

        VerticaType dt; // data type of range values
        EE::ValueSort sortedness; // sortedness of values in the range
        char* loBound; // lower bound (inclusive)
        char* upBound; // upper bound (inclusive)
        bool canHaveNulls; // can there be NULL values in the range?
    };

    size_t narg; // number of argument ranges
    std::vector<ValueRange> ranges; // range bounds
    std::vector<VString> svWrappersLo; // pre-create VString objects to avoid per-call creation
    std::vector<VString> svWrappersUp;
    std::vector<VNumeric> vnWrappersLo; // pre-create VNumeric objects to avoid per-call creation
    std::vector<VNumeric> vnWrappersUp;

   /**
     * Add a value range of a particular function argument
     *
     * @param loBound Base location to find the lower bound data
     * @param upBound Base location to find the upper bound data
     * @param sortedness Sortedness of values in the range
     * @param dt The data type of range bounds
     */
    void addArg(char* loBound, char* upBound, const VerticaType &dt, EE::ValueSort sortedness, bool canHaveNulls) {
        ranges.push_back(ValueRange(loBound, upBound, dt, sortedness, canHaveNulls));
        int32 typmod = (dt.getTypeOid() == NumericOID
                        ? dt.getTypeMod()
                        : VerticaType::makeNumericTypeMod(2,1));
        vnWrappersLo.push_back(VNumeric(0, typmod));
        vnWrappersUp.push_back(VNumeric(0, typmod));
        svWrappersLo.push_back(VString(0, NULL, dt.isStringType() ? dt.getStringLength() : 0));
        svWrappersUp.push_back(VString(0, NULL, dt.isStringType() ? dt.getStringLength() : 0));
    }

    /**
     * \brief Set the sortedness of values in the range.
     *
     * @param idx The range argument number.
     * @param s The sortedness value to set. Possible values are:
     *  EE::SORT_UNORDERED - Unsorted
     *  EE::SORT_MONOTONIC_INCREASING - Ascending
     *  EE::SORT_MONOTONIC_DECREASING - Descending
     *  EE::SORT_CONSTANT - Single value
     *
     * Example:
     * @code
     * range.setSortedness(0, EE:SORT_CONSTANT); // defines a single-valued range
     * @endcode
     */
    void setSortedness(size_t idx, EE::ValueSort s) {
        ranges.at(idx).sortedness = s;
    }

    /**
     * \brief Set a flag to indicate that some values in this range can be NULL.
     *
     * @param idx The argument range number.
     * @param u true if there can be NULL values in the range, else false.
     */
    void setCanHaveNulls(size_t idx, bool u) {
        ranges.at(idx).canHaveNulls = u;
    }

    enum BoundType {LO_BOUND, UP_BOUND};

public:
    VerticaValueRange(size_t narg):
        narg(narg) {
        ranges.reserve(narg);
        vnWrappersLo.reserve(narg);
        vnWrappersUp.reserve(narg);
        svWrappersLo.reserve(narg);
        svWrappersUp.reserve(narg);
    }

    virtual ~VerticaValueRange() { }

    template <class T, BoundType b>
    const T *getColPtr(size_t idx) {
        if (b == LO_BOUND)
            return reinterpret_cast<const T *>(ranges.at(idx).loBound);
        if (b == UP_BOUND)
            return reinterpret_cast<const T *>(ranges.at(idx).upBound);
        return NULL;
    }

    template <class T, BoundType b>
    T *getColPtrForWrite(size_t idx) {
        if (b == LO_BOUND)
            return reinterpret_cast<T *>(ranges.at(idx).loBound);
        if (b == UP_BOUND)
            return reinterpret_cast<T *>(ranges.at(idx).upBound);
        return NULL;
    }

    /**
     * \brief Gets the sortedness of values in a range.
     *
     * @param idx the range argument number.
     *
     * @return the range sortedness. Possible values are:
     *  EE::SORT_UNORDERED - Unsorted
     *  EE::SORT_MONOTONIC_INCREASING - Ascending
     *  EE::SORT_MONOTONIC_DECREASING - Descending
     *  EE::SORT_CONSTANT - Single value
     */
    EE::ValueSort getSortedness(size_t idx) const {
        return ranges.at(idx).sortedness;
    }

    /**
     * \brief Indicates if there can be NULL values in the range.
     *
     * @param idx the range argument number.
     *
     * @return TRUE if some range values can be NULL, else FALSE.
     */
    bool canHaveNulls(size_t idx) const {
        return ranges.at(idx).canHaveNulls;
    }

    /**
     * \brief Retrieve the number of range arguments.
     *
     * @return the number of range arguments held by this object.
     */
    size_t getNumRanges() const { return narg; }

    /**
     * \brief Returns the data type of the values in a range.
     *
     * @param idx The index of the range
     *
     * @return a VerticaType object describing the data type of the range values.
     */
    const VerticaType &getRangeType(size_t idx) const {
        return ranges.at(idx).dt;
    }
};

/**
 * \brief This class represents the value ranges of the arguments of a UDSF,
 * one range per argument.
 *
 * Instances of this class are used to let UDSF developers specify the output range
 * of UDSFs via the optional ScalarFunction::getOutputRange() function.
 */
class ValueRangeReader: public VerticaValueRange {
public:
    ValueRangeReader(size_t narg):
        VerticaValueRange(narg) { }

    virtual ~ValueRangeReader() {}

    /**
     * \brief Check if this range has lower and upper bounds set.
     *
     * @note Callers of the get reference or get pointer functions should always first call
     *       this method, and only access the range bounds when this function returns TRUE.
     *
     * Example:
     * @code
     *   if (rangeReader.hasBounds(0)) { // Check if bounds of the 1st range argument are set
     *     // OK to access bounds
     *     const vint& i = rangeReader.getIntRefLo(0);
     *     const vint& j = rangeReader.getIntRefUp(0);
     *   }
     * @endcode
     *
     * @return TRUE if the range bounds are safe to access, else FALSE.
     */
    bool hasBounds(size_t idx) const {
        return (ranges.at(idx).loBound != NULL && ranges.at(idx).upBound != NULL);
    }

    /**
     * @brief Get a pointer to an INTEGER value from a range bound.
     *
     * @param idx The argument number to retrieve the range bound.
     * @return a pointer to a chosen bound value, cast appropriately.
     *
     * Example:
     * @code
     * const vint *a = range->getIntPtrLo(0); // gets a pointer to the lower bound of 1st arg
     * @endcode
     */
    const vint *getIntPtrLo(size_t idx) { return getColPtr<vint, LO_BOUND>(idx); }
    const vint *getIntPtrUp(size_t idx) { return getColPtr<vint, UP_BOUND>(idx); }

    /// @brief Get a pointer to a FLOAT value from a range bound.
    ///
    /// @param idx The argument number to retrieve the range bound.
    ///
    /// @return A pointer to the retrieved value cast as a FLOAT.
    const vfloat *getFloatPtrLo(size_t idx) { return getColPtr<vfloat, LO_BOUND>(idx); }
    const vfloat *getFloatPtrUp(size_t idx) { return getColPtr<vfloat, UP_BOUND>(idx); }

    /// @brief Get a pointer to a BOOLEAN value from a range bound.
    ///
    /// @param idx The argument number to retrieve the range bound.
    ///
    /// @return A pointer to the retrieved value cast as a BOOLEAN.
    const vbool *getBoolPtrLo(size_t idx) { return getColPtr<vbool, LO_BOUND>(idx); }
    const vbool *getBoolPtrUp(size_t idx) { return getColPtr<vbool, UP_BOUND>(idx); }

    /// @brief Get a pointer to a DATE value from a range bound.
    ///
    /// @param idx The argument number to retrieve the range bound.
    ///
    /// @return A pointer to the retrieved value cast as a DATE.
    const DateADT *getDatePtrLo(size_t idx) { return getColPtr<DateADT, LO_BOUND>(idx); }
    const DateADT *getDatePtrUp(size_t idx) { return getColPtr<DateADT, UP_BOUND>(idx); }

    /// @brief Get a pointer to an INTERVAL value from a range bound.
    ///
    /// @param idx The argument number to retrieve the range bound.
    ///
    /// @return A pointer to the retrieved value cast as an INTERVAL.
    const Interval *getIntervalPtrLo(size_t idx) { return getColPtr<Interval, LO_BOUND>(idx); }
    const Interval *getIntervalPtrUp(size_t idx) { return getColPtr<Interval, UP_BOUND>(idx); }

    /// @brief Get a pointer to a INTERVAL YEAR TO MONTH value from a range bound.
    ///
    /// @param idx The argument number to retrieve the range bound.
    ///
    /// @return A point to the retrieved value cast as a INTERVAL YEAR TO MONTH.
    const IntervalYM *getIntervalYMPtrLo(size_t idx) { return getColPtr<IntervalYM, LO_BOUND>(idx); }
    const IntervalYM *getIntervalYMPtrUp(size_t idx) { return getColPtr<IntervalYM, UP_BOUND>(idx); }

    /// @brief Get a pointer to a TIMESTAMP value from a range bound.
    ///
    /// @param idx The argument number to retrieve the range bound.
    ///
    /// @return A pointer to the retrieved value cast as a TIMESTAMP.
    const Timestamp *getTimestampPtrLo(size_t idx) { return getColPtr<Timestamp, LO_BOUND>(idx); }
    const Timestamp *getTimestampPtrUp(size_t idx) { return getColPtr<Timestamp, UP_BOUND>(idx); }

    /// @brief Get a pointer to a TIMESTAMP WITH TIMEZONE value from a range bound.
    ///
    /// @param idx The argument number to retrieve the range bound.
    ///
    /// @return A pointer to the retrieved value cast as a TIMESTAMP WITH TIMEZONE.
    const TimestampTz *getTimestampTzPtrLo(size_t idx) { return getColPtr<TimestampTz, LO_BOUND>(idx); }
    const TimestampTz *getTimestampTzPtrUp(size_t idx) { return getColPtr<TimestampTz, UP_BOUND>(idx); }

    /// @brief Get a pointer to a TIME value from a range bound.
    ///
    /// @param idx The argument number to retrieve the range bound.
    ///
    /// @return A pointer to the retrieved value cast as a TIME.
    const TimeADT *getTimePtrLo(size_t idx) { return getColPtr<TimeADT, LO_BOUND>(idx); }
    const TimeADT *getTimePtrUp(size_t idx) { return getColPtr<TimeADT, UP_BOUND>(idx); }

    /// @brief Get a pointer to a TIME WITH TIMEZONE value from a range bound.
    ///
    /// @param idx The argument number to retrieve the range bound.
    ///
    /// @return A pointer to the retrieved value cast as a TIME WITH TIMEZONE.
    const TimeTzADT *getTimeTzPtrLo(size_t idx) { return getColPtr<TimeTzADT, LO_BOUND>(idx); }
    const TimeTzADT *getTimeTzPtrUp(size_t idx) { return getColPtr<TimeTzADT, UP_BOUND>(idx); }

    /// @brief Get a pointer to a VNumeric value from a range bound.
    ///
    /// @param idx The argument number to retrieve the range bound.
    ///
    /// @return A pointer to the retrieved value cast as a Numeric.
    const VNumeric *getNumericPtrLo(size_t idx)
    {
        const uint64 *words = reinterpret_cast<const uint64 *>(getColPtr<vint, LO_BOUND>(idx));
        vnWrappersLo[idx].words = const_cast<uint64*>(words);
        return &vnWrappersLo[idx];
    }
    const VNumeric *getNumericPtrUp(size_t idx)
    {
        const uint64 *words = reinterpret_cast<const uint64 *>(getColPtr<vint, UP_BOUND>(idx));
        vnWrappersUp[idx].words = const_cast<uint64*>(words);
        return &vnWrappersUp[idx];
    }

    /// @brief Get a pointer to a VString value from a range bound.
    ///
    /// @param idx The argument number to retrieve the range bound.
    ///
    /// @return A pointer to the retrieved value cast as a VString.
    const VString *getStringPtrLo(size_t idx)
    {
        const EE::StringValue *sv = reinterpret_cast<const EE::StringValue *>(getColPtr<vint, LO_BOUND>(idx));
        svWrappersLo[idx].sv = const_cast<EE::StringValue*>(sv);
        return &svWrappersLo[idx];
    }
    const VString *getStringPtrUp(size_t idx)
    {
        const EE::StringValue *sv = reinterpret_cast<const EE::StringValue *>(getColPtr<vint, UP_BOUND>(idx));
        svWrappersUp[idx].sv = const_cast<EE::StringValue*>(sv);
        return &svWrappersUp[idx];
    }

    /**
     * @brief Get a reference to an INTEGER value from a range bound.
     *
     * @param idx The argument number to retrieve the range bound.
     *
     * @return a reference to the chosen bound value, cast as INTEGER.
     *
     * Example:
     *
     * @code
     * const vint& a = range->getIntRefUp(0); // get upper bound of the 1st range argument
     * @endcode
     */
    const vint &getIntRefLo(size_t idx) { return *getIntPtrLo(idx); }
    const vint &getIntRefUp(size_t idx) { return *getIntPtrUp(idx); }

    /**
     * @brief Get a reference to a FLOAT value from a range bound.
     *
     * @param idx The argument number to retrieve the range bound.
     *
     * @return A reference to the idx'th argument, cast as FLOAT.
     */
    const vfloat &getFloatRefLo(size_t idx) { return *getFloatPtrLo(idx); }
    const vfloat &getFloatRefUp(size_t idx) { return *getFloatPtrUp(idx); }

    /**
     * @brief Get a reference to a BOOLEAN value from a range bound.
     *
     * @param idx The argument number to retrieve the range bound.
     *
     * @return a reference to the idx'th argument, cast as BOOLEAN.
     */
    const vbool &getBoolRefLo(size_t idx) { return *getBoolPtrLo(idx); }
    const vbool &getBoolRefUp(size_t idx) { return *getBoolPtrUp(idx); }

    /**
     * @brief Get a reference to a DATE value from a range bound.
     *
     * @param idx The argument number to retrieve the range bound.
     *
     * @return a reference to the idx'th argument, cast as DATE.
     */
    const DateADT &getDateRefLo(size_t idx) { return *getDatePtrLo(idx); }
    const DateADT &getDateRefUp(size_t idx) { return *getDatePtrUp(idx); }

    /**
     * @brief Get a reference to an INTERVAL value from a range bound.
     *
     * @param idx The argument number to retrieve the range bound.
     *
     * @return a reference to the idx'th argument, cast as an INTERVAL.
     */
    const Interval &getIntervalRefLo(size_t idx) { return *getIntervalPtrLo(idx); }
    const Interval &getIntervalRefUp(size_t idx) { return *getIntervalPtrUp(idx); }

    /**
     * @brief Get a reference to an INTERVAL YEAR TO MONTH value from a range bound.
     *
     * @param idx The argument number to retrieve the range bound.
     *
     * @return a reference to the idx'th argument, cast as INTERVAL YEAR TO MONTH.
     */
    const IntervalYM &getIntervalYMRefLo(size_t idx) { return *getIntervalYMPtrLo(idx); }
    const IntervalYM &getIntervalYMRefUp(size_t idx) { return *getIntervalYMPtrUp(idx); }

    /**
     * @brief Get a reference to a TIMESTAMP value from a range bound.
     *
     * @param idx The argument number to retrieve the range bound.
     *
     * @return a reference to the idx'th argument, cast as TIMESTAMP.
     */
    const Timestamp &getTimestampRefLo(size_t idx) { return *getTimestampPtrLo(idx); }
    const Timestamp &getTimestampRefUp(size_t idx) { return *getTimestampPtrUp(idx); }

    /**
     * @brief Get a reference to a TIMESTAMP WITH TIMEZONE value from a range bound.
     *
     * @param idx The argument number to retrieve the range bound.
     *
     * @return a reference to the idx'th argument, cast as TIMESTAMP WITH TIMEZONE.
     */
    const TimestampTz &getTimestampTzRefLo(size_t idx) { return *getTimestampTzPtrLo(idx); }
    const TimestampTz &getTimestampTzRefUp(size_t idx) { return *getTimestampTzPtrUp(idx); }

    /**
     * @brief Get a reference to a TIME value from a range bound.
     *
     * @param idx The argument number to retrieve the range bound.
     *
     * @return a reference to the idx'th argument, cast as TIME.
     */
    const TimeADT &getTimeRefLo(size_t idx) { return *getTimePtrLo(idx); }
    const TimeADT &getTimeRefUp(size_t idx) { return *getTimePtrUp(idx); }

    /**
     * @brief Get a reference to a TIME WITH TIMEZONE value from a range bound.
     *
     * @param idx The argument number to retrieve the range bound.
     *
     * @return a reference to the idx'th argument, cast as TIME WITH TIMEZONE.
     */
    const TimeTzADT &getTimeTzRefLo(size_t idx) { return *getTimeTzPtrLo(idx); }
    const TimeTzADT &getTimeTzRefUp(size_t idx) { return *getTimeTzPtrUp(idx); }

    /**
     * @brief Get a reference to a VNumeric value from a range bound.
     *
     * @param idx The argument number to retrieve the range bound.
     *
     * @return a reference to the idx'th argument, cast as VNumeric.
     */
    const VNumeric &getNumericRefLo(size_t idx) { return *getNumericPtrLo(idx); }
    const VNumeric &getNumericRefUp(size_t idx) { return *getNumericPtrUp(idx); }

    /**
     * @brief Get a reference to an VString value from a range bound.
     *
     * @param idx The argument number to retrieve the range bound.
     *
     * @return a reference to the idx'th argument, cast as VString.
     *
     * @note the returned object is a variable-length prefix of the actual string
     * values in the range. For example, if the range values are {'abc', 'abb', 'bc', 'cab'}
     * the range bounds may be like: Lo='a' and Up='cb'.
     * In the example, the upper bound is not even a prefix of any value in the range, but
     * all values in the range are greater than or equal than \a Lo and less than or equal to \a Up.
     */
    const VString &getStringRefLo(size_t idx) { return *getStringPtrLo(idx); }
    const VString &getStringRefUp(size_t idx) { return *getStringPtrUp(idx); }

    /**
     * @brief Check if all values in the idx'th input range are NULL.
     *
     * @param idx The argument range number.
     *
     * @return true if all values in the range are NULL, false otherwise.
     */
    bool isNull(int idx)
    {
        if (!ranges.at(idx).canHaveNulls || ranges.at(idx).sortedness != EE::SORT_CONSTANT) {
            return false;
        }

        const VerticaType &dt = ranges.at(idx).dt;
        const BaseDataOID oid = dt.getUnderlyingType();

        switch (oid)
        {
        case BoolOID: {
            return (getBoolRefLo(idx) == vbool_null && getBoolRefUp(idx) == vbool_null);
        }
            break;
        case Float8OID:     {
            return (vfloatIsNull(getFloatRefLo(idx)) && vfloatIsNull(getFloatRefUp(idx)));
        }
            break;
        case BinaryOID:
        case VarbinaryOID:
        case LongVarbinaryOID:
        case CharOID:
        case VarcharOID:
        case LongVarcharOID: {
            return (getStringRefLo(idx).isNull() && getStringRefUp(idx).isNull());
        }
        break;
        case NumericOID: {
            return (getNumericRefLo(idx).isNull() && getNumericRefUp(idx).isNull());
        }
            break;
        case Int8OID:{
            return (getIntRefLo(idx) == vint_null && getIntRefUp(idx) == vint_null);
        }
            break;
        case IntervalOID:{
            return (getIntervalRefLo(idx) == vint_null && getIntervalRefUp(idx) == vint_null);
        }
            break;
        case TimestampOID:{
            return (getTimestampRefLo(idx) == vint_null && getTimestampRefUp(idx) == vint_null);
        }
            break;
        case TimestampTzOID:{
            return (getTimestampTzRefLo(idx) == vint_null && getTimestampTzRefUp(idx) == vint_null);
        }
            break;
        case TimeOID:{
            return (getTimeRefLo(idx) == vint_null && getTimeRefUp(idx) == vint_null);
        }
            break;
        case TimeTzOID:{
            return (getTimeTzRefLo(idx) == vint_null && getTimeTzRefUp(idx) == vint_null);
        }
            break;
        case DateOID:{
            return (getDateRefLo(idx) == vint_null && getDateRefUp(idx) == vint_null);
        }
            break;
        case IntervalYMOID:{
            return (getIntervalYMRefLo(idx) == vint_null && getIntervalYMRefUp(idx) == vint_null);
        }
            break;
        default:
            ereport(ERROR,
                    (errmsg("Unknown data type"),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }
        //should never get here, prevents compiler warning
        return false;
    }

private:
    friend class EE::VEval;
};

/**
 * \brief This class represents the output value range of a UDSF.
 *
 * Instances of this class are used to allow UDSF developers specify the output
 * range of UDSFs via the optional ScalarFunction::getOutputRange() function.
 */
class ValueRangeWriter : public VerticaValueRange {
public:
    ValueRangeWriter(char *outLoBound, char *outUpBound,
                     const VerticaType &dt, EE::ValueSort sortedness,
                     bool canHaveNulls) : VerticaValueRange(1) {
        isRangeBounded = false;
        addArg(outLoBound, outUpBound, dt, sortedness, canHaveNulls);
    }

    virtual ~ValueRangeWriter() {}

    /// @brief Sets a range bound as an INTEGER value.
    /// @param r The INTEGER value to set the range bound.
    void setIntLo(vint r) { *getColPtrForWrite<vint, LO_BOUND>(0) = r; }
    void setIntUp(vint r) { *getColPtrForWrite<vint, UP_BOUND>(0) = r; }

    /// @brief Sets a range bound as a FLOAT value to the output row.
    /// @param r The FLOAT value to set the range bound.
    void setFloatLo(vfloat r) { *getColPtrForWrite<vfloat, LO_BOUND>(0) = r; }
    void setFloatUp(vfloat r) { *getColPtrForWrite<vfloat, UP_BOUND>(0) = r; }

    /// @brief Sets a range bound as a BOOLEAN value.
    /// @param r The BOOLEAN value to set the range bound.
    void setBoolLo(vbool r) { *getColPtrForWrite<vbool, LO_BOUND>(0) = r; }
    void setBoolUp(vbool r) { *getColPtrForWrite<vbool, UP_BOUND>(0) = r; }

    /// @brief Sets a range bound as a DATE value.
    /// @param r The DATE value to set the range bound.
    void setDateLo(DateADT r) { *getColPtrForWrite<DateADT, LO_BOUND>(0) = r; }
    void setDateUp(DateADT r) { *getColPtrForWrite<DateADT, UP_BOUND>(0) = r; }

    /// @brief Sets a range bound as an INTERVAL value.
    /// @param r The INTERVAL value to set the range bound.
    void setIntervalLo(Interval r) { *getColPtrForWrite<Interval, LO_BOUND>(0) = r; }
    void setIntervalUp(Interval r) { *getColPtrForWrite<Interval, UP_BOUND>(0) = r; }

    /// @brief Sets a range bound as an INTERVAL YEAR TO MONTH value.
    /// @param r The INTERVAL YEAR TO MONTH value to set the range bound.
    void setIntervalYMLo(IntervalYM r) { *getColPtrForWrite<IntervalYM, LO_BOUND>(0) = r; }
    void setIntervalYMUp(IntervalYM r) { *getColPtrForWrite<IntervalYM, UP_BOUND>(0) = r; }

    /// @brief Sets a range bound as a TIMESTAMP value.
    /// @param r The TIMESTAMP value to set the range bound.
    void setTimestampLo(Timestamp r) { *getColPtrForWrite<Timestamp, LO_BOUND>(0) = r; }
    void setTimestampUp(Timestamp r) { *getColPtrForWrite<Timestamp, UP_BOUND>(0) = r; }

    /// @brief Sets a range bound as a TIMESTAMP WITH TIMEZONE value.
    /// @param r The TIMESTAMP WITH TIMEZONE value to set the range bound.
    void setTimestampTzLo(TimestampTz r) { *getColPtrForWrite<TimestampTz, LO_BOUND>(0) = r; }
    void setTimestampTzUp(TimestampTz r) { *getColPtrForWrite<TimestampTz, UP_BOUND>(0) = r; }

    /// @brief Sets a range bound as a TIMESTAMP value.
    /// @param r The TIMESTAMP value to set the range bound.
    void setTimeLo(TimeADT r) { *getColPtrForWrite<TimeADT, LO_BOUND>(0) = r; }
    void setTimeUp(TimeADT r) { *getColPtrForWrite<TimeADT, UP_BOUND>(0) = r; }

    /// @brief Sets a range bound as a TIMESTAMP WITH TIMEZONE value.
    /// @param r The TIMESTAMP WITH TIMEZONE value to set the range bound.
    void setTimeTzLo(TimeTzADT r) { *getColPtrForWrite<TimeTzADT, LO_BOUND>(0) = r; }
    void setTimeTzUp(TimeTzADT r) { *getColPtrForWrite<TimeTzADT, UP_BOUND>(0) = r; }

    /// @brief Gets a VNumeric object reference to set the range lower bound.
    /// @return A VNumeric object reference.
    VNumeric &getNumericRefLo()
    {
        uint64 *words = getColPtrForWrite<uint64, LO_BOUND>(0);
        vnWrappersLo[0].words = words;
        return vnWrappersLo[0];
    }

    /// @brief Gets a VNumeric object reference to set the range upper bound.
    /// @return A VNumeric object reference.
    VNumeric &getNumericRefUp()
    {
        uint64 *words = getColPtrForWrite<uint64, UP_BOUND>(0);
        vnWrappersUp[0].words = words;
        return vnWrappersUp[0];
    }

    /// @brief Gets a VString object reference to set the range lower bound.
    /// @return A VString object reference.
    VString &getStringRefLo()
    {
        EE::StringValue *sv = getColPtrForWrite<EE::StringValue, LO_BOUND>(0);
        svWrappersLo[0].sv = sv;
        return svWrappersLo[0];
    }

    /// @brief Gets a VString object reference to set the range upper bound.
    /// @return A VString object reference.
    VString &getStringRefUp()
    {
        EE::StringValue *sv = getColPtrForWrite<EE::StringValue, UP_BOUND>(0);
        svWrappersUp[0].sv = sv;
        return svWrappersUp[0];
    }

    /**
     * \brief Sets to NULL all values in this range.
     *
     * @note As side effect of this method, this range will be marked as having NULL values and
     * its sortedness set to EE::SORT_CONSTANT.
     */
    void setNull()
    {
        const VerticaType &dt = ranges.at(0).dt;
        BaseDataOID oid = dt.getUnderlyingType();
        setCanHaveNulls(true);
        setSortedness(EE::SORT_CONSTANT);

        switch (oid)
        {
        case Int8OID: {
            setIntLo(vint_null);
            setIntUp(vint_null);
        }
            break;
        case Float8OID: {
            setFloatLo(vfloat_null);
            setFloatUp(vfloat_null);
        }
            break;
        case BoolOID: {
            setBoolLo(vbool_null);
            setBoolUp(vbool_null);
        }
            break;
        case DateOID: {
            setDateLo(vint_null);
            setDateUp(vint_null);
        }
            break;
        case IntervalYMOID:
        case IntervalOID: {
            setIntervalLo(vint_null);
            setIntervalUp(vint_null);
        }
            break;
        case TimestampOID: {
            setTimestampLo(vint_null);
            setTimestampUp(vint_null);
        }
            break;
        case TimestampTzOID: {
            setTimestampTzLo(vint_null);
            setTimestampTzUp(vint_null);
        }
            break;
        case TimeOID: {
            setTimeLo(vint_null);
            setTimeUp(vint_null);
        }
            break;
        case TimeTzOID: {
            setTimeTzLo(vint_null);
            setTimeTzUp(vint_null);
        }
            break;
        case BinaryOID:
        case VarbinaryOID:
        case LongVarbinaryOID:
        case CharOID:
        case VarcharOID:
        case LongVarcharOID: {
            getStringRefLo().setNull();
            getStringRefUp().setNull();
        }
            break;
        case NumericOID: {
            getNumericRefLo().setNull();
            getNumericRefUp().setNull();
        }
            break;
        default:
            ereport(ERROR,
                    (errmsg("Unknown data type"),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }
    }

    void setSortedness(EE::ValueSort s) {
        VerticaValueRange::setSortedness(0, s);
    }

    EE::ValueSort getSortedness() const {
        return VerticaValueRange::getSortedness(0);
    }

    void setCanHaveNulls(bool u) {
        VerticaValueRange::setCanHaveNulls(0, u);
    }

    bool canHaveNulls() const {
        return VerticaValueRange::canHaveNulls(0);
    }

    /**
     * \brief Lets Vertica know that this output range has user-defined bounds.
     */
    void setHasBounds() {
        isRangeBounded = true;
    }

    bool hasBounds() const {
        return isRangeBounded;
    }

private:
    friend class EE::VEval;
    bool isRangeBounded; // If set to TRUE, the server will use the range bounds in expr. analysis
};

/**
 * @brief A wrapper around a single intermediate aggregate value.
 *
 */
class IntermediateAggs : public VerticaBlock
{
public:
    IntermediateAggs() : VerticaBlock(0, 0, NULL) {}
    IntermediateAggs(size_t ninter) : VerticaBlock(ninter, 1, NULL) {}

    /**
     * @brief Get a pointer to an INTEGER value from the intermediate results set.
     *
     * @return a pointer to the idx'th argument, cast appropriately.
     *
     * @param idx The column number to retrieve from the intermediate results set.
     *
     * Example:
     *
     * @code
     *  vint *a = arg_reader->getIntPtr(0);
     * @endcode
     */
    vint *getIntPtr(size_t idx)                { return getColPtrForWrite<vint>(idx); }
    /// @brief Get a pointer to a FLOAT value from the intermediate results set.
    ///
    /// @param idx The column number in the intermediate results set to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a FLOAT.
    vfloat *getFloatPtr(size_t idx)            { return getColPtrForWrite<vfloat>(idx); }
    /// @brief Get a pointer to a BOOLEAN value from the intermediate results set.
    ///
    /// @param idx The column number in the intermediate results set to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a BOOLEAN.
    vbool *getBoolPtr(size_t idx)              { return getColPtrForWrite<vbool>(idx); }
    /// @brief Get a pointer to a DATE value from the intermediate results set.
    ///
    /// @param idx The column number in the intermediate results set to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a DATE.
    DateADT *getDatePtr(size_t idx)            { return getColPtrForWrite<DateADT>(idx); }
    /// @brief Get a pointer to an INTERVAL value from the intermediate results set.
    ///
    /// @param idx The column number in the intermediate results set to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as an INTERVAL.
    Interval *getIntervalPtr(size_t idx)       { return getColPtrForWrite<Interval>(idx); }
    /// @brief Get a pointer to a INTERVAL YEAR TO MONTH value from the intermediate results set.
    ///
    /// @param idx The column number in the intermediate results set to retrieve.
    ///
    /// @return A point to the retrieved value cast as a INTERVAL YEAR TO MONTH.
    IntervalYM *getIntervalYMPtr(size_t idx)   { return getColPtrForWrite<IntervalYM>(idx); }
    /// @brief Get a pointer to a TIMESTAMP value from the intermediate results set.
    ///
    /// @param idx The column number in the intermediate results set to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a TIMESTAMP.
    Timestamp *getTimestampPtr(size_t idx)     { return getColPtrForWrite<Timestamp>(idx); }
    /// @brief Get a pointer to a TIMESTAMP WITH TIMEZONE value from the intermediate results set.
    ///
    /// @param idx The column number in the intermediate results set to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a TIMESTAMP WITH TIMEZONE .
    TimestampTz *getTimestampTzPtr(size_t idx) { return getColPtrForWrite<TimestampTz>(idx); }
    /// @brief Get a pointer to a TIME value from the intermediate results set.
    ///
    /// @param idx The column number in the intermediate results set to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a TIME.
    TimeADT *getTimePtr(size_t idx)            { return getColPtrForWrite<TimeADT>(idx); }
    /// @brief Get a pointer to a TIME WITH TIMEZONE value from the intermediate results set.
    ///
    /// @param idx The column number in the intermediate results set to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a TIME WITH TIMEZONE.
    TimeTzADT *getTimeTzPtr(size_t idx)        { return getColPtrForWrite<TimeTzADT>(idx); }
    /// @brief Get a pointer to a VNumeric value from the intermediate results set.
    ///
    /// @param idx The column number to retrieve from the intermediate results set.
    ///
    /// @return A pointer to the retrieved value cast as a Numeric.
    VNumeric *getNumericPtr(size_t idx)
    {
        uint64 *words = reinterpret_cast< uint64 *>(getColPtrForWrite<vint>(idx));
        vnWrappers[idx].words = words;
        return &vnWrappers[idx];
    }
    /// @brief Get a pointer to a VString value from the intermediate results set.
    ///
    /// @param idx The column number to retrieve from the intermediate results set.
    ///
    /// @return A pointer to the retrieved value cast as a VString.
    VString *getStringPtr(size_t idx)
    {
        EE::StringValue *sv = reinterpret_cast<EE::StringValue *>(getColPtrForWrite<vint>(idx));
        svWrappers[idx].sv = sv;
        return &svWrappers[idx];
    }

    /**
     * @brief Get a reference to an INTEGER value from the intermediate results set.

     * @param idx The column number to retrieve from the intermediate results set.
     *
     * @return a reference to the idx'th argument, cast as an INTEGER.
     *
     * Example:
     *
     * @code
     *  vint a = arg_reader->getIntRef(0);
     * @endcode
     */
    vint        &getIntRef(size_t idx)         { return *getIntPtr(idx); }
    /**
     * @brief Get a reference to a FLOAT value from the intermediate results set.

     * @param idx The column number to retrieve from the intermediate results set.
     *
     * @return A reference to the idx'th argument, cast as an FLOAT.
     */
    vfloat      &getFloatRef(size_t idx)       { return *getFloatPtr(idx); }
    /**
     * @brief Get a reference to a BOOLEAN value from the intermediate results set.

     * @param idx The column number to retrieve from the intermediate results set.
     *
     * @return a reference to the idx'th argument, cast as an BOOLEAN.
     */
    vbool       &getBoolRef(size_t idx)        { return *getBoolPtr(idx); }
    /**
     * @brief Get a reference to a DATE value from the intermediate results set.

     * @param idx The column number to retrieve from the intermediate results set.
     *
     * @return a reference to the idx'th argument, cast as an DATE.
     */
    DateADT     &getDateRef(size_t idx)        { return *getDatePtr(idx); }
    /**
     * @brief Get a reference to an INTERVAL value from the intermediate results set.

     * @param idx The column number to retrieve from the intermediate results set.
     *
     * @return a reference to the idx'th argument, cast as an INTERVAL.
     */
    Interval    &getIntervalRef(size_t idx)    { return *getIntervalPtr(idx); }
    /**
     * @brief Get a reference to an INTERVAL YEAR TO MONTH value from the intermediate results set.

     * @param idx The column number to retrieve from the intermediate results set.
     *
     * @return a reference to the idx'th argument, cast as an INTERVAL YEAR TO MONTH.
     */
    IntervalYM  &getIntervalYMRef(size_t idx)  { return *getIntervalYMPtr(idx); }
    /**
     * @brief Get a reference to a TIMESTAMP value from the intermediate results set.

     * @param idx The column number to retrieve from the intermediate results set.
     *
     * @return a reference to the idx'th argument, cast as a TIMESTAMP.
     */
    Timestamp   &getTimestampRef(size_t idx)   { return *getTimestampPtr(idx); }
    /**
     * @brief Get a reference to a TIMESTAMP WITH TIMEZONE value from the intermediate results set.

     * @param idx The column number to retrieve from the intermediate results set.
     *
     * @return a reference to the idx'th argument, cast as a TIMESTAMP WITH TIMEZONE.
     */
    TimestampTz &getTimestampTzRef(size_t idx) { return *getTimestampTzPtr(idx); }
    /**
     * @brief Get a reference to a TIME value from the intermediate results set.

     * @param idx The column number to retrieve from the intermediate results set.
     *
     * @return a reference to the idx'th argument, cast as a TIME.
     */
    TimeADT     &getTimeRef(size_t idx)       { return *getTimePtr(idx); }
    /**
     * @brief Get a reference to a TIME WITH TIMEZONE value from the intermediate results set.

     * @param idx The column number to retrieve from the intermediate results set.
     *
     * @return a reference to the idx'th argument, cast as a TIME WITH TIMEZONE.
     */
    TimeTzADT   &getTimeTzRef(size_t idx)     { return *getTimeTzPtr(idx); }
    /**
     * @brief Get a reference to a VNumeric value from the intermediate results set.

     * @param idx The column number to retrieve from the intermediate results set.
     *
     * @return a reference to the idx'th argument, cast as an VNumeric.
     */
    VNumeric    &getNumericRef(size_t idx)     { return *getNumericPtr(idx); }
    /**
     * @brief Get a reference to an VString value from the intermediate results set.

     * @param idx The column number to retrieve from the intermediate results set.
     *
     * @return a reference to the idx'th argument, cast as an VString.
     */
    VString     &getStringRef(size_t idx)      { return *getStringPtr(idx); }

private:
};


/**
 * @brief A wrapper around multiple intermediate aggregates.
 *
 */
class MultipleIntermediateAggs : public BlockReader
{
public:
    MultipleIntermediateAggs() :
        BlockReader(0,0,NULL) {}

    MultipleIntermediateAggs(size_t narg) :
    BlockReader(narg, 0, NULL) {}
};

/**
 * @brief Provides read access to  a set of named parameters.  This class 
 * provides type-specific methods to retrieve values.
 */
class ParamReader : public VerticaBlock
{
public:
    ParamReader(size_t nparams) : VerticaBlock(nparams, 1, NULL) {}
    ParamReader() : VerticaBlock(0, 1, NULL) {}

    /**
     * @brief Copy the other ParamReader. Discard any existing parameters
     * TODO: create an alternate version that only copies if this ParamReader is empty,
     *       is a no-op if they are non-empty and the same,
     *       and error if they are non-empty and different
     */
    void copy(const ParamReader &other)
    {
        paramNameToIndex = other.paramNameToIndex;
        VerticaBlock::copy((const VerticaBlock &)other);
    }

    /**
     * @brief Function to see if the ParamReader has a value for the parameter
     */
    bool containsParameter(std::string paramName) const
    {
        std::transform(paramName.begin(), paramName.end(), paramName.begin(), ::tolower);
        return paramNameToIndex.find(paramName) != paramNameToIndex.end();
    }

    /**
     * @brief Returns true if there are no parameters
     */
    bool isEmpty() const { return paramNameToIndex.empty(); }

    /**
     * @brief Return the names of all parameters stored in this ParamReader
     */
    std::vector<std::string> getParamNames() const {
        std::vector<std::string> paramNames;

        for (std::map<std::string, size_t>::const_iterator iter = paramNameToIndex.begin();
                iter != paramNameToIndex.end(); ++iter) {
            paramNames.push_back(iter->first);
        }

        return paramNames;
    }

    /**
     * @brief Return the type of the given parameter
     */
    VerticaType getType(std::string paramName) const {
        return getTypeMetaData().getColumnType(getIndex(paramName));
    }

    /**
     * @brief Get a pointer to an INTEGER value from the input row.
     *
     * @return a pointer to the idx'th argument, cast appropriately.
     *
     * @param paramName The name of the parameter to retrieve
     *
     * Example:
     *
     * @code
     *  vint *a = arg_reader->getIntPtr("max");
     * @endcode
     */
    const vint *getIntPtr(std::string paramName) const { return getColPtr<vint>(getIndex(paramName)); }
    /// @brief Get a pointer to a FLOAT value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a FLOAT.
    const vfloat *getFloatPtr(std::string paramName) const { return getColPtr<vfloat>(getIndex(paramName)); }
    /// @brief Get a pointer to a BOOLEAN value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a BOOLEAN.
    const vbool *getBoolPtr(std::string paramName) const { return getColPtr<vbool>(getIndex(paramName)); }
    /// @brief Get a pointer to a DATE value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a DATE.
    const DateADT *getDatePtr(std::string paramName) const { return getColPtr<DateADT>(getIndex(paramName)); }
    /// @brief Get a pointer to an INTERVAL value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as an INTERVAL.
    const Interval *getIntervalPtr(std::string paramName) const { return getColPtr<Interval>(getIndex(paramName)); }
    /// @brief Get a pointer to a INTERVAL YEAR TO MONTH value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A point to the retrieved value cast as a INTERVAL YEAR TO MONTH.
    const IntervalYM *getIntervalYMPtr(std::string paramName) const { return getColPtr<IntervalYM>(getIndex(paramName)); }
    /// @brief Get a pointer to a TIMESTAMP value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a TIMESTAMP.
    const Timestamp *getTimestampPtr(std::string paramName) const { return getColPtr<Timestamp>(getIndex(paramName)); }
    /// @brief Get a pointer to a TIMESTAMP WITH TIMEZONE value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a TIMESTAMP WITH TIMEZONE .
    const TimestampTz *getTimestampTzPtr(std::string paramName) const { return getColPtr<TimestampTz>(getIndex(paramName)); }
    /// @brief Get a pointer to a TIME value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a TIME.
    const TimeADT *getTimePtr(std::string paramName) const { return getColPtr<TimeADT>(getIndex(paramName)); }
    /// @brief Get a pointer to a TIME WITH TIMEZONE value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a TIME WITH TIMEZONE.
    const TimeTzADT *getTimeTzPtr(std::string paramName) const { return getColPtr<TimeTzADT>(getIndex(paramName)); }
    /// @brief Get a pointer to a VNumeric value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a Numeric.
    const VNumeric *getNumericPtr(std::string paramName)
    {
        size_t idx = getIndex(paramName);
        const uint64 *words = reinterpret_cast<const uint64 *>(getColPtr<vint>(idx));
        vnWrappers[idx].words = const_cast<uint64*>(words);
        return &vnWrappers[idx];
    }
    /// @brief Get a pointer to a VString value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a VString.
    const VString *getStringPtr(std::string paramName)
    {
        size_t idx = getIndex(paramName);
        const EE::StringValue *sv = reinterpret_cast<const EE::StringValue *>(getColPtr<vint>(idx));
        svWrappers[idx].sv = const_cast<EE::StringValue*>(sv);
        return &svWrappers[idx];
    }

    /**
     * @brief Get a reference to an INTEGER value from the input row.

     * @param paramName The name of the parameter to retrieve
     *
     * @return a reference to the parameter value, cast as an INTEGER.
     *
     * Example:
     *
     * @code
     *  vint a = arg_reader->getIntRef("max");
     * @endcode
     */
    const vint        &getIntRef(std::string paramName) const { return *getIntPtr(paramName); }
    /**
     * @brief Get a reference to a FLOAT value from the input row.

     * @param paramName The name of the parameter to retrieve
     *
     * @return A reference to the parameter value, cast as an FLOAT.
     */
    const vfloat      &getFloatRef(std::string paramName) const { return *getFloatPtr(paramName); }
    /**
     * @brief Get a reference to a BOOLEAN value from the input row.

     * @param paramName The name of the parameter to retrieve
     *
     * @return a reference to the parameter value, cast as an BOOLEAN.
     */
    const vbool       &getBoolRef(std::string paramName) const { return *getBoolPtr(paramName); }
    /**
     * @brief Get a reference to a DATE value from the input row.

     * @param paramName The name of the parameter to retrieve
     *
     * @return a reference to the parameter value, cast as an DATE.
     */
    const DateADT     &getDateRef(std::string paramName) const { return *getDatePtr(paramName); }
    /**
     * @brief Get a reference to an INTERVAL value from the input row.

     * @param paramName The name of the parameter to retrieve
     *
     * @return a reference to the parameter value, cast as an INTERVAL.
     */
    const Interval    &getIntervalRef(std::string paramName) const { return *getIntervalPtr(paramName); }
    /**
     * @brief Get a reference to an INTERVAL YEAR TO MONTH value from the input row.

     * @param paramName The name of the parameter to retrieve
     *
     * @return a reference to the parameter value, cast as an INTERVAL YEAR TO MONTH.
     */
    const IntervalYM  &getIntervalYMRef(std::string paramName) const { return *getIntervalYMPtr(paramName); }
    /**
     * @brief Get a reference to a TIMESTAMP value from the input row.

     * @param paramName The name of the parameter to retrieve
     *
     * @return a reference to the parameter value, cast as a TIMESTAMP.
     */
    const Timestamp   &getTimestampRef(std::string paramName) const { return *getTimestampPtr(paramName); }
    /**
     * @brief Get a reference to a TIMESTAMP WITH TIMEZONE value from the input row.

     * @param paramName The name of the parameter to retrieve
     *
     * @return a reference to the parameter value, cast as a TIMESTAMP WITH TIMEZONE.
     */
    const TimestampTz &getTimestampTzRef(std::string paramName) const { return *getTimestampTzPtr(paramName); }
    /**
     * @brief Get a reference to a TIME value from the input row.

     * @param paramName The name of the parameter to retrieve
     *
     * @return a reference to the parameter value, cast as a TIME.
     */
    const TimeADT     &getTimeRef(std::string paramName) const { return *getTimePtr(paramName); }
    /**
     * @brief Get a reference to a TIME WITH TIMEZONE value from the input row.

     * @param paramName The name of the parameter to retrieve
     *
     * @return a reference to the parameter value, cast as a TIME WITH TIMEZONE.
     */
    const TimeTzADT   &getTimeTzRef(std::string paramName) const { return *getTimeTzPtr(paramName); }
    /**
     * @brief Get a reference to a VNumeric value from the input row.

     * @param paramName The name of the parameter to retrieve
     *
     * @return a reference to the parameter value, cast as an VNumeric.
     */
    const VNumeric    &getNumericRef(std::string paramName)     { return *getNumericPtr(paramName); }
    /**
     * @brief Get a reference to an VString value from the input row.

     * @param paramName The name of the parameter to retrieve
     *
     * @return a reference to the parameter value, cast as an VString.
     */
    const VString     &getStringRef(std::string paramName)      { return *getStringPtr(paramName); }

    /**
     * Add a parameter to the block and stores it name and corresponding index
     * in the paramNameToIndex map
     */
    void addParameter(std::string paramName, const char *arg, const VerticaType &dt)
    {
        std::transform(paramName.begin(), paramName.end(), paramName.begin(), ::tolower);
        addCol(arg, dt.getMaxSize(), dt);
        size_t index = paramNameToIndex.size();
        paramNameToIndex[paramName] = index;
    }

    size_t getIndex(std::string paramName)  const
    {
        if (!containsParameter(paramName)) {
            ereport(ERROR, (errmsg("Function attempted to access parameter %s when it was not provided", paramName.c_str())));
        }
        std::transform(paramName.begin(), paramName.end(), paramName.begin(), ::tolower);
        return paramNameToIndex.at(paramName);
    }

    /** Bookkeepinp to make a parameter and its position in the block */
    std::map<std::string, size_t> paramNameToIndex;

public:

    friend class VerticaBlockSerializer;
};


/**
 * @brief A wrapper around a map from column to ParamReader.
 */
class PerColumnParamReader
{
private:
    typedef std::map<std::string, ParamReader> StringToParamReader;
    StringToParamReader paramReaderByCol;

public:

    /**
     * @brief Returns true if a ParamReader exists for the given column
     */
    bool containsColumn(std::string columnName) const {
        return paramReaderByCol.count(columnName) > 0;
    }

    /**
     * @brief Gets the names of all columns with column specific arguments
     *
     * @return a vector of column names
     */
    std::vector<std::string> getColumnNames() const {
        std::vector<std::string> ret;
        for (StringToParamReader::const_iterator i=paramReaderByCol.begin();
             i!=paramReaderByCol.end(); ++i)
        {
            ret.push_back(i->first);
        }
        return ret;
    }

    /**
     * @brief Gets the parameters of the given column
     *
     * @param the name of the column of interest
     *
     * @return the parameters of the given column
     */
    ParamReader& getColumnParamReader(const std::string &column) {
        return paramReaderByCol[column];
    }
    const ParamReader& getColumnParamReader(const std::string &column) const {
        return paramReaderByCol.at(column);
    }
};

/**
 * @brief
 *
 * This class provides an interface for reading the Used defined session parameters
 */

class SessionParamReaderMap
{
public:
    SessionParamReaderMap(){
        ParamReader libParamReader;
        ParamReader publicParamReader;
        nspToParamReaderMap["library"]=libParamReader;
        nspToParamReaderMap["public"]=publicParamReader;
    }
    /**
     * @brief Add a Session ParamReader for a namespace
     *
     * @param The namespace and the ParamReader
     */
    void addUDSessionParamReader(const std::string &nsp, ParamReader &sessionParamReader)
    {
        this->nspToParamReaderMap[nsp]=sessionParamReader;
    }

     /**
     * @brief Get a SessionParamReader for a namespace
     *
     * @param The namespace
     *
     * @return The SessionParamReader
     */
    void getUDSessionParamReader(const std::string &nsp, ParamReader& pReader)
    {
        if(nspToParamReaderMap.find(nsp)!=nspToParamReaderMap.end())
        {
            pReader=nspToParamReaderMap[nsp];
        }
    }

    /**
     * @brief Check if a SessionParamReader for the Namespace exists in the map
     *
     * @param  The namespace
     *
     * @return true if the SessionParamReader for the Namespace exists false otherwise
     */

    bool containsNamespace(const std::string nsp) const
    {
        return nspToParamReaderMap.count(nsp) > 0;
    }

     /**
     * @brief Get the SessionParamReader for the Namespace
     *
     * @param  The namespace
     *
     * @return The ParamReader
     */

    const ParamReader& getUDSessionParamReader(const std::string &nsp) const
    {
        if(nspToParamReaderMap.find(nsp)==nspToParamReaderMap.end())
        {    ereport(ERROR,
                    (errmsg("The sessionParamReader for namespace '%s' doesn't exist", nsp.c_str()),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }

        return nspToParamReaderMap.at(nsp);
    }
    /**
     * @brief Set the SessionParamReaderMap
     *
     * @param  The SessionParamReaderMap
     */
    void setUDSessionParamReaderMap(std::map<std::string, ParamReader> paramReaderMap){
        this->nspToParamReaderMap=paramReaderMap;
    }

    /**
     * @brief Gets the names of all namespaces
     *
     * @return a vector of column namespaces
     */
    std::vector<std::string> getNamespaces() const {
        std::vector<std::string> ret;
        for (nspToParamReader::const_iterator i=nspToParamReaderMap.begin();
             i!=nspToParamReaderMap.end(); ++i)
        {
            ret.push_back(i->first);
        }
        return ret;
    }

private:
    typedef std::map<std::string, ParamReader> nspToParamReader;
    nspToParamReader nspToParamReaderMap;

};


/**
 * \brief provides an iterator-based read interface over all input
 * data in a single partition. Automatically fetches data a block-at-a-time,
 * as needed.
 */
class PartitionReader : public BlockReader
{
public:
    PartitionReader(size_t narg):
        BlockReader(narg, 0, NULL), udx_object(NULL), dh(0) {}
    PartitionReader(size_t narg, EE::UserDefinedProcess *udx_object) :
        BlockReader(narg, 0, NULL), udx_object(udx_object), dh(0) {}

    virtual ~PartitionReader() {}

    bool next()
    {
        if (!count) return false; // Do nothing if we have no rows to read

        if (++index >= count)
            return readNextBlock();

        for (size_t i = 0; i < ncols; ++i)
            cols[i] += colstrides[i];

        return true;
    }

    bool hasMoreData()
    {
        return index < count;
    }

    /** Reads in the next block of data and positions cursor at the
    * beginning.
    *
    * @return false if there's no more input data
    */
    virtual bool readNextBlock();


protected:
    union {
        EE::UserDefinedProcess *udx_object;
        EE::UserDefinedTransform *udt;
        EE::UserDefinedAnalytic *udan;
    };
    EE::DataHolder *dh;
    vpos pstart; // the offset of the start of this partition within the DataHolder
    virtual void setupColsAndStrides();

    friend class VerticaBlockSerializer;
    friend class EE::UserDefinedProcess;
    friend class EE::UserDefinedTransform;
    friend class EE::UserDefinedAnalytic;
    friend struct FullPartition;

private:
    void setUDxObject(EE::UserDefinedProcess *_udx_object){
        udx_object=_udx_object;
    }
    void reset(){
        udx_object=NULL;
        dh=0;
        BlockReader::reset();
    }

};






/**
 * \brief Base class for vectorized writing to columns.  Efficiently writes
 * each element in a single column.
 */
class BasePartitionWriterColumn {
public:
    /** Returns the size of each element in the column  */
    int getStrideLength() { return stridelength; }

protected:
    BasePartitionWriterColumn(int _stridelength, char * _start, char * _end ) :
    stridelength(_stridelength), start(_start), end(_end), curr(_start) {}
    virtual ~BasePartitionWriterColumn() {}

    const int stridelength;  // Size of each element
    const char * start;      // Pointer to first element
    const char * end;        // Pointer to first-after-the-last-element
    char * curr;             // Pointer to the next element to write

    friend class PartitionWriterColumns;

    char * getAndAdvance() {
        if (curr >= end)
            ereport(ERROR,
                    (errmsg("Attempted to write past the end of a column"),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));

        char * result = curr;
        curr += stridelength;
        return result;
    }

    // Returns a read-only pointer to the memory
    const char * getPtr(size_t offset_from_start) {
        return start + (offset_from_start * stridelength);
    }

    // Returns the number of elements that have already been written to
    size_t getWrittenElementCount() {
        return (curr - start) / stridelength;
    }
private:
    // Disallow copies
    BasePartitionWriterColumn(const BasePartitionWriterColumn & other);
};

/**
 * PartitionWriterColumns for basic types.
 */
template<class T>
class PartitionWriterColumn : public BasePartitionWriterColumn {
public:
    // Appends the value to the column
    void append(const T & value) {
        *(reinterpret_cast<T *>(getAndAdvance())) = value;
    }

    // Appends a type-appropriate null value to the column
    void appendNull() {
        return append(null_value);
    }

    // Appends a block of elements to the column.  The source must be the
    //    same type and size as the column.  Check the stride length of the
    //    column before-hand to make sure
    void directCopy(T * src, size_t elementsToCopy, size_t elementSize) {
        if (static_cast<int>(elementSize) != stridelength)
            ereport(ERROR,
                    (errmsg("Source items are not the correct size for direct copies"),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));

        size_t copySize = elementSize * elementsToCopy;
        if (curr + copySize > end)
            ereport(ERROR,
                    (errmsg("Attempted to write past the end of a column"),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));

        memcpy(curr, src, copySize);
        curr += copySize;
    }

    // Returns a previously-written element
    T getVal(size_t offset_from_start) {
        return *(reinterpret_cast<const T *>(getPtr(offset_from_start)));
    }

protected:
    PartitionWriterColumn(int _stridelength, char * _start, char * _end, const T & _null_value ) :
       BasePartitionWriterColumn(_stridelength, _start, _end), null_value(_null_value)
       {}

    const T & null_value;

    friend class PartitionWriterColumns;
};

class PartitionWriterNumericColumn : public BasePartitionWriterColumn {
public:
    /**
     * Returns a reference to the next element to write.  Setting its members
     * will update the data in the PartitionWriter
     *
     * Note that the underlying object is re-used, so the object references
     * should not be retained between calls to append
     */
    VNumeric & append() {
        char * data = getAndAdvance();
        wrapper.words = reinterpret_cast<uint64 *>(data);
        return wrapper;
    }

    /**
     * Returns a reference to a previously written element.
     *
     * Note that the underlying object is re-used, so the object references
     * should not be retained between calls to append
     */
    const VNumeric & getRef(size_t offset_from_start) {
        const char * data = getPtr(offset_from_start);
        wrapper.words = const_cast<uint64 *>(reinterpret_cast<const uint64 *>(data));
        return wrapper;
    }

    // Appends a type-appropriate null value to the column
    void appendNull() {
        append().setNull();
    }

    /**
     * Elements must be in 2s compliment and in a compatible encoding;
     * check the value of getTypmod to ensure that encodings are identical
     */
    void directCopy(uint64 * src, size_t elementsToCopy, int srcTypMod, size_t elementSize) {
        if (static_cast<int>(elementSize) != stridelength)
            ereport(ERROR,
                    (errmsg("Source items are not the correct size for direct copies"),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));

        if (srcTypMod != wrapper.typmod)
            ereport(ERROR,
                    (errmsg("Source items are not the correct encoding for direct copies"),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));

        size_t copySize = elementSize * elementsToCopy;
        if (curr + copySize > end)
            ereport(ERROR,
                    (errmsg("Attempted to write past the end of a column"),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));

        memcpy(curr, src, copySize);
        curr += copySize;
    }

    // Returns the Numeric typmod of the encoded data
    int getTypmod() { return wrapper.typmod; };
protected:
    PartitionWriterNumericColumn(int _stridelength, char * _start, char * _end, const VNumeric & wrapperTemplate ) :
        BasePartitionWriterColumn(_stridelength, _start, _end), wrapper(0, wrapperTemplate.typmod) {}

    VNumeric wrapper;

    friend class PartitionWriterColumns;
};

class PartitionWriterStringColumn : public BasePartitionWriterColumn{
public:

    /**
     * Returns a reference to the next element to write.  Setting its members
     * will update the data in the PartitionWriter
     *
     * Note that the underlying object is re-used, so the object references
     * should not be retained between calls to append
     */
    VString & append() {
        char * data = getAndAdvance();
        wrapper.sv = reinterpret_cast<EE::StringValue *>(data);
        wrapper.setNull();

        return wrapper;
    }

    /**
     * Returns a reference to a previously written element.
     *
     * Note that the underlying object is re-used, so the object references
     * should not be retained between calls to append
     */
    const VString & getRef(size_t offset_from_start) {
        const char * data = getPtr(offset_from_start);
        wrapper.sv = const_cast<EE::StringValue *>(reinterpret_cast<const EE::StringValue *>(data));

        return wrapper;
    }

    // Appends a type-appropriate null value to the column
    void appendNull() {
        append(); // append automatically sets it to null
    }

    /**
     * Returns the maximum size that can be set in the objects returned
     * from append()
     */
    vsize getMaxSize() { return wrapper.max_size; }
protected:
    PartitionWriterStringColumn(int _stridelength, char * _start, char * _end, vsize maxlen ) :
        BasePartitionWriterColumn(_stridelength, _start, _end), wrapper(0, NULL, maxlen) {}

    VString wrapper;

    friend class PartitionWriterColumns;
};


/**
 * \brief PartitionWriterColumns allow column-by-column access to PartitionWriter
 * data.
 */
class PartitionWriterColumns
{
public:
    virtual ~PartitionWriterColumns()
            { clearCols(); }


    // Get a type-appropriate accessor for the column
    PartitionWriterColumn<vint> & getIntColumn(size_t idx) {return getCol<vint>(idx);}
    PartitionWriterColumn<vbool> & getBoolColumn(size_t idx) {return getCol<vbool>(idx);}
    PartitionWriterColumn<vfloat> & getFloatColumn(size_t idx) {return getCol<vfloat>(idx);}
    PartitionWriterStringColumn & getStringColumn(size_t idx) {return getCol_<PartitionWriterStringColumn>(idx);}
    PartitionWriterColumn<DateADT> & getDateColumn(size_t idx) {return getCol<DateADT>(idx);}
    PartitionWriterColumn<Interval> & getIntervalColumn(size_t idx) {return getCol<Interval>(idx);}
    PartitionWriterColumn<Timestamp> & getTimestampColumn(size_t idx) {return getCol<Timestamp>(idx);}
    PartitionWriterColumn<TimestampTz> & getTimestampTzColumn(size_t idx) {return getCol<TimestampTz>(idx);}
    PartitionWriterColumn<TimeADT> & getTimeColumn(size_t idx) {return getCol<TimeADT>(idx);}
    PartitionWriterColumn<TimeTzADT> & getTimeTzColumn(size_t idx) {return getCol<TimeTzADT>(idx);}
    PartitionWriterNumericColumn & getNumericColumn(size_t idx) {return getCol_<PartitionWriterNumericColumn>(idx);}

    int getMaxRows() { return maxRows; }

    int getRemaining();

    void commit();
    void rollback() { resetPointers(); }

protected:
    PartitionWriterColumns(PartitionWriter & _writer) :
            writer(_writer), maxRows (0)
            { populateCols(); }

    // Ensures we're asking for the right type and returns the column writer
    template <class T>
    T & getCol_(size_t idx) {
        T * result = dynamic_cast<T *>(cols[idx]);
        if (NULL == result)
            ereport(ERROR,
                (errmsg("Attempted to use a %s as a %s", typeid(*cols[idx]).name(), typeid(T).name()),
                 errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        return *result;
    }

    template <class T>
    PartitionWriterColumn<T> & getCol(size_t idx) { return getCol_<PartitionWriterColumn<T> >(idx); }

    // Regenerates the columns and their internal pointers
    void populateCols();

    // Resets the pointers for all cols to the values in the current block
    void resetPointers();

    // Frees all the memory associated with the columns
    void clearCols();

    PartitionWriter & writer;
    size_t maxRows;
    std::vector<BasePartitionWriterColumn *> cols;

    friend class PartitionWriter;
};


/**
 * \brief Provides an iterator-based write interface over output data
 * for a single partition. Automatically makes space a block-at-a-time, as
 * needed.
 */
class PartitionWriter : public VerticaBlock
{
public:
    PartitionWriter(size_t narg):
        VerticaBlock(narg, 0, NULL), udx_object(NULL), pstart(0) {}
    PartitionWriter(size_t narg, EE::UserDefinedProcess *udx_object) :
        VerticaBlock(narg, 0, NULL), udx_object(udx_object), pstart(0) {}

    virtual ~PartitionWriter() {}

    /**
     * Setter methods
     */
    void setInt(size_t idx, vint r)                {
        if(Int8OID != processBlockUserInfoVector[idx]){
              throwInCorrectUsageError(idx);
        }
        getColRefForWrite<vint>(idx) = r;
    }

    void setFloat(size_t idx, vfloat r)            {
        if(Float8OID != processBlockUserInfoVector[idx]){
             throwInCorrectUsageError(idx);
        }
        getColRefForWrite<vfloat>(idx) = r;
    }
    void setBool(size_t idx, vbool r)              {
        if(BoolOID != processBlockUserInfoVector[idx]){
            throwInCorrectUsageError(idx);
        }
        getColRefForWrite<vbool>(idx) = r;
    }
    void setDate(size_t idx, DateADT r)            {
        if(!checkTimeUserBlockInfo(idx)){
            throwInCorrectUsageError(idx);
        }
        getColRefForWrite<DateADT>(idx) = r;
    }
    void setInterval(size_t idx, Interval r)       {
        if(!(IntervalOID == processBlockUserInfoVector[idx] || IntervalYMOID == processBlockUserInfoVector[idx])){
            throwInCorrectUsageError(idx);
        }
        getColRefForWrite<Interval>(idx) = r;
    }
    void setTimestamp(size_t idx, Timestamp r)     {
        if(!checkTimeUserBlockInfo(idx)){
            throwInCorrectUsageError(idx);
        }
        getColRefForWrite<Timestamp>(idx) = r;
    }
    void setTimestampTz(size_t idx, TimestampTz r) {
        if(!checkTimeUserBlockInfo(idx)){
           throwInCorrectUsageError(idx);
        }
        getColRefForWrite<TimestampTz>(idx) = r;
    }
    void setTime(size_t idx, TimeADT r)            {
        if(!checkTimeUserBlockInfo(idx)){
           throwInCorrectUsageError(idx);
        }
        getColRefForWrite<TimeADT>(idx) = r;
    }
    void setTimeTz(size_t idx, TimeTzADT r)        {
        if(!checkTimeUserBlockInfo(idx)){
           throwInCorrectUsageError(idx);
        }
        getColRefForWrite<TimeTzADT>(idx) = r;
    }

    VNumeric &getNumericRef(size_t idx)
    {
        if(NumericOID != processBlockUserInfoVector[idx]){
           throwInCorrectUsageError(idx);
        }
        uint64 *words = getColPtrForWrite<uint64>(idx);
        vnWrappers[idx].words = words;
        return vnWrappers[idx];
    }
    VString &getStringRef(size_t idx)
    {
        if(!checkStringUserBlockInfo(idx)){
           throwInCorrectUsageError(idx);
        }
        EE::StringValue *sv = getColPtrForWrite<EE::StringValue>(idx);
        if (svWrappers[idx].sv != sv) {
            svWrappers[idx].sv = sv;
            svWrappers[idx].setNull(); // Initialize to the null string, for safety
        }
        return svWrappers[idx];
    }
    VString &getStringRefNoClear(size_t idx)  // Don't initialize the string first; unsafe unless we're pointing at a pre-written block
    {
        if(!checkStringUserBlockInfo(idx)){
            throwInCorrectUsageError(idx);
        }
        EE::StringValue *sv = getColPtrForWrite<EE::StringValue>(idx);
        if (svWrappers[idx].sv != sv) {
            svWrappers[idx].sv = sv;
        }
        return svWrappers[idx];
    }

    void throwInCorrectUsageError(size_t idx){
         ereport(ERROR,
                    (errmsg("Incorrect use of setter in processPartition for [%zu] column", idx),
                    errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
    }

    void validateColumn(size_t idx) {
        // Validate any sized data types
        // Note that if you add any additional validations here, you need to
        //   add them to PartitionWriterColumns::commit() also)
        const VerticaType &t = typeMetaData.getColumnType(idx);
        if (t.isStringOid(processBlockUserInfoVector[idx])) {
            validateStringColumn(idx, getStringRefNoClear(idx), t.getStringLength(false));
        } else if (t.isBool()) {
            vbool val = getColRef<vbool>(idx);
            if (val != vbool_true && val != vbool_false && val != vbool_null) {
                ereport(ERROR,
                        (errmsg("UDx set BOOLEAN column %zu to non-boolean value %d",
                                idx, val),
                         errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
            }
        }
    }

    void *getVoidPtr(size_t idx) { return reinterpret_cast<void *>(cols.at(idx)); }

    /**
     * Copies a column from the input reader to the output writer. The data
     * types and sizes of the source and destination columns must match exactly.
     *
     * @param dstIdx The destination column index (in the output writer)
     *
     * @param input_reader The input reader from which to copy a column
     *
     * @param srcIdx The source column index (in the input reader)
     */
    void copyFromInput(size_t dstIdx, PartitionReader &input_reader, size_t srcIdx)
    {
        const char *srcBytes = input_reader.getColPtr<char>(srcIdx);
        char *dstBytes = getColPtrForWrite<char>(dstIdx);

        // Get number of bytes for columns
        const VerticaType &dt = typeMetaData.getColumnType(dstIdx);
        const VerticaType &st = input_reader.getTypeMetaData().getColumnType(srcIdx);
        int dstsize = dt.getMaxSize();
        int srcsize = st.getMaxSize();
        if (dstsize < srcsize)
        {
            ereport(ERROR,
                    (errmsg("The types/sizes of source column (index %zu, length %d) "
                            "and destination column (index %zu, length %d) "
                            "do not match",
                            srcIdx, st.getMaxSize(),
                            dstIdx, dt.getMaxSize()),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }

        if (dt.isStringType())
            getStringRef(dstIdx).copy(input_reader.getStringPtr(srcIdx));
        else
            VMEMCPY(dstBytes, srcBytes, dt.getMaxSize());
    }

    bool next()
    {
        if (!count) return false; // do nothing if there's no more rows to write to

        // validate each column
        for (size_t i=0; i < ncols; ++i)
        {
            // check backwards (in case of later overwrites)
            size_t idx = ncols - 1 - i;
            validateColumn(idx);
        }

        if (++index >= count)
        {
            return getWriteableBlock();
        }

         for (size_t i = 0; i < ncols; ++i)
             cols[i] += colstrides[i];

        return true;
    }


    PartitionWriterColumns getColumns() { return PartitionWriterColumns(*this); }

    /**
     * @brief  Set the idx'th argument to null

     * @param idx The column number in the row to set to null
     */
    void setNull(size_t idx)
    {
        VIAssert(idx < ncols );
        const SizedColumnTypes &inTypes = getTypeMetaData();
        const VerticaType &t = inTypes.getColumnType(idx);
        BaseDataOID oid = t.getTypeOid();
        switch (oid)
        {
        case Int8OID:
            setInt(idx, vint_null);
            break;
        case Float8OID:
            setFloat(idx, vfloat_null);
            break;
        case BoolOID:
            setBool(idx, vbool_null);
            break;
        case DateOID:
            setDate(idx, vint_null);
            break;
        case IntervalYMOID:
        case IntervalOID:
            setInterval(idx, vint_null);
            break;
        case TimestampOID:
            setTimestamp(idx, vint_null);
            break;
        case TimestampTzOID:
            setTimestampTz(idx, vint_null);
            break;
        case TimeOID:
            setTime(idx, vint_null);
            break;
        case TimeTzOID:
            setTimeTz(idx, vint_null);
            break;
        case BinaryOID:
        case VarbinaryOID:
        case LongVarbinaryOID:
        case CharOID:
        case VarcharOID:
        case LongVarcharOID:
            getStringRef(idx).setNull();
            break;
        case NumericOID:
            getNumericRef(idx).setNull();
            break;
        default:
            ereport(ERROR,
                    (errmsg("Unknown data type"),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }
    }

    /**  Gets a writeable block of data and positions cursor at the beginning.
    */
    virtual bool getWriteableBlock();


protected:
    // Opaque pointer to internal server state
    union {
        EE::UserDefinedProcess *udx_object;
        EE::UserDefinedTransform *udt;
        EE::UserDefinedAnalytic *udan;
    };
    int pstart;

    // Used by the PartitionWriterColumns.commit())
    bool commitPrefilledRows(int nRows)
    {
        // All data must be pre-validated before this is called
        index += nRows;
        return getWriteableBlock();

        return true;
    }

    friend class EE::UserDefinedProcess;
    friend class EE::UserDefinedTransform;
    friend class EE::Loader::UserDefinedLoad;
    friend class EE::UserDefinedAnalytic;
    friend class PartitionWriterColumns;

    void reset(){
        udx_object=NULL;
        pstart=0;
        VerticaBlock::reset();
    }

private:
    void setUDxObject(EE::UserDefinedProcess *_udx_object){
        udx_object=_udx_object;
    }
};


/**
 * \brief Provides an iterator-based write interface over output data
 * for a stream of blocks. Automatically makes space a block-at-a-time, as
 * needed.
 */
class StreamWriter : public PartitionWriter
{
public:
    StreamWriter(size_t narg, void *udl) :
        PartitionWriter(narg, NULL), parent(udl), cumulative_rows(0) {}

    /**  Gets a writeable block of data and positions cursor at the beginning.
    */
    virtual bool getWriteableBlock();

    uint64 getTotalRowCount() const { return cumulative_rows + index; }

protected:
    // Opaque pointer to internal server state
    void *parent;

    // Accumulator of rows already emitted to upstream
    uint64 cumulative_rows;

    friend class EE::Loader::UserDefinedLoad;
};


/**
 * \brief Provides an iterator-based read interface over all the
 * partition_by keys, order_by keys, and function arguments in a partition.
 */
class AnalyticPartitionReader : public PartitionReader
{
public:
    AnalyticPartitionReader(size_t narg, EE::UserDefinedProcess *udx_object) :
        PartitionReader(narg, udx_object)
        { }

    bool isNewOrderByKey()
    {
        if (!count) return false; // no rows to read - default not a new order by
        return newOrderMarkers[index];
    }

    virtual bool readNextBlock();

protected:
    virtual void setupColsAndStrides();

    std::vector<bool> newOrderMarkers;

    friend class EE::UserDefinedAnalytic;
    friend class EE::UserDefinedProcess;
    friend class ::AnalyticPartitionReaderHelper;
};


/**
 * \brief Partition writer for all input data in a single partition.
 * It automatically makes space as needed.
 */
class AnalyticPartitionWriter : public PartitionWriter
{
public:
    AnalyticPartitionWriter(size_t narg, EE::UserDefinedProcess *udx_object) :
        PartitionWriter(narg, udx_object) { }

    virtual ~AnalyticPartitionWriter() {}

    //  Gets a writeable block of data and positions cursor at the beginning.
    virtual bool getWriteableBlock();
};

/////////////////////////////// Multi-Phase UDTs ////////////////////////////////
/**
 * \brief Interface to provide compile-time information for a single phase of a
 * multi-phase user-defined transform function.
 *
 * Note that even though this
 * class shares some methods with TransformFunctionFactory, it is explicitly
 * not a subclass (because it is incorrect to implement a getPrototype() method
 * for this class).
 */
class TransformFunctionPhase
{
public:

    TransformFunctionPhase() : prepass(false) {}

    virtual ~TransformFunctionPhase() {}

    /**
     * Function to tell Vertica what the return types (and length/precision if
     * necessary) and partition-by, order-by of this phase are.
     *
     * For CHAR/VARCHAR types, specify the max length,
     *
     * For NUMERIC types, specify the precision and scale.
     *
     * For Time/Timestamp types (with or without time zone),
     * specify the precision, -1 means unspecified/don't care
     *
     * For IntervalYM/IntervalDS types, specify the precision and range
     *
     * For all other types, no length/precision specification needed
     *
     * @param argTypes Provides the data types of arguments that this phase
     *                 was called with, along with partition and order
     *                 information. This may be used to modify the return
     *                 types accordingly.
     *
     * @param returnTypes User code must fill in the names and data types
     *                    returned by this phase, along with the partition-by
     *                    and order-by column information (if any).
     */
    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes) = 0;

    /**
     * Called when Vertica needs a new TransformFunction object to process
     * this phase of a multi-phase UDT.
     *
     * @return a TransformFunction object which implements the UDx API described
     * by this metadata.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     *
     * @note More than one object may be instantiated per query.
     *
     */
    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface) = 0;

    /// Indicates that this phase is a pre-pass (i.e., runs before any data movement)
    void setPrepass() { prepass = true; }

    bool isPrepass() { return prepass; }

private:
    bool prepass;
};

/**
 * @brief Interface for declaring parameters and return types for, and
 * instantiating, an associated multi-phase transform function. This class
 * is similar to, but not a subclass of, TransformFunctionFactory.
 *
 * A multi-phase transform function is made up of TransformFunctionPhase
 * instances. For each phase of your transform, extend this class (not
 * TransformFunction).  In getPhases(), return a vector of instances of
 * those classes.  Semantically, getPhases() replaces 
 * TransformFunctionFactory.createTransformFunction().
 */
class MultiPhaseTransformFunctionFactory : public UDXFactory
{
public:
    virtual ~MultiPhaseTransformFunctionFactory() {}

    virtual void getPrototype(ServerInterface &srvInterface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType)
    {
        argTypes.addAny();
        returnType.addAny();
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnType)
    {
        std::vector<TransformFunctionPhase *> phases;
        getPhases(srvInterface, phases);

        SizedColumnTypes phase_itypes = argTypes;
        for (uint i=0; i<phases.size(); i++)
        {
            SizedColumnTypes phase_rtypes;
            if (!phases[i])
                ereport(ERROR, (errmsg("Phase %u cannot be NULL", i)));
            phases[i]->getReturnType(srvInterface, phase_itypes, phase_rtypes);
            phase_itypes = phase_rtypes; // output of this phase is input to next phase
            returnType = phase_rtypes; // update the last phase's rtypes
        }
    }

    /** 
     * Called when Vertica needs a new TransformFunctionPhase pipeline
     * to process a multi-phase UDTF function call. Vertica invokes the
     * phases in order; collectively they behave like a TransformFunction.
     *
     * @param srvInterface a ServerInterface object used to communicate with Ver
tica
     * @param phases output TransformFunctionPhase objects
     *
     * @note More than one object may be instantiated per query.
     */
    virtual void getPhases(ServerInterface &srvInterface, std::vector<TransformFunctionPhase *> &phases) = 0;

protected:
    /**
     * @return the object type internally used by Vertica
     */
    virtual UDXType getUDXFactoryType() { return MULTI_TRANSFORM; }

};

/////////////////////////////// User-Defined Loads ////////////////////////////////

class UDLFactory : public UDXFactory
{
public:
    virtual ~UDLFactory() {}

    /**
     * Provides the argument and return types of the UDL.
     * UDL's take no input tuples; as such, their prototype is empty.
     */
    void getPrototype(ServerInterface &srvInterface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType) {}

    /**
     * Not used in this form
     */
    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnType) {}

    virtual void getPerInstanceResources(ServerInterface &srvInterface, VResources &res) {
        // UDLs work with buffers of memory,
        // so almost certainly want a nonzero memory allocation.
        // User can always override this.
        res.scratchMemory = 10LL * 1024LL * 1024LL;
    }

    virtual void getPerInstanceResources(ServerInterface &srvInterface, VResources &res,
                                         const SizedColumnTypes &inputTypes) {

        getPerInstanceResources(srvInterface, res);
    }
};


/**
 * \brief Provides UDxs with access to the Vertica server.  
 * 
 * Vertica passes a ServerInterface instance to the setup(), destroy(), and 
 * main processing functions of each UDx.  Key methods for UDx use include
 * log() (to write to the Vertica log), getParamReader(), 
 * setParamReader(), getSessionParamReader(), and setSessionParamReader().
 */
class ServerInterface
{
public:
    // Prototype for function to log messages to the Vertica log
    typedef void (*LoggingFunc)(ServerInterface *, const char *fmt, va_list ap);

    /**
     * Create a new ServerInterface.
     *
     * @note that Only Vertica Server should use this method. It is not
     * guaranteed to stay the same between releases.
     */
    ServerInterface(VTAllocator *allocator,
                    FileManager *fileManager,
                    LoggingFunc func,
                    const std::string &sqlName,
                    const ParamReader &paramReader,
                    vint udxDebugLogLevel = 0)
        : udxDebugLogLevel(udxDebugLogLevel),
          allocator(allocator),
          fileManager(fileManager),
          vlogPtr(func),
          sqlName(sqlName),
          paramReader(paramReader) {}
   // Constructor overload without file manager
   ServerInterface(VTAllocator *allocator,
                   LoggingFunc func,
                   const std::string &sqlName,
                   const ParamReader &paramReader,
                   vint udxDebugLogLevel = 0)
        : udxDebugLogLevel(udxDebugLogLevel),
          allocator(allocator),
          vlogPtr(func),
          sqlName(sqlName),
          paramReader(paramReader) {}

    // Used for the VSimulator
    ServerInterface(VTAllocator *allocator,
                    LoggingFunc func,
                    const std::string &sqlName,
                    vint udxDebugLogLevel = 0)
        : udxDebugLogLevel(udxDebugLogLevel),
          allocator(allocator),
          vlogPtr(func),
          sqlName(sqlName) {}

    virtual ~ServerInterface() {}

    /**
     * Returns the ParamReader that allows accessing parameter values using
     * their names
     */
    ParamReader getParamReader() const { return paramReader; }

    /**
    * Returns the FileManager that allows interfaction with Catalog and storage system.
    */
    //FileManager *getFileManager() {return fileManager;}

    /**
     * Writes a message to a log file stored in the UDxLogs directory of 
     * the database's catalog directory. The SQL name of the UDx is added 
     * to the log message, along with the string [UserMessage] to mark the 
     * entry as a message added by a call to this method. 
     *
     * @param format a printf-style format string specifying the log message
     * format.
     **/
    void log(const char *format, ...) __attribute__((format(printf, 2, 3)))
    {
        std::string formatWithName(sqlName);
        formatWithName.append(" - ");
        formatWithName.append(format);
        format = formatWithName.c_str();
        va_list ap;
        va_start(ap, format);
        vlog(format, ap);
        va_end(ap);
    }

    /**
     * Get the file system corresponding to the provided path.
     */
    virtual const UDFileSystem *getFileSystem(const char *path) = 0;

    /**
     * Write a message to the vertica.log system log.
     *
     * @param format a printf style format string specifying the log message
     * format.
     *
     * @param ap va_list for variable arguments
     **/
    void vlog(const char *format, va_list ap) { vlogPtr(this, format, ap); }

    /**
     * The level of UDx debug logging which is turned on as a UDXDebugLevel
     *  set of enumeration values. Used so UDxs may forgo generating debug
     *  log messages if debug logging is off.
     */
    // XXX jfraumeni TODO XXX Migrate to Alexi's communication storage, when ready XXX TODO XXX
    vint udxDebugLogLevel;

    /**
     * Memory source which is managed and freed by the server.
     */
    VTAllocator *allocator;

    /**
    * File manager of the session context
    */
    FileManager *fileManager;

    /**
     * Set the paramReader of this ServerInterface when delayed creation is required
     * Used by the code when delayed creation of the parameters is needed
     * Users should not call this function
     */
    void setParamReader(const ParamReader &paramReader) {
        this->paramReader = paramReader;
    }

    /**
     * Set the sessionParamReader
     */
    void setSessionParamReader(const ParamReader &sessionParamReader)
    {
        this->sessionParamReader = sessionParamReader;
    }

    /**
     * Get the sessionParamReader
     */
    ParamReader getSessionParamReader() const { return sessionParamReader; }

    /**
     * @return the name of the vertica node on which this code is executed.
     */
    const std::string &getCurrentNodeName() const { return nodeName; }

    /**
     * @return the locale of the current session.
     */
    const std::string &getLocale() const          { return locale; }

    /**
     * @return a list of paths to rhe dependencies
     */
    const std::map<std::string, std::string> &getDependencies() const { return dependencies; }

     /**
     * @return set the paths to the dependencies
     */
    void setDependencies(const std::map<std::string, std::string> dependencies) {
        this->dependencies=dependencies;
    }

    /**
     * Set the UDSessionParamReaderMap
     */
    void setUDSessionParameterMap(const SessionParamReaderMap &udSessionParams)
    {
        this->udSessionParamReaderMap = udSessionParams;
    }

    /**
     * Get the UDSessionParamReaderMap
     */
    SessionParamReaderMap getUDSessionParamReaderMap() const
    {
        return udSessionParamReaderMap;
    }

    /**
     * Get the UDSessionParamReader for a namespace
     */
    ParamReader getUDSessionParamReader(std::string nsp)
    {
        ParamReader pReader;
        udSessionParamReaderMap.getUDSessionParamReader(nsp, pReader);
        return pReader;
    }

    /**
     * Get the UDSessionParamReader for the default namespace
     */
    ParamReader getUDSessionParamReader() const
    {
        return udSessionParamReaderMap.getUDSessionParamReader("library");
    }

    void setFunctionOid(Oid FuncOid)
    {
        funcOid = FuncOid;
    }

    Oid getFunctionOid() const
    {
        return funcOid;
    }

    void setLibraryOid(Oid libOid_)
    {
        libOid = libOid_;
    }

    void setPublicOid(Oid publicOid_)
    {
        publicOid = publicOid_;
    }


    /**
     * Structure to describe the name and properties of a table or projection,
     *   It is to be used in the methods below.
     */
    struct RelationDescription
    {
        /** Database name */
        std::string databaseName;
        /** Schema name; on lookup if empty the search path will be consulted */
        std::string schemaName;
        /** Table name, projection basename, or projection name */
        std::string relationName;
        /** Relation OID, for lookups if this is nonzero it takes precedence */
        Oid relationOid;
        /** Column names */
        std::vector<std::string> colNames;
        /** Column types */
        std::vector<VerticaType> colTypes;
        /**
         *  For projections, mapping to table column number (0-based); negative
         *   numbers would indicate things that are not present in the table
         */
        std::vector<int> tabColMapping;
        /** For projections, indictor if encoding is run-optimized */
        std::vector<bool> colRLE;
        /** For projections, number of columns in sort order, as a prefix */
        int nSortedCols;
        /** For projections, columns involved in segmentation */
        std::vector<int> segCols;
        /** For buddies, segmentation offset */
        int segOffset;
        /** For projections, hosting nodes */
        std::set<Oid> presentOnNodes;
        /** For projections, hosting node names */
        std::set<std::string> presentOnNodeNames;
        /** For tables / projections, columns involved in partitioning */
        std::vector<int> partitionCols;
        /** For tables / projections, columns involved in a primary key */
        std::vector<int> pkCols;

        /** Derivation from a base object (table/projection) */
        Oid baseRelation;
        /** Columns in base that participate in derivation */
        std::vector<int> baseAttrIdx;
        /** Is aggregate, index, etc */
        enum RelDerivationType
        {
            RDT_NONE,
            RDT_PROJECTION,
            RDT_AGGREGATE,
            RDT_TOPK,
            RDT_UDT,
            RDT_INDEX,
            RDT_FLEX_KEYS,
        } relDerivationType;
        /** Derivation function, for UDT proj, etc. */
        std::vector<Oid> derivationFunctions;
        /** Further description of derivation */
        std::string additionalDerivationInfo;

        /** Is this a system object */
        bool isSystem;
        /** Is this a flex object */
        bool isFlex;
        /** Is this up to date / active */
        bool isUpToDate;
        /** Is for a temp table? */
        bool isTemp;
        /** Is for external table? */
        bool isExternal;

        RelationDescription() :
            relationOid(0), nSortedCols(0), segOffset(-1),
            baseRelation(0), relDerivationType(RDT_NONE),
            isSystem(false), isFlex(false), isUpToDate(false),
            isTemp(false), isExternal(false) {}
    };

    struct BlobIdentifier {
        /** Name of the blob */
        std::string name;

        /**
         * Namespace of this blob
         */
        enum Namespace
        {
            // This blob will be available to all UDx's regardless of library
            NSP_PUBLIC,
            // This blob will only be available to UDx's from this library
            NSP_LIBRARY
        } nsp;

        BlobIdentifier(std::string name_) : name(name_), nsp(NSP_LIBRARY) {}
        BlobIdentifier(std::string name_, Namespace nsp_) : name(name_), nsp(nsp_) {}
    };

    struct BlobDescription {
        /**
         * Name of the blob
         */
        std::string name;

        /**
         * Namespace of this blob
         */
        BlobIdentifier::Namespace nsp;

        /**
         * Whether or not the blob will be mutable. If false, the blob will be
         * read-only on future accesses.
         */
        bool isMutable;

        /**
         * Size of the blob in bytes
         */
        vpos size;

        /**
         * Number of chunks
         */
        int nChunks;

        /**
         * The sizes of each chunk
         */
        std::vector<vpos> chunkSizes;

        /**
         * Number of bytes the blob reserved from the resource manager
         */
        vpos reservedMemory;

        /**
         * How the blob is stored
         */
        enum StorageType {
            ST_IN_MEMORY,
            ST_SPILL_TO_DISK,
            ST_DISK_BACKED,
        } storageType;

        /**
         * How the blob is distributed across the cluster. Segmented implies that
         * the blob is split across the nodes it's created on, whereas unsegmented
         * means that the blob is replicated across the nodes it's created on
         */
        enum DistributionType {
            DT_SEGMENTED,
            DT_UNSEGMENTED,
        } distributionType;
    };

    /** List tables given properties (schema, name, etc.) */
    virtual void listTables(const RelationDescription &lookup,
                            std::vector<Oid> &tables,
                            bool errorIfNotFound = false) = 0;
    /** List projections given projection name */
    virtual void listProjections(const RelationDescription &lookup,
                                 std::vector<Oid> &projections,
                                 bool errorIfNotFound = false) = 0;
    /** List projections given base table properties (schema + name, OID, etc.) */
    virtual void listTableProjections(const RelationDescription &baseTable,
                                      std::vector<Oid> &projections,
                                      bool errorIfNotFound = false) = 0;
    /** List out derived tables, such as text indexes */
    virtual void listDerivedTables(const RelationDescription &baseTable,
                                   std::vector<Oid> &tables,
                                   bool errorIfNotFound = false) = 0;
    /** Given a table OID or name (to which search path will be applied), find the rest of the details */
    virtual bool describeTable(RelationDescription &baseTable,
                               bool errorIfNotFound = true) = 0;
    /** Given a projection OID or name, find the rest of the details */
    virtual bool describeProjection(RelationDescription &proj,
                                    bool errorIfNotFound = true) = 0;

    /**
     * Structure to describe the name and properties of a function,
     *   It is to be used in the methods below.
     */
    struct FunctionDescription
    {
        /** Database name */
        std::string databaseName;
        /** Schema name; on lookup if empty the search path will be consulted */
        std::string schemaName;
        /** Table name, projection basename, or projection name */
        std::string functionName;
        /** Function OID, for lookups if this is nonzero it takes precedence */
        Oid functionOid;

        enum FunctionType
        {
            FT_SCALAR,
            FT_AGGREGATE,
            FT_ANALYTIC,
            FT_EXTPROC,
            FT_UDSF,
            FT_UDAGG,
            FT_UDAN,
            FT_UDT,
            FT_UDT_MULTI,
            FT_UDT_CURSOR,
            FT_UDPARSE,
            FT_UDFILTER,
            FT_UDSOURCE,
        } functionType;

        std::vector<std::string> argNames;
        std::vector<VerticaType> argTypes;
        std::vector<std::string> returnNames;
        std::vector<VerticaType> returnTypes;

        std::string source;
        std::string binary;

        Oid libraryOid;
        std::string libraryName;
        std::string libraryFileName;
        std::string language;

        /** Is internal / built in function */
        bool isInternal;
        /** Is fenced */
        bool isFenced;
        /** Is strict */
        bool isStrict;
        /** Volatility */
        char volatility;

        FunctionDescription() :
            functionOid (0), functionType(FT_SCALAR), libraryOid(0),
            isInternal(false), isFenced(false), isStrict(false), volatility('i') {}
    };

    /** Describe a function */
    virtual bool describeFunction(FunctionDescription &func,
                                  bool errorIfNotFound = true) = 0;


    /**
     * Structure to describe the name and properties of a type,
     *   It is to be used in the methods below.
     */
    struct TypeDescription
    {
        /** Database name */
        std::string databaseName;
        /** Schema name; on lookup if empty the search path will be consulted */
        std::string schemaName;
        /** Table name, projection basename, or projection name */
        std::string typeName;
        /** Function OID, for lookups if this is nonzero it takes precedence */
        Oid typeOid;

        /** Base type */
        VerticaType baseType;
        /** Is internal? */
        bool isInternal;

        TypeDescription() : typeOid(0), baseType(0, 0), isInternal(false) {}
    };

    /** Describe a type */
    virtual bool describeType(TypeDescription &type,
                              bool errorIfNotFound = true) = 0;

    /** Given a blob name and namespace (to which search path will be applied),
     *  find the rest of the details */
    virtual bool describeBlob(const BlobIdentifier &blobId,
                              BlobDescription &blobDescription,
                              bool errorIfNotFound = true) = 0;

    /** List the blobs in the given namespace **/
    virtual std::vector<BlobDescription> listBlobs(BlobIdentifier::Namespace nsp = BlobIdentifier::NSP_LIBRARY) = 0;

protected:
    /**
     * Callback for logging, set by the server
     */
    LoggingFunc vlogPtr;

    /**
     * Store the name for error logging
     */
    std::string sqlName;

    /**
     * Store the name of the current node
     */
    std::string nodeName;

    /**
     * A reader for paremeters that have been toknized using the following format:
     * key1=val1,key2=val2,key3=val3. Has accessor methods like BlockReader to be
     * able to access parameters of different data types
     */
    ParamReader paramReader;

    /**
     * A map for session paremeters
     * UDx might specify what session parameters it wants in its "manifest"
     * Server will try to provide, if it agrees with that request
     */
    ParamReader sessionParamReader;

    /**
     * A map for session UDParameters that were set in the session by a user or by a UDx
     *
     */
    SessionParamReaderMap udSessionParamReaderMap;

    /**
     * The locale of the current session
     */
    std::string locale;

    /**
     * The list of all dependencies
     */
    std::map<std::string, std::string>dependencies;

    /**
     * The Oid of the function
     */
    Oid funcOid;

    /**
     * The Oid of the library
     */
    Oid libOid;

    /**
     * The Oid for public user-defined session parameters and blobs
     */
    Oid publicOid;

    friend class EE::UserDefinedAnalytic;
    friend class EE::UserDefinedTransform;
    friend class EE::UserDefinedAggregate;
    friend class ::UdfSupport;
};

/**
 * @brief Provides write access to a set of named parameters.  This class 
 * extends ParamReader to add type-specific methods to write values.
 */
class ParamWriter : public ParamReader  // All writers should be readable as well
{
public:
    ParamWriter(VTAllocator *allocator = NULL) :
        ParamReader(0), allocator(allocator)
    {}

    /**
     * Setter methods
     */

    /// @brief Adds an INTEGER value to the output row.
    /// @param r The INTEGER value to insert into the output row.
    void setInt(std::string fieldName, vint r)                { addField<vint>(fieldName, r); typeMetaData.addInt(fieldName); }
    /// @brief Adds a FLOAT value to the output row.
    /// @param r The FLOAT value to insert into the output row.
    void setFloat(std::string fieldName, vfloat r)            { addField<vfloat>(fieldName, r); typeMetaData.addFloat(fieldName); }
    /// @brief Adds a BOOLEAN value to the output row.
    /// @param r The BOOLEAN value to insert into the output row.
    void setBool(std::string fieldName, vbool r)              { addField<vbool>(fieldName, r); typeMetaData.addBool(fieldName); }
    /// @brief Adds a BOOLEAN value to the output row.
    /// @param r The BOOLEAN value to insert into the output row.
    void setDate(std::string fieldName, DateADT r)            { addField<DateADT>(fieldName, r); typeMetaData.addDate(fieldName); }
    /// @brief Adds an INTERVAL value to the output row.
    /// @param r The INTERVAL value to insert into the output row.
    void setInterval(std::string fieldName, Interval r, int32 precision, int32 range) { addField<Interval>(fieldName, r); typeMetaData.addInterval(precision, range, fieldName); }
    /// @brief Adds an INTERVAL YEAR TO MONTH value to the output row.
    /// @param r The INTERVAL YEAR TO MONTH value to insert into the output row.
    void setIntervalYM(std::string fieldName, IntervalYM r, int32 range)              { addField<IntervalYM>(fieldName, r); typeMetaData.addIntervalYM(range, fieldName); }
    /// @brief Adds a TIMESTAMP value to the output row.
    /// @param r The TIMESTAMP value to insert into the output row.
    void setTimestamp(std::string fieldName, Timestamp r, int32 precision)            { addField<Timestamp>(fieldName, r); typeMetaData.addTimestamp(precision, fieldName); }
    /// @brief Adds a TIMESTAMP WITH TIMEZONE value to the output row.
    /// @param r The TIMESTAMP WITH TIMEZONE value to insert into the output row.
    void setTimestampTz(std::string fieldName, TimestampTz r, int32 precision)        { addField<TimestampTz>(fieldName, r); typeMetaData.addTimestampTz(precision, fieldName); }
    /// @brief Adds a TIME value to the output row.
    /// @param r The TIME value to insert into the output row.
    void setTime(std::string fieldName, TimeADT r, int32 precision)                   { addField<TimeADT>(fieldName, r); typeMetaData.addTime(precision, fieldName); }
    /// @brief Adds a TIME WITH TIMEZONE value to the output row.
    /// @param r The TIME WITH TIMEZONE value to insert into the output row.
    void setTimeTz(std::string fieldName, TimeTzADT r, int32 precision)               { addField<TimeTzADT>(fieldName, r); typeMetaData.addTimeTz(precision, fieldName); }

    /// @brief Allocate a new VNumeric object to use as output.
    ///
    /// @return A new VNumeric object to hold output. This object
    ///         automatically added to the output row.
    VNumeric &getNumericRef(std::string fieldName)
    {
        std::transform(fieldName.begin(), fieldName.end(), fieldName.begin(), ::tolower);
        if (paramNameToIndex.find(fieldName) == paramNameToIndex.end()) {
            VNumeric vn(0, 0, 0);
            addFieldNoWrite(fieldName, vn);
            typeMetaData.addNumeric(0, 0, fieldName);  // Fake data for now, since we don't actually know
        }

        size_t idx = paramNameToIndex.at(fieldName);
        uint64 *words = getColPtrForWrite<uint64>(idx);
        vnWrappers[idx].words = words;
        return vnWrappers[idx];
    }

    /// @brief Allocates a new VString object to use as output.
    ///
    /// @return A new VString object to hold output. This object
    ///         automatically added to the output row.
    VString &getStringRef(std::string fieldName)
    {
        std::transform(fieldName.begin(), fieldName.end(), fieldName.begin(), ::tolower);
        if (paramNameToIndex.find(fieldName) == paramNameToIndex.end()) {
            VString str(0);
            addFieldNoWrite(fieldName, str);
            typeMetaData.addVarchar(MAX_STRING_LENGTH, fieldName);  // Fake data for now, since we don't actually know
        }

        size_t idx = paramNameToIndex.at(fieldName);
        EE::StringValue *sv = getColPtrForWrite<EE::StringValue>(idx);
        if (svWrappers.at(idx).sv != sv) {
            svWrappers[idx].sv = sv;
        }
        return svWrappers[idx];
    }

    /// @brief Allocates a new VString object to use as output.
    ///        Sets it to be a 32mb LONG type by default.
    ///
    /// @return A new VString object to hold output. This object
    ///         automatically added to the output row.
    VString &getLongStringRef(std::string fieldName)
    {
        std::transform(fieldName.begin(), fieldName.end(), fieldName.begin(), ::tolower);
        if (paramNameToIndex.find(fieldName) == paramNameToIndex.end()) {
            VString str(0);
            addFieldNoWrite(fieldName, str, MAX_LONG_STRING_LENGTH);
            typeMetaData.addLongVarchar(MAX_LONG_STRING_LENGTH, fieldName);  // Fake data for now, since we don't actually know
        }

        size_t idx = paramNameToIndex.at(fieldName);
        EE::StringValue *sv = getColPtrForWrite<EE::StringValue>(idx);
        if (svWrappers.at(idx).sv != sv) {
            svWrappers[idx].sv = sv;
        }
        return svWrappers[idx];
    }

    void clearParameter(std::string fieldName) {
        vint value = 1;
        setInt(fieldName, value);
    }

    /// @brief Sets the allocator to be used to allocate variable size param values
    ///        such as VString. Given allocator live longer than this ParamWriter.
    ///
    /// @param allocator Used to allocate param values.
    void setAllocator(VTAllocator *allocator) {
        this->allocator = allocator;
    }

private:
    VTAllocator *allocator;

    VNumeric &getNumericRef(size_t idx)
    {
        uint64 *words = getColPtrForWrite<uint64>(idx);
        vnWrappers[idx].words = words;
        return vnWrappers[idx];
    }
    VString &getStringRef(size_t idx)
    {
        EE::StringValue *sv = getColPtrForWrite<EE::StringValue>(idx);
        if (svWrappers[idx].sv != sv) {
            svWrappers[idx].sv = sv;
        }
        return svWrappers[idx];
    }

    template <class T>
    void addField(std::string fieldName, T& datum) {
        addFieldNoWrite(fieldName, datum);
        writeDatum(cols.size()-1LL, datum);
    }

    template <class T>
    void addFieldNoWrite(std::string fieldName, T& datum, int svlen=MAX_STRING_LENGTH) {
        std::transform(fieldName.begin(), fieldName.end(), fieldName.begin(), ::tolower);
        if (paramNameToIndex.find(fieldName) != paramNameToIndex.end()) {
            ereport(ERROR,
                    (errmsg("Tried to add field '%s' that already exists", fieldName.c_str()),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }
        paramNameToIndex[fieldName] = cols.size();

        VIAssert(allocator);
        size_t sz = sizeOf(datum);
        if (size_t(svlen + 8) > sz) sz = svlen+8;
        cols.push_back((char*)allocator->alloc(sz));
        colstrides.push_back(sz);
        vnWrappers.push_back(VNumeric(0, typemodOf(datum)));
        svWrappers.push_back(VString(0, NULL, svlen));

        if (ncols < cols.size()) ncols = cols.size();
    }

    template <class T> size_t sizeOf(T& datum) { return sizeof(datum); }
    size_t sizeOf(VNumeric& datum) { return 65536; /* max block size */ }
    size_t sizeOf(VString& datum) { return 65536; /* max block size */ }

    template <class T> int32 typemodOf(T& datum) { return VerticaType::makeNumericTypeMod(2,1); }
    int32 typemodOf(VNumeric& datum) { return datum.typmod; }

    template <class T> void writeDatum(size_t colIndex, T& datum) { getColRefForWrite<T>(colIndex) = datum; }
    void writeDatum(size_t colIndex, VNumeric& datum) { getNumericRef(colIndex).copy(&datum); }
    void writeDatum(size_t colIndex, VString& datum) { getStringRef(colIndex).copy(&datum); }

    friend class VerticaBlockSerializer;
};


/**
 * @brief
 * An ExecutorParamWriter is a ParamWriter whose scope is limited to a single executor node -
 * it will never be serialized for transfer to a different node.  As a result, any object
 * which is allocated from a Vertica memory pool (i.e. a ServerInterface's allocator object)
 * can have its address saved in the ExecutorParamWriter (see setPointer()),
 * and be safely retrieved later on (see getPointer()) from a different API call in
 * the same query.
 */
class ExecutorParamWriter : public ParamWriter {
public:
    ExecutorParamWriter(VTAllocator *allocator = NULL) :
        ParamWriter(allocator)
    {}

    template<class T>
    inline void setPointer(std::string fieldName, T *ptr) {
        setRawPointer(fieldName, static_cast<void *>(ptr));
    }

    template<class T>
    inline T *getPointer(std::string fieldName) const {
        return static_cast<T *>(getRawPointer(fieldName));
    }

private:
    void setRawPointer(std::string fieldName, void *ptr) {
        const vint asInt = ptr == NULL ? vint_null : reinterpret_cast<vint>(ptr);
        setInt(fieldName, asInt);
    }

    void *getRawPointer(std::string fieldName) const {
        const vint &asInt = getIntRef(fieldName);
        return asInt == vint_null ? NULL : reinterpret_cast<void *>(asInt);
    }
};


/**
 * @brief
 *
 * This class provides an interface for writing the Used defined session parameters
 */

class SessionParamWriterMap
{
public:
    SessionParamWriterMap(VTAllocator *allocator = NULL) : allocator(allocator) {
        ParamWriter libParamWriter(this->allocator);
        ParamWriter publicParamWriter(this->allocator);
        nspToParamWriterMap["library"]=libParamWriter;
        nspToParamWriterMap["public"]=publicParamWriter;
    }

    /**
     * @brief Add a SessionParamWriter for a namespace
     *
     * @param The namespace and the ParamReader
     */
    void addUDSessionParamWriter(const std::string &nsp, ParamWriter &sessionParamWriter)
    {
        nspToParamWriterMap[nsp]=sessionParamWriter;
    }

    /**
     * @brief Check if a SessionParamWriter for the Namespace exists in the map
     *
     * @param  The namespace
     *
     * @return true if the SessionParamWriter for the Namespace exists false otherwise
     */
    bool containsNamespace(const std::string nsp) const
    {
        return nspToParamWriterMap.count(nsp) > 0;
    }

    /**
     * @brief Get a SessionParamWriter for a namespace
     *
     * @param The namespace
     *
     * @return The SessionParamWriter
     */
    ParamWriter& getUDSessionParamWriter(const std::string &nsp)
    {
        if(nspToParamWriterMap.find(nsp)==nspToParamWriterMap.end())
        {
            if( nsp =="library" || nsp == "public"){
                ParamWriter pWriter(this->allocator);
                addUDSessionParamWriter(nsp, pWriter);
            }
            else{
                ereport(ERROR,(errmsg("The sessionParamWriter for namespace '%s' doesn't exist",
                                      nsp.c_str()),
                               errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
            }
        }
        return nspToParamWriterMap[nsp];
    }

    /**
     * @brief Get a SessionParamWriter for a namespace
     *
     * @param The namespace
     *
     * @return The SessionParamWriter
     */
    const ParamWriter& getUDSessionParamWriter(const std::string &nsp) const
    {
        return nspToParamWriterMap.at(nsp);
    }

    /**
     * @brief Get a SessionParamWriter for a namespace library
     *
     * @return The SessionParamWriter
     */
    ParamWriter& getUDSessionParamWriter()
    {
        return getUDSessionParamWriter("library");
    }

    void setAllocator(VTAllocator *allocator)
    {
        this->allocator=allocator;
    }

     /**
     * @brief Gets the names of all namespaces
     *
     * @return a vector of column namespaces
     */
    std::vector<std::string> getNamespaces() const {
        std::vector<std::string> ret;
        for (nspToParamWriter::const_iterator i=nspToParamWriterMap.begin();
             i!=nspToParamWriterMap.end(); ++i)
        {
            ret.push_back(i->first);
        }
        return ret;
    }

private:
    typedef std::map<std::string, ParamWriter> nspToParamWriter;
    nspToParamWriter nspToParamWriterMap;
    VTAllocator *allocator;
};
/**
 * \brief Shared query-plan state, used
 * when different parts of query planning take place on different nodes.
 * For example, if some work is done on the query initiator node and
 * some is done on each node executing the query, this class stores shared
 * state.
 */
class PlanContext {
public:
    PlanContext(ParamWriter &writer) : writer(writer) {}
    PlanContext(ParamWriter &writer, std::vector<std::string> clusterNodes)
        : writer(writer), clusterNodes(clusterNodes) {}

    /**
     * Get the current context for writing
     */
    virtual ParamWriter& getWriter() {
        return writer;
    }

    /**
     * Get a read-only instance of the current context
     */
    virtual ParamReader& getReader() {
        return writer; /* ParamWriter is a subclass of ParamReader; ParamReader is the read-only form */
    }

    /**
     * Get a list of all of the nodes in the current cluster, by node name
     */
    const std::vector<std::string> &getClusterNodes() {
        return clusterNodes;
    }

private:
    ParamWriter &writer;
    std::vector<std::string> clusterNodes;
};

/**
 * \brief Interface that allows storage of query-plan state,
 * when different parts of query planning take place on different computers.
 *
 * For example, if some work is done on the query initiator node and
 * some is done on each node executing the query.
 *
 * In addition to the functionality provided by PlanContext,
 * NodeSpecifyingPlanContext allows you to specify which nodes
 * the query should run on.  This is used for UDLs.
 */
class NodeSpecifyingPlanContext : public PlanContext {
public:
    NodeSpecifyingPlanContext(ParamWriter &writer,
            std::vector<std::string> clusterNodes, bool canApportion)
        : PlanContext(writer, clusterNodes), _canApportionSource(canApportion),
          targetNodes(clusterNodes /* Default to "ALL NODES" */) {}
    NodeSpecifyingPlanContext(ParamWriter &writer, std::vector<std::string> clusterNodes, std::vector<std::string> targetNodes, bool canApportion)
        : PlanContext(writer, clusterNodes), _canApportionSource(canApportion),
          targetNodes(targetNodes) {}

    /**
     * return whether the UDL source can be apportioned among multiple nodes or threads
     */
    bool canApportionSource() const {
        return _canApportionSource;
    }

    /**
     * Return the set of nodes that this query is currently set to run on
     */
    const std::vector<std::string>& getTargetNodes() const {
        return targetNodes;
    }

    /**
     * Change the set of nodes that the query is intended to run on.
     * Throws UnknownNodeException if any of the specified node names is not actually the name of any node in the cluster.
     */
    void setTargetNodes(const std::vector<std::string>& nodes) {
        const std::vector<std::string> &knownNodes = this->getClusterNodes();

        // Validate the nodes list.
        for (std::vector<std::string>::const_iterator iter = nodes.begin();
                iter != nodes.end(); ++iter) {
            if (std::find(knownNodes.begin(), knownNodes.end(), *iter) == knownNodes.end()) {
                ereport(ERROR,
                        (errmsg("Tried to add unknown node '%s' to user-defined query plan",
                                iter->c_str()),
                        errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
            }
        }

        targetNodes.assign(nodes.begin(), nodes.end());
    }

private:
    bool _canApportionSource;
    std::vector<std::string> targetNodes;
};

//
// Only supposed to be used to create UDScalarFuncObj in UDScalarFuncInfo::createFuncObj().
//
// Compatibility note: The vt_createFuncObject function templates replace the vt_createFuncObj
// macro provided by earlier releases. Note the difference in name ("Object" vs. "Obj");
// also, the class name or type is now an explicit template parameter instead of being passed
// as the second argument:
//
//   OLD WAY: return vt_createFuncObj(srvInterface.allocator, MyClass, ctorArg1, ctorArg2);
//
//   NEW WAY: return Vertica::vt_createFuncObject<MyClass>(srvInterface.allocator, ctorArg1, ctorArg2);
//
// The 'Vertica::' prefix may be omitted if the 'using namespace Vertica;' statement occurs
// in the containing source module before the call site.
//
// A maximum of 10 constructor arguments is supported for older versions of C++ (prior to C++11).
//
// The templates work around a bug in some versions of gcc, including 4.4.5, where use of
// the vt_createFuncObj macro could trigger the build-time failure "internal compiler error:
// in build_zero_init".
// The old macro is still defined in BasicsUDxShared.h, for backward compatibility; however,
// use of the templates is recommended.


#if __cplusplus >= 201103L
template <typename TRET, typename... Args>
TRET *vt_createFuncObject(VTAllocator *allocator, Args&&... args)
{
    void *where = allocator->alloc(sizeof(TRET));
    return new (where) TRET(args...);
}
#else
template<typename TRET>
TRET* vt_createFuncObject(VTAllocator* allocator)
{
   void* where = allocator->alloc(sizeof(TRET));
   return new (where) TRET;
}
template<typename TRET, typename T1>
TRET* vt_createFuncObject(VTAllocator* allocator, T1 arg1)
{
   void* where = allocator->alloc(sizeof(TRET));
   return new (where) TRET(arg1);
}
template<typename TRET, typename T1, typename T2>
TRET* vt_createFuncObject(VTAllocator* allocator, T1 arg1, T2 arg2)
{
   void* where = allocator->alloc(sizeof(TRET));
   return new (where) TRET(arg1, arg2);
}
template<typename TRET, typename T1, typename T2, typename T3>
TRET* vt_createFuncObject(VTAllocator* allocator, T1 arg1, T2 arg2, T3 arg3)
{
   void* where = allocator->alloc(sizeof(TRET));
   return new (where) TRET(arg1, arg2, arg3);
}
template<typename TRET, typename T1, typename T2, typename T3, typename T4>
TRET* vt_createFuncObject(VTAllocator* allocator, T1 arg1, T2 arg2, T3 arg3, T4 arg4)
{
   void* where = allocator->alloc(sizeof(TRET));
   return new (where) TRET(arg1, arg2, arg3, arg4);
}
template<typename TRET, typename T1, typename T2, typename T3, typename T4, typename T5>
TRET* vt_createFuncObject(VTAllocator* allocator, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5)
{
   void* where = allocator->alloc(sizeof(TRET));
   return new (where) TRET(arg1, arg2, arg3, arg4, arg5);
}
template<typename TRET, typename T1, typename T2, typename T3, typename T4, typename T5, typename T6>
TRET* vt_createFuncObject(VTAllocator* allocator, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6)
{
   void* where = allocator->alloc(sizeof(TRET));
   return new (where) TRET(arg1, arg2, arg3, arg4, arg5, arg6);
}
template<typename TRET, typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7>
TRET* vt_createFuncObject(VTAllocator* allocator, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6, T7 arg7)
{
   void* where = allocator->alloc(sizeof(TRET));
   return new (where) TRET(arg1, arg2, arg3, arg4, arg5, arg6, arg7);
}
template<typename TRET, typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7, typename T8>
TRET* vt_createFuncObject(VTAllocator* allocator, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6, T7 arg7, T8 arg8)
{
   void* where = allocator->alloc(sizeof(TRET));
   return new (where) TRET(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
}
template<typename TRET, typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7, typename T8, typename T9>
TRET* vt_createFuncObject(VTAllocator* allocator, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6, T7 arg7, T8 arg8, T9 arg9)
{
   void* where = allocator->alloc(sizeof(TRET));
   return new (where) TRET(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9);
}
template<typename TRET, typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7, typename T8, typename T9, typename T10>
TRET* vt_createFuncObject(VTAllocator* allocator, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6, T7 arg7, T8 arg8, T9 arg9, T10 arg10)
{
   void* where = allocator->alloc(sizeof(TRET));
   return new (where) TRET(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10);
}
#endif

}

#endif
