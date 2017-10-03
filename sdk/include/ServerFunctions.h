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
 * Create Date: Mar 13, 2012
 */
#ifndef SERVERFUNCTIONS_H_
#define SERVERFUNCTIONS_H_

namespace Vertica
{

// INTERNAL class
// NOT FOR instantiation or overloading/subclassing in external code
// DOES NOT CONTAIN any implementations;
// all functions are abstract virtual and are implemented by the server.
/// @cond INTERNAL
class ServerFunctions {
public:

    // *************
    // !! WARNING !!
    // *************
    //
    // The following are INTERNAL FUNCTIONS.
    // They should NEVER be called directly except in Vertica-provided code!
    // THE FOLLOWING APIs WILL CHANGE between Vertica versions!
    // Their functionality is exposed directly in other (public) methods in
    // Vertica.h; use those function calls in the public API instead, as they
    // are supported and will receive updates and optimizations in the future.

    virtual DateADT dateIn(const char *str, bool report_error) = 0;

    virtual DateADT dateInNoTzNameCheck(const char *str, bool report_error) = 0;

    virtual int32 dateToChar(DateADT d, char *res, int32 max_size, int32 date_style, bool report_error) = 0;

    virtual TimeADT timeIn(const char *str, int32 typmod, bool report_error) = 0;

    virtual TimeADT timeInNoTzNameCheck(const char *str, int32 typmod, bool report_error) = 0;

    virtual int32 timeToChar(TimeADT t, char *res, int32 max_size, bool report_error) = 0;

    virtual TimeTzADT timetzIn(const char *str, int32 typmod, bool report_error) = 0;

    virtual TimeTzADT timetzInNoTzNameCheck(const char *str, int32 typmod, bool report_error) = 0;

    virtual int32 timetzToChar(TimeTzADT tz, char *res, int32 max_size, bool report_error) = 0;

    virtual Timestamp timestampIn(const char *str, int32 typmod, bool report_error) = 0;

    virtual Timestamp timestampInNoTzNameCheck(const char *str, int32 typmod, bool report_error) = 0;

    virtual int32 timestampToChar(Timestamp dt, char *res, int32 max_size, int32 date_style, bool report_error) = 0;

    virtual TimestampTz timestamptzIn(const char *str, int32 typmod, bool report_error) = 0;

    virtual TimestampTz timestamptzInNoTzNameCheck(const char *str, int32 typmod, bool report_error) = 0;

    virtual int32 timestamptzToChar(TimestampTz dtz, char *res, int32 max_size, int32 date_style, bool report_error) = 0;

    virtual Interval intervalIn(const char *str, int32 typmod, bool report_error) = 0;

    virtual int32 intervalToChar(Interval i, int32 typmod, char *res, int32 max_size, int32 date_style, bool report_error) = 0;

    virtual DateADT dateInFormatted(const char *str, const std::string &format, bool report_error) = 0;

    virtual TimeADT timeInFormatted(const char *str, int32 typmod, const std::string &format, bool report_error) = 0;

    virtual TimeTzADT timetzInFormatted(const char *str, int32 typmod, const std::string &format, bool report_error) = 0;

    virtual Timestamp timestampInFormatted(const char *str, int32 typmod, const std::string &format, bool report_error) = 0;

    virtual TimestampTz timestamptzInFormatted(const char *str, int32 typmod, const std::string &format, bool report_error) = 0;

    virtual void log(const char* format, va_list ap) = 0;

    virtual vint getUDxDebugLogLevel() = 0;

    virtual Oid getUDTypeOid(const char *typeName) = 0;

    virtual Oid getUDTypeUnderlyingOid(Oid typeOid) = 0;

    virtual bool isUDType(Oid typeOid) = 0;

    virtual void setNumericBoundsFromType(uint64 *numUpperBound,
                                          uint64 *numLowerBound, 
                                          int nwdso, int32 typmod) = 0;

    virtual bool checkOverflowNN(const void *po, int nwdso, int32 typmodo) = 0;

    virtual bool checkOverflowNN(const void *po, int nwo, int nwdso, int32 typmodo) = 0;

    virtual void NumericRescaleDown(uint64 *wordso, int nwdso, int32 typmodo,
                                    uint64 *wordsi, int nwdsi, int32 typmodi) = 0;
    
    virtual void NumericRescaleUp(uint64 *wordso, int nwdso, int32 typmodo,
                                  uint64 *wordsi, int nwdsi, int32 typmodi) = 0;
    
    virtual void NumericRescaleSameScaleSmallerPrec(uint64 *wordso, int nwdso, int32 typmodo,
                                                    uint64 *wordsi, int nwdsi, int32 typmodi) = 0;
    
    virtual void castNumeric(uint64 *wordso, int nwdso, int32 typmodo,
                             uint64 *wordsi, int nwdsi, int32 typmodi) = 0;
    
    virtual bool rescaleNumeric(void *out, int ow, int pout, int sout,
                                void *in, int iw, int pin, int sin) = 0;
    
    virtual void numericDivide(const uint64 *pa, int nwdsa, int32 typmoda,
                               const uint64 *pb, int nwdsb, int32 typmodb,
                               uint64 *outNum, int nwdso, int32 typmodo) = 0;   

    virtual bool setFromFloat(void *bbuf, int bwords, int typmod,
                              long double value, bool round) = 0;

    virtual bool charToNumeric(const char *c, int clen, bool allowE,
                               int64 *&pout, int &outWords,
                               int &precis, int &scale, bool packInteger = false) = 0;

    virtual void binaryToDecScale(const void *bbuf, int bwords, char *outBuf,
                                  int olen, int scale) = 0;

    // Virtual destructor; may be overridden
    virtual ~ServerFunctions() {}
};
/// @endcond

}

#endif // SERVERFUNCTIONS_H_
