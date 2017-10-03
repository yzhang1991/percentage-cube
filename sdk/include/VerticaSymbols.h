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
 
 /**
 * \internal
 * \file   VerticaSymbols.h
 * \brief  Defines error-reporting macros. 
 */
 
#ifndef VERTICA_SYMBOLS_H
#define VERTICA_SYMBOLS_H

#include <cstddef>

/* macros for representing SQLSTATE strings compactly */
#define PGSIXBIT(ch)    (((ch) - '0') & 0x3F)
#define PGUNSIXBIT(val) (((val) & 0x3F) + '0')

#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5)  \
    (PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
     (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))

#define ERROR_BELONGS_TO_CLASS(err,ch1,ch2)  \
    ((PGUNSIXBIT(err) == ch1)) && (PGUNSIXBIT(err >> 6) == ch2)

#include "VerticaErrorCodes.h"

namespace Vertica
{

/*
 * Include symbols that have different definition for UDx and Vertica server code
 */

/** \def vt_report_error(errcode, args...)
 * Generates a runtime error from user code which is reported by the
 * server. Internally this throws a C++ exception with the same stack unwinding
 * semantics as normal C++ exceptions.
 *
 * @param errcode: produce the with the specified errcode.
 * @param args: error message with printf (varargs style) format specifiers.
 */

#ifndef VERTICA_INTERNAL
#define vt_report_error(errcode, args...) \
    do { \
        std::stringstream err_msg; \
        Vertica::makeErrMsg(err_msg, args); \
        Vertica::vt_throw_exception(errcode, err_msg.str(), __FILE__, __LINE__); \
    } while(0)
#define ereport(elevel, rest) \
    do { \
        std::stringstream err_msg; \
        rest; \
        std::string msg = "User code caused Vertica to throw exception \""; \
        msg += err_msg.str() + "\"";                                    \
        Vertica::vt_throw_exception(0, msg, __FILE__, __LINE__); \
    } while(0)

#define errcode(arg) dummy()
#define errdetail(args...) Vertica::makeErrMsg(err_msg << "  ", args)
#define errhint(args...) Vertica::makeErrMsg(err_msg << "  ", args)
#define errmsg(args...) Vertica::makeErrMsg(err_msg, args)


/**
 * Debug "warning" logging for UDxs.
 *
 * Must pass a ServerInterface instance
 * FUTURE: Add log level prefix to the message (WARN:)
**/
#define LogDebugUDxWarn(srvInt, c...) \
do {\
    if (((srvInt).udxDebugLogLevel & UDXDebugging_WARNING) != 0) \
        (srvInt).log(c); \
} while(false) 

/**
 * Debug "warning" logging for UDxs.
 *
 * FUTURE: Add log level prefix to the message (WARN:)
**/
#define LogDebugUDWarn(c...) \
do {\
    if ((getUDxDebugLogLevel() & UDXDebugging_WARNING) != 0) \
        log(c); \
} while(false) 

/**
 * Debug "info" logging for UDxs.
 *
 * Must pass a ServerInterface instance
 * FUTURE: Add log level prefix to the message (INFO:)
**/
#define LogDebugUDxInfo(srvInt, c...) \
do {\
    if (((srvInt).udxDebugLogLevel & UDXDebugging_INFO) != 0) \
        (srvInt).log(c); \
} while(false) 

/**
 * Debug "info" logging for UDxs.
 *
 * FUTURE: Add log level prefix to the message (INFO:)
**/
#define LogDebugUDInfo(c...) \
do {\
    if ((getUDxDebugLogLevel() & UDXDebugging_INFO) != 0) \
        log(c); \
} while(false) 

/**
 * Debug "basic" logging for UDxs.
 *
 * Must pass a ServerInterface instance
 * FUTURE: Add log level prefix to the message (BASIC:)
**/
#define LogDebugUDxBasic(srvInt, c...) \
do {\
    if (((srvInt).udxDebugLogLevel & UDXDebugging_BASIC) != 0) \
        (srvInt).log(c); \
} while(false) 

/**
 * Debug "basic" logging for UDxs.
 *
 * FUTURE: Add log level prefix to the message (BASIC:)
**/
#define LogDebugUDBasic(c...) \
do {\
    if ((getUDxDebugLogLevel() & UDXDebugging_BASIC) != 0) \
        log(c); \
} while(false) 


#else // VERTICA_INTERNAL

#define vt_report_error(errcode, args...) \
    do { \
        std::stringstream err_msg; \
        std::string info_msg; \
        Vertica::makeErrMsg(err_msg, args); \
        Vertica::vt_throw_internal_exception(errcode, err_msg.str(), info_msg, __FILE__, __LINE__, __PRETTY_FUNCTION__); \
    } while(0)
#define ereport(elevel, rest) \
    do { \
        std::stringstream err_msg; \
        std::stringstream info_msg; \
        int err_code = 0; \
        rest; \
        Vertica::vt_throw_internal_exception(err_code, err_msg.str(), info_msg.str(), __FILE__, __LINE__, __PRETTY_FUNCTION__); \
    } while(0)

#define errcode(arg) err_code = arg
#define errdetail(args...) Vertica::makeErrMsg(info_msg, args)
#define errhint(args...) Vertica::makeErrMsg(info_msg, args)
#define errmsg(args...) (Vertica::vt_server_errmsg ? Vertica::vt_server_errmsg(args) : Vertica::makeErrMsg(err_msg, args))


/**
 * Debug "warning" logging for UDxs.
 *
 * Must pass a ServerInterface instance
 * FUTURE: Add log level prefix to the message (WARN:)
**/
#define LogDebugUDxWarn(srvInt, c...) \
do {\
    if (((srvInt).udxDebugLogLevel & UDXDebugging_WARNING) != 0) \
        (srvInt).log(c); \
} while(false) 

/**
 * Debug "warning" logging for UDxs.
 *
 * FUTURE: Add log level prefix to the message (WARN:)
**/
#define LogDebugUDWarn(c...) \
do {\
    if ((getUDxDebugLogLevel() & UDXDebugging_WARNING) != 0) \
        log(c); \
} while(false) 

/**
 * Debug "info" logging for UDxs.
 *
 * Must pass a ServerInterface instance
 * FUTURE: Add log level prefix to the message (INFO:)
**/
#define LogDebugUDxInfo(srvInt, c...) \
do {\
    if (((srvInt).udxDebugLogLevel & UDXDebugging_INFO) != 0) \
        (srvInt).log(c); \
} while(false) 

/**
 * Debug "info" logging for UDxs.
 *
 * FUTURE: Add log level prefix to the message (INFO:)
**/
#define LogDebugUDInfo(c...) \
do {\
    if ((getUDxDebugLogLevel() & UDXDebugging_INFO) != 0) \
        log(c); \
} while(false) 

/**
 * Debug "basic" logging for UDxs.
 *
 * Must pass a ServerInterface instance
 * FUTURE: Add log level prefix to the message (BASIC:)
**/
#define LogDebugUDxBasic(srvInt, c...) \
do {\
    if (((srvInt).udxDebugLogLevel & UDXDebugging_BASIC) != 0) \
        (srvInt).log(c); \
} while(false) 

/**
 * Debug "basic" logging for UDxs.
 *
 * FUTURE: Add log level prefix to the message (BASIC:)
**/
#define LogDebugUDBasic(c...) \
do {\
    if ((getUDxDebugLogLevel() & UDXDebugging_BASIC) != 0) \
        log(c); \
} while(false) 


#endif // VERTICA_INTERNAL

#define VIAssert(expr) \
    do { \
        if (!(expr)) { ereport(INTERNAL, (errmsg("VIAssert(%s) failed", __STRING(expr)))); } \
    } while(0)
#define VAssert(expr) \
    do { \
        if (!(expr)) { ereport(PANIC, (errmsg("VAssert(%s) failed", __STRING(expr)))); } \
    } while(0)
#define Assert(expr) VAssert(expr)

}

#endif
