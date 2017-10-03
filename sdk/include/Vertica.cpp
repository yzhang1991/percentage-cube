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
#include <Vertica.h>
#include <BuildInfo.h>
#include <VerticaUDFSImpl.h>
#include <VerticaDFS.h>

namespace Vertica {

extern "C" {
    VT_THROW_EXCEPTION vt_throw_exception = 0; // function pointer to throw exception
    ServerFunctions *VERTICA_INTERNAL_fns = NULL;
    VT_THROW_INTERNAL_EXCEPTION vt_throw_internal_exception = 0; // function pointer to throw exception
    VT_ERRMSG vt_server_errmsg = 0;
}

// setup global function pointers, called once the UDx library is loaded
extern "C" void setup_global_function_pointers(VT_THROW_EXCEPTION throw_exception, VT_THROW_INTERNAL_EXCEPTION throw_internal_exception, VT_ERRMSG server_errmsg, ServerFunctions *fns)
{
    vt_throw_exception = throw_exception;
    vt_throw_internal_exception = throw_internal_exception;
    vt_server_errmsg = server_errmsg;
    VERTICA_INTERNAL_fns = fns;
}

extern "C" void get_vertica_build_info(VerticaBuildInfo &vbi)
{
    vbi.brand_name    = VERTICA_BUILD_ID_Brand_Name;
    vbi.brand_version = VERTICA_BUILD_ID_Brand_Version;
    vbi.sdk_version   = VERTICA_BUILD_ID_SDK_Version;
    vbi.codename      = VERTICA_BUILD_ID_Codename;
    vbi.build_date    = VERTICA_BUILD_ID_Date;
    vbi.build_machine = VERTICA_BUILD_ID_Machine;
    vbi.branch        = VERTICA_BUILD_ID_Branch;
    vbi.revision      = VERTICA_BUILD_ID_Revision;
    vbi.checksum      = VERTICA_BUILD_ID_Checksum;
}

// This big enough for anything reasonable
#define MSG_BUF 2048

int makeErrMsg(std::basic_ostream<char, std::char_traits<char> > &err_msg, const char *fmt, ...)
{
    va_list va;
    char msg[MSG_BUF];

    va_start(va, fmt);
    vsnprintf(msg, MSG_BUF, gettext(fmt), va);
    va_end(va);

    err_msg << msg;
    return 0;
}

void dummy() {}


// Global Library Manifest object & getter function
UserLibraryManifest &GlobalLibraryManifest()
{
    static UserLibraryManifest singleton;
    return singleton;
}

extern "C" UserLibraryManifest* get_library_manifest()
{
    return &GlobalLibraryManifest();
}

// Date/Time-parsing functions
DateADT dateIn(const char *str, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->dateIn(str, report_error);
}

DateADT dateInNoTzNameCheck(const char *str, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->dateInNoTzNameCheck(str, report_error);
}

int32 dateToChar(DateADT d, char *res, int32 max_size, int32 date_style, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->dateToChar(d, res, max_size, date_style, report_error);
}

TimeADT timeIn(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timeIn(str, typmod, report_error);
}

TimeADT timeInNoTzNameCheck(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timeInNoTzNameCheck(str, typmod, report_error);
}

int32 timeToChar(TimeADT t, char *res, int32 max_size, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timeToChar(t, res, max_size, report_error);
}

TimeTzADT timetzIn(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timetzIn(str, typmod, report_error);
}

TimeTzADT timetzInNoTzNameCheck(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timetzInNoTzNameCheck(str, typmod, report_error);
}

int32 timetzToChar(TimeTzADT tz, char *res, int32 max_size, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timetzToChar(tz, res, max_size, report_error);
}

Timestamp timestampIn(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timestampIn(str, typmod, report_error);
}

Timestamp timestampInNoTzNameCheck(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timestampInNoTzNameCheck(str, typmod, report_error);
}

int32 timestampToChar(Timestamp dt, char *res, int32 max_size, int32 date_style, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timestampToChar(dt, res, max_size, date_style, report_error);
}

TimestampTz timestamptzIn(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timestamptzIn(str, typmod, report_error);
}

TimestampTz timestamptzInNoTzNameCheck(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timestamptzInNoTzNameCheck(str, typmod, report_error);
}

int32 timestamptzToChar(TimestampTz dtz, char *res, int32 max_size, int32 date_style, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timestamptzToChar(dtz, res, max_size, date_style, report_error);
}

Interval intervalIn(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->intervalIn(str, typmod, report_error);
}

int32 intervalToChar(Interval i, int32 typmod, char *res, int32 max_size, int32 date_style, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->intervalToChar(i, typmod, res, max_size, date_style, report_error);
}

DateADT dateInFormatted(const char *str, const std::string format, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->dateInFormatted(str, format, report_error);
}

TimeADT timeInFormatted(const char *str, int32 typmod, const std::string format, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timeInFormatted(str, typmod, format, report_error);
}

TimeTzADT timetzInFormatted(const char *str, int32 typmod, const std::string format, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timetzInFormatted(str, typmod, format, report_error);
}

Timestamp timestampInFormatted(const char *str, int32 typmod, const std::string format, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timestampInFormatted(str, typmod, format, report_error);
}

TimestampTz timestamptzInFormatted(const char *str, int32 typmod, const std::string format, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timestamptzInFormatted(str, typmod, format, report_error);
}

Oid getUDTypeOid(const char *typeName) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);
    return VERTICA_INTERNAL_fns->getUDTypeOid(typeName);
}

Oid getUDTypeUnderlyingOid(Oid typeOid) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);
    return VERTICA_INTERNAL_fns->getUDTypeUnderlyingOid(typeOid);
}

bool isUDType(Oid typeOid) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);
    return VERTICA_INTERNAL_fns->isUDType(typeOid);
}

void log(const char *format, ...)
{
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    va_list ap;
    va_start(ap, format);
    VERTICA_INTERNAL_fns->log(format, ap);
    va_end(ap);
}

vint getUDxDebugLogLevel()
{
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->getUDxDebugLogLevel();
}

void AggregateFunction::updateCols(BlockReader &arg_reader, char *arg, int count,
                                   IntermediateAggs &intAggs, char *aggPtr,
                                   std::vector<int> &intOffsets) {
    int colstride = (arg_reader.colstrides.empty() ? 0 : arg_reader.colstrides[0]);
    arg_reader.cols[0] = arg + (arg_reader.indices ? colstride*arg_reader.indices[0] : 0);
    arg_reader.count = count;
    arg_reader.index = 0;

    for (uint i=0; i<intOffsets.size(); ++i)
        intAggs.cols[i] = aggPtr + intOffsets[i];
}

// Defined outside the class to get the dependency order right with VerticaType
bool VNumeric::charToNumeric(const char *str, const VerticaType &type, VNumeric &target) {
    uint64 *tmpStorage = (uint64*)alloca(target.nwds*sizeof(uint64) * 2); // Extra factor of 2 just for good measure
    VNumeric tmpVal(tmpStorage, target.typmod);
    int64 *pout = (int64*) tmpVal.words;

    int p, s;
    if (!Basics::BigInt::charToNumeric(str, strlen(str), true,
            pout, target.nwds, p, s, true)) {
        return false;
    }
    if (!Basics::BigInt::rescaleNumeric(target.words,
            type.getNumericLength() >> 3, Basics::getNumericPrecision(
                    target.typmod), Basics::getNumericScale(target.typmod),
            pout, target.nwds, p, s)) {
        return false;
    }

    return true;
}

// If you modify this class, do same modifications to VerticaSDK/com/vertica/sdk/DFSFile.java
void DFSFile::create(DFSScope dfsScope, DFSDistribution dfsDistrib)
{
    validateFileOrThrow();
    scope = dfsScope;
    dist = dfsDistrib;
    status = WRITE_CREATED;
        isFfile = true;
    fileWriter = fileManager->openForWrite(fileName, scope, dist);
}

/**
* Deletes a DFS file.
* @returns 0 if successful, throw exceptions if there are errors.
**/
int DFSFile::deleteIt(bool isRecursively)
{
    validateFileOrThrow();
    // Directory deletes are not yet supported
    if (!isFfile) ereport(ERROR,(errmsg("Directory deletes are not supported through API. Use dfs_delete() meta function")));
    fileManager->deleteIt(fileName, isRecursively);
    isExists = false;
    return 0;
}

/**
* Lists file under the path specified by 'fileName'
* @returns a list of DFSFile found under the path.
**/
void DFSFile::listFiles(std::vector<Vertica::DFSFile> &fileList)
{
    if (isFfile)
    {
        ereport(ERROR,(errmsg("File path [%s] is not a directory. File listings are only permitted on directories",
                            fileName.c_str())));
    }
    fileManager->listFiles(fileName, fileList);
}

/**
* Renames file identified by 'srcFilePath' to 'destFilePath'
* returns 0, throws exceptions if there are errors.
int DFSFile::rename(std::string newName)
{
    validateFileOrThrow();
    if (!isExists) ereport(ERROR, (errmsg("Source file/directory does not exists")));
    fileName = fileManager->rename(fileName, newName);
    return 0;
}
**/

/**
* Copy a file/directory from 'srcFilePath' to 'destFilePath'.
* returns 0, throws exceptions if there are errors.
int DFSFile::copy(DFSFile &dfsFile, bool isRecursively)
{
    validateFileOrThrow();
    return fileManager->copy(fileName, dfsFile.getName(), isRecursively);
}
**/

/**
* Make a directory, identified by 'dirPath'
* returns 0, throws exceptions if there are errors.
int DFSFile::makeDir()
{
    validateFileOrThrow();
    return fileManager->makeDir(fileName);
}
**/

void DFSFile::setName(std::string fName)
{
    fileName = fName;
    init();
}

bool DFSFile::exists()
{
    validateFileOrThrow();
    return isExists;
}

/**
* Do basic validations such as emptiness of file path
* and existence of a pointer to FileManager instance.
**/
void DFSFile::validateFileOrThrow()
{
    if (fileName.empty())
    {
        ereport(ERROR,(errmsg("File path [%s] is not set/ empty.",
                                            fileName.c_str())));
    }
    if (fileManager == NULL)
    {
        ereport(ERROR,(errmsg("DFSFile has not initiated properly")));
    }
}
 /**
* Initiates the DFS file by validating given path against
* existing DFS.
*/
void DFSFile::init()
{
    validateFileOrThrow();
    isExists = fileManager->initDFSFile(*this);
}

/**
  * DFSFileReader
  * If you modify this class, do same modifications to VerticaSDK/com/vertica/sdk/DFSFileReader.java
**/
void DFSFileReader::open()
{
    if (!(fileManager))
        ereport(ERROR,(errmsg("DFSFileReader is not initialized, cannot open for reading")));

    if (isOpenForRead)  return;
    fileReader = fileManager->openForRead(fileName);
    isOpenForRead = true;
}

/**
* Reads 'size' of bytes into buffer pointed by 'ptr' from the file
* opened for reading.
* @return number of bytes read, 0 if no bytes were read, indicates the EOF.
* throws exceptions if there are errors.
**/
size_t DFSFileReader::read(void* ptr, size_t size)
{
    if (!(fileManager))
        ereport(ERROR,(errmsg("DFSFileReader is not initialized, cannot use for reading")));
    if (!isOpenForRead)
    {
        ereport(ERROR,(errmsg("File is not opened for reading.")));
    }
    return fileManager->read(fileReader, ptr, size);
}

/*
* Set the read position to a new place within the file stream.
* @offset Number of bytes to offset from @whence
* @whence Values could be SEEK_SET (0 - begining of the file), SEEK_CUR(1 - current position of the file pointer),
* SEEK_END(2- End of the file)
* @return resulting offset position as measured in bytes from begining of the file, throw an exception on error.
*/
off_t DFSFileReader::seek(off_t offset, int whence)
{
    if (!(fileManager))
        ereport(ERROR,(errmsg("DFSFileReader is not initialized, cannot use for seeking")));
    if (offset > (off_t)fsize)
    {
        ereport(ERROR,(errmsg("Specified offset [%jd] is larger than file size.", offset)));
    }
    if (!isOpenForRead)
    {
        ereport(ERROR,(errmsg("File is not opened for reading.")));
    }
    return fileManager->seek(fileReader, offset, whence);
}

/**
* Closes the file opened for reading.
**/
void DFSFileReader::close()
{
    if (!(fileManager))
        return; // There is no reason to throw an error if there is nothing to close.
    if (isOpenForRead)
    {
        fileManager->closeReader(fileReader);
        isOpenForRead = false;
        fileReader = 0;
    }
}


//  DFSFileWriter
/**
* Opens a file for writing.
* If you modify this class, do same modifications to VerticaSDK/com/vertica/sdk/DFSFileReader.java
**/
void DFSFileWriter::open()
{
    if (!(fileManager))
        ereport(ERROR,(errmsg("DFSFileWriter is not initialized, cannot open for writing")));
    if (isOpenForWrite)  return;
    if (!(fileWriter)) fileWriter = fileManager->openForWrite(
                    fileName, scope, dist);
    isOpenForWrite = true;
}

/**
* Writes 'size' of bytes into the file identified by 'writerID' from the
* buffer pointed by 'ptr'.
* @return number of bytes written, less than 0 if there are any errors.
**/
size_t DFSFileWriter::write(const void* ptr, size_t size)
{
    if (!(fileManager))
        ereport(ERROR,(errmsg("DFSFileWriter is not initialized, cannot use for writing")));
    if (!isOpenForWrite)
    {
        ereport(ERROR,(errmsg("File is not opened for writing.")));
    }
    return fileManager->write(fileWriter, ptr, size);
}

/**
* Closes the file opened for writing.
**/
void DFSFileWriter::close()
{
    if (!(fileManager))
        return; // There is no reason to throw an error if there is nothing to close.
    if (isOpenForWrite)
    {
        fileManager->closeWriter(fileWriter);
        isOpenForWrite = false;
    }
}

} // namespace Vertica

namespace Basics {

// BigInt (Numeric) functions
// Simple ones are provided below for performance reasons.

#define NUMERIC_ROUTINES_PRE_RT
#define NUMERIC_ROUTINES_POST_RT BigInt::
#define NUMERIC_ROUTINES_SKIP_SOME
#include <NumericRoutines.ci>
#include <NumericRoutinesExt.ci>

// More BigInt (Numeric) functions
//
// More-complex ones call back into the server; the extra
// per-call overhead is small because the functions are
// more expensive, and this keeps the SDK lightweight.
//
// The implementations of many of these functions were
// provided in previous SDK versions.  If they are needed,
// they are still available as part of the fenced-mode
// source code package.  But virtually all uses should
// simply use the provided callbacks.

void BigInt::setNumericBoundsFromType(uint64 *numUpperBound,
                                     uint64 *numLowerBound,
                                     int nwdso, int32 typmod) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    VERTICA_INTERNAL_fns->setNumericBoundsFromType(numUpperBound,
                                                   numLowerBound,
                                                   nwdso,
                                                   typmod);
}

bool BigInt::checkOverflowNN(const void *po, int nwdso, int32 typmodo) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->checkOverflowNN(po, nwdso, typmodo);
}

bool BigInt::checkOverflowNN(const void *po, int nwo, int nwdso, int32 typmodo) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->checkOverflowNN(po, nwo, nwdso, typmodo);
}

void BigInt::NumericRescaleDown(uint64 *wordso, int nwdso, int32 typmodo,
                                uint64 *wordsi, int nwdsi, int32 typmodi) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->NumericRescaleDown(wordso, nwdso, typmodo,
                                                    wordsi, nwdsi, typmodi);
}

void BigInt::NumericRescaleUp(uint64 *wordso, int nwdso, int32 typmodo,
                              uint64 *wordsi, int nwdsi, int32 typmodi) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->NumericRescaleUp(wordso, nwdso, typmodo,
                                                  wordsi, nwdsi, typmodi);
}

void BigInt::NumericRescaleSameScaleSmallerPrec(uint64 *wordso, int nwdso, int32 typmodo,
                                                uint64 *wordsi, int nwdsi, int32 typmodi) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->NumericRescaleSameScaleSmallerPrec(wordso, nwdso, typmodo,
                                                                    wordsi, nwdsi, typmodi);
}

void BigInt::castNumeric(uint64 *wordso, int nwdso, int32 typmodo,
                        uint64 *wordsi, int nwdsi, int32 typmodi) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->castNumeric(wordso, nwdso, typmodo,
                                             wordsi, nwdsi, typmodi);
}

bool BigInt::rescaleNumeric(void *out, int ow, int pout, int sout,
                            void *in, int iw, int pin, int sin) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->rescaleNumeric(out, ow, pout, sout,
                                                in, iw, pin, sin);
}

void BigInt::numericDivide(const uint64 *pa, int nwdsa, int32 typmoda,
                          const uint64 *pb, int nwdsb, int32 typmodb,
                          uint64 *outNum, int nwdso, int32 typmodo) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->numericDivide(pa, nwdsa, typmoda,
                                               pb, nwdsb, typmodb,
                                               outNum, nwdso, typmodo);
}

bool BigInt::setFromFloat(void *bbuf, int bwords, int typmod,
                          long double value, bool round) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->setFromFloat(bbuf, bwords, typmod, value, round);
}

bool BigInt::charToNumeric(const char *c, int clen, bool allowE,
                           int64 *&pout, int &outWords,
                           int &precis, int &scale, bool packInteger /* = false */) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);
    return VERTICA_INTERNAL_fns->charToNumeric(c, clen, allowE,
                                               pout, outWords,
                                               precis, scale, packInteger);
}

void BigInt::binaryToDecScale(const void *bbuf, int bwords, char *outBuf, int olen, int scale) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);
    return VERTICA_INTERNAL_fns->binaryToDecScale(bbuf, bwords, outBuf, olen, scale);
}

} // namespace Basics

















