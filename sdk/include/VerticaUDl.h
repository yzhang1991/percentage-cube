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

#ifndef VERTICA_UDL_H
#define VERTICA_UDL_H

#include <string>
#include <algorithm>
using std::min;
using std::max;

namespace Vertica
{
class ParamReader;
class ParamWriter;

/**
 * A contiguous in-memory buffer of char *
 */
struct DataBuffer {
    /// Pointer to the start of the buffer
    char * buf;

    /// Size of the buffer in bytes
    size_t size;

    /// Number of bytes that have been processed by the UDL
    size_t offset;
};

/// @cond INTERNAL
/**
 * LengthBuffer
 *
 * A contiguous in-memory buffer of uint32 *
 * size is the number of uint32 values that can be written
 * NOT the number of bytes in the buffer
 * Used for tracking record lengths
 */
struct LengthBuffer {
    uint32 *buf;
    size_t size;
    size_t offset;

    size_t getByteOffset() { return sizeof(uint32) * offset; }
};
/// @endcond

/**
 * Information about a rejected record.
 */
struct RejectedRecord {
    // Reason the record was rejected.
    std::string reason;

    // Record data.
    const char *data;

    // Record length.
    size_t length;

    // Terminator to writer after the record in the rejected output.
    std::string terminator;

    RejectedRecord() : data(NULL), length(0)
    {}

    RejectedRecord(const std::string &reason, const char *data = NULL, size_t length = 0,
                   const std::string &terminator = "\n")
        : reason(reason), data(data), length(length), terminator(terminator)
    {}
};

/**
 * StreamState
 *
 * Indicates the state of a stream after process() has handled some input and some output data.
 *
 *
 * The different enum values have the following meanings:
 *
 * INPUT_NEEDED
 *     Indicates that a stream is unable to continue without being given more input.
 *     It may not have consumed all of its available input yet.  It does not need to have consumed every byte of input.
 *     Not valid for output-only streams, ie., UDSources.
 *
 * OUTPUT_NEEDED
 *     Indicates that a stream is unable to write more output without being
 *     given a new or larger output buffer.  Basically that it has "filled"
 *     the buffer as much as it is reasonably able to (which may be every
 *     byte full, some bytes full, even completely empty -- in which case
 *     Vertica assumes that the UDL needs a larger buffer).
 *     Not valid for input-only streams, ie., UDParsers.
 *
 * DONE
 *     The stream has completed; it will not be writing any more output nor consuming any more input.
 *
 * REJECT
 *     Only valid for UDParsers.
 *     Indicates that the last row the Parser consumed is invalid and should be processed as a rejected row.
 *
 * KEEP_GOING
 *     Not commonly used.
 *     The stream has neither filled all of its output buffer nor consumed all of its input buffer,
 *     but would like to yield to the server.
 *     Typically it has neither consumed data nor produced data.
 *     This state should be used instead of a "wait" loop; a stream that is waiting for some external
 *     operation to complete should periodically return KEEP_GOING rather than simply blocking forever.
 *
 * CHUNK_ALIGNED
 *    Used by UDChunker only.
 *    The chunker has found as many rows as possible in current block, and would like to hand off to parser
 *    a data block that is aligned with record boundary (input offset points to the start of the row after
 *    last row found in current block).
 *
 * See the UDSource, UDFilter, and UDParser classes for how these streams are used.
 */
typedef enum { INPUT_NEEDED = 0, OUTPUT_NEEDED = 1, DONE = 2, REJECT = 3, KEEP_GOING = 4, CHUNK_ALIGNED = 5} StreamState;

/**
 * InputState
 *
 * Applies only to input streams; namely, UDFilter and UDParser.
 *
 * OK
 *     Currently at the start of or in the middle of a stream.
 *
 * END_OF_FILE
 *     Reached the end of the input stream.
 *     No further data is available.  Returning a StreamState of INPUT_NEEDED at this point is invalid, as there is no more input.
 *
 * END_OF_CHUNK
 *     Reached the end of an input chunk.
 *     Applies only to a parser and only when fed by a Chunker, it means the current data
 *     block ends on a record boundary. In this state a parser should consume all data in
 *     the block before returning from process.
 *
 * START_OF_PORTION
 *     Used when a load stack is apportioned.
 *     This mode indicates input does not start at the beginning of a source.
 *     Typical response of a parser is to find the first end-of-record and return PORTION_START. Subsequent calls to process will
 *     use InputState = OK, until the end of the portion/source is reached.
 *
 * END_OF_PORTION
 *     Used when a load stack is apportioned.
 *     This mode indicates the underlying source has reached the end of its allotted portion. The typical response from a parser
 *     is to finish the last record it started but proceed no farther.
 *
 */
typedef enum { OK = 0, END_OF_FILE = 1, END_OF_CHUNK = 2, START_OF_PORTION = 3, END_OF_PORTION = 4} InputState;

struct Portion {
    vint offset;
    vint size;
    bool is_first_portion;
    Portion(vint o = -1, vint s = -1, bool t = false): offset(o), size(s), is_first_portion(t) {}
    bool isValid() const {
        return !(offset == -1 && size == -1 && is_first_portion == false);
    } // non-default value is considered valid

    std::string str() const {
        std::stringstream desc;
        desc << "[first = " << is_first_portion
            << ", offset = " << offset
            << ", size = " << size
            << "]";
        return desc.str();
    }
};

class SourceIterator;

/**
 * @brief
 * An ExecutorPlanContext is a PlanContext object whose scope is limited to a single executor node:
 * it and its parameters will never be serialized.  In addition to providing information about
 * nodes involved in a query, it indicates how many threads are available and allocated for a load.
 */
class ExecutorPlanContext : public NodeSpecifyingPlanContext {
private:
    size_t maxPossibleThreads;
    ssize_t actualThreads;
    SourceIterator *sources;

public:
    ExecutorPlanContext(ExecutorParamWriter &writer,
            std::vector<std::string> clusterNodes, std::vector<std::string> targetNodes,
            bool canApportionSource, size_t maxPossibleThreads, ssize_t actualThreads = -1) :
        NodeSpecifyingPlanContext(writer, clusterNodes, targetNodes, canApportionSource),
        maxPossibleThreads(maxPossibleThreads), actualThreads(actualThreads), sources(NULL) {}

    size_t getMaxAllowedThreads() const {
        return maxPossibleThreads;
    }

    ssize_t getLoadConcurrency() const {
        return actualThreads;
    }

    void setSources(SourceIterator *sources) {
        this->sources = sources;
    }

    SourceIterator *getSources() const {
        return this->sources;
    }

    virtual ExecutorParamWriter &getWriter() {
        return static_cast<ExecutorParamWriter &>(NodeSpecifyingPlanContext::getWriter());
    }
};


/**
 * @brief Base class for UDSource.  Use, with IterativeSourceFactory,
 * if computing the size of a source up front would be
 * prohibitively expensive, or if the number of distinct sources would
 * be prohibitively large to use the standard API. Typically you should
 * use UDSource instead.
 *
 * Not intended or optimized for typical applications.
 */
class UnsizedUDSource {
public:
    /// @cond INTERNAL
    virtual bool useSideChannel() { return false; }
    /// @endcond

    virtual void setup(ServerInterface &srvInterface) {}
    virtual StreamState process(ServerInterface &srvInterface, DataBuffer &output) = 0;

    /// @cond INTERNAL
    // entry-point function from the server so exceptions will be tagged as process instead of processWithMetadata
    StreamState process(ServerInterface &srvInterface, DataBuffer &output, LengthBuffer &output_lengths) {
        return processWithMetadata(srvInterface, output, output_lengths);
    }

    virtual StreamState processWithMetadata(ServerInterface &srvInterface, DataBuffer &output, LengthBuffer &output_lengths) {
        return process(srvInterface, output);
    }
    /// @endcond

    virtual void destroy(ServerInterface &srvInterface) {}
    virtual void destroy(ServerInterface &srvInterface, SessionParamWriterMap &udSessionParams) {
        destroy(srvInterface);
    }
    /**
     * UnsizedUDSource::getUri()
     *
     * Return the URI of the current source of data.
     *
     * This function will be invoked during execution to fill in
     * monitoring information.
     *
     */
    virtual std::string getUri() {return "<UNKNOWN>";}

    /**
     * Return the portion this source object is responsible for (if any).
     *
     * This is used by the parser to determine how much data to parse.
     * It is also used to uniquely identify a source in the load_sources view.
     * As such, this will be called BEFORE setup() (for load_sources) and
     * after setup (for parsing).
     */
    virtual Portion getPortion() = 0;

#if __cplusplus >= 201103L
    virtual ~UnsizedUDSource() noexcept(false) {}
#else
    virtual ~UnsizedUDSource() {}
#endif
};

/**
 * @brief Acquires data from an external source (such as a file or
 * URL) and produces that data in a streaming manner.  
 *
 * The output of a
 * UDSource can be sent through one or more UDFilters followed by one
 * UDParser.
 *
 */
class UDSource : public UnsizedUDSource {
public:

    virtual ~UDSource() {}

    /**
     * UDSource::setup()
     *
     * Will be invoked during query execution, prior to the first time
     * that process() is called on this UDSource instance.
     *
     * May optionally be overridden to perform setup/initialzation.
     */
    virtual void setup(ServerInterface &srvInterface) {}

    /**
     * UDSource::process()
     *
     * Reads data from the input source and processes it. Vertica invokes
     * this function repeatedly until it returns DONE or the query is
     * cancelled by the user.
     *
     * Input: an external data source.<br/>
     * Output: a stream of bytes.<br/>
     *
     * On each invocation, process() should acquire more data and write that data to the
     * buffer specified by `output`.
     *
     * process() must set `output.offset` to the number of bytes that were written to the
     * `output` buffer.  It is common, though not necessary, for this to be the
     * same as `output.size`.  `output.offset` is initially uninitialized.
     * If it is set to 0, this indicates that the `output` buffer is too small for process()
     * to be able to write a unit of input to it.
     *
     * Note that, unless OUTPUT_NEEDED is returned, `output` will be UNMODIFIED the next
     * time process() is called.  This means that pointers into the buffer will continue to
     * be valid.  It also means that `output.offset` may be set.  So, in general, process()
     * code should assume that buffers start at `output.buf[output.offset]`.
     * Note also that, as a performance optimization, upstream operators may start processing
     * emitted data (data between output.buf[0] and output.buf[output.offset])
     * before OUTPUT_NEEDED is returned.  For this reason, `output.offset` must be strictly
     * increasing.
     *
     * process() must not block indefinitely.  If it cannot proceed for an extended period of time,
     * it should return KEEP_GOING.  It will be called again shortly.  Failure to do this will,
     * among other things, prevent the query from being canceled by the user.
     *
     * @return OUTPUT_NEEDED if this UDSource has more data to produce; DONE if has no more data to produce.
     *
     * Note that it is UNSAFE to maintain pointers or references to any of these arguments
     * (or any other argument passed by reference into any other function in this API) beyond
     * the scope of the function call in question.
     * For example, do not store a reference to the server interface or the input block on an
     * instance variable.  Vertica may free and replace these objects.
     */
    virtual StreamState process(ServerInterface &srvInterface, DataBuffer &output) = 0;

    /**
     * UDSource::destroy()
     *
     * Will be invoked during query execution, after the last time
     * that process() is called on this UDSource instance.
     *
     * May optionally be overridden to perform tear-down/destruction.
     */
    virtual void destroy(ServerInterface &srvInterface) {}
    virtual void destroy(ServerInterface &srvInterface, SessionParamWriterMap &udSessionParams) {
        destroy(srvInterface);
    }

    /**
     * UDSource::getSize()
     *
     * Returns the estimated number of bytes that process() will return.
     *
     * This value is treated as advisory only.  It is used to
     * indicate the file size in the LOAD_STREAMS and LOAD_SOURCES tables.
     *
     * IMPORTANT:  getSize() can be called at any time, even
     * before setup() is called!  (Though not before or during the
     * constructor.)
     *
     * In the case of Sources whose factories can potentially produce
     * many UDSource instances, getSize() should avoid acquiring resources
     * that last for the life of the object.  Doing otherwise can defeat
     * Vertica's attempts to limit the maximum number of Sources
     * that are consuming system resources at any given time.
     * For example, if it opens a file handle and leaves that file handle
     * open for use by process(), and if a large number of UDSources are
     * loaded in a single statement, the query may exceed the operating
     * system limit on file handles and crash, even though Vertica only
     * operates on a small number of files at once.
     * This doesn't apply to singleton Sources, Sources whose factory
     * will only ever produce one UDSource instance.
     */
    virtual vint getSize() { return vint_null; }

    /**
     * UDSource::getPortion()
     *
     * Gets this source's split if the source is apportioned.
     * Default implementation return a Portion instance
     * from default constructor {o=-1, s=-1, t=false}.
     */
    virtual Portion getPortion() {
        Portion portion;
        return portion;
    }
};


/**
 * @brief Reads input data from a UDSource or another UDFilter and transforms 
 * it. 
 * 
 * For example, a UDFilter might unzip a file, convert UTF-16 to UTF-8, or
 * remove personally identifying information such as social security numbers.
 * UDFilters can be chained, for example unzipping, converting encodings, and
 * then stripping personal information.
 * The first UDFilter in a chain receives its input from a UDSource, and the
 * output of the last one in the chain is sent to a UDParser.
 * 
 * UDFilter is part of the load pipeline.  The load pipeline consists of
 * up to one UDSource, any number of UDFilters, and up to one UDParser.
 */
class UDFilter {
public:

    /**
     * UDFilter::setup()
     *
     * Will be invoked during query execution, prior to the first time
     * that process() is called on this UDFilter instance for a particular
     * input file.
     *
     * May optionally be overridden to perform setup/initialzation.
     *
     * Note that UDFilters MUST BE RESTARTABLE!  If loading large numbers
     * of files, a given UDFilter may be re-used for multiple files.
     * Vertica follows the worker-pool design pattern:  At the start of
     * COPY execution, several Parsers and several Filters are instantiated
     * per node, by calling the corresponding prepare() method multiple times.
     * Each Filter/Parser pair is then internally assigned to an initial
     * Source (UDSource or internal).  At that point, setup() is called;
     * then process() is called until it is finished; then destroy() is called.
     * If there are still sources in the pool waiting to be processed, then
     * the UDFilter/UDSource pair will be given a second Source;
     * setup() will be called a second time, then process() until it is
     * finished, then destroy().  This repeats until all sources have been
     * read.
     */
    virtual void setup(ServerInterface &srvInterface) {}

    /**
     * UDFilter::process()
     *
     * Reads data from the input stream and emits data to the output steeam.
     * Vertica invokes this function repeatedly during query execution, until 
     * it returns DONE or until the query is canceled by the user.
     *
     * Input: a stream of bytes.<br/>
     * Output: a stream of bytes.<br/>
     * 
     * On each invocation, process() is handed some input data and a buffer to
     * write output data into.  It is expected to read and process some amount
     * of the input data, write some amount of output data, and return a value
     * that informs Vertica what needs to happen next.
     *
     * process() must set `input.offset` to the number of bytes that were
     * successfully read from the `input` buffer, and that will not need to be
     * re-consumed by a subsequent invocation of process().  This may not be
     * larger than `input.size`.  (`input.size` is the size of the buffer.)
     * If it is set to 0, this indicates that process() cannot process any
     * part of an input buffer of this size, and requires more data per
     * invocation.  (For example, a block-based decompression algorithm might
     * return 0 if the input buffer does not contain a complete block.)
     *
     * Note that `input` may contain null bytes, if the source file contains
     * null bytes.  Note also that `input` is NOT automatically
     * null-terminated.
     *
     * If `input_state` == END_OF_FILE, then the last byte in `input` is the
     * last byte in the input stream.  Returning INPUT_NEEDED will not result
     * in any new input appearing.  process() should return DONE in this case
     * as soon as this operator has finished producing all output that it is
     * going to produce.
     *
     * process() must set `output.offset` to the number of bytes that were
     * written to the `output` buffer.  This may not be larger than
     * `output.size`.  If it is set to 0, this indicates that process()
     * requires a larger output buffer.
     *
     * Note that, unless OUTPUT_NEEDED is returned, `output` will be UNMODIFIED the next
     * time process() is called.  This means that pointers into the buffer will continue to
     * be valid.  It also means that `output.offset` may be set.  So, in general, process()
     * code should assume that buffers start at `output.buf[output.offset]`.  The
     * same goes for `input` and INPUT_NEEDED.
     * Note also that, as a performance optimization, upstream operators may start processing
     * emitted data (data between output.buf[0] and output.buf[output.offset])
     * before OUTPUT_NEEDED is returned.  For this reason, `output.offset` must be strictly
     * increasing.
     *
     * process() must not block indefinitely.  If it cannot proceed for an
     * extended period of time, it should return KEEP_GOING.  It will be
     * called again shortly.  Failure to do this will, among other things,
     * prevent the query from being canceled by the user.
     *
     * @return OUTPUT_NEEDED if this UDFilter has more data to produce;
     *         INPUT_NEEDED if it needs more data to continue working;
     *         DONE if has no more data to produce.
     *
     * Note that it is UNSAFE to maintain pointers or references to any of
     * these arguments (or any other argument passed by reference into any
     * other function in this API) beyond the scope of the function call in
     * question. For example, do not store a reference to the server interface
     * or the input block on an instance variable.  Vertica may free and
     * replace these objects.
     */
    virtual StreamState process(ServerInterface &srvInterface, DataBuffer &input, InputState input_state, DataBuffer &output) = 0;

    /// @cond INTERNAL
    // entry-point function from the server so exceptions will be tagged as process instead of processWithMetadata
    StreamState process(ServerInterface &srvInterface, DataBuffer &input, LengthBuffer &input_lengths,
                                InputState input_state, DataBuffer &output, LengthBuffer &output_lengths) {
        return processWithMetadata(srvInterface, input, input_lengths, input_state, output, output_lengths);
    }

    virtual StreamState processWithMetadata(ServerInterface &srvInterface, DataBuffer &input, LengthBuffer &input_lengths,
                                            InputState input_state, DataBuffer &output, LengthBuffer &output_lengths) {
        return process(srvInterface, input, input_state, output);
    }
    /// @endcond

    /**
     * UDFilter::destroy()
     *
     * Will be invoked during query execution, after the last time
     * that process() is called on this UDFilter instance for a
     * particular input file.
     *
     * May optionally be overridden to perform tear-down/destruction.
     *
     * See UDFilter::setup() for a note about the restartability of
     * UDFilters.
     */
    virtual void destroy(ServerInterface &srvInterface) {}
    virtual void destroy(ServerInterface &srvInterface, SessionParamWriterMap &udSessionParams) {
        destroy(srvInterface);
    }

    /// @cond INTERNAL
    virtual bool useSideChannel() { return false; }
    /// @endcond

#if __cplusplus >= 201103L
    virtual ~UDFilter() noexcept(false) {}
#else
    virtual ~UDFilter() {}
#endif
};

/**
 * @brief Parses an input stream into Vertica tuples (rows to be 
 * inserted into a table).  
 *
 * A UDParser can be used with up to one UDSource 
 * and any number of UDfilters.
 *
 */
class UDParser {
protected:

    /**
     * Writer to write parsed tuples to.
     * Has the same API as PartitionWriter, from the UDT framework.
     */
    StreamWriter *writer;
    vint recordsAcceptedInBatch;
    bool seen_eob;
    bool isParserApportionable;
    Portion portion;
    std::string srcname;

    /**
     * Return a description of the portion of the input source which this
     * parser is currently processing.
     */
    Portion getPortion() {
        return portion;
    }

public:

    /**
     * UDParser::setup()
     *
     * Will be invoked during query execution, prior to the first time
     * that process() is called on this UDParser instance for a particular
     * input source.
     *
     * May optionally be overridden to perform setup/initialzation.
     *
     * Note that UDParsers MUST BE RESTARTABLE!  If loading large numbers
     * of files, a given UDParsers may be re-used for multiple files.
     * Vertica follows the worker-pool design pattern:  At the start of
     * COPY execution, several Parsers and several Filters are instantiated
     * per node, by calling the corresponding prepare() method multiple times.
     * Each Filter/Parser pair is then internally assigned to an initial
     * Source (UDSource or internal).  At that point, setup() is called;
     * then process() is called until it is finished; then destroy() is called.
     * If there are still sources in the pool waiting to be processed, then
     * the UDFilter/UDSource pair will be given a second Source;
     * setup() will be called a second time, then process() until it is
     * finished, then destroy().  This repeats until all sources have been
     * read.
     */
    virtual void setup(ServerInterface &srvInterface, SizedColumnTypes &returnType) {}

    /**
     * UDParser::prepareToCooperate()
     *
     * Notification to this parser that it should prepare to share parsing input with
     * another. This can only happen when a parser has an associated chunker.
     * Default implementation does nothing.
     */
    virtual void prepareToCooperate(ServerInterface &srvInterface, bool isLeader) {}

    /**
     * UDParser::isReadyToCooperate()
     *
     * Called after UDParser::prepareToCooperate(), returns false if this parser is
     * not yet ready to cooperate. Once this method returns true the parser can
     * begin to cooperate.
     * Default implementation returns true, override if some preparation is required
     * before the parser can cooperate (e.g. a certain # of rows must be skipped).
     */
    virtual bool isReadyToCooperate(ServerInterface &srvInterface) { return true; }

    /**
     * UDParser::process()
     *
     * Reads data from the input stream and produces tuples to be loaded.
     * Vertica invokes this function repeatedly during query execution, 
     * until it returns DONE or until the query is canceled by the user.
     * 
     * Input: a stream of bytes.<br/>
     * Output: a stream of tuples.<br/>
     *
     * On each invocation, process() will be given an input buffer.  It should
     * read data from that buffer, converting it to fields and tuples and
     * writing those tuples via `writer`.  Once it has consumed as much as it
     * reasonably can (for example, once it has consumed the last complete row
     * in the input buffer), it should return INPUT_NEEDED to indicate that
     * more data is needed, or DONE to indicate that it has completed parsing
     * this input stream and will not be reading more bytes from it.
     *
     * If `input_state` == END_OF_FILE, then the last byte in `input` is the
     * last byte in the input stream.  Returning INPUT_NEEDED will not result
     * in any new input appearing.  process() should return DONE in this case
     * as soon as this operator has finished producing all output that it is
     * going to produce.
     *
     * Note that `input` may contain null bytes, if the source file contains
     * null bytes.  Note also that `input` is NOT automatically
     * null-terminated.
     *
     * process() must not block indefinitely.  If it cannot proceed for an
     * extended period of time, it should return KEEP_GOING.  It will be
     * called again shortly.  Failure to do this will, among other things,
     * prevent the query from being canceled by the user.
     *
     * Note that, unless INPUT_NEEDED is returned, `input` will be UNMODIFIED the next
     * time process() is called.  This means that pointers into the buffer will continue to
     * be valid.  It also means that `input.offset` may be set.  So, in general, process()
     * code should assume that buffers start at `input.buf[input.offset]`.
     *
     *
     * Row Rejection
     *
     * process() can "reject" a row, causing it to be logged by Vertica's
     * rejected-rows mechanism.
     * Rejected rows should not be emitted as tuples.
     * All previous input must have been consumed by a call to process().
     * To reject a row, set `input.offset` to the size of the row, and return
     * REJECT.
     *
     *
     * @return INPUT_NEEDED if this UDParser has more data to produce;
     *         DONE if has no more data to produce; REJECT to reject a row
     *
     * Note that it is UNSAFE to maintain pointers or references to any of
     * these arguments (or any other argument passed by reference into any
     * other function in this API) beyond the scope of the function call in
     * question. For example, do not store a reference to the server interface
     * or the input block on an instance variable.  Vertica may free and
     * replace these objects.
     */
    virtual StreamState process(ServerInterface &srvInterface, DataBuffer &input, InputState input_state) = 0;

    /// @cond INTERNAL
    // entry-point function from the server so exceptions will be tagged as process instead of processWithMetadata
    StreamState process(ServerInterface &srvInterface, DataBuffer &input, LengthBuffer &input_lengths,
                        InputState input_state) {
        return processWithMetadata(srvInterface, input, input_lengths, input_state);
    }

    virtual StreamState processWithMetadata(ServerInterface &srvInterface, DataBuffer &input, LengthBuffer &input_lengths,
                                            InputState input_state) {
        // use default method
        return process(srvInterface, input, input_state);
    }
    /// @endcond

    /** Returns information about the rejected record */
    virtual RejectedRecord getRejectedRecord() {
        return RejectedRecord("Rejected by user-defined parser");
    }

    /**
     * UDParser::destroy()
     *
     * Will be invoked during query execution, after the last time
     * that process() is called on this UDParser instance for a particular
     * input file.
     *
     * May optionally be overridden to perform tear-down/destruction.
     *
     * See UDParser::setup() for a note about the restartability of UDParsers.
     */
    virtual void destroy(ServerInterface &srvInterface, SizedColumnTypes &returnType) {}
    virtual void destroy(ServerInterface &srvInterface, SizedColumnTypes &returnType, SessionParamWriterMap &udSessionParams) {
        destroy(srvInterface,returnType);
    }

    /// @cond INTERNAL
    void setStreamWriter(StreamWriter *writer) { this->writer = writer; }
    StreamWriter* getStreamWriter() { return writer; }
    vint getRecordsAcceptedInBatch()        {return recordsAcceptedInBatch;}
    void setRecordsAcceptedInBatch(vint i)  {recordsAcceptedInBatch = i;}
    void increRecordsAcceptedInBatch()  {recordsAcceptedInBatch++;}
    bool getSeenEOB() {return seen_eob;}
    void setSeenEOB(bool b) {seen_eob=b;}
    bool getIsParserApportionable() { return isParserApportionable;}
    void setIsParserApportionable(bool v) { isParserApportionable = v;}
    void setPortion(Portion p) { portion = p;}
    std::string getSourceInfo() { return srcname;}
    void setSourceInfo(std::string v) { srcname = v; }
    /// @endcond

    UDParser():recordsAcceptedInBatch(0),seen_eob(false), isParserApportionable(false), srcname("") {}

    /// @cond INTERNAL
    virtual bool useSideChannel() { return false; }
    /// @endcond

#if __cplusplus >= 201103L
    virtual ~UDParser() noexcept(false) {}
#else
    virtual ~UDParser() {}
#endif
};

/**
 * @brief Separates parser input into chunks at record boundaries, allowing 
 * parsing to proceed in parallel.  
 *
 * Vertica invokes a UDChunker (if present) as part
 * of the parsing phase.  Chunkers are tightly coupled to parsers.
 */
class UDChunker {
private:
    Portion portion;

protected:
    /**
     * Return a description of the portion of the input source which
     * this chunker is currently processing.
     */
    Portion getPortion() const {
        return portion;
    }

public:

    /**
     * UDChunker::setup()
     *
     * Will be invoked during query execution, prior to the first time
     * that process() is called on this UDChunker instance for a particular
     * input source.
     *
     * May optionally be overridden to perform setup/initialzation.
     *
     * Note that UDChunkers MUST BE RESTARTABLE!  If loading large numbers
     * of files, a given UDChunker may be re-used for multiple files.
     * Vertica follows the worker-pool design pattern:  At the start of
     * COPY execution, several Chunkers and several Filters are instantiated
     * per node, by calling the corresponding prepare() method multiple times.
     * Each Filter/Chunker pair is then internally assigned to an initial
     * Source (UDSource or internal).  At that point, setup() is called;
     * then process() is called until it is finished; then destroy() is called.
     * If there are still sources in the pool waiting to be processed, then
     * the UDFilter/UDChunker pair will be given a second Source;
     * setup() will be called a second time, then process() until it is
     * finished, then destroy().  This repeats until all sources have been
     * read.
     */
    virtual void setup(ServerInterface &srvInterface, SizedColumnTypes &returnType) {}

    /**
     * UDChunker::alignPortion()
     *
     * See "Apportioned Load".
     *
     * Reads data from the input stream and aligns the chunker to the beginning
     * of the first chunk in the portion of the stream it is processing.
     *
     * This is invoked repeatedly during query execution, before any calls to process(),
     * if we are doing an apportioned load.  The input buffer initially contains contents
     * from the start of the current portion; future calls may advance along the stream.
     * This method should identify the start of the first chunk that this UDChunker
     * will produce, and indicate that it is now "aligned" to that boundary by setting
     * `input.offset` to the corresponding offset and returning DONE.
     *
     * Note that `input` may contain null bytes, if the source contains null bytes.
     * Note also that `input` is NOT automatically null-terminated.
     *
     * alignPortion() must not block indefinitely.  If it cannot proceed for an extended
     * period of time, it should return KEEP_GOING.  It will be called again shortly.
     * Failure to do this will, among other things, prevent the query from being
     * canceled by the user.
     *
     * Valid return values for this method are INPUT_NEEDED, KEEP_GOING, and DONE.
     * @return KEEP_GOING if the UDChunker must block;
     *         INPUT_NEEDED if a record boundary cannot be found in the input.
     *             Leaving `input.offset` unchanged will result in a larger buffer
     *                 for the next invocation.
     *             Advance `input.offset` to advance through the data stream.
     *         DONE if a portion boundary has been identified.  Set `input.offset`
     *             to the location of that boundary.
     */
    virtual StreamState alignPortion(ServerInterface &srvInterface, DataBuffer &input, InputState inputState) {
        vt_throw_exception(0, "Apportioned load is not supported by this chunker", __FILE__, __LINE__);
        return INPUT_NEEDED;
    }

    /**
     * UDChunker::process()
     *
     * Will be invoked repeatedly during query execution, until it returns
     * DONE or until the query is canceled by the user.
     *
     * On each invocation, process() will be given an input buffer.  It should
     * read data from that buffer, find record boundaries and align input.offset
     * with the end of the last record in the buffer. Once it has processed as
     * much as it reasonably can (for example, once it has processed the last
     * complete row in the input buffer), it should return INPUT_NEEDED to indicate
     * that more data is needed, or DONE to indicate that it has completed "parsing"
     * this input stream and will not be reading more bytes from it.
     *
     * if a few rows were found in current block, move offset forward to point
     * at the start of next (potential) row, and mark state as CHUNK_ALIGNED,
     * indicating the chunker is ready to hand this chunk to parser.
     *
     * If `input_state` == END_OF_FILE, then the last byte in `input` is the
     * last byte in the input stream.  Returning INPUT_NEEDED will not result
     * in any new input appearing.  process() should return DONE in this case.
     *
     * If `input_state` == END_OF_PORTION, then the last byte in `input` is the
     * last byte in the current portion.  Returning CHUNK_ALIGNED or INPUT_NEEDED
     * will result in retrieval of data beyond the portion boundary.
     * If a chunker which supports apportioned load sees END_OF_PORTION,
     * it should (a) align a chunk as usual, and return CHUNK_ALIGNED or INPUT_NEEDED
     * to retrieve the next block, and then (b) align a chunk to the FIRST record
     * boundary which appears in the next portion.
     *
     * Note that `input` may contain null bytes, if the source file contains
     * null bytes.  Note also that `input` is NOT automatically
     * null-terminated.
     *
     * process() must not block indefinitely.  If it cannot proceed for an
     * extended period of time, it should return KEEP_GOING.  It will be
     * called again shortly.  Failure to do this will, among other things,
     * prevent the query from being canceled by the user.
     *
     *
     *
     * @return INPUT_NEEDED if this UDChunker has more data to process;
     *         DONE if has no more data to produce;
     *         CHUNK_ALIGNED if this UDChunker successfully aligned some rows
     *         and is ready to give them to parser.
     *
     * Note that it is UNSAFE to maintain pointers or references to any of
     * these arguments (or any other argument passed by reference into any
     * other function in this API) beyond the scope of the function call in
     * question. For example, do not store a reference to the server interface
     * or the input block on an instance variable.  Vertica may free and
     * replace these objects.
     */
    virtual StreamState process(ServerInterface &srvInterface, DataBuffer &input, InputState input_state) = 0;

    /// @cond INTERNAL
    // entry-point function from the server so exceptions will be tagged as process instead of processWithMetadata
    StreamState process(ServerInterface &srvInterface, DataBuffer &input, LengthBuffer &input_lengths,
                        InputState input_state) {
        return processWithMetadata(srvInterface, input, input_lengths, input_state);
    }

    virtual StreamState processWithMetadata(ServerInterface &srvInterface, DataBuffer &input, LengthBuffer &input_lengths,
                                            InputState input_state) {
        return process(srvInterface, input, input_state);
    }
    /// @endcond

    /**
     * UDChunker::destroy()
     *
     * Will be invoked during query execution, after the last time
     * that process() is called on this UDChunker instance for a particular
     * input.
     *
     * May optionally be overridden to perform tear-down/destruction.
     *
     * See UDChunker::setup() for a note about the restartability of UDChunkers.
     */
    virtual void destroy(ServerInterface &srvInterface, SizedColumnTypes &returnType) {}
    virtual void destroy(ServerInterface &srvInterface, SizedColumnTypes &returnType, SessionParamWriterMap &udSessionParams) {
        destroy(srvInterface, returnType);
    }

    /// @cond INTERNAL
    virtual bool useSideChannel() {
        return false;
    }
    void setPortion(Portion p) {
        this->portion = p;
    }
    /// @endcond

    /**
     * Dtor.
     */
    virtual ~UDChunker() {}
};

//////////////////////////////// UDL Factories ////////////////////////////////
/**
 * Wrappers to help construct and manage UDLs
 */

/**
 * @brief Constructs a set of Sources to be used by an IterativeSourceFactory.
 * 
 * createNextSource() will be called repeatedly until it returns NULL.
 * Each resulting Source will be read to completion, and the contained data
 * passed to the Filter and Parser.
 */
class SourceIterator {
public:

    /**
     * @brief Set up this SourceIterator
     *
     * Should perform setup that should not take place in the constructor
     * due to the exception-handling semantics of constructors
     */
    virtual void setup(ServerInterface &srvInterface, NodeSpecifyingPlanContext &planCtxt) {}

    /**
     * @brief Create the next UDSource to process.
     *
     * Should return NULL if no further sources are available for processing.
     *
     * Note that the previous Source may still be open and in use on a different
     * thread when this function is called.
     *
     * @return a new Source instance corresponding to a new input stream
     */
    virtual UnsizedUDSource* createNextSource(ServerInterface &srvInterface) = 0;

    /**
     * @brief Tear down this SourceIterator
     *
     * Should perform clean-up that should not take place in the destructor
     * due to the exception-handling semantics of destructors.
     */
    virtual void destroy(ServerInterface &srvInterface, NodeSpecifyingPlanContext &planCtxt) {}

    /**
     * @return the total number of Sources that this factory will produce
     */
    virtual size_t getNumberOfSources() = 0;

    /**
     * @return the raw-data size of the sourceNum'th source that will be
     * produced by createNextSource().
     * Should return `vint_null` if the size is unknown.
     *
     * This value is used as a hint, and is used by the "load_streams" table
     * to display load progress.  If incorrect or not set, "load_streams" may
     * contain incorrect or unhelpful information.
     */
    virtual size_t getSizeOfSource(size_t sourceNum) { return vint_null; }

    virtual ~SourceIterator() {}
};


/**
 * @brief Source factory that works with a SourceIterator to produce UDSources.
 * 
 * Usually it is easier to subclass SourceFactory and have its
 * prepareUDSources() method return a vector of UDSource objects. Behind
 * the scenes, Vertica uses a built-in iterator to pull sources from that
 * vector.  However, if you need to be able to control source management
 * at this level, you can instead write your own SourceIterator and use it
 * with an IterativeSourceFactory.
 * 
 * This class performs initial validation and planning of a query, before it is
 * distributed over the network.  It also instantiates objects to perform 
 * further initialization on each node, once the query has been distributed.
 *
 * Note that SourceFactories are singletons.  Subclasses should be stateless, with
 * no fields containing data, just methods.  plan() and prepare() methods must never
 * modify any global variables or state; they may only modify the variables that they
 * are given as arguments.  (If global state must be modified, use SourceIterator.)
 *
 * Factories should be registered using the RegisterFactory() macro, defined
 * in Vertica.h.
 */
class IterativeSourceFactory : public UDLFactory
{
public:
    UDXFactory::UDXType getUDXFactoryType() { return LOAD_SOURCE; }

    /**
     * Execute any planning logic required at query plan time.
     * This method is run once per query, during query initialization.
     * Its job is to perform parameter validation, and to modify the
     * set of nodes that the COPY statement will run on.
     *
     * plan() runs exactly once per query, on the initiator node.
     * If it throws an exception, the query will not proceed; it will
     * be aborted prior to distributing the query to the other nodes and
     * running prepare().
     */
    virtual void plan(ServerInterface &srvInterface,
            NodeSpecifyingPlanContext &planCtxt) {}

    /**
     * Request the number of threads that the SourceFactory wants to use.
     * This function is called on each node before prepare().
     *
     * 'planCtxt' contains the same parameters stored by plan() method.
     * It also exposes the maximum number of allowed threads, via 'getMaxAllowedThreads()'.
     * Further computations can be stored in its 'ExecutorParamWriter' object, for use
     * in prepare().
     */
    virtual ssize_t getDesiredThreads(ServerInterface &srvInterface, ExecutorPlanContext &planCtxt) {
        return -1;
    }

    /**
     * Prepare this SourceFactory to start creating sources.
     * This function will be called on each node, prior to the Load
     * operator starting to execute and prior to any other
     * virtual functions on this class being called.
     *
     * 'planCtxt' contains the same data that was placed there by the
     * plan() and getDesiredThreads() methods.
     */
    virtual SourceIterator* prepareExecutor(ServerInterface &srvInterface,
            ExecutorPlanContext &planCtxt) {
        return prepare(srvInterface, static_cast<NodeSpecifyingPlanContext &>(planCtxt));
    }
    virtual SourceIterator* prepare(ServerInterface &srvInterface,
            NodeSpecifyingPlanContext &planCtxt) {
        vt_throw_exception(0, __FUNCTION__ +
                std::string(" not implemented.  Subclasses of IterativeSourceFactory must override either prepare() or prepareExecutor()"),
                __FILE__, __LINE__);
        return NULL; /* unreachable but avoids compiler warnings */
    }

    virtual bool isSourceApportionable() {return false;}

};

// Internal class; should not be used directly
class DefaultSourceIterator : public SourceIterator {
private:
    std::vector<UDSource*> sources;
    size_t num_sources;
public:
    DefaultSourceIterator(std::vector<UDSource*> sources)
    : sources(sources.rbegin(), sources.rend() /* FIFO-by-copy */), num_sources(sources.size()) {}
    UnsizedUDSource* createNextSource(ServerInterface &srvInterface) {
        UDSource* retVal = sources.back();
        sources.pop_back();
        return retVal;
    }
    size_t getNumberOfSources() { return num_sources; }
    size_t getSizeOfSource(size_t sourceNum) { return sources[num_sources - sourceNum - 1]->getSize(); }
};

/**
 * @brief Produces and initializes UDSources to process input data sources.
 * 
 * Subclassing SourceFactory is the preferred way to implement a factory
 * corresponding to a UDSource. If you need more flexibility, implement both 
 * IterativeSourceFactory and SourceIterator.  (See the documentation of
 * IterativeSourceFactory.)
 * 
 * Factories should be registered using the RegisterFactory() macro, defined
 * in Vertica.h.
 */
class SourceFactory : public IterativeSourceFactory {
public:
    /**
     * Execute any planning logic required at query plan time.
     * This method is run once per query, during query initialization.
     * Its job is to perform parameter validation, and to modify the
     * set of nodes that the COPY statement will run on.
     *
     * plan() runs exactly once per query, on the initiator node.
     * If it throws an exception, the query will not proceed; it will
     * be aborted prior to distributing the query to the other nodes and
     * running prepare().
     *
     * @param srvInterface Interface to server operations and functionality,
     *          including (not-per-column) parameter lookup
     *
     * @param planCtxt Context for storing and retrieving arbitrary data,
     *          for use just by this instance of this query.
     *          The same context is shared with plan().
     *          Also provides functionality for specifying which nodes this
     *          query will run on.
     */
    virtual void plan(ServerInterface &srvInterface,
            NodeSpecifyingPlanContext &planCtxt) {}

    /**
     * Request the number of threads that the SourceFactory wants to use.
     * This function is called on each node before prepareUDSources().
     *
     * 'planCtxt' contains the same parameters stored by plan().
     * It exposes the maximum number of allowed threads, via 'getMaxAllowedThreads()',
     * and also the actual number of threads, via 'getLoadConcurrency()'.
     *
     * The default implementation of this invokes prepare(), saves the result in the
     * ExecutorPlanContext, and returns the number of sources as the number of desired threads.
     */
    virtual ssize_t getDesiredThreads(ServerInterface &srvInterface, ExecutorPlanContext &planCtxt) {
        SourceIterator *sources = planCtxt.getSources();
        if (sources == NULL) {
            planCtxt.setSources(prepareExecutor(srvInterface, planCtxt));
            sources = planCtxt.getSources();
        }

        return sources->getNumberOfSources();
    }

    /**
     * Create UDSources.
     * This function will be called on each node, prior to the Load
     * operator starting to execute and prior to any other
     * virtual functions on this class being called.
     *
     * 'planCtxt' contains the same data that was placed there by the
     * plan() and getDesiredThreads() methods.  It also exposes the
     * maximum execution parallelism with 'getMaxAllowedThreads()'
     * and the actual load concurrency with 'getLoadConcurrency()'.
     *
     * Unlike the standard SourceFactory, this method directly
     * instantiates all of its UDSources, and returns a vector of them.
     * This requires that all UDSources be resident in memory for
     * the duration of the query, which is fine in the common case
     * but which may not be acceptable for some resource-intensive
     * UDSources.
     *
     * @param srvInterface Interface to server operations and functionality,
     *          including (not-per-column) parameter lookup
     *
     * @param planCtxt Context for storing and retrieving arbitrary data,
     *          for use just by this instance of this query.
     *          The same context is shared with plan() and getDesiredThreads().
     *          Also provides functionality for determining which nodes
     *          this query is running on and getting the number of available threads.
     *
     * @return A vector of UDSources to use for this query.  Sources will
     *          be loaded in a pooled manner, several at a time.
     */
    virtual std::vector<UDSource*> prepareUDSourcesExecutor(ServerInterface &srvInterface,
                                                    ExecutorPlanContext &planCtxt) {
        return prepareUDSources(srvInterface, static_cast<NodeSpecifyingPlanContext &>(planCtxt));
    }
    virtual std::vector<UDSource*> prepareUDSources(ServerInterface &srvInterface,
                                                    NodeSpecifyingPlanContext &planCtxt) {
        vt_throw_exception(0, __FUNCTION__ +
                std::string(" not implemented.  Subclasses of SourceFactory must override either prepareUDSources() or prepareUDSourcesExecutor()"),
                __FILE__, __LINE__);
        return std::vector<UDSource *>(); /* unreachable but let's avoid warnings */
    }

    /* @cond INTERNAL */
    SourceIterator* prepareExecutor(ServerInterface &srvInterface,
                            ExecutorPlanContext &planCtxt) {
        SourceIterator *sources = planCtxt.getSources();
        if (sources == NULL) {
            /* this probably means getDesiredThreads() was overridden */
            sources = vt_createFuncObject<DefaultSourceIterator>(srvInterface.allocator,
                    prepareUDSourcesExecutor(srvInterface, planCtxt));
            planCtxt.setSources(sources);
        }
        return sources;
    }
    /* @endcond */
};


/**
 * @brief Produces and initializes UDFilters to transform input data before 
 * parsing.
 * 
 * Note that FilterFactories are singletons.  Subclasses should be stateless, with
 * no fields containing data, just methods.  plan() and prepare() methods must never
 * modify any global variables or state; they may only modify the variables that they
 * are given as arguments.  (If global state must be modified, use SourceIterator.)
 *
 * Factories should be registered using the RegisterFactory() macro, defined
 * in Vertica.h.
 */
class FilterFactory : public UDLFactory
{
public:
    UDXFactory::UDXType getUDXFactoryType() { return LOAD_FILTER; }

    /**
     * Execute any planning logic required at query plan time.
     * This method is run once per query, during query initialization.
     * Its job is to perform parameter validation, and to modify the
     * set of nodes that the COPY statement will run on (through srvInterface).
     *
     * plan() runs exactly once per query, on the initiator node.
     * If it throws an exception, the query will not proceed; it will
     * be aborted prior to distributing the query to the other nodes and
     * running prepare().
     *
     * @param srvInterface Interface to server operations and functionality,
     *          including (not-per-column) parameter lookup
     *
     * @param planCtxt Context for storing and retrieving arbitrary data,
     *          for use just by this instance of this query.
     *          The same context is shared with plan().
     */
    virtual void plan(ServerInterface &srvInterface,
                      PlanContext &planCtxt) {}

    /**
     * Initialize a UDFilter.
     * This function will be called on each node, prior to the Load
     * operator starting to execute.
     *
     * 'planData' contains the same data that was placed there by the
     * plan() static method.
     *
     * @param srvInterface Interface to server operations and functionality,
     *          including (not-per-column) parameter lookup
     *
     * @param planCtxt Context for storing and retrieving arbitrary data,
     *          for use just by this instance of this query.
     *          The same context is shared with plan().
     *
     * @return UDFilter instance to use for this query
     */
    virtual UDFilter* prepare(ServerInterface &srvInterface,
                                PlanContext &planCtxt) = 0;
};


/**
 * @brief Produces and initializes UDParsers to parse input into tuples.
 * 
 * Note that ParserFactories are singletons.  Subclasses should be stateless, with
 * no fields containing data, just methods.  plan() and prepare() methods must never
 * modify any global variables or state; they may only modify the variables that they
 * are given as arguments.  (If global state must be modified, use SourceIterator.)
 *
 * Factories should be registered using the RegisterFactory() macro, defined
 * in Vertica.h.
 */
class ParserFactory : public UDLFactory
{
public:
    UDXFactory::UDXType getUDXFactoryType() { return LOAD_PARSER; }

    /**
     * Execute any planning logic required at query plan time.
     * This method is run once per query, during query initialization.
     * Its job is to perform parameter validation, and to modify the
     * set of nodes that the COPY statement will run on (through srvInterface).
     *
     * plan() runs exactly once per query, on the initiator node.
     * If it throws an exception, the query will not proceed; it will
     * be aborted prior to distributing the query to the other nodes and
     * running prepare().
     *
     * @param srvInterface Interface to server operations and functionality,
     *          including (not-per-column) parameter lookup
     *
     * @param perColumnParamReader Per-column parameters passed into the query
     *
     * @param planCtxt Context for storing and retrieving arbitrary data,
     *          for use just by this instance of this query.
     *          The same context is shared with plan().
     */
    virtual void plan(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt) {}

    // Returns an UDChunker if the associated parser supports the type of consumer/producer parallelism
    // returns NULL if it's not implemented
    virtual UDChunker* prepareChunker(ServerInterface &srvInterface,
                                      PerColumnParamReader &perColumnParamReader,
                                      PlanContext &planCtxt,
                                      const SizedColumnTypes &returnType)
    { return NULL;}

    /**
     * Instantiate a UDParser instance.
     * This function will be called on each node, prior to the Load
     * operator starting to execute.
     *
     * 'planData' contains the same data that was placed there by the
     * plan() static method.
     *
     * @param srvInterface Interface to server operations and functionality,
     *          including (not-per-column) parameter lookup
     *
     * @param perColumnParamReader Per-column parameters passed into the query
     *
     * @param planCtxt Context for storing and retrieving arbitrary data,
     *          for use just by this instance of this query.
     *          The same context is shared with plan().
     *
     * @param returnType The data types of the columns that this Parser must
     *          produce
     *
     * @return The UDParser instance to be used by this query
     */
    virtual UDParser* prepare(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt,
            const SizedColumnTypes &returnType) = 0;
    /**
     * Return true if this factory returns an UDParser that knows what to do with
     * InputState START_OF_PORTION and END_OF_PORTION.
     * Such a parser can start at some offset into that source and parse just a
     * portion of it
     *
     * (this func only called by SDK parsers)
     */
    virtual bool isParserApportionable() { return false;}

    /**
     * Return true if this factory returns a UDChunker that implements alignPortion()
     * and knows what to do with InputState END_OF_PORTION.
     * Such a chunker can start at some offset into a source and chunk just a
     * portion of it.
     *
     * This does not have to be the same as isParserApportionable() - the two
     * methods can return true/false independent of each other.
     */
    virtual bool isChunkerApportionable(ServerInterface &srvInterface) {
        return false;
    }

    /**
     * Function to tell Vertica what the return types (and length/precision if
     * necessary) of this UDX are.  Called, possibly multiple times, on each
     * node executing the query.
     *
     * The default provided implementation configures Vertica to use the same
     * output column types as the destination table.  This requires that the
     * UDParser validate the expected output column types and emit appropriate
     * tuples.
     * Note that the default provided implementation of this function should
     * be sufficient for most Parsers, so this method should not be overridden
     * by most Parser implementations.
     * If a COPY statement has a return type that doesn't match the destination
     * table, Vertica will emit an appropriate error.  Users can use COPY
     * expressions to perform typecasting and conversion if necessary.
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
     * @param srvInterface Interface to server operations and functionality,
     *          including (not-per-column) parameter lookup
     *
     * @param perColumnParamReader Per-column parameters passed into the query
     *
     * @param planCtxt Context for storing and retrieving arbitrary data,
     *          for use just by this instance of this query.
     *          The same context is shared with plan().
     *
     * @param argTypes Provides the data types of arguments that
     *          this UDT was called with. This may be used
     *          to modify the return types accordingly.
     *
     * @param returnType User code must fill in the names and data
     *          types returned by the UDT.
     */
    virtual void getParserReturnType(ServerInterface &srvInterface,
                                     PerColumnParamReader &perColumnParamReader,
                                     PlanContext &planCtxt,
                                     const SizedColumnTypes &argTypes,
                                     SizedColumnTypes &returnType) {
        // Parsers should generally have the same return type as their arg types.
        for (size_t i = 0; i < argTypes.getColumnCount(); i++) {
            returnType.addArg(argTypes.getColumnType(i), argTypes.getColumnName(i));
        }
    }

    /**
     * Inherited from the parent "UDXFactory" class in VerticaUDx.h
     */
    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {}
};

}

#endif
