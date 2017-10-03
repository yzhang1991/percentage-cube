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
 * Description: Support code for Vertica Distributed File System (DFS)
 *
 * Create Date: April 14, 2013
 */
#ifndef VERTICA_DFS_H
#define VERTICA_DFS_H

#include "PGUDxShared.h"
#include "BasicsUDxShared.h"
#include "EEUDxShared.h"
#include "TimestampUDxShared.h"
#include "IntervalUDx.h"
#include "VerticaUDx.h"

/// @cond INTERNAL

class UdfSupport; // fwd declarations for Vertica internal classes
class UDxSideProcessInfo;

namespace Vertica
{

    class FileManager;
    class DFSFile;      // Equavalant to Java file path element. Represents
                    // both file and a directory
    class DFSFileReader;  // DFS file reader
    class DFSFileWriter;  // DFS file writer
    class Model;          // Class To represent Vertica Machine Learning Model

    /**
    * Defines the scope of the file. Used at the file creation time.
    **/
    enum DFSScope
    {
        NS_GLOBAL,      // Visible to all users, by default persistent and
                        // replicated as indicated by DISTRIBUTION
        NS_TRANSACTION, // Out of scope of Crane MS2. Visible to created
                        // user in current transaction, temp file.
        NS_SESSION      // Out of scope of Crane MS2. Visible to created
                        // user in current session, temp file.
    };

    /**
    * Defines how a file is replicated across nodes in the cluster. Used at
    * the file creation time.
    **/
    enum DFSDistribution
    {
        HINT_INITIATOR, // Out of scope of Crane MS2. Persisted on initiator
                        // node only
        HINT_REPLICATE, // Replicate file to all nodes.
        HINT_SEGMENTED  // Out of scope of Crane MS2. Segmented/chunked file
                        // across nodes.
    };

    /**
    * Internal DFSFile status to indicate it state.
    **/
    enum DFSFileStatus
    {
        WRITE_CREATED,  // New file created and open for writing
        WRITE_OPEN,    // Existing file, opened for writing
        READ_OPEN      // Existing file, opened for reading
    };


    /**
    * FileManager is a session level co-ordinator, which will be used by DFSFile,
    * DFSFileReader and DFSFileWriter to interact with Catalog and Storage system
    * of Vertica.

      If you modify this file, do the same modifications to:
        VerticaSDK/com/vertica/sdk/FileManager.java
        Commands/FileManagerPlan.h
    **/
    class FileManager
    {
        public:
        virtual ~FileManager() {}

        /**
        * Opens a file for reading.
        * @returns A unique identifier for the file opened. Return value is less
        * than 0 if there are errors.
        **/
        virtual vint openForRead(std::string fileName) = 0;

        /**
        * Reads 'size' of bytes into buffer pointed by 'ptr' from the file
        * identified by 'readerID'.
        * @return number of bytes read, 0 if no bytes were read, indicates the EOF.
        * throws exceptions if there are errors.
        **/
        virtual size_t read(vint readerID, void* ptr, size_t size) = 0;

        /*
        * Set the read position to a new place within the file stream.
        * @offset Number of bytes to offset from @whence
        * @whence Values could be SEEK_SET (0 - begining of the file), SEEK_CUR(1 - current position of the file pointer),
        * SEEK_END(2- End of the file)
        * @return resulting offset position as measured in bytes from begining of the file, throw an exception on error.
        */
        virtual off_t seek(vint readerID, off_t offset, int whence) =0;

        /**
        * Opens a file for writing.
        * @returns A unique identifier for the file opened. Return value is less
        * than 0 if there are errors.
        */
        virtual vint openForWrite(std::string fileName, DFSScope dfsScope, DFSDistribution dfsDistrib) = 0;

        /**
        * Writes 'size' of bytes into the file identified by 'writerID' from the
        * buffer pointed by 'ptr'.
        * @return number of bytes written, less than 0 if there are any errors.
        **/
        virtual size_t write(vint writerID, const void* ptr, size_t size) = 0;

        /**
        * Closes the file opened for reading, identified by 'readerID'
        */
        virtual void closeReader(vint readerID) = 0;

        /**
        * Closes the file opened for writing, identified by 'writerID'
        */
        virtual void closeWriter(vint writerID) = 0;

        /**
        * Deletes a DFS file, identified by full path 'fileName'.
        * @returns 0 if successful, throw exceptions if there are errors.
        **/
        virtual int deleteIt(std::string fileName, bool isRecursively) = 0;

        /**
        * Lists file under the path specified by 'fileName'
        * @returns a list of DFSFile found under the path.
        **/
        virtual void listFiles(std::string fileName,  std::vector<Vertica::DFSFile> &fileList) = 0;

        /**
        * Renames file identified by 'srcFilePath' to 'destFilePath'
        * returns 0, throws exceptions if there are errors.
        * virtual std::string rename(std::string srcFilePath, std::string destFilePath) = 0;
        **/

        /**
        * Copy a file/directory from 'srcFilePath' to 'destFilePath'.
        * returns 0, throws exceptions if there are errors.
        * virtual int copy(std::string srcFilePath, std::string destFilePath, bool isRecursively) = 0;
        **/

        /**
        * Make a directory, identified by 'dirPath'
        * returns 0, throws exceptions if there are errors.
        * virtual int makeDir(std::string dirPath) = 0;
        **/

        /**
        * Finalizes a plan/query/statement. Should only invoke on the initiator
        * node of a query. Complete file replication and commit metadata into
        * the catalog.
        * returns nothing, throws exceptions if there are errors.
        **/
        virtual void finalize() = 0;

        /**
        * Initialize a DFSFile upon constructing.
        * returns true if file exists in the DFS, false otherwise, throws exceptions if
        * there are errors.
        **/
        virtual bool initDFSFile(DFSFile &file) = 0;

        /*
         * Populate Model MetaData
         */
        virtual void populateModelMetaData(Model &m) = 0;

    }; //class FileManager

    /**
    * The main class used by users to initiate DFS operations.
    **/
    class DFSFile
    {

        public:
        /**
        * DFSFile INITIATION IS ONLY AVAILABLE DURING THE
        * PLANNING/SETUP AND FINALIZE/DESTROY PHASES OF A PLAN.
        * NOT AVAILABLE DURING EXECUTION/PROCESSING.
        **/
        DFSFile():fileName(""),fileManager(NULL),serverInterface(NULL){}

        DFSFile(ServerInterface &srvInterface):fileName(""), fileManager(srvInterface.fileManager),
            scope(NS_GLOBAL), dist(HINT_REPLICATE), status(READ_OPEN), isDirectory(false),
                isFfile(false),isExists(false), serverInterface(&srvInterface){}
        DFSFile(ServerInterface &srvInterface, std::string fName):fileName(fName), fileManager(srvInterface.fileManager),
                        scope(NS_GLOBAL), dist(HINT_REPLICATE), status(READ_OPEN), isDirectory(false),
                    isFfile(false), isExists(false), serverInterface(&srvInterface)
        {
            init();
        }
        // Overload for fenced mode
        DFSFile(const std::string &fName, FileManager *fmgr) :
          fileName(fName), fileManager(fmgr),
          scope(NS_GLOBAL), dist(HINT_REPLICATE),
          status(READ_OPEN), isDirectory(false),
          isFfile(false), isExists(false), size(0),
          serverInterface(NULL)
        {
                init();
        }

        // Overload if most of the field values are known. e.g. when retrieved from the Catalog.
        DFSFile(std::string fName, FileManager *fmgr, bool isDir, bool isFile, bool exists, vint fsize):
          fileName(fName), fileManager(fmgr),
          scope(NS_GLOBAL), dist(HINT_REPLICATE),
          status(READ_OPEN), isDirectory(isDir),
          isFfile(isFile), isExists(exists),size(fsize),
          serverInterface(NULL)
        {}


        ~DFSFile(){}

        void create(DFSScope dfsScope, DFSDistribution dfsDistrib);

        /**
        * THIS METHOD IS ONLY AVAILABLE DURING THE PLANNING/SETUP
        * AND FINALIZE/DESTROY PHASES OF A PLAN. NOT AVAILABLE
        * DURING EXECUTION/PROCESSING
        * Deletes a DFS file.
        * @returns 0 if successful, throw exceptions if there are errors.
        **/
        int deleteIt(bool isRecursively);

        /**
        * THIS METHOD IS ONLY AVAILABLE DURING THE PLANNING/SETUP
        * AND FINALIZE/DESTROY PHASES OF A PLAN. NOT AVAILABLE
        * DURING EXECUTION/PROCESSING
        * Lists file under the path specified by 'fileName'
        * @returns a list of DFSFile found under the path.
        **/
        void listFiles(std::vector<Vertica::DFSFile> &fileList);

        /**
        * THIS METHOD IS ONLY AVAILABLE DURING THE PLANNING/SETUP
        * AND FINALIZE/DESTROY PHASES OF A PLAN. NOT AVAILABLE
        * DURING EXECUTION/PROCESSING
        * Renames file identified by 'srcFilePath' to 'destFilePath'
        * returns 0, throws exceptions if there are errors.
        * int rename(std::string newName);
        **/

        /**
        * THIS METHOD IS ONLY AVAILABLE DURING THE PLANNING/SETUP
        * AND FINALIZE/DESTROY PHASES OF A PLAN. NOT AVAILABLE
        * DURING EXECUTION/PROCESSING
        * Copy a file/directory from 'srcFilePath' to 'destFilePath'.
        * returns 0, throws exceptions if there are errors.
        * int copy(DFSFile &dfsFile, bool isRecursively);
        **/

        /**
        * THIS METHOD IS ONLY AVAILABLE DURING THE PLANNING/SETUP
        * AND FINALIZE/DESTROY PHASES OF A PLAN. NOT AVAILABLE
        * DURING EXECUTION/PROCESSING
        * Make a directory, identified by 'dirPath'
        * returns 0, throws exceptions if there are errors.
        * int makeDir();
        **/

        /**
        * THIS METHOD IS ONLY AVAILABLE DURING THE PLANNING/SETUP
        * AND FINALIZE/DESTROY PHASES OF A PLAN. NOT AVAILABLE
        * DURING EXECUTION/PROCESSING
        **/
        void setName(std::string fName);

        /**
        * THIS METHOD IS ONLY AVAILABLE DURING THE PLANNING/SETUP
        * AND FINALIZE/DESTROY PHASES OF A PLAN. NOT AVAILABLE
        * DURING EXECUTION/PROCESSING
        **/
        std::string getName() const {return fileName;}

        /**
        * THIS METHOD IS ONLY AVAILABLE DURING THE PLANNING/SETUP
        * AND FINALIZE/DESTROY PHASES OF A PLAN. NOT AVAILABLE
        * DURING EXECUTION/PROCESSING
        **/
        void setFileManager(ServerInterface &srvInterface)
        {
            fileManager = srvInterface.fileManager;
        }

        void setFileManager(FileManager* fmgr)
        {
            fileManager = fmgr;
        }

        FileManager *getFileManager()
        {
            return fileManager;
        }

        /**
        * THIS METHOD IS ONLY AVAILABLE DURING THE PLANNING/SETUP
        * AND FINALIZE/DESTROY PHASES OF A PLAN. NOT AVAILABLE
        * DURING EXECUTION/PROCESSING
        * Check existence of the DFS file.
        * returns true if file exists in the DFS, false otherwise, throws exceptions if
        * there are errors.
        **/
        bool exists();

        void setScope(DFSScope dfsScope) { scope = dfsScope;}
        void setDistribution(DFSDistribution dfsDist) {dist = dfsDist;}
        void setStatus(DFSFileStatus dfsStatus) {status = dfsStatus;}
        void setDir(bool thisIsaDirectory) {isDirectory = thisIsaDirectory;}
        void setFile(bool thisIsaFile) { isFfile = thisIsaFile;}
        // Type of "size" is size_t, which is big enough to hold vint
        void setSize(vint fSize) {size=fSize;}
        bool isDir() {return isDirectory;}
        bool isFile() {return isFfile;}

        DFSScope getScope() {return scope;}
        DFSDistribution getDistribution() {return dist;}
        DFSFileStatus getStatus() {return status;}
        size_t getSize() {return size;}
        vint getFileWriter() {return fileWriter;}
        ServerInterface* getServerInterface(){return serverInterface;}

        private:

        std::string fileName;
        FileManager *fileManager;
        DFSScope scope;
        DFSDistribution dist;
        DFSFileStatus status;
        bool isDirectory;
        bool isFfile;
        bool isExists;
        size_t size;
        // In case if user call create() method.
        vint fileWriter;
        ServerInterface *serverInterface;
        /**
        * Do basic validations such as emptiness of file path
        * and existence of a pointer to FileManager instance.
        **/
        void validateFileOrThrow();
        /**
        * Initiates the DFS file by validating given path against
        * existing DFS.
        */
        void init();
    }; //class DFSFile

    /**
    * The file reader for reading existing DFS files.
    **/
    class DFSFileReader
    {
        /**
        * THESE METHODS ARE ONLY AVAILABLE DURING THE PLANNING/SETUP
        * AND FINALIZE/DESTROY PHASES OF A PLAN. NOT AVAILABLE
        * DURING EXECUTION/PROCESSING
        **/
        public:
        DFSFileReader():isOpenForRead(false), fileManager(0), fileReader(0),
                        fsize(0), fileName(""){}
        DFSFileReader(DFSFile &dfsFile):isOpenForRead(false), fileManager(dfsFile.getFileManager()),
            fileReader(0), fsize(dfsFile.getSize()), fileName(dfsFile.getName()){}

        /**
        * THIS METHOD IS ONLY AVAILABLE DURING THE PLANNING/SETUP
        * AND FINALIZE/DESTROY PHASES OF A PLAN. NOT AVAILABLE
        * DURING EXECUTION/PROCESSING
        * Opens a file for reading.
        **/
        void open();

        /**
        * Check whether file is opened for reading.
        * @returns true if it's open, false otherwise.
        **/
        bool isOpen() {return isOpenForRead;}

        /**
        * Reads 'size' of bytes into buffer pointed by 'ptr' from the file
        * opened for reading.
        * @return number of bytes read, 0 if no bytes were read, indicates the EOF.
        * throws exceptions if there are errors.
        **/
        size_t read(void* ptr, size_t size);

        /*
        * Set the read position to a new place within the file stream.
        * @offset Number of bytes to offset from @whence
        * @whence Values could be SEEK_SET (0 - begining of the file), SEEK_CUR(1 - current position of the file pointer),
        * SEEK_END(2- End of the file)
        * @return resulting offset position as measured in bytes from begining of the file, throw an exception on error.
        */
        off_t seek(off_t offset, int whence);

        /**
        * THIS METHOD IS ONLY AVAILABLE DURING THE PLANNING/SETUP
        * AND FINALIZE/DESTROY PHASES OF A PLAN. NOT AVAILABLE
        * DURING EXECUTION/PROCESSING
        * Closes the file opened for reading.
        **/
        void close();

        /**
        * Gets the size of the file.
        * returns size.
        **/
        size_t size(){   return fsize;}

        private:
        bool isOpenForRead;
        FileManager *fileManager;
        vint fileReader;
        size_t fsize;
        std::string fileName;

    };// class DFSFileReader

    /**
    * The file writer for writing data to and existing/newly created file.
    **/
    class DFSFileWriter
    {
        public:
        /**
        * THESE METHODS IS ONLY AVAILABLE DURING THE PLANNING/SETUP
        * AND FINALIZE/DESTROY PHASES OF A PLAN. NOT AVAILABLE
        * DURING EXECUTION/PROCESSING
        **/
        DFSFileWriter():isOpenForWrite(false), fileManager(0), fileWriter(0),
                                    fileName(""), scope(NS_GLOBAL), dist(HINT_REPLICATE){}
        DFSFileWriter(DFSFile &dfsFile):isOpenForWrite(false), fileManager(dfsFile.getFileManager()),
            fileWriter(0), fileName(dfsFile.getName()), scope(dfsFile.getScope()), dist(dfsFile.getDistribution())
        {
            if (dfsFile.getStatus() == WRITE_CREATED)
            {
                fileWriter = dfsFile.getFileWriter();
                isOpenForWrite = true;
            }
        }


        /**
        * THIS METHOD IS ONLY AVAILABLE DURING THE PLANNING/SETUP
        * AND FINALIZE/DESTROY PHASES OF A PLAN. NOT AVAILABLE
        * DURING EXECUTION/PROCESSING
        * Opens a file for writing.
        **/
        void open();

        /**
        * Check whether file is opened for writing.
        * @returns true if it's open, false otherwise.
        **/
        bool isOpen() { return isOpenForWrite;}

        /**
        * Writes 'size' of bytes into the file identified by 'writerID' from the
        * buffer pointed by 'ptr'.
        * @return number of bytes written, less than 0 if there are any errors.
        **/
        size_t write(const void* ptr, size_t size);

        /**
        * THIS METHOD IS ONLY AVAILABLE DURING THE PLANNING/SETUP
        * AND FINALIZE/DESTROY PHASES OF A PLAN. NOT AVAILABLE
        * DURING EXECUTION/PROCESSING
        * Closes the file opened for writing.
        **/
        void close();

        private:
        bool isOpenForWrite;
        FileManager *fileManager;
        vint fileWriter;
        std::string fileName;
        DFSScope scope;
        DFSDistribution dist;
    }; // class DFSFileWriter


    class Model{
    public:

        Model(ServerInterface &srvInterface, std::string modelName):
            fileManager(srvInterface.fileManager), modelName(modelName){
            fileManager->populateModelMetaData(*this);
        }

        std::string getModelRootDirectory() { return dfsDirectory; }
        std::string getModelName() { return modelName; }
        FileManager *getFileManager() { return fileManager; }
        std::string getOwner() { return owner; }
        std::string getModelType() { return modelType; }
        std::string getModelCategory() { return category; }
        bool isModelComplete() { return isComplete; }
        std::string getSchema() { return schema; }

        void setModelRootDirectory(std::string dfsDir) { dfsDirectory = dfsDir; }
        void setOwner(std::string o) { owner = o; }
        void setModelType(std::string mt) { modelType = mt; }
        void setModelCategory(std::string mc) { category = mc; }
        void setModelProgress(bool p) { isComplete = p; }
        void setSchema(std::string s) { schema = s; }

    private:

        FileManager *fileManager;
        std::string modelName;
        std::string owner;
        std::string schema;
        std::string modelType;
        std::string category;
        std::string dfsDirectory;
        bool isComplete;

    }; // class Model

} //namespace Vertica

/// @endcond

#endif // VERTICA_DFS_H
