/* POLE - Portable C++ library to access OLE Storage 
   Copyright (C) 2002-2005 Ariya Hidayat <ariya@kde.org>

   Performance optimization: Dmitry Fedorov 
   Copyright 2009 <www.bioimage.ucsb.edu> <www.dimin.net> 

   Fix for more than 236 mbat block entries : Michel Boudinot
   Copyright 2010 <Michel.Boudinot@inaf.cnrs-gif.fr>

   Considerable rework to allow for creation and updating of structured storage: Stephen Baum
   Copyright 2013 <srbaum@gmail.com>

   Added GetAllStreams, reworked datatypes
   Copyright 2013 Felix Gorny from Bitplane

   More datatype changes to allow for 32 and 64 bit code, some fixes involving incremental updates, flushing
   Copyright 2013 <srbaum@gmail.com>
   
   Version: 0.5.2

   Redistribution and use in source and binary forms, with or without 
   modification, are permitted provided that the following conditions 
   are met:
   * Redistributions of source code must retain the above copyright notice, 
     this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above copyright notice, 
     this list of conditions and the following disclaimer in the documentation 
     and/or other materials provided with the distribution.
   * Neither the name of the authors nor the names of its contributors may be 
     used to endorse or promote products derived from this software without 
     specific prior written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
   AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
   ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE 
   LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
   CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
   SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
   INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
   CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
   ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
   THE POSSIBILITY OF SUCH DAMAGE.
*/

#ifndef POLE_H
#define POLE_H

#include <cstdio>
#include <string>
#include <list>

namespace POLE
{

#if defined WIN32 || defined WIN64 || defined _WIN32 || defined _WIN64 || defined _MSVC
typedef __int32 int32;
typedef __int64 int64;
typedef unsigned __int32 uint32;
typedef unsigned __int64 uint64;
#else
typedef int int32;
typedef long long int64;
typedef unsigned int uint32;
typedef unsigned long long uint64;
#endif

class StorageIO;
class Stream;
class StreamIO;

class Storage
{
  friend class Stream;
  friend class StreamOut;

public:

  // for Storage::result()
  enum { Ok, OpenFailed, NotOLE, BadOLE, UnknownError, BadOp_ReadOnlyFile };
  
  /**
   * Constructs a storage with name filename.
   **/
  Storage( const char* filename );

  /**
   * Destroys the storage.
   **/
  ~Storage();
  
  /**
   * Opens the storage. Returns true if no error occurs.
   **/
  bool open(bool bWriteAccess = false, bool bCreate = false);

  /**
   * Closes the storage.
   **/
  void close();
  
  /**
   * Returns the error code of last operation.
   **/
  int result();

  /**
   * Finds all stream and directories in given path.
   **/
  std::list<std::string> entries( const std::string& path = "/" );
  
  /**
   * Returns true if specified entry name is a directory.
   */
  bool isDirectory( const std::string& name );

  /**
   * Returns true if specified entry name exists.
   */
  bool exists( const std::string& name );

  /**
   * Returns true if storage can be modified.
   */
  bool isWriteable();

  /**
   * Deletes a specified stream or directory. If directory, it will
   * recursively delete everything underneath said directory.
   * returns true for success
   */
  bool deleteByName( const std::string& name );

  /**
   * Returns an accumulation of information, hopefully useful for determining if the storage
   * should be defragmented.
   */

  void GetStats(size_t *pEntries, size_t *pUnusedEntries,
      size_t *pBigBlocks, size_t *pUnusedBigBlocks,
      size_t *pSmallBlocks, size_t *pUnusedSmallBlocks);

  std::list<std::string> GetAllStreams( const std::string& storageName );

  /**
   * Run some audits to verify the correctness of the open file. Returns 0 on success, else
   * a negative number and a string describing the problem.
   */

  int audit(std::string &errDesc);

  /**
   * Check for a particular problem in a KES file. The old approach of handling KES files on the Macintosh
   * could doubly commit some sectors, if any sectors were used by a DIFAT (in POLE terminology, the number
   * of doubly commited sectors equals the value in header->mbat_num. The sectors would be used both for
   * directory entries and for the big bat. Determine if a problem exists in the file, and further, give
   * an indication of its severity.
   */

  int auditForKESMacBug(bool bRepairIfPossible, bool *pRepaired);

  /**
   * write anything that has changed since the last time we flushed (or opened) this storage.
   */

  void flush();

  void setDebug(bool doDebug, bool toFile);
  

private:
  StorageIO* io;
  // no copy or assign
  Storage( const Storage& );
  Storage& operator=( const Storage& );

};

class Stream
{
  friend class Storage;
  friend class StorageIO;
  
public:

  /**
   * Creates a new stream.
   */
  // name must be absolute, e.g "/Workbook"
  Stream( Storage* storage, const std::string& name, bool bCreate = false, size_t streamSize = 0);

  /**
   * Destroys the stream.
   */
  ~Stream();

  /**
   * Returns the full stream name.
   */
  std::string fullName(); 
  
  /**
   * Returns the stream size.
   **/
  size_t size();

  /**
   * Changes the stream size (note this is done automatically if you write beyond the old size.
   * Use this primarily as a preamble to rewriting a stream that is already open. Of course, you
   * could simply delete the stream first).
   **/
  void setSize(size_t newSize);

  /**
   * Returns the current read/write position.
   **/
  size_t tell();

  long BlockIdx2FileBlockNum(size_t blockIdx);

  /**
   * Sets the read/write position.
   **/
  void seek( size_t pos ); 

  /**
   * Reads a byte.
   **/
  int32 getch();

  /**
   * Reads a block of data.
   **/
  size_t read( unsigned char* data, size_t maxlen );
  
  /**
   * Writes a block of data.
   **/
  size_t write( unsigned char* data, size_t len );

  /**
   * Makes sure that any changes for the stream (and the structured storage) have been written to disk.
   **/
  void flush();
  
  /**
   * Returns true if the read/write position is past the file.
   **/
  bool eof();
  
  /**
   * Returns true whenever error occurs.
   **/
  bool fail();

  /**
   * Indicates that the Stream is OK - that is, it has a StreamIO pointe
   **/
  bool isValid();

private:
  StreamIO* io;

  // no copy or assign
  Stream( const Stream& );
  Stream& operator=( const Stream& );    
};

};

#endif // POLE_H
