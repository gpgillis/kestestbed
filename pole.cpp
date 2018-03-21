/* POLE - Portable C++ library to access OLE Storage
   Copyright (C) 2002-2005 Ariya Hidayat <ariya@kde.org>

   Performance optimization: Dmitry Fedorov
   Copyright 2009 <www.bioimage.ucsb.edu> <www.dimin.net>

   Fix for more than 236 mbat block entries : Michel Boudinot
   Copyright 2010 <Michel.Boudinot@inaf.cnrs-gif.fr>

   Considerable rework to allow for creation and updating of structured storage : Stephen Baum
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

#include <fstream>
#include <iostream>
#include <list>
#include <string>
#include <vector>
#include <queue>
#include <limits>

#include <cstring>

#include "pole.h"

// enable to activate debugging output
#define POLE_DEBUG
#define CACHEBUFSIZE 4096 //a presumably reasonable size for the read cache

namespace POLE
{

class Header
{
  public:
    unsigned char id[8];       // signature, or magic identifier
    size_t b_shift;          // bbat->blockSize = 1 << b_shift
    size_t s_shift;          // sbat->blockSize = 1 << s_shift
    size_t num_bat;          // blocks allocated for big bat
    size_t dirent_start;     // starting block for directory info
    size_t threshold;        // switch from small to big file (usually 4K)
    size_t sbat_start;       // starting block index to store small bat
    size_t num_sbat;         // blocks allocated for small bat
    size_t mbat_start;       // starting block to store meta bat
    size_t num_mbat;         // blocks allocated for meta bat
    size_t bb_blocks[109];
    bool dirty;                // Needs to be written

    Header();
    bool valid();
    void load( const unsigned char* buffer );
    void save( unsigned char* buffer );
    void debug(std::ofstream& outFile);
};

class AllocTable
{
  public:
    static const size_t Eof;
    static const size_t Avail;
    static const size_t Bat;
    static const size_t MetaBat;
    size_t blockSize;
    AllocTable();
    void clear();
    size_t count();
    size_t unusedCount();
	std::vector<size_t> getUnusedBlocks();
    void resize( size_t newsize );
    void preserve( size_t n );
    void set( size_t index, size_t val );
    size_t unused();
    void setChain( std::vector<size_t> );
    std::vector<size_t> follow( size_t start );
    size_t operator[](size_t index );
    void load( const unsigned char* buffer, size_t len );
    void save( unsigned char* buffer );
    size_t size();
    void debug(std::ofstream& outFile, std::string tableName);
    bool isDirty();
    void markAsDirty(size_t dataIndex, size_t bigBlockSize);
    void flush(std::vector<size_t> blocks, StorageIO *const io, size_t bigBlockSize);
  private:
    std::vector<size_t> data;
	size_t firstUnusedIdx;
    std::vector<size_t> dirtyBlocks;
    AllocTable( const AllocTable& );
    AllocTable& operator=( const AllocTable& );
};

class DirEntry
{
  public:
    DirEntry(): valid(), name(), dir(), size(), start(), prev(), next(), child() {}

    bool valid;            // false if invalid (should be skipped)
    std::string name;      // the name, not in unicode anymore
    bool dir;              // true if directory
    size_t size;		   // size (not valid if directory)
    size_t start;		   // starting block
    size_t prev;		   // previous sibling
    size_t next;		   // next sibling
    size_t child;		   // first child

    int compare(const DirEntry& de);
    int compare(const std::string& name2);
};

class DirTree
{
  public:
    static const size_t End;
    DirTree();
    void clear();
    inline size_t entryCount();
    size_t unusedEntryCount();
    DirEntry* entry( size_t index );
    DirEntry* entry( const std::string& name, bool create = false, size_t bigBlockSize = 0, StorageIO *const io = 0, size_t streamSize = 0);
    size_t indexOf( DirEntry* e );
    size_t parent( size_t index );
    std::string fullName( size_t index );
    std::vector<size_t> children( size_t index );
    size_t find_child( size_t index, const std::string& name, size_t &closest );
    void load( unsigned char* buffer, size_t len );
    void save( unsigned char* buffer, size_t bigBlockSize );
    size_t size();
    void debug(std::ofstream& outFile);
    bool isDirty();
    void markAsDirty(size_t dataIndex, size_t bigBlockSize);
    void flush(std::vector<size_t> blocks, StorageIO *const io, size_t bigBlockSize, size_t sb_start, size_t sb_size);
    size_t unused();
    void findParentAndSib(size_t inIdx, const std::string& inFullName, size_t &parentIdx, size_t &sibIdx);
    size_t findSib(size_t inIdx, size_t sibIdx);
    void deleteEntry(DirEntry *entry, const std::string& inFullName, size_t bigBlockSize);
  private:
    std::vector<DirEntry> entries;
    std::vector<size_t> dirtyBlocks;
    DirTree( const DirTree& );
    DirTree& operator=( const DirTree& );
};

class StorageIO
{
  public:
    Storage* storage;         // owner
    std::string filename;     // filename
    std::fstream file;        // associated with above name
    int32 result;               // result of operation
    bool opened;              // true if file is opened
	std::streamoff filesize;  // size of the file
    bool writeable;           // true if the file can be modified

    Header* header;           // storage header
    DirTree* dirtree;         // directory tree
    AllocTable* bbat;         // allocation table for big blocks
    AllocTable* sbat;         // allocation table for small blocks

    std::vector<size_t> sb_blocks; // blocks for "small" files
	size_t sb_length;		  // Length of the Ministream
    std::vector<size_t> mbat_blocks; // blocks for doubly indirect indices to big blocks
    std::vector<size_t> mbat_data; // the additional indices to big blocks
    bool mbatDirty;           // If true, mbat_blocks need to be written

    std::list<Stream*> streams;

    StorageIO( Storage* storage, const char* filename );
    ~StorageIO();

    bool open(bool bWriteAccess = false, bool bCreate = false);
    void close();
    void flush();
    void load(bool bWriteAccess);
    void create();
    void init();
    bool deleteByName(const std::string& fullName);
    void setDebug(bool doDebug, bool toFile);

    bool deleteNode(DirEntry *entry, const std::string& fullName);

    bool deleteLeaf(DirEntry *entry, const std::string& fullName);

    size_t loadBigBlocks( std::vector<size_t> blocks, size_t offset, unsigned char* buffer, size_t maxlen );

    size_t loadBigBlock( size_t block, size_t offset, unsigned char* buffer, size_t maxlen );

    size_t saveBigBlocks( std::vector<size_t> blocks, size_t offset, unsigned char* buffer, size_t len );

    size_t saveBigBlock( size_t block, size_t offset, unsigned char*buffer, size_t len );

    size_t loadSmallBlocks( std::vector<size_t> blocks, unsigned char* buffer, size_t maxlen );

    size_t loadSmallBlock( size_t block, unsigned char* buffer, size_t maxlen );

    size_t saveSmallBlocks( std::vector<size_t> blocks, size_t offset, unsigned char* buffer, size_t len, size_t startAtBlock = 0  );

    size_t saveSmallBlock( size_t block, size_t offset, unsigned char* buffer, size_t len );

    StreamIO* streamIO( const std::string& name, bool bCreate = false, size_t streamSize = 0 );

    void flushbbat();

    void flushsbat();

    std::vector<size_t> getbbatBlocks(bool bLoading);

    size_t ExtendFile( std::vector<size_t> *chain );

    void addbbatBlock();

	int verifyNodes(size_t nodeNum, std::string &errDesc);
	int nodeDataAccessible(size_t nodeNum, std::string &errDesc);
	int xlinkCheck(std::vector<size_t> &bigBlocks, std::vector<size_t> &smlBlocks, std::string &errDesc);
	int	xlinkCheckChain(std::vector<size_t> &blockDat, POLE::AllocTable *paTab, size_t start,
		bool bBig, size_t chainTag, std::string &errDesc);
	void chainTagName(size_t chainTag, std::string &name);
	int allocTableAudit(std::string &errDesc);
	int allocTableCheck(std::vector<size_t> &bigBlocks, std::vector<size_t> &smlBlocks, std::string &errDesc);
	int auditForKESMacBug(bool bRepairIfPossible);
	int checkKESMacChain(std::vector<size_t> &chain, size_t nBlocksExpected, size_t loBad, size_t hiBad,
		size_t *pReplacements);
	bool sectorInUse(size_t sectNum);
  private:
    // no copy or assign
    StorageIO( const StorageIO& );
    StorageIO& operator=( const StorageIO& );
    bool _doDebug = false;
    bool _toFile = false;
};

class StreamIO
{
  public:
    StorageIO* io;
    size_t entryIdx; //needed because a pointer to DirEntry will change whenever entries vector changes.
    std::string fullName;
    bool eof;
    bool fail;

    StreamIO( StorageIO* io, DirEntry* entry );
    ~StreamIO();
    size_t size();
    void setSize(size_t newSize);
    void seek( size_t pos );
    size_t tell();
    int32 getch();
    size_t read( unsigned char* data, size_t maxlen );
    size_t read( size_t pos, unsigned char* data, size_t maxlen );
    size_t write( unsigned char* data, size_t len );
    size_t write( size_t pos, unsigned char* data, size_t len );
    void flush();
	std::vector<size_t> getBlocks();
  private:
    std::vector<size_t> blocks;

    // no copy or assign
    StreamIO( const StreamIO& );
    StreamIO& operator=( const StreamIO& );

    // pointer for read
    size_t m_pos;

    // simple cache system to speed-up getch()
    unsigned char* cache_data;
    size_t cache_size;
    size_t cache_pos;
    void updateCache();
};

}; // namespace POLE

using namespace POLE;

static void fileCheck(std::fstream &file)
{
    bool bGood, bFail, bEof, bBad;
    bool bNOTOK;
    bGood = file.good();
    bFail = file.fail();
    bEof = file.eof();
    bBad = file.bad();
    if (bFail || bEof || bBad)
        bNOTOK = true; //this doesn't really do anything, but it is a good place to set a breakpoint!
    file.clear();
}

static inline size_t readU16( const unsigned char* ptr )
{
  return ptr[0]+(ptr[1]<<8);
}

static inline size_t readU32( const unsigned char* ptr )
{
  return ptr[0]+(ptr[1]<<8)+(ptr[2]<<16)+(ptr[3]<<24);
}

static inline void writeU16( unsigned char* ptr, size_t data )
{
  ptr[0] = (unsigned char)(data & 0xff);
  ptr[1] = (unsigned char)((data >> 8) & 0xff);
}

static inline void writeU32( unsigned char* ptr, size_t data )
{
  ptr[0] = (unsigned char)(data & 0xff);
  ptr[1] = (unsigned char)((data >> 8) & 0xff);
  ptr[2] = (unsigned char)((data >> 16) & 0xff);
  ptr[3] = (unsigned char)((data >> 24) & 0xff);
}

static const unsigned char pole_magic[] =
 { 0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1 };

// =========== Header ==========

Header::Header()
:   b_shift(9),                 // [1EH,02] size of sectors in power-of-two; typically 9 indicating 512-byte sectors
    s_shift(6),                 // [20H,02] size of mini-sectors in power-of-two; typically 6 indicating 64-byte mini-sectors
    num_bat(0),                 // [2CH,04] number of SECTs in the FAT chain
	dirent_start(AllocTable::Eof),            // [30H,04] first SECT in the directory chain
    threshold(4096),            // [38H,04] maximum size for a mini stream; typically 4096 bytes
	sbat_start(AllocTable::Eof),              // [3CH,04] first SECT in the MiniFAT chain
    num_sbat(0),                // [40H,04] number of SECTs in the MiniFAT chain
    mbat_start(AllocTable::Eof),// [44H,04] first SECT in the DIFAT chain
    num_mbat(0),                // [48H,04] number of SECTs in the DIFAT chain
    dirty(true)

{
  for( size_t i = 0; i < 8; i++ )
    id[i] = pole_magic[i];
  for( size_t i=0; i<109; i++ )
    bb_blocks[i] = AllocTable::Avail;
}

bool Header::valid()
{
  if( threshold != 4096 ) return false;
  if( num_bat == 0 ) return false;
  //if( (num_bat > 109) && (num_bat > (num_mbat * 127) + 109)) return false; // dima: incorrect check, number may be arbitrary larger
  if( (num_bat < 109) && (num_mbat != 0) ) return false;
  if( s_shift > b_shift ) return false;
  if( b_shift <= 6 ) return false;
  if( b_shift >=31 ) return false;

  return true;
}

void Header::load( const unsigned char* buffer ) {
  b_shift      = readU16( buffer + 0x1e ); // [1EH,02] size of sectors in power-of-two; typically 9 indicating 512-byte sectors and 12 for 4096
  s_shift      = readU16( buffer + 0x20 ); // [20H,02] size of mini-sectors in power-of-two; typically 6 indicating 64-byte mini-sectors
  num_bat      = readU32( buffer + 0x2c ); // [2CH,04] number of SECTs in the FAT chain
  dirent_start = readU32( buffer + 0x30 ); // [30H,04] first SECT in the directory chain
  threshold    = readU32( buffer + 0x38 ); // [38H,04] maximum size for a mini stream; typically 4096 bytes
  sbat_start   = readU32( buffer + 0x3c ); // [3CH,04] first SECT in the MiniFAT chain
  num_sbat     = readU32( buffer + 0x40 ); // [40H,04] number of SECTs in the MiniFAT chain
  mbat_start   = readU32( buffer + 0x44 ); // [44H,04] first SECT in the DIFAT chain
  num_mbat     = readU32( buffer + 0x48 ); // [48H,04] number of SECTs in the DIFAT chain

  for( size_t i = 0; i < 8; i++ )
    id[i] = buffer[i];

  // [4CH,436] the SECTs of first 109 FAT sectors
  for( size_t i=0; i<109; i++ )
    bb_blocks[i] = readU32( buffer + 0x4C+i*4 );
  dirty = false;
}

void Header::save( unsigned char* buffer )
{
  memset( buffer, 0, 0x4c );
  memcpy( buffer, pole_magic, 8 );        // ole signature
  writeU32( buffer + 8, 0 );              // unknown
  writeU32( buffer + 12, 0 );             // unknown
  writeU32( buffer + 16, 0 );             // unknown
  writeU16( buffer + 24, 0x003e );        // revision ?
  writeU16( buffer + 26, 3 );             // version ?
  writeU16( buffer + 28, 0xfffe );        // unknown
  writeU16( buffer + 0x1e, (size_t) b_shift );
  writeU16( buffer + 0x20, (size_t) s_shift );
  writeU32( buffer + 0x2c, (size_t) num_bat );
  writeU32( buffer + 0x30, (size_t) dirent_start );
  writeU32( buffer + 0x38, (size_t) threshold );
  writeU32( buffer + 0x3c, (size_t) sbat_start );
  writeU32( buffer + 0x40, (size_t) num_sbat );
  writeU32( buffer + 0x44, (size_t) mbat_start );
  writeU32( buffer + 0x48, (size_t) num_mbat );

  for( size_t i=0; i<109; i++ )
    writeU32( buffer + 0x4C+i*4, (size_t) bb_blocks[i] );
  dirty = false;
}

void Header::debug(std::ofstream& outFile)
{
    std::streambuf* coutbuf = std::cout.rdbuf();
    if (outFile.is_open())
        std::cout.rdbuf(outFile.rdbuf());

  std::cout << "Header:" << std::endl;
  std::cout << "b_shift " << b_shift << std::endl;
  std::cout << "s_shift " << s_shift << std::endl;
  std::cout << "num_bat " << num_bat << std::endl;
  std::cout << "dirent_start " << dirent_start << std::endl;
  std::cout << "threshold " << threshold << std::endl;
  std::cout << "sbat_start " << sbat_start << std::endl;
  std::cout << "num_sbat " << num_sbat << std::endl;
  std::cout << "mbat_start " << mbat_start << std::endl;
  std::cout << "num_mbat " << num_mbat << std::endl;

  size_t s = (num_bat<=109) ? num_bat : 109;
  std::cout << "bat blocks: ";
  for (size_t i = 0; i < s; i++)
  {
      std::cout << bb_blocks[i] << " ";
  }

  std::cout << std::endl;

  if (outFile.is_open())
      std::cout.rdbuf(coutbuf);
}

// =========== AllocTable ==========

const size_t AllocTable::Avail = 0xffffffff;
const size_t AllocTable::Eof = 0xfffffffe;
const size_t AllocTable::Bat = 0xfffffffd;
const size_t AllocTable::MetaBat = 0xfffffffc;

AllocTable::AllocTable()
:   blockSize(4096),
    data(),
	firstUnusedIdx(0),
    dirtyBlocks()
{
  // initial size
  resize( 128 );
}

size_t AllocTable::count()
{
  return static_cast<size_t>(data.size());
}

size_t AllocTable::unusedCount()
{
    size_t maxIdx = count();
    size_t nFound = 0;
    for (size_t idx = 0; idx < maxIdx; idx++)
    {
        if( data[idx] == Avail )
            nFound++;
    }
    return nFound;
}

std::vector<size_t> AllocTable::getUnusedBlocks()
{
	std::vector<size_t> toReturn;
	size_t maxIdx = count();
	for (size_t idx = 0; idx < maxIdx; idx++)
	{
		if (data[idx] == Avail)
			toReturn.push_back(idx);
	}
	return toReturn;
}

void AllocTable::resize( size_t newsize )
{
	newsize = (((newsize-1)/128)+1)*128;	//resize in blocksize chunks - 128 elements per block
	size_t oldsize = static_cast<size_t>(data.size());
	data.resize( newsize );
	if( newsize > oldsize )
	{
		for( size_t i = oldsize; i<newsize; i++ )
			data[i] = Avail;
	}
	if (oldsize < firstUnusedIdx)
		firstUnusedIdx = oldsize;
}

// make sure there're still free blocks (probably doesn't work really if n>1 SRB 10/25/2013)
void AllocTable::preserve( size_t n )
{
  std::vector<size_t> pre;
  for( size_t i=0; i < n; i++ )
    pre.push_back( unused() );
}

size_t AllocTable::operator[]( size_t index )
{
  size_t result;
  result = data[index];
  return result;
}

void AllocTable::set( size_t index, size_t value )
{
  if( index >= count() ) resize(index+1);
  data[ index ] = value;
  if (value == Avail && index < firstUnusedIdx)
	  firstUnusedIdx = index;
}

void AllocTable::setChain( std::vector<size_t> chain )
{
  if( chain.size() )
  {
    for( size_t i=0; i<chain.size()-1; i++ )
      set( chain[i], chain[i+1] );
    set( chain[ chain.size()-1 ], AllocTable::Eof );
  }
}

// follow
std::vector<size_t> AllocTable::follow( size_t start )
{
  std::vector<size_t> chain;

  if( start >= count() ) return chain;

  size_t p = start;
  while( p < count() )
  {
    if( p == (size_t)Eof ) break;
    if( p == (size_t)Bat ) break;
    if( p == (size_t)MetaBat ) break;
    if( p >= count() ) break;
    chain.push_back( p );
    if( data[p] >= count() ) break;
    p = data[ p ];
  }

  return chain;
}

size_t AllocTable::unused()
{
  // find first available block
  size_t maxIdx = data.size();
  for( size_t i = firstUnusedIdx; i < maxIdx; i++ )
  {
    if( data[i] == Avail )
	{
	  firstUnusedIdx = i;
      return i;
	}
  }
  // completely full, so enlarge the table
  size_t block = maxIdx;
  resize( maxIdx+1 );
  firstUnusedIdx = block;
  return block;
}

void AllocTable::load( const unsigned char* buffer, size_t len )
{
  resize( len / 4 );
  for( size_t i = 0; i < count(); i++ )
    set( i, readU32( buffer + i*4 ) );
}

// return space required to save this dirtree
size_t AllocTable::size()
{
  return count() * 4;
}

void AllocTable::save( unsigned char* buffer )
{
  for( size_t i = 0; i < count(); i++ )
    writeU32( buffer + i*4, (size_t) data[i] );
}

bool AllocTable::isDirty()
{
    return (dirtyBlocks.size() > 0);
}

void AllocTable::markAsDirty(size_t dataIndex, size_t bigBlockSize)
{
    size_t dbidx = dataIndex / (bigBlockSize / 4); //was sizeof(size_t), which is very bad for 64 bit code. Allocation entries are always 4 bytes long.
    for (size_t idx = 0; idx < static_cast<size_t>(dirtyBlocks.size()); idx++)
    {
        if (dirtyBlocks[idx] == dbidx)
            return;
    }
    dirtyBlocks.push_back(dbidx);
}

void AllocTable::flush(std::vector<size_t> blocks, StorageIO *const io, size_t bigBlockSize)
{
    unsigned char *buffer = new unsigned char[bigBlockSize * blocks.size()];
    save(buffer);
    for (size_t idx = 0; idx < static_cast<size_t>(blocks.size()); idx++)
    {
        bool bDirty = false;
        for (size_t idx2 = 0; idx2 < static_cast<size_t>(dirtyBlocks.size()); idx2++)
        {
            if (dirtyBlocks[idx2] == idx)
            {
                bDirty = true;
                break;
            }
        }
        if (bDirty)
            io->saveBigBlock(blocks[idx], 0, &buffer[bigBlockSize*idx], bigBlockSize);
    }
    dirtyBlocks.clear();
    delete[] buffer;
}

void AllocTable::debug(std::ofstream& outFile, std::string tableName)
{
    std::streambuf* coutbuf = std::cout.rdbuf();
    if (outFile.is_open())
        std::cout.rdbuf(outFile.rdbuf());

    std::cout << tableName << ":" << std::endl;
    std::cout << "block size " << data.size() << std::endl;
    for( size_t i=0; i< data.size(); i++ )
    {
        if( data[i] == Avail ) continue;
        std::cout << i << ": ";
        if( data[i] == Eof ) std::cout << "[eof]";
        else if( data[i] == Bat ) std::cout << "[bat]";
        else if( data[i] == MetaBat ) std::cout << "[metabat]";
        else std::cout << data[i];
        std::cout << std::endl;
    }

    if (outFile.is_open())
        std::cout.rdbuf(coutbuf);
}

// =========== DirEntry ==========
// "A node with a shorter name is less than a node with a inter name"
// "For nodes with the same length names, compare the two names."
// --Windows Compound Binary File Format Specification, Section 2.5
int DirEntry::compare(const DirEntry& de)
{
    return compare(de.name);
}

int DirEntry::compare(const std::string& name2)
{
    if (name.length() < name2.length())
        return -1;
    else if (name.length() > name2.length())
        return 1;
    else
        return name.compare(name2);
}

// =========== DirTree ==========

const size_t DirTree::End = 0xffffffff;

DirTree::DirTree()
:   entries(),
    dirtyBlocks()
{
  clear();
}

void DirTree::clear()
{
  // leave only root entry
  entries.resize( 1 );
  entries[0].valid = true;
  entries[0].name = "Root Entry";
  entries[0].dir = true;
  entries[0].size = 0;
  entries[0].start = End;
  entries[0].prev = End;
  entries[0].next = End;
  entries[0].child = End;
}

inline size_t DirTree::entryCount()
{
  return entries.size();
}

size_t DirTree::unusedEntryCount()
{
    size_t nFound = 0;
    for (size_t idx = 0; idx < entryCount(); idx++)
    {
        if (!entries[idx].valid)
            nFound++;
    }
    return nFound;
}

DirEntry* DirTree::entry( size_t index )
{
  if( index >= entryCount() ) return (DirEntry*) 0;
  return &entries[ index ];
}

size_t DirTree::indexOf( DirEntry* e )
{
  for( size_t i = 0; i < entryCount(); i++ )
    if( entry( i ) == e ) return i;

  return 0;
}

size_t DirTree::parent( size_t index )
{
  // brute-force, basically we iterate for each entries, find its children
  // and check if one of the children is 'index'
  for( size_t j=0; j<entryCount(); j++ )
  {
    std::vector<size_t> chi = children( j );
    for( size_t i=0; i<chi.size();i++ )
      if( chi[i] == index )
        return j;
  }

  return 0;
}

std::string DirTree::fullName( size_t index )
{
  // don't use root name ("Root Entry"), just give "/"
  if( index == 0 ) return "/";

  std::string result = entry( index )->name;
  result.insert( 0,  "/" );
  size_t p = parent( index );
  DirEntry * _entry = 0;
  while( p > 0 )
  {
    _entry = entry( p );
    if (_entry->dir && _entry->valid)
    {
      result.insert( 0,  _entry->name);
      result.insert( 0,  "/" );
    }
    --p;
    index = p;
    if( index <= 0 ) break;
  }
  return result;
}

// given a fullname (e.g "/ObjectPool/_1020961869"), find the entry
// if not found and create is false, return 0
// if create is true, a new entry is returned
DirEntry* DirTree::entry( const std::string& name, bool create, size_t bigBlockSize, StorageIO *const io, size_t streamSize)
{
   if( !name.length() ) return (DirEntry*)0;

   // quick check for "/" (that's root)
   if( name == "/" ) return entry( 0 );

   // split the names, e.g  "/ObjectPool/_1020961869" will become:
   // "ObjectPool" and "_1020961869"
   std::list<std::string> names;
   std::string::size_type start = 0, end = 0;
   if( name[0] == '/' ) start++;
   int levelsLeft = 0;
   while( start < name.length() )
   {
     end = name.find_first_of( '/', start );
     if( end == std::string::npos ) end = name.length();
     names.push_back( name.substr( start, end-start ) );
     levelsLeft++;
     start = end+1;
   }

   // start from root
   size_t index = 0 ;

   // trace one by one
   std::list<std::string>::iterator it;

   for( it = names.begin(); it != names.end(); ++it )
   {
     // find among the children of index
     levelsLeft--;
     size_t child = 0;


     /*
     // dima: this block is really inefficient
     std::vector<unsigned> chi = children( index );
     for( unsigned i = 0; i < chi.size(); i++ )
     {
       DirEntry* ce = entry( chi[i] );
       if( ce )
       if( ce->valid && ( ce->name.length()>1 ) )
       if( ce->name == *it ) {
             child = chi[i];
             break;
       }
     }
     */
     // dima: performance optimisation of the previous
     size_t closest = End;
     child = find_child( index, *it, closest );

     // traverse to the child
     if( child > 0 ) index = child;
     else
     {
       // not found among children
       if( !create || !io->writeable) return (DirEntry*)0;

       // create a new entry
       size_t parent2 = index;
       index = unused();
       DirEntry* e = entry( index );
       e->valid = true;
       e->name = *it;
       e->dir = (levelsLeft > 0);
       if (!e->dir)
           e->size = streamSize;
       else
           e->size = 0;
       e->start = AllocTable::Eof;
       e->child = End;
       if (closest == End)
       {
           e->prev = End;
           e->next = entry(parent2)->child;
           entry(parent2)->child = index;
           markAsDirty(parent2, bigBlockSize);
       }
       else
       {
           DirEntry* closeE = entry( closest );
           if (closeE->compare(*e) < 0)
           {
               e->prev = closeE->next;
               e->next = End;
               closeE->next = index;
           }
           else
           {
               e->next = closeE->prev;
               e->prev = End;
               closeE->prev = index;
           }
           markAsDirty(closest, bigBlockSize);
       }
       markAsDirty(index, bigBlockSize);
       size_t bbidx = index / (bigBlockSize / 128);
       std::vector <size_t> blocks = io->bbat->follow(io->header->dirent_start);
       while (blocks.size() <= bbidx)
       {
           size_t nblock = io->bbat->unused();
           if (blocks.size() > 0)
           {
               io->bbat->set(blocks[static_cast<size_t>(blocks.size())-1], nblock);
               io->bbat->markAsDirty(blocks[static_cast<size_t>(blocks.size())-1], bigBlockSize);
           }
		   else
		   {
			   io->header->dirent_start = nblock;
			   io->header->dirty = true;
		   }
           io->bbat->set(nblock, AllocTable::Eof);
           io->bbat->markAsDirty(nblock, bigBlockSize);
           blocks.push_back(nblock);
           size_t bbidx2 = nblock / (io->bbat->blockSize / 4);  //was sizeof(size_t), which is very bad for 64 bit code. Allocation entries are always 4 bytes long.
           while (bbidx2 >= io->header->num_bat)
               io->addbbatBlock();
       }
     }
   }

   return entry( index );
}

// helper function: recursively find siblings of index
static
void dirtree_find_siblings( DirTree* dirtree, std::vector<size_t>& result,
  size_t index )
{
    DirEntry* e = dirtree->entry( index );
    if (!e) return;
    if (e->prev != DirTree::End)
        dirtree_find_siblings(dirtree, result, e->prev);
    result.push_back(index);
    if (e->next != DirTree::End)
        dirtree_find_siblings(dirtree, result, e->next);
}

std::vector<size_t> DirTree::children( size_t index )
{
  std::vector<size_t> result;

  DirEntry* e = entry( index );
  if( e ) if( e->valid && e->child < entryCount() )
    dirtree_find_siblings( this, result, e->child );

  return result;
}

static
size_t dirtree_find_sibling( DirTree* dirtree, size_t index, const std::string& name, size_t& closest ) {

    size_t count = dirtree->entryCount();
    DirEntry* e = dirtree->entry( index );
    if (!e || !e->valid) return 0;
    int cval = e->compare(name);
    if (cval == 0)
        return index;
    if (cval > 0)
    {
        if (e->prev > 0 && e->prev < count)
            return dirtree_find_sibling( dirtree, e->prev, name, closest );
    }
    else
    {
        if (e->next > 0 && e->next < count)
            return dirtree_find_sibling( dirtree, e->next, name, closest );
    }
    closest = index;
    return 0;
}

size_t DirTree::find_child( size_t index, const std::string& name, size_t& closest ) {

  size_t count = entryCount();
  DirEntry* p = entry( index );
  if (p && p->valid && p->child < count )
    return dirtree_find_sibling( this, p->child, name, closest );

  return 0;
}

void DirTree::load( unsigned char* buffer, size_t in_size )
{
  entries.clear();

  for( size_t i = 0; i < in_size/128; i++ )
  {
    size_t p = i * 128;

    // would be < 32 if first char in the name isn't printable
    size_t prefix = 32;

    // parse name of this entry, which stored as Unicode 16-bit
    std::string name;
    size_t name_len = readU16( buffer + 0x40+p );
    if( name_len > 64 )
		name_len = 64;
    for( int j=0; ( buffer[j+p]) && (j<(int)name_len); j+= 2 )
      name.append( 1, buffer[j+p] );

    // first char isn't printable ? remove it...
    if( buffer[p] < 32 )
    {
      prefix = buffer[0];
      name.erase( 0,1 );
    }

    // 2 = file (aka stream), 1 = directory (aka storage), 5 = root
    size_t type = buffer[ 0x42 + p];

    DirEntry e;
    e.valid = ( type != 0 );
    e.name = name;
    e.start = readU32( buffer + 0x74+p );
    e.size = readU32( buffer + 0x78+p );
    e.prev = readU32( buffer + 0x44+p );
    e.next = readU32( buffer + 0x48+p );
    e.child = readU32( buffer + 0x4C+p );
    e.dir = ( type!=2 );

    // sanity checks
    if( (type != 2) && (type != 1 ) && (type != 5 ) ) e.valid = false;
    if( name_len < 1 ) e.valid = false;

    entries.push_back( e );
  }
}

// return space required to save this dirtree
size_t DirTree::size()
{
  return entryCount() * 128;
}

void DirTree::save( unsigned char* buffer, size_t bigBlockSize )
{
  memset( buffer, 0, size() );

  // root is fixed as "Root Entry"
  DirEntry* root = entry( 0 );
  std::string name = "Root Entry";
  for( size_t j = 0; j < name.length(); j++ )
    buffer[ j*2 ] = name[j];
  writeU16( buffer + 0x40, static_cast<size_t>(name.length()*2 + 2) );
  writeU32( buffer + 0x74, 0xffffffff );
  writeU32( buffer + 0x78, 0 );
  writeU32( buffer + 0x44, 0xffffffff );
  writeU32( buffer + 0x48, 0xffffffff );
  writeU32( buffer + 0x4c, (size_t) root->child );
  buffer[ 0x42 ] = 5;
  //buffer[ 0x43 ] = 1;

  for( size_t i = 1; i < entryCount(); i++ )
  {
    DirEntry* e = entry( i );
    if( !e ) continue;
    if( e->dir )
    {
      e->start = 0xffffffff;
      e->size = 0;
    }

    // max length for name is 32 chars
    name = e->name;
    if( name.length() > 32 )
      name.erase( 32, name.length() );

    // write name as Unicode 16-bit
    for( size_t j = 0; j < name.length(); j++ )
      buffer[ i*128 + j*2 ] = name[j];

	size_t nameLen = name.length()*2 + 2;
	if (nameLen > 64)
	{
		nameLen = 64;
		markAsDirty(i, bigBlockSize);
	}
    writeU16( buffer + i*128 + 0x40, nameLen );
    writeU32( buffer + i*128 + 0x74, (size_t) e->start );
    writeU32( buffer + i*128 + 0x78, (size_t) e->size );
    writeU32( buffer + i*128 + 0x44, (size_t) e->prev );
    writeU32( buffer + i*128 + 0x48, (size_t) e->next );
    writeU32( buffer + i*128 + 0x4c, (size_t) e->child );
    if (!e->valid)
        buffer[ i*128 + 0x42 ] = 0; //STGTY_INVALID
    else
        buffer[ i*128 + 0x42 ] = e->dir ? 1 : 2; //STGTY_STREAM or STGTY_STORAGE
    buffer[ i*128 + 0x43 ] = 1; // always black
  }
}

bool DirTree::isDirty()
{
    return (dirtyBlocks.size() > 0);
}


void DirTree::markAsDirty(size_t dataIndex, size_t bigBlockSize)
{
    size_t dbidx = dataIndex / (bigBlockSize / 128);
    for (size_t idx = 0; idx < static_cast<size_t>(dirtyBlocks.size()); idx++)
    {
        if (dirtyBlocks[idx] == dbidx)
            return;
    }
    dirtyBlocks.push_back(dbidx);
}

void DirTree::flush(std::vector<size_t> blocks, StorageIO *const io, size_t bigBlockSize, size_t sb_start, size_t sb_size)
{
	// It is very important to initialize directory entries for every entry that can be written in every block.
	// We'll do that here.
	size_t entriesPerBlock = bigBlockSize / 128;
	while (entryCount() % entriesPerBlock)
	{
		markAsDirty(entryCount()-1, bigBlockSize);
		entries.push_back(DirEntry());
	}
    size_t bufLen = size();
    unsigned char *buffer = new unsigned char[bufLen];
    save(buffer, bigBlockSize);
    writeU32( buffer + 0x74, (size_t) sb_start );
    writeU32( buffer + 0x78, (size_t) sb_size );
	markAsDirty(0, bigBlockSize);
    for (size_t idx = 0; idx < static_cast<size_t>(blocks.size()); idx++)
    {
        bool bDirty = false;
        for (size_t idx2 = 0; idx2 < static_cast<size_t>(dirtyBlocks.size()); idx2++)
        {
            if (dirtyBlocks[idx2] == idx)
            {
                bDirty = true;
                break;
            }
        }
        size_t bytesToWrite = bigBlockSize;
        size_t pos = bigBlockSize*idx;
        if ((bufLen - pos) < bytesToWrite)
            bytesToWrite = bufLen - pos;
        if (bDirty)
            io->saveBigBlock(blocks[idx], 0, &buffer[pos], bytesToWrite);
    }
    dirtyBlocks.clear();
    delete[] buffer;
}

size_t DirTree::unused()
{
    for (size_t idx = 0; idx < static_cast<size_t>(entryCount()); idx++)
    {
        if (!entries[idx].valid)
            return idx;
    }
    entries.push_back(DirEntry());
    return entryCount()-1;
}

// Utility function to get the index of the parent dirEntry, given that we already have a full name it is relatively fast.
// Then look for a sibling dirEntry that points to inIdx. In some circumstances, the dirEntry at inIdx will be the direct child
// of the parent, in which case sibIdx will be returned as 0. A failure is indicated if both parentIdx and sibIdx are returned as 0.

void DirTree::findParentAndSib(size_t inIdx, const std::string& inFullName, size_t& parentIdx, size_t& sibIdx)
{
    sibIdx = 0;
    parentIdx = 0;
    if (inIdx == 0 || inIdx >= entryCount() || inFullName == "/" || inFullName == "")
        return;
    std::string localName = inFullName;
    if (localName[0] != '/')
        localName = '/' + localName;
    std::string parentName = localName;
    if (parentName[parentName.size()-1] == '/')
        parentName = parentName.substr(0, parentName.size()-1);
    std::string::size_type lastSlash;
    lastSlash = parentName.find_last_of('/');
    if (lastSlash == std::string::npos)
        return;
    if (lastSlash == 0)
        lastSlash = 1; //leave root
    parentName = parentName.substr(0, lastSlash);
    DirEntry *parent2 = entry(parentName);
    parentIdx = indexOf(parent2);
    if (parent2->child == inIdx)
        return; //successful return, no sibling points to inIdx
    sibIdx = findSib(inIdx, parent2->child);
}

// Utility function to get the index of the sibling dirEntry which points to inIdx. It is the responsibility of the original caller
// to start with the root sibling - i.e., sibIdx should be pointed to by the parent node's child.

size_t DirTree::findSib(size_t inIdx, size_t sibIdx)
{
    DirEntry *sib = entry(sibIdx);
    if (!sib || !sib->valid)
        return 0;
    if (sib->next == inIdx || sib->prev == inIdx)
        return sibIdx;
    DirEntry *targetSib = entry(inIdx);
    int cval = sib->compare(*targetSib);
    if (cval > 0)
        return findSib(inIdx, sib->prev);
    else
        return findSib(inIdx, sib->next);
}

void DirTree::deleteEntry(DirEntry *dirToDel, const std::string& inFullName, size_t bigBlockSize)
{
    size_t parentIdx;
    size_t sibIdx;
    size_t inIdx = indexOf(dirToDel);
    size_t nEntries = entryCount();
    findParentAndSib(inIdx, inFullName, parentIdx, sibIdx);
    size_t replIdx;
    if (!dirToDel->next || dirToDel->next > nEntries)
        replIdx = dirToDel->prev;
    else
    {
        DirEntry *sibNext = entry(dirToDel->next);
        if (!sibNext->prev || sibNext->prev > nEntries)
        {
            replIdx = dirToDel->next;
            sibNext->prev = dirToDel->prev;
            markAsDirty(replIdx, bigBlockSize);
        }
        else
        {
            DirEntry *smlSib = sibNext;
            size_t smlIdx = dirToDel->next;
            DirEntry *smlrSib;
            size_t smlrIdx;
            for ( ; ; )
            {
                smlrIdx = smlSib->prev;
                smlrSib = entry(smlrIdx);
                if (!smlrSib->prev || smlrSib->prev > nEntries)
                    break;
                smlSib = smlrSib;
                smlIdx = smlrIdx;
            }
            replIdx = smlSib->prev;
            smlSib->prev = smlrSib->next;
            smlrSib->prev = dirToDel->prev;
            smlrSib->next = dirToDel->next;
            markAsDirty(smlIdx, bigBlockSize);
            markAsDirty(smlrIdx, bigBlockSize);
        }
    }
    if (sibIdx)
    {
        DirEntry *sib = entry(sibIdx);
        if (sib->next == inIdx)
            sib->next = replIdx;
        else
            sib->prev = replIdx;
        markAsDirty(sibIdx, bigBlockSize);
    }
    else
    {
        DirEntry *parNode = entry(parentIdx);
        parNode->child = replIdx;
        markAsDirty(parentIdx, bigBlockSize);
    }
    dirToDel->valid = false; //indicating that this entry is not in use
    markAsDirty(inIdx, bigBlockSize);
}

void DirTree::debug(std::ofstream& outFile)
{
    std::streambuf* coutbuf = std::cout.rdbuf();
    if (outFile.is_open())
        std::cout.rdbuf(outFile.rdbuf());

    std::cout << "DirTree:" << std::endl;

    for( size_t i = 0; i < entryCount(); i++ )
    {
    DirEntry* e = entry( i );
    if( !e ) continue;
    std::cout << i << ": ";
    if( !e->valid ) std::cout << "INVALID ";
    std::cout << e->name << " ";
    if( e->dir ) std::cout << "(Dir) ";
    else std::cout << "(File) ";
    std::cout << e->size << " ";
    std::cout << "s:" << e->start << " ";
    std::cout << "(";
    if( e->child == End ) std::cout << "-"; else std::cout << e->child;
    std::cout << " ";
    if( e->prev == End ) std::cout << "-"; else std::cout << e->prev;
    std::cout << ":";
    if( e->next == End ) std::cout << "-"; else std::cout << e->next;
    std::cout << ")";
    std::cout << std::endl;
    }

    if (outFile.is_open())
        std::cout.rdbuf(coutbuf);
}

// =========== StorageIO ==========

StorageIO::StorageIO( Storage* st, const char* fname )
: storage(st),
  filename(fname),
  file(),
  result(Storage::Ok),
  opened(false),
  filesize(0),
  writeable(false),
  header(new Header()),
  dirtree(new DirTree()),
  bbat(new AllocTable()),
  sbat(new AllocTable()),
  sb_blocks(),
  mbat_blocks(),
  mbat_data(),
  mbatDirty(),
  streams()
{
  bbat->blockSize = (size_t) 1 << header->b_shift;
  sbat->blockSize = (size_t) 1 << header->s_shift;
}

StorageIO::~StorageIO()
{
  if( opened ) close();
  delete sbat;
  delete bbat;
  delete dirtree;
  delete header;
}

bool StorageIO::open(bool bWriteAccess, bool bCreate)
{
  // already opened ? close first
  if (opened)
      close();
  if (bCreate)
  {
      create();
      init();
      writeable = true;
  }
  else
  {
      writeable = bWriteAccess;
      load(bWriteAccess);
  }

  return result == Storage::Ok;
}

void StorageIO::load(bool bWriteAccess)
{
  unsigned char* buffer = 0;
  size_t buflen = 0;
  std::vector<size_t> blocks;

  // open the file, check for error
  result = Storage::OpenFailed;

  if (bWriteAccess)
      file.open( filename.c_str(), std::ios::binary | std::ios::in | std::ios::out );
  else
      file.open( filename.c_str(), std::ios::binary | std::ios::in );
  if( !file.good() ) return;

  // find size of input file
  file.seekg(0, std::ios::end );
  filesize = (file.tellg());

  // load header
  buffer = new unsigned char[512];
  file.seekg( 0 );
  file.read( (char*)buffer, 512 );
  fileCheck(file);
  header->load( buffer );
  delete[] buffer;

  // check OLE magic id
  result = Storage::NotOLE;
  for( size_t i=0; i<8; i++ )
    if( header->id[i] != pole_magic[i] )
      return;

  // sanity checks
  result = Storage::BadOLE;
  if( !header->valid() ) return;
  if( header->threshold != 4096 ) return;

  // important block size
  bbat->blockSize = (size_t) 1 << header->b_shift;
  sbat->blockSize = (size_t) 1 << header->s_shift;

  blocks = getbbatBlocks(true);

  // load big bat
  buflen = static_cast<size_t>(blocks.size())*bbat->blockSize;
  if( buflen > 0 )
  {
    buffer = new unsigned char[ buflen ];
    loadBigBlocks( blocks, 0, buffer, buflen );
    bbat->load( buffer, buflen );
    delete[] buffer;
  }

  // load small bat
  blocks.clear();
  blocks = bbat->follow( header->sbat_start );
  buflen = static_cast<size_t>(blocks.size())*bbat->blockSize;
  if( buflen > 0 )
  {
    buffer = new unsigned char[ buflen ];
    loadBigBlocks( blocks, 0, buffer, buflen );
    sbat->load( buffer, buflen );
    delete[] buffer;
  }

  // load directory tree
  blocks.clear();
  blocks = bbat->follow( header->dirent_start );
  buflen = static_cast<size_t>(blocks.size())*bbat->blockSize;
  buffer = new unsigned char[ buflen ];
  loadBigBlocks( blocks, 0, buffer, buflen );
  dirtree->load( buffer, buflen );
  size_t sb_start = readU32( buffer + 0x74 );
  sb_length = readU32( buffer + 0x78 );
  delete[] buffer;

  // fetch block chain as data for small-files
  sb_blocks = bbat->follow( sb_start ); // small files

  // for troubleshooting, just enable this block
  if (_doDebug)
  {
    std::ofstream results;
    if (_toFile)
        results.open(filename + ".DEBUG", std::fstream::out);

    header->debug(results);
    sbat->debug(results, "Small Block");
    bbat->debug(results, "Large Block");
    dirtree->debug(results);
    if (results.is_open())
    results.close();
  }

  // so far so good
  result = Storage::Ok;
  opened = true;
}

void StorageIO::create()
{
  // std::cout << "Creating " << filename << std::endl;

  file.open( filename.c_str(), std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);
  if( !file.good() )
  {
    std::cerr << "Can't create " << filename << std::endl;
    result = Storage::OpenFailed;
    return;
  }

  // so far so good
  opened = true;
  result = Storage::Ok;
}

void StorageIO::init()
{
    // Initialize parts of the header, directory entries, and big and small allocation tables
    header->bb_blocks[0] = 0;
    header->dirent_start = 1;
    header->sbat_start = 2;
    header->num_bat = 1;
    header->num_sbat = 1;
    header->dirty = true;
    bbat->set(0, AllocTable::Eof);
    bbat->markAsDirty(0, bbat->blockSize);
    bbat->set(1, AllocTable::Eof);
    bbat->markAsDirty(1, bbat->blockSize);
    bbat->set(2, AllocTable::Eof);
    bbat->markAsDirty(2, bbat->blockSize);
    bbat->set(3, AllocTable::Eof);
    bbat->markAsDirty(3, bbat->blockSize);
    sb_blocks = bbat->follow( 3 );
	sb_length = 0;
    mbatDirty = false;
}

void StorageIO::flush()
{
    if (header->dirty)
    {
        unsigned char *buffer = new unsigned char[512];
        header->save( buffer );
        file.seekp( 0 );
        file.write( (char*)buffer, 512 );
        fileCheck(file);
        delete[] buffer;
    }
    if (bbat->isDirty())
        flushbbat();
    if (sbat->isDirty())
        flushsbat();
    if (dirtree->isDirty())
    {
        std::vector<size_t> blocks;
        blocks = bbat->follow(header->dirent_start);
        size_t sb_start = 0xffffffff;
        if (sb_blocks.size() > 0)
            sb_start = sb_blocks[0];
        dirtree->flush(blocks, this, bbat->blockSize, sb_start, sb_length);
    }
    if (mbatDirty && mbat_blocks.size() > 0)
    {
        size_t nBytes = bbat->blockSize * static_cast<size_t>(mbat_blocks.size());
        unsigned char *buffer = new unsigned char[nBytes];
        size_t dcount = 0;
        size_t blockCapacity = bbat->blockSize / 4 - 1;  //was sizeof(size_t), which is very bad for 64 bit code. Allocation entries are always 4 bytes long.
        size_t blockIdx = 0;
		size_t initMax = nBytes / 4;  //was sizeof(size_t), which is very bad for 64 bit code. Allocation entries are always 4 bytes long.
        size_t sIdx = 0;
		for (size_t initIdx = 0; initIdx < initMax; initIdx++)
		{
			writeU32(buffer + sIdx, AllocTable::Avail);	//initialize buffer - some audits require it
			sIdx += 4;
		}
		sIdx = 0;
        for (size_t mdIdx = 0; mdIdx < mbat_data.size(); mdIdx++)
        {
            writeU32(buffer + sIdx, (size_t) mbat_data[mdIdx]);
            sIdx += 4;
            dcount++;
            if (dcount == blockCapacity)
            {
                blockIdx++;
                if (blockIdx == mbat_blocks.size())
                    writeU32(buffer + sIdx, AllocTable::Eof);
                else
                    writeU32(buffer + sIdx, (size_t) mbat_blocks[blockIdx]);
                sIdx += 4;
                dcount = 0;
            }
        }
		if (dcount != 0)
			writeU32(buffer + nBytes - 4, AllocTable::Eof);
        saveBigBlocks(mbat_blocks, 0, buffer, nBytes);
        delete[] buffer;
        mbatDirty = false;
    }
    file.flush();
    fileCheck(file);

  /* Note on Microsoft implementation:
     - directory entries are stored in the last block(s)
     - BATs are as second to the last
     - Meta BATs are third to the last
  */
}

void StorageIO::close()
{
  if( !opened ) return;

  file.close();
  opened = false;

  std::list<Stream*>::iterator it;
  for( it = streams.begin(); it != streams.end(); ++it )
    delete *it;
}

void StorageIO::setDebug(bool doDebug, bool toFile)
{
    _doDebug = doDebug;
    _toFile = toFile;
}

StreamIO* StorageIO::streamIO( const std::string& name, bool bCreate, size_t streamSize )
{
  // sanity check
  if( !name.length() ) return (StreamIO*)0;

  // search in the entries
  DirEntry* entry = dirtree->entry( name, bCreate, bbat->blockSize, this, streamSize );
  //if( entry) std::cout << "FOUND\n";
  if( !entry ) return (StreamIO*)0;
  //if( !entry->dir ) std::cout << "  NOT DIR\n";
  if( entry->dir ) return (StreamIO*)0;

  StreamIO* result2 = new StreamIO( this, entry );
  result2->fullName = name;

  return result2;
}

bool StorageIO::deleteByName(const std::string& fullName)
{
    if (!fullName.length())
        return false;
    if (!writeable)
        return false;
    DirEntry* entry = dirtree->entry(fullName);
    if (!entry)
        return false;
    bool retVal;
    if (entry->dir)
        retVal = deleteNode(entry, fullName);
    else
        retVal = deleteLeaf(entry, fullName);
    if (retVal)
        flush();
    return retVal;
}

bool StorageIO::deleteNode(DirEntry *entry, const std::string& fullName)
{
    std::string lclName = fullName;
    if (lclName[lclName.size()-1] != '/')
        lclName += '/';
    bool retVal = true;
    while (entry->child && entry->child < dirtree->entryCount())
    {
        DirEntry* childEnt = dirtree->entry(entry->child);
        std::string childFullName = lclName + childEnt->name;
        if (childEnt->dir)
            retVal = deleteNode(childEnt, childFullName);
        else
            retVal = deleteLeaf(childEnt, childFullName);
        if (!retVal)
            return false;
    }
    dirtree->deleteEntry(entry, fullName, bbat->blockSize);
    return retVal;
}

bool StorageIO::deleteLeaf(DirEntry *entry, const std::string& fullName)
{
    std::vector<size_t> blocks;
    if (entry->size >= header->threshold)
    {
        blocks = bbat->follow(entry->start);
        for (size_t idx = 0; idx < blocks.size(); idx++)
        {
            bbat->set(blocks[idx], AllocTable::Avail);
            bbat->markAsDirty(blocks[idx], bbat->blockSize);
        }
    }
    else
    {
        blocks = sbat->follow(entry->start);
        for (size_t idx = 0; idx < blocks.size(); idx++)
        {
            sbat->set(blocks[idx], AllocTable::Avail);
            sbat->markAsDirty(blocks[idx], bbat->blockSize);
        }
    }
    dirtree->deleteEntry(entry, fullName, bbat->blockSize);
    return true;
}

size_t StorageIO::loadBigBlocks( std::vector<size_t> blocks,
  size_t offset, unsigned char* data, size_t maxlen )
{
  // sentinel
  if( !data ) return 0;
  fileCheck(file);
  if( !file.good() ) return 0;
  if( blocks.size() < 1 ) return 0;
  if( maxlen == 0 ) return 0;

  size_t bytes = 0;
  for( size_t i=0; (i < blocks.size() ) & ( bytes<maxlen ); )
  {
	  size_t lastBlock = blocks[i];
	  size_t bytesToRead = bbat->blockSize - offset;
	  size_t j = i;
      for ( ; ; )
	  {
		  j++;
		  if (j == blocks.size())
			  break;
		  if (lastBlock+1 != blocks[j])
			  break;
		  if (bytesToRead + bytes >= maxlen)
			  break;
		  bytesToRead += bbat->blockSize;
		  lastBlock = blocks[j];
	  }
	  size_t block = blocks[i];
	  std::streamoff pos = bbat->blockSize * (block+1) + offset;
	  size_t p = bytesToRead;
	  if (p + bytes > maxlen)
		  p = maxlen - bytes;
	  if ((unsigned)pos + p > (unsigned)filesize)
		  p = (size_t) (filesize - pos);
	  file.seekg(pos);
	  file.read((char*)data + bytes, p);
	  fileCheck(file);
	  offset = 0;
	  bytes += p;
	  i = j;
  }
  return bytes;
}

size_t StorageIO::loadBigBlock( size_t block, size_t offset,
  unsigned char* data, size_t maxlen )
{
  // sentinel
  if( !data ) return 0;
  fileCheck(file);
  if( !file.good() ) return 0;

  // wraps call for loadBigBlocks
  std::vector<size_t> blocks;
  blocks.resize( 1 );
  blocks[ 0 ] = block;

  return loadBigBlocks( blocks, offset, data, maxlen );
}

size_t StorageIO::saveBigBlocks( std::vector<size_t> blocks, size_t offset, unsigned char* data, size_t len )
{
  // sentinel
  if( !data ) return 0;
  fileCheck(file);
  if( !file.good() ) return 0;
  if( blocks.size() < 1 ) return 0;
  if( len == 0 ) return 0;

  size_t bytes = 0;
  for( size_t i=0; (i < blocks.size() ) && ( bytes<len ); )
  {
	  size_t lastBlock = blocks[i];
	  size_t bytesToWrite = bbat->blockSize - offset;
	  size_t j = i;
      for ( ; ; )
	  {
		  j++;
		  if (j == blocks.size())
			  break;
		  if (lastBlock+1 != blocks[j])
			  break;
		  if (bytesToWrite + bytes >= len)
			  break;
		  bytesToWrite += bbat->blockSize;
		  lastBlock = blocks[j];
	  }
	  size_t block = blocks[i];
	  std::streamoff pos = (bbat->blockSize * (block+1)) + offset;
	  size_t p = bytesToWrite;
	  if (p + bytes > len)
		  p = len - bytes;
	  file.seekg(pos);
	  file.write((char*)data + bytes, p);
	  fileCheck(file);
	  bytes += p;
	  if ((unsigned)filesize < (unsigned)pos + p)
		  filesize = (std::streamoff) ((unsigned)pos + p);
	  offset = 0;
	  i = j;
  }
  return bytes;

}

size_t StorageIO::saveBigBlock( size_t block, size_t offset, unsigned char* data, size_t len )
{
    if ( !data ) return 0;
    fileCheck(file);
    if ( !file.good() ) return 0;
    //wrap call for saveBigBlocks
    std::vector<size_t> blocks;
    blocks.resize( 1 );
    blocks[ 0 ] = block;
    return saveBigBlocks(blocks, offset, data, len );
}

// return number of bytes which has been read
size_t StorageIO::loadSmallBlocks( std::vector<size_t> blocks,
  unsigned char* data, size_t maxlen )
{
  // sentinel
  if( !data ) return 0;
  fileCheck(file);
  if( !file.good() ) return 0;
  if( blocks.size() < 1 ) return 0;
  if( maxlen == 0 ) return 0;

  // our own local buffer
  unsigned char* buf = new unsigned char[ bbat->blockSize ];

  // read small block one by one
  size_t bytes = 0;
  for( size_t i=0; ( i<blocks.size() ) & ( bytes<maxlen ); i++ )
  {
    size_t block = blocks[i];

    // find where the small-block exactly is
    size_t pos = block * sbat->blockSize;
    size_t bbindex = pos / bbat->blockSize;
    if( bbindex >= sb_blocks.size() ) break;

    loadBigBlock( sb_blocks[ bbindex ], 0, buf, bbat->blockSize );

    // copy the data
    size_t offset = pos % bbat->blockSize;
    size_t p = (maxlen-bytes < bbat->blockSize-offset ) ? maxlen-bytes :  bbat->blockSize-offset;
    p = (sbat->blockSize<p ) ? sbat->blockSize : p;
    memcpy( data + bytes, buf + offset, p );
    bytes += p;
  }

  delete[] buf;

  return bytes;
}

size_t StorageIO::loadSmallBlock( size_t block,
  unsigned char* data, size_t maxlen )
{
  // sentinel
  if( !data ) return 0;
  fileCheck(file);
  if( !file.good() ) return 0;

  // wraps call for loadSmallBlocks
  std::vector<size_t> blocks;
  blocks.resize( 1 );
  blocks.assign( 1, block );

  return loadSmallBlocks( blocks, data, maxlen );
}

size_t StorageIO::saveSmallBlocks( std::vector<size_t> blocks, size_t offset,
                                        unsigned char* data, size_t len, size_t startAtBlock )
{
  // sentinel
  if( !data ) return 0;
  fileCheck(file);
  if( !file.good() ) return 0;
  if( blocks.size() < 1 ) return 0;
  if( len == 0 ) return 0;

  // write block one by one, seems fast enough
  size_t bytes = 0;
  for( size_t i = startAtBlock; (i < blocks.size() ) & ( bytes<len ); i++ )
  {
    size_t block = blocks[i];
     // find where the small-block exactly is
	std::streamoff pos = block * sbat->blockSize;
    size_t bbindex = (size_t) (pos / bbat->blockSize);
    if( bbindex >= sb_blocks.size() ) break;
	std::streamsize offset2 = pos % bbat->blockSize;
	std::streamsize maxWrite = sbat->blockSize - offset;
	std::streamsize tobeWritten = len - bytes;
    if (tobeWritten > maxWrite)
        tobeWritten = maxWrite;
    saveBigBlock( sb_blocks[ bbindex ], (size_t) (offset2 + offset), data + bytes, (size_t) tobeWritten);
    bytes += (size_t) tobeWritten;
    if (filesize < pos + tobeWritten)
        filesize = pos + tobeWritten;
	// If we are at the end of the Ministream, update its size.
	if (bbindex == sb_blocks.size()-1)
	{
		sb_length = (size_t) (((sb_blocks.size()-1) * bbat->blockSize) + offset2 + offset + tobeWritten);
        dirtree->markAsDirty(0, bbat->blockSize);
	}
    offset = 0;
  }
  return bytes;
}

size_t StorageIO::saveSmallBlock( size_t block, size_t offset, unsigned char* data, size_t len )
{
    if ( !data ) return 0;
    fileCheck(file);
    if ( !file.good() ) return 0;
    //wrap call for saveSmallBlocks
    std::vector<size_t> blocks;
    blocks.resize( 1 );
    blocks[ 0 ] = block;
    return saveSmallBlocks(blocks, offset, data, len );
}

void StorageIO::flushbbat()
{
    std::vector<size_t> blocks;
    blocks = getbbatBlocks(false);
    bbat->flush(blocks, this, bbat->blockSize);
}

void StorageIO::flushsbat()
{
    std::vector<size_t> blocks;
    blocks = bbat->follow(header->sbat_start);
    sbat->flush(blocks, this, bbat->blockSize);
}

std::vector<size_t> StorageIO::getbbatBlocks(bool bLoading)
{
    std::vector<size_t> blocks;
    // find blocks allocated to store big bat
    // the first 109 blocks are in header, the rest in meta bat
    blocks.clear();
    blocks.resize( header->num_bat );

    for( size_t i = 0; i < 109; i++ )
    {
        if( i >= header->num_bat )
            break;
        else
            blocks[i] = header->bb_blocks[i];
    }
    if (bLoading)
    {
        mbat_blocks.clear();
        mbat_data.clear();
        if( (header->num_bat > 109) && (header->num_mbat > 0) )
        {
            unsigned char* buffer2 = new unsigned char[ bbat->blockSize ];
            size_t k = 109;
            size_t sector;
            size_t mdidx = 0;
            for( size_t r = 0; r < header->num_mbat; r++ )
            {
                if(r == 0) // 1st meta bat location is in file header.
                    sector = header->mbat_start;
                else      // next meta bat location is the last current block value.
                {
                    sector = blocks[--k];
                    mdidx--;
                }
                mbat_blocks.push_back(sector);
                mbat_data.resize(mbat_blocks.size()*(bbat->blockSize/4));
                loadBigBlock( sector, 0, buffer2, bbat->blockSize );
                for( size_t s=0; s < bbat->blockSize; s+=4 )
                {
                    if( k >= header->num_bat )
                        break;
                    else
                    {
                        blocks[k] = readU32( buffer2 + s );
                        mbat_data[mdidx++] = blocks[k];
                        k++;
                    }
                }
            }
            if (mbat_data.size() != mdidx) mbat_data.resize(mdidx);
            delete[] buffer2;
        }
    }
    else
    {
        size_t i = 109;
        for (size_t idx = 0; idx < mbat_data.size(); idx++)
        {
            blocks[i++] = mbat_data[idx];
            if (i == header->num_bat)
                break;
        }
    }
    return blocks;
}

size_t StorageIO::ExtendFile( std::vector<size_t> *chain )
{
    size_t newblockIdx = bbat->unused();
    bbat->set(newblockIdx, AllocTable::Eof);
    size_t bbidx = newblockIdx / (bbat->blockSize / 4);  //was sizeof(size_t), which is very bad for 64 bit code. Allocation entries are always 4 bytes long.
    while (bbidx >= header->num_bat)
        addbbatBlock();
    bbat->markAsDirty(newblockIdx, bbat->blockSize);
    if (chain->size() > 0)
    {
        bbat->set((*chain)[chain->size()-1], newblockIdx);
        bbat->markAsDirty((*chain)[chain->size()-1], bbat->blockSize);
    }
    chain->push_back(newblockIdx);
    return newblockIdx;
}

void StorageIO::addbbatBlock()
{
    size_t newblockIdx = bbat->unused();
    bbat->set(newblockIdx, AllocTable::MetaBat);

    if (header->num_bat < 109)
        header->bb_blocks[header->num_bat] = newblockIdx;
    else
    {
        mbatDirty = true;
        mbat_data.push_back(newblockIdx);
        size_t metaIdx = header->num_bat - 109;
        size_t idxPerBlock = bbat->blockSize / 4 - 1; //reserve room for index to next block
        size_t idxBlock = metaIdx / idxPerBlock;
        if (idxBlock == mbat_blocks.size())
        {
            size_t newmetaIdx = bbat->unused();
            bbat->set(newmetaIdx, AllocTable::MetaBat);
            mbat_blocks.push_back(newmetaIdx);
            if (header->num_mbat == 0)
                header->mbat_start = newmetaIdx;
            header->num_mbat++;
        }
    }
    header->num_bat++;
    header->dirty = true;
}

// returns 0 on success, else small negative number in range established by nodeDataAccessible.
// it is the latter that does all the work - this simply traverses the directory structures to
// find nodes to be checked by nodeDataAccessible.
int StorageIO::verifyNodes(size_t nodeNum, std::string &errDesc)
{
	int result2;
	result2 = nodeDataAccessible(nodeNum, errDesc);
	if (result2 != 0)
		return result2;

	std::vector<size_t> children = dirtree->children(nodeNum);
    for( size_t i = 0; i < children.size(); i++)
	{
		result2 = verifyNodes(children[i], errDesc);
		if (result2 != 0)
			return result2;
	}
	return 0; //success!
}

// can return 0 on OK, else negative number in the range of -1 to -10.
// Verify that any leaf (a node containing data) has only as many data blocks as its data size
// indicates it should have, and that the sectors linked from its start are within the
// proper range.
int StorageIO::nodeDataAccessible(size_t nodeNum, std::string &errDesc)
{
	DirEntry *pde = dirtree->entry(nodeNum);
	if (pde->dir)
		return 0;	//not a leaf - simply return so children can be checked.
	if (!pde->valid)
		return 0;
	POLE::AllocTable *allocTab;
	// Really only truncation makes sense here, I think - release blocks if necessary.
	if (pde->size >= header->threshold)
		allocTab = bbat;
	else
		allocTab = sbat;
	std::vector<size_t> blocks = allocTab->follow(pde->start);
	size_t leafDataOffset = 0;
	for (size_t idx = 0; idx < blocks.size(); idx++)
	{
		if (leafDataOffset >= pde->size)
		{
			errDesc = dirtree->fullName(nodeNum);
			errDesc += " has more blocks than its size would permit.";
			return -1;
		}
		size_t dataLenInBlock = allocTab->blockSize;
		if (dataLenInBlock > (pde->size - leafDataOffset))
			dataLenInBlock = pde->size - leafDataOffset;
		leafDataOffset += dataLenInBlock;
		size_t blockNum = blocks[idx];
		if (allocTab == sbat)
		{
			size_t smallPos = blockNum * sbat->blockSize;
			size_t bbindex = smallPos / bbat->blockSize;
			if( bbindex >= sb_blocks.size())
			{
				errDesc = dirtree->fullName(nodeNum);
				errDesc += " has a mini block number that is too large.";
				return -2;
			}
			blockNum = sb_blocks[bbindex];
		}
		if (blockNum >= bbat->count())
		{
			errDesc = dirtree->fullName(nodeNum);
			errDesc += " points to a sector which is not in the list of big sectors.";
			return -3;
		}
		std::streamoff pos = bbat->blockSize * (blockNum+1);
		if (pos + ((std::streamoff)(dataLenInBlock - 1)) > filesize)
		{
			errDesc = dirtree->fullName(nodeNum);
			errDesc += " has data which points beyond the end of the file.";
			return -4;
		}
	}
	return 0;
}

int StorageIO::xlinkCheck(std::vector<size_t> &bigBlocks, std::vector<size_t> &smlBlocks, std::string &errDesc)
{
	int retVal = 0;
	//Check DIFSECT values (called here MetaBat). There are none for files smaller than 7,143,936 bytes
	retVal = xlinkCheckChain(bigBlocks, bbat, 0, true, AllocTable::MetaBat, errDesc);
	if (retVal != 0)
		return retVal;
	//Check FATSECT values (called here bat)
	retVal = xlinkCheckChain(bigBlocks, bbat, 0, true, AllocTable::Bat, errDesc);
	if (retVal != 0)
		return retVal;
	//Check directory sectors - blocks used for Directory Entries
	retVal = xlinkCheckChain(bigBlocks, bbat, 0, true, 0xfffffffb, errDesc);
	if (retVal != 0)
		return retVal;
	//Check Minifat sectors - blocks used for small (under 4096) leaves
	retVal = xlinkCheckChain(bigBlocks, bbat, 0, true, 0xfffffffa, errDesc);
	if (retVal != 0)
		return retVal;
	//Check Minifat data - the "stream" that makes up the small block chain
	retVal = xlinkCheckChain(bigBlocks, bbat, 0, true, 0xfffffff9, errDesc);
	if (retVal != 0)
		return retVal;
	//Go through directory entries, verifying that the sectors they use for data are valid and marked as used.
	for (size_t dirIdx = 0; dirIdx < dirtree->entryCount(); dirIdx++)
	{
		DirEntry *pde = dirtree->entry(dirIdx);
		if (!pde->valid)
			continue;
		if (pde->dir)
			continue;	//handle leaves only
		if (pde->size >= header->threshold)
			retVal = xlinkCheckChain(bigBlocks, bbat, pde->start, true, dirIdx, errDesc);
		else
			retVal = xlinkCheckChain(smlBlocks, sbat, pde->start, false, dirIdx, errDesc);
		if (retVal != 0)
			return retVal;
	}
	return 0;
}
// For a variety of possible cases, create a vector of those sectors that are marked as being
// in use for that case. Verify that each of those sectors are within the apropriate range, as
// established by the size of the blockDat Vector. That vector was initialized to the appropriate
// size for the type of block (big or small), and originally set so that all its entries were marked
// as available. Make sure a sector has not been used yet, and then change blockDat to indicate the type
// of use it was put to. Return 0 for success, or -11 for a sector that is out of range, -12 for
// a sector that is cross-linked (used for more than one case)
int	StorageIO::xlinkCheckChain(std::vector<size_t> &blockDat, POLE::AllocTable *paTab, size_t start,
		bool bBig, size_t chainTag, std::string &errDesc)
{
	std::vector<size_t> blocks;
	if (chainTag == AllocTable::Bat)
		blocks = getbbatBlocks(false);
	else if (chainTag == AllocTable::MetaBat)
		blocks = mbat_blocks;
	else if (chainTag == 0xfffffffb)
		blocks = paTab->follow(header->dirent_start);
	else if (chainTag == 0xfffffffa)
		blocks = paTab->follow(header->sbat_start);
	else if (chainTag == 0xfffffff9)
	{
		if (sb_blocks.size() > 0)
			blocks = paTab->follow(sb_blocks[0]);
		else
			return 0;
	}
	else
		blocks = paTab->follow(start);
	for (unsigned int idx = 0; idx < blocks.size(); idx++)
	{
		if (blockDat.size() < blocks[idx])
		{
			chainTagName(chainTag, errDesc);
			errDesc += " owns a block number that exceeds the number of ";
			if (bBig)
				errDesc += "big blocks.";
			else
				errDesc += "small blocks.";
			return -11;
		}
		if (blockDat[blocks[idx]] != AllocTable::Avail)
		{
			chainTagName(chainTag, errDesc);
			errDesc += " and ";
			std::string otherName;
			chainTagName(blockDat[blocks[idx]], otherName);
			errDesc += otherName;
			errDesc += " are crosslinked.";
			return -12;
		}
		blockDat[blocks[idx]] = chainTag;
	}
	return 0;
}

int StorageIO::allocTableAudit(std::string &errDesc)
{
	for (size_t idx = 0; idx < bbat->count(); idx++)
	{
		if ((*bbat)[idx] != AllocTable::Avail && (*bbat)[idx] != AllocTable::Eof &&
			(*bbat)[idx] != AllocTable::MetaBat && (*bbat)[idx] != AllocTable::Bat &&
			(*bbat)[idx] > bbat->count())
		{
			errDesc = "A bad value was found in the big block table";
			return -23;
		}
	}
	for (size_t idx = 0; idx < sbat->count(); idx++)
	{
		if ((*sbat)[idx] != AllocTable::Avail && (*sbat)[idx] != AllocTable::Eof && (*sbat)[idx] > sbat->count())
		{
			errDesc = "A bad value was found in the small block table";
			return -24;
		}
	}
	return 0;
}

int StorageIO::allocTableCheck(std::vector<size_t> &bigBlocks, std::vector<size_t> &smlBlocks, std::string &errDesc)
{
	for (size_t idx = 0; idx < bigBlocks.size(); idx++)
	{
		if (bigBlocks[idx] == AllocTable::Avail || (*bbat)[idx] == AllocTable::Avail)
		{
			if (bigBlocks[idx] != (*bbat)[idx])
			{
				errDesc = "Discrepancy found in the big block table";
				return -21;
			}
		}
	}
	for (size_t idx = 0; idx < smlBlocks.size(); idx++)
	{
		if (smlBlocks[idx] == AllocTable::Avail || (*sbat)[idx] == AllocTable::Avail)
		{
			if (smlBlocks[idx] != (*sbat)[idx])
			{
				errDesc = "Discrepancy found in the small block table";
				return -22;
			}
		}
	}

	return 0;
}

void StorageIO::chainTagName(size_t chainTag, std::string &name)
{
	if (chainTag == AllocTable::Bat)
		name = "FAT Data";
	else if (chainTag == AllocTable::MetaBat)
		name = "FAT MetaData";
	else if (chainTag == 0xfffffffb)
		name = "Directory entries";
	else if (chainTag == 0xfffffffa)
		name = "MiniFAT Data";
	else if (chainTag == 0xfffffff9)
		name = "MiniFat MetaData";
	else
		name = dirtree->fullName(chainTag);
}

/* KES File specific code!!! Return 0 if file does NOT have the KES Mac bug described in the declaration in
 * pole.h. If it does have the problem, return 1 if there have been no lost data streams, 2 if there are, 3
 * if one of those lost data streams has been truncated, 4 if more than one has. If bRepairIfPossible is true,
 * attempt a fix in memory. */

int StorageIO::auditForKESMacBug(bool bRepairIfPossible)
{
	if (header->num_mbat == 0)
		return 0;	//No problem - the file isn't big enough to have an mbat
	std::vector<size_t> bbats;
	bbats = getbbatBlocks(false);
	int idxBad = -1;
	size_t candidateBbat = header->dirent_start;
	for (size_t idx = 0; idx < bbats.size(); idx++)
	{
		if (bbats[idx] == candidateBbat)
		{
			idxBad = (int) idx;
			break;
		}
	}
	if (idxBad == -1)
		return 0;		//File is big enough, but the start of the directory entries isn't crosslinked
	bool bTruncatedFile = false;
	size_t loSectBad = idxBad*128;
	size_t hiSectBad = (idxBad + header->num_mbat)*128 - 1;
	size_t nBadSects = hiSectBad - loSectBad + 1;
	size_t *pReplacements = 0;
	if (bRepairIfPossible)
	{
		pReplacements = new size_t[nBadSects];
		for (size_t idx = 0; idx < nBadSects; idx++)
			*(pReplacements+idx) = AllocTable::Avail;
	}
	int retVal = checkKESMacChain(bbats, header->num_bat, loSectBad, hiSectBad, pReplacements);
	if (retVal == 2)
		bTruncatedFile = true;
	int ret2 = checkKESMacChain(mbat_blocks, header->num_mbat, loSectBad, hiSectBad, pReplacements);
	if (ret2 == 2)
	{
		if (bTruncatedFile)
			ret2 = 3;
		else
			bTruncatedFile = true;
	}
	if (ret2 > retVal)
		retVal = ret2;
	std::vector<size_t> blocks = bbat->follow(header->dirent_start);
	ret2 = checkKESMacChain(blocks, blocks.size(), loSectBad, hiSectBad, pReplacements);
	if (ret2 == 2)
	{
		if (bTruncatedFile)
			ret2 = 3;
		else
			bTruncatedFile = true;
	}
	if (ret2 > retVal)
		retVal = ret2;
	blocks = bbat->follow(header->sbat_start);
	ret2 = checkKESMacChain(blocks, header->num_sbat, loSectBad, hiSectBad, pReplacements);
	if (ret2 == 2)
	{
		if (bTruncatedFile)
			ret2 = 3;
		else
			bTruncatedFile = true;
	}
	if (ret2 > retVal)
		retVal = ret2;
	size_t expectedBlocks = (sb_length + bbat->blockSize - 1) / bbat->blockSize;
	ret2 = checkKESMacChain(sb_blocks, expectedBlocks, loSectBad, hiSectBad, pReplacements);
	if (ret2 == 2)
	{
		if (bTruncatedFile)
			ret2 = 3;
		else
			bTruncatedFile = true;
	}
	if (ret2 > retVal)
		retVal = ret2;
	for (size_t idx = 0; idx < dirtree->entryCount(); idx++)
	{
		DirEntry *pDirent = dirtree->entry(idx);
		if (!pDirent->dir && pDirent->size >= header->threshold)
		{
			blocks = bbat->follow(pDirent->start);
			expectedBlocks = (pDirent->size + bbat->blockSize - 1) / bbat->blockSize;
			ret2 = checkKESMacChain(blocks, expectedBlocks, loSectBad, hiSectBad, pReplacements);
			if (ret2 == 2)
			{
				if (bTruncatedFile)
					ret2 = 3;
				else
					bTruncatedFile = true;
			}
			if (ret2 > retVal)
				retVal = ret2;
		}
	}
	if (pReplacements != 0)
	{
		for (size_t idx = 0; idx < nBadSects; idx++)
		{
			bbat->set(loSectBad+idx,*(pReplacements+idx));
			bbat->markAsDirty(loSectBad+idx, bbat->blockSize);
		}
		delete[] pReplacements;
		for (size_t idx = 0; idx < header->num_mbat; idx++)
		{
			size_t nSect = bbat->unused();
			bbat->set(nSect, AllocTable::MetaBat);
			bbat->markAsDirty(nSect, bbat->blockSize);
			mbat_data[idxBad-109] = nSect;
			idxBad++;
		}
		mbatDirty = true;
	}
	retVal += 1;
	return retVal;
}

/* part of KES Specific investigation - does this chain have anything to do with the faulty bbat?
** return 0 for no, return 1 for yes, but not truncated, return 2 for yes and truncated.
*/
int StorageIO::checkKESMacChain(std::vector<size_t> &chain, size_t nBlocksExpected, size_t loBad, size_t hiBad,
								size_t *pReplacements)
{
	if (chain.size() == 0)
		return 0;

	int retVal = 0;
	size_t lastIdx = chain.size() - 1;
	if (chain[lastIdx] >= loBad && chain[lastIdx] <= hiBad)
	{
		if (lastIdx+1 < nBlocksExpected)
			retVal = 2;
		else
			retVal = 1;
	}
	if (pReplacements != 0)
	{
		if (retVal == 1)
			*(pReplacements+(chain[lastIdx]-loBad)) = AllocTable::Eof;
		if (retVal == 2)
		{
			while (lastIdx+1 < nBlocksExpected)
			{
				size_t freeIdx;
				for (freeIdx = chain[lastIdx]+1; freeIdx < hiBad+1; freeIdx++)
				{
					if ((*bbat)[freeIdx] == AllocTable::Avail && sectorInUse(freeIdx))
						break;
				}
				if (freeIdx > hiBad && freeIdx >= bbat->count())
					freeIdx = AllocTable::Eof;
				*(pReplacements+(chain[lastIdx]-loBad)) = freeIdx;
				if (freeIdx > hiBad)
					break;
				chain.push_back(freeIdx);
				lastIdx++;
			}
		}
	}
	return retVal;
}

bool StorageIO::sectorInUse(size_t sectNum)
{
	bool bUsed = false;
	unsigned char *buffer = new unsigned char[512];
	loadBigBlock(sectNum, 0, buffer, 512);
	for (size_t idx = 0; idx < 512; idx++)
	{
		if (buffer[idx] != 0)
		{
			bUsed = true;
			break;
		}
	}
	delete[] buffer;
	return bUsed;
}

// =========== StreamIO ==========

StreamIO::StreamIO( StorageIO* s, DirEntry* e)
:   io(s),
    entryIdx(io->dirtree->indexOf(e)),
    fullName(),
    blocks(),
    eof(false),
    fail(false),
    m_pos(0),
    cache_data(new unsigned char[CACHEBUFSIZE]),
    cache_size(0),         // indicating an empty cache
    cache_pos(0)
{
  if( e->size >= io->header->threshold )
    blocks = io->bbat->follow( e->start );
  else
    blocks = io->sbat->follow( e->start );
}

// FIXME tell parent we're gone
StreamIO::~StreamIO()
{
  delete[] cache_data;
}

void StreamIO::setSize(size_t newSize)
{
    bool bThresholdCrossed = false;
    bool bOver = false;

    if(!io->writeable )
        return;
    DirEntry *entry = io->dirtree->entry(entryIdx);
    if (newSize >= io->header->threshold && entry->size < io->header->threshold)
    {
        bThresholdCrossed = true;
        bOver = true;
    }
    else if (newSize < io->header->threshold && entry->size >= io->header->threshold)
    {
        bThresholdCrossed = true;
        bOver = false;
    }
    if (bThresholdCrossed)
    {
        // first, read what is already in the stream, limited by the requested new size. Note
        // that the read can work precisely because we have not yet reset the size.
        size_t len = newSize;
        if (len > entry->size)
            len = entry->size;
        unsigned char *buffer = 0;
        size_t savePos = tell();
        if (len)
        {
            buffer = new unsigned char[len];
            seek(0);
            read(buffer, len);
        }
        // Now get rid of the existing blocks
        if (bOver)
        {
            for (size_t idx = 0; idx < blocks.size(); idx++)
            {
                io->sbat->set(blocks[idx], AllocTable::Avail);
                io->sbat->markAsDirty(blocks[idx], io->bbat->blockSize);
            }
        }
        else
        {
            for (size_t idx = 0; idx < blocks.size(); idx++)
            {
                io->bbat->set(blocks[idx], AllocTable::Avail);
                io->bbat->markAsDirty(blocks[idx], io->bbat->blockSize);
            }
        }
        blocks.clear();
        entry->start = DirTree::End;
        // Now change the size, and write the old data back into the stream, if any
        entry->size = newSize;
        io->dirtree->markAsDirty(io->dirtree->indexOf(entry), io->bbat->blockSize);
        if (len)
        {
            write(0, buffer, len);
            delete buffer;
        }
        if (savePos <= entry->size)
            seek(savePos);
    }
    else if (entry->size != newSize) //no threshold was crossed, so just change the size
    {
		size_t sizeFound = 0;
		size_t firstBadBlockIdx = blocks.size();
		POLE::AllocTable *allocTab;
		// Really only truncation makes sense here, I think - release blocks if necessary.
		if (entry->size >= io->header->threshold)
			allocTab = io->bbat;
		else
			allocTab = io->sbat;
		for (size_t idx = 0; idx < blocks.size(); idx++)
		{
			if (sizeFound < newSize)
			{
				sizeFound += allocTab->blockSize;
				if (sizeFound >= newSize)
				{
					allocTab->set(blocks[idx], AllocTable::Eof);
					allocTab->markAsDirty(blocks[idx], io->bbat->blockSize);
				}
			}
			else
			{
				if (idx == 0)
					entry->start = AllocTable::Eof;
				if (firstBadBlockIdx > idx)
					firstBadBlockIdx = idx;
                allocTab->set(blocks[idx], AllocTable::Avail);
                allocTab->markAsDirty(blocks[idx], io->bbat->blockSize);
			}
		}
        entry->size = newSize;
        io->dirtree->markAsDirty(io->dirtree->indexOf(entry), io->bbat->blockSize);
		while (blocks.size() > firstBadBlockIdx)
			blocks.pop_back();
    }

}

void StreamIO::seek( size_t pos )
{
  m_pos = pos;
}

size_t StreamIO::tell()
{
  return m_pos;
}

std::vector<size_t> StreamIO::getBlocks()
{
	return blocks;
}

int32 StreamIO::getch()
{
  // past end-of-file ?
  DirEntry *entry = io->dirtree->entry(entryIdx);
  if( m_pos >= entry->size ) return -1;

  // need to update cache ?
  if( !cache_size || ( m_pos < cache_pos ) ||
    ( m_pos >= cache_pos + cache_size ) )
      updateCache();

  // something bad if we don't get good cache
  if( !cache_size ) return -1;

  int32 data = cache_data[m_pos - cache_pos];
  m_pos++;

  return data;
}

size_t StreamIO::read( size_t pos, unsigned char* data, size_t maxlen )
{
  // sanity checks
  if( !data ) return 0;
  if( maxlen == 0 ) return 0;

  size_t totalbytes = 0;

  DirEntry *entry = io->dirtree->entry(entryIdx);
  if (pos + maxlen > entry->size)
      maxlen = entry->size - pos;
  if ( entry->size < io->header->threshold )
  {
    // small file
    size_t index = pos / io->sbat->blockSize;

    if( index >= blocks.size() ) return 0;

    unsigned char* buf = new unsigned char[ io->sbat->blockSize ];
    size_t offset = pos % io->sbat->blockSize;
    while( totalbytes < maxlen )
    {
      if( index >= blocks.size() ) break;
      io->loadSmallBlock( blocks[index], buf, io->bbat->blockSize );
      size_t count = io->sbat->blockSize - offset;
      if( count > maxlen-totalbytes ) count = maxlen-totalbytes;
      memcpy( data+totalbytes, buf + offset, count );
      totalbytes += count;
      offset = 0;
      index++;
    }
    delete[] buf;

  }
  else
  {
    // big file
    size_t index = pos / io->bbat->blockSize;
    if( index >= blocks.size() ) return 0;
	std::vector<size_t> blocksToRead;
    size_t offset = pos % io->bbat->blockSize;
	size_t tmpOffset = offset;
	while (totalbytes < maxlen)
	{
		if (index >= blocks.size())
			break;
		blocksToRead.push_back(blocks[index]);
		totalbytes += io->bbat->blockSize - tmpOffset;
		tmpOffset = 0;
		index++;
	}
	if (totalbytes > maxlen)
		totalbytes = maxlen;
	io->loadBigBlocks(blocksToRead, offset, data, totalbytes);
  }
  return totalbytes;
}

size_t StreamIO::read( unsigned char* data, size_t maxlen )
{
  size_t bytes = read( tell(), data, maxlen );
  m_pos += bytes;
  return bytes;
}

size_t StreamIO::write( unsigned char* data, size_t len )
{
  return write( tell(), data, len );
}

size_t StreamIO::write( size_t pos, unsigned char* data, size_t len )
{
  // sanity checks
  if( !data ) return 0;
  if( len == 0 ) return 0;
  if( !io->writeable ) return 0;

  DirEntry *entry = io->dirtree->entry(entryIdx);
  if (pos + len > entry->size)
      setSize(pos + len); //reset size, possibly changing from small to large blocks
  size_t totalbytes = 0;
  if ( entry->size < io->header->threshold )
  {
    // small file
    size_t index = (pos + len - 1) / io->sbat->blockSize;
    while (index >= blocks.size())
    {
        size_t nblock = io->sbat->unused();
        if (blocks.size() > 0)
        {
            io->sbat->set(blocks[blocks.size()-1], nblock);
            io->sbat->markAsDirty(blocks[blocks.size()-1], io->bbat->blockSize);
        }
        io->sbat->set(nblock, AllocTable::Eof);
        io->sbat->markAsDirty(nblock, io->bbat->blockSize);
		blocks.push_back(nblock);
        size_t bbidx = nblock / (io->bbat->blockSize / sizeof(uint32));
        while (bbidx >= io->header->num_sbat)
        {
            std::vector<size_t> sbat_blocks = io->bbat->follow(io->header->sbat_start);
            io->ExtendFile(&sbat_blocks);
            io->header->num_sbat++;
			io->header->sbat_start = sbat_blocks[0]; //SRB 9/13/2013 - sbat_start might otherwise be AllocTable::Eof!
			io->header->dirty = true; //Header will have to be rewritten
        }
        size_t sidx = nblock * io->sbat->blockSize / io->bbat->blockSize;
        while (sidx >= io->sb_blocks.size())
		{
            io->ExtendFile(&io->sb_blocks);
			io->dirtree->markAsDirty(0, io->bbat->blockSize); //make sure to rewrite first directory block
		}
    }
    size_t offset = pos % io->sbat->blockSize;
    index = pos / io->sbat->blockSize;
    totalbytes = io->saveSmallBlocks(blocks, offset, data, len, index);
  }
  else
  {
    // big file
    size_t index = (pos + len - 1) / io->bbat->blockSize;
    while (index >= blocks.size())
        io->ExtendFile(&blocks);
    size_t offset = pos % io->bbat->blockSize;
	size_t tmpOffset = offset;
	std::vector<size_t> blocksToWrite;
    index = pos / io->bbat->blockSize;
	while (totalbytes < len)
	{
		if (index >= blocks.size())
			break;
		blocksToWrite.push_back(blocks[index]);
		totalbytes += io->bbat->blockSize - tmpOffset;
		tmpOffset = 0;
		index++;
	}
	if (totalbytes > len)
		totalbytes = len;
	io->saveBigBlocks(blocksToWrite, offset, data, totalbytes);
  }
  if (blocks.size() > 0 && entry->start != blocks[0])
  {
      entry->start = blocks[0];
      io->dirtree->markAsDirty(io->dirtree->indexOf(entry), io->bbat->blockSize);
  }
  m_pos += len;
  return totalbytes;
}

void StreamIO::flush()
{
    io->flush();
}

void StreamIO::updateCache()
{
  // sanity check
  if( !cache_data ) return;

  DirEntry *entry = io->dirtree->entry(entryIdx);
  cache_pos = m_pos - (m_pos % CACHEBUFSIZE);
  size_t bytes = CACHEBUFSIZE;
  if( cache_pos + bytes > entry->size ) bytes = entry->size - cache_pos;
  cache_size = read( cache_pos, cache_data, bytes );
}


// =========== Storage ==========

Storage::Storage( const char* filename )
:   io()
{
    io = new StorageIO( this, filename );
}

Storage::~Storage()
{
  delete io;
}

int Storage::result()
{
  return (int) io->result;
}

bool Storage::open(bool bWriteAccess, bool bCreate)
{
  return io->open(bWriteAccess, bCreate);
}

void Storage::close()
{
  io->close();
}

void Storage::flush()
{
	io->flush();
}

void Storage::setDebug(bool doDebug, bool toFile)
{
    io->setDebug(doDebug, toFile);
}

std::list<std::string> Storage::entries( const std::string& path )
{
  std::list<std::string> localResult;
  DirTree* dt = io->dirtree;
  DirEntry* e = dt->entry( path, false );
  if( e  && e->dir )
  {
    size_t parent = dt->indexOf( e );
    std::vector<size_t> children = dt->children( parent );
    for( size_t i = 0; i < children.size(); i++ )
      localResult.push_back( dt->entry( children[i] )->name );
  }

  return localResult;
}

bool Storage::isDirectory( const std::string& name )
{
  DirEntry* e = io->dirtree->entry( name, false );
  return e ? e->dir : false;
}

bool Storage::exists( const std::string& name )
{
    DirEntry* e = io->dirtree->entry( name, false );
    return (e != 0);
}

bool Storage::isWriteable()
{
    return io->writeable;
}

bool Storage::deleteByName( const std::string& name )
{
  return io->deleteByName(name);
}

void Storage::GetStats(size_t *pEntries, size_t *pUnusedEntries,
      size_t *pBigBlocks, size_t *pUnusedBigBlocks,
      size_t *pSmallBlocks, size_t *pUnusedSmallBlocks)
{
    *pEntries = io->dirtree->entryCount();
    *pUnusedEntries = io->dirtree->unusedEntryCount();
	size_t rawBBCount = io->bbat->count();
	std::vector<size_t> unusedBlocks = io->bbat->getUnusedBlocks();
	std::streamoff fileOffset;
	int toBeDefragmented = 0;
	for (size_t idx = 0; idx < unusedBlocks.size(); idx++)
	{
		fileOffset = io->bbat->blockSize * (unusedBlocks.at(idx)+1);
		if (fileOffset < io->filesize)
			toBeDefragmented++;
		else
			rawBBCount--;
	}
	*pBigBlocks = rawBBCount;
    *pUnusedBigBlocks = toBeDefragmented;
	size_t rawSBCount = io->sbat->count();
	unusedBlocks = io->sbat->getUnusedBlocks();
	size_t bbidx;
	toBeDefragmented = 0;
	for (size_t idx = 0; idx < unusedBlocks.size(); idx++)
	{
		std::streamoff pos = unusedBlocks.at(idx) * io->sbat->blockSize;
		bbidx = (size_t) (pos / io->bbat->blockSize);
		if (bbidx < io->sb_blocks.size())
		{
			fileOffset = io->bbat->blockSize * (io->sb_blocks.at(bbidx) + 1);
			if (fileOffset < io->filesize)
				toBeDefragmented++;
			else
				rawSBCount--;
		}
		else
			rawSBCount--;
	}
	*pSmallBlocks = rawSBCount;
    *pUnusedSmallBlocks = toBeDefragmented;
}

// recursively collect stream names
static
void CollectStreams( std::list<std::string>& result, DirTree* tree, DirEntry* parent, const std::string& path )
{
  DirEntry* c = tree->entry( parent->child );
  std::queue<DirEntry*> queue;
  if ( c ) queue.push( c );
  while ( !queue.empty() ) {
    DirEntry* e = queue.front();
    queue.pop();
    if ( e->dir )
      CollectStreams( result, tree, e, path + e->name + "/" );
    else
      result.push_back( path + e->name );
    DirEntry* p = tree->entry( e->prev );
    if ( p ) queue.push( p );
    DirEntry* n = tree->entry( e->next );
    if ( n ) queue.push( n );
    // not testing if p or n have already been processed; potential infinite loop in case of closed Entry chain
    // it seems not to happen, though
  }
}

std::list<std::string> Storage::GetAllStreams( const std::string& storageName )
{
  std::list<std::string> vresult;
  DirEntry* e = io->dirtree->entry( storageName, false );
  if ( e && e->dir ) CollectStreams( vresult, io->dirtree, e, storageName );
  return vresult;
}

/* KES File specific code!!! Return 0 if file does NOT have the KES Mac bug described in the declaration in
 * pole.h. If it does have the problem, return 1 if there have been no lost data streams, 2 if there are, 3
 * if one or more of those lost data streams has been truncated. If bRepairIfPossible is true, attempt a fix
 * in memory. If bRepairIfPossible is true, set pRepaired based on whether or not this routine thinks the fix
 * worked. Note that if a repair is attempted and fails, the caller should close the Storage object! */
int Storage::auditForKESMacBug(bool bRepairIfPossible, bool *pRepaired)
{
	int retVal = io->auditForKESMacBug(bRepairIfPossible);
	if (bRepairIfPossible)
	{
		if (io->auditForKESMacBug(false) == 0)
			*pRepaired = true;
		else
			*pRepaired = false;
	}
	return retVal;
}

int Storage::audit(std::string &errDesc)
{
	int retVal = io->allocTableAudit(errDesc);
	if (retVal != 0)
		return retVal;
	//First, verify all nodes that are accessible from the root directory
	retVal = io->verifyNodes(0, errDesc);
	if (retVal != 0)
		return retVal;
	std::vector<size_t> bigBlocks (io->bbat->count(), AllocTable::Avail);
	std::vector<size_t> smlBlocks (io->sbat->count(), AllocTable::Avail);
	//Next make sure sectors are valid for all types of uses, and are each used only once, if at all
	retVal = io->xlinkCheck(bigBlocks, smlBlocks, errDesc);
	if (retVal != 0)
		return retVal;
	retVal = io->allocTableCheck(bigBlocks, smlBlocks, errDesc);
	return retVal;
}

// =========== Stream ==========

Stream::Stream( Storage* storage, const std::string& name, bool bCreate, size_t streamSize )
:   io(storage->io->streamIO( name, bCreate, (int) streamSize ))
{
}

// FIXME tell parent we're gone
Stream::~Stream()
{
  delete io;
}

std::string Stream::fullName()
{
  return io ? io->fullName : std::string();
}

size_t Stream::tell()
{
  return io ? io->tell() : 0;
}

long Stream::BlockIdx2FileBlockNum(size_t blockIdx)
{
	std::vector<size_t> blocks;
	if (io)
	{
		blocks = io->getBlocks();
		if (blocks.size() < blockIdx)
			return 0;
		return blocks[blockIdx];
	}
	return 0;
}

void Stream::seek( size_t newpos )
{
  if( io )
      io->seek(newpos);
}

size_t Stream::size()
{
    if (!io)
        return 0;
    DirEntry *entry = io->io->dirtree->entry(io->entryIdx);
    return entry->size;
}

void Stream::setSize(size_t newSize)
{
    if (!io)
        return;
    if (newSize > std::numeric_limits<size_t>::max())
        return;
    io->setSize(newSize);
}

int32 Stream::getch()
{
  return io ? io->getch() : 0;
}

size_t Stream::read( unsigned char* data, size_t maxlen )
{
  return io ? io->read( data, maxlen ) : 0;
}

size_t Stream::write( unsigned char* data, size_t len )
{
    return io ? io->write( data, len ) : 0;
}

void Stream::flush()
{
    if (io)
        io->flush();
}

bool Stream::eof()
{
  return io ? io->eof : false;
}

bool Stream::fail()
{
  return io ? io->fail : true;
}

bool Stream::isValid()
{
	return (io != 0);
}
