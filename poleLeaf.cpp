/**
 * @file
 * @version 1.0
 *
 * @section DESCRIPTION
 *
 * Implementation of the CPoleLeaf class.
 * poleLeaf.cpp - This is a portable class designed to be used with pole.cpp, which is itself a portable library to read and write structured storage.
 * This one is a bit more specific, in that it is designed to handle nodes which directly handle data, and does so in a manner that is appropriate for
 * KES files.
 */

#include "poleLeaf.h"
#include <iostream>
#include <fstream>

#define VALUE_STREAM_ID "KESI Value Stream 1.0"
#define PLERR_TRUNCATED -1
#define PLERR_CORRUPTED -2
#define PLERR_BADVALUETYPE -3
#define PLERR_NOTVALUESLEAF -4

CPoleLeaf::CPoleLeaf(POLE::Stream *s, CEncrypter *penc)
:   pStream(s), pEncrypter(penc), m_valueMap(), m_overriddenValueLen(), m_deletedValues(), bCheckedForValues(false), bHasValues(), m_errorCode(),  m_modified(false)
{
}

CPoleLeaf::~CPoleLeaf()
{
	if (pStream)
		delete pStream;
	clearValues();
	// Note that this does NOT delete pEncrypter - it is owned by a file instance
}

void CPoleLeaf::Refresh()
{
	if (pStream)
	{
		pStream->seek(0);
		pStream->setSize(0);
		if (pEncrypter)
			pEncrypter->Reset();
	}
}

bool CPoleLeaf::HasValues()
{
	if (!bCheckedForValues)
	{
		if (!loadValues())
			bHasValues = false;
	}
	return bHasValues;
}

int CPoleLeaf::ValueCount()
{
	if (!bCheckedForValues)
	{
		if (!loadValues())
			bHasValues = false;
	}
	if (bHasValues)
		return (int) m_valueMap.size();
	else
		return 0;
}

/* This works, but it is rather slow, as advancing an iterator through a map is not a reasonable way
** to grab all values. See GetFirstValueIterator() and IsLastValueIterator() as a way to go through all
** of the values
*/

const char* CPoleLeaf::GetValueName(long index)
{
	if (!bCheckedForValues)
	{
		if (!loadValues())
			bHasValues = false;
	}
	if (bHasValues)
	{
		if (index >= (int)m_valueMap.size())
			return (char *) 0;
		STRVALMAP::iterator itr;
		itr = m_valueMap.begin();
		std::advance(itr, index);
		return itr->first.c_str();
	}
	else
		return (char *) 0;
}

bool CPoleLeaf::GetFirstValueIterator(STRVALMAP::iterator &valIter)
{
	if (!bCheckedForValues)
	{
		if (!loadValues())
			bHasValues = false;
	}
	if (bHasValues)
	{
		valIter = m_valueMap.begin();
		return true;
	}
	else
		return false;
}

bool CPoleLeaf::IsValidIterator(STRVALMAP::iterator &valIter)
{
	if (!bCheckedForValues)
	{
		if (!loadValues())
			bHasValues = false;
	}
	if (bHasValues)
		return (valIter != m_valueMap.end());
	else
		return false;
}

bool CPoleLeaf::loadValues()
{
	unsigned char charBuf[5000];
	bool bDeleteStr = false;
	unsigned int bufSize = sizeof(charBuf);
	unsigned char *pStr = charBuf;
	bool bSkipValue = false;

	bCheckedForValues = true;
	std::string valueHead = VALUE_STREAM_ID;
	unsigned char buf[100];
	if (valueHead.size()+1 > 100)
		return false;	//should just be an assert to crash, but I don't know how to portably
	encSeek(0);
	decRead(buf, (unsigned int) valueHead.size()+1);
	std::string strID;
	strID = (const char *) buf;
	bHasValues = (valueHead.compare(strID) == 0);
	if (!bHasValues)
	{
		encSeek(0);
		return false;
	}
	else
	{
		clearValues();
		m_overriddenValueLen = 0;
		m_deletedValues = false;
		for (; ;)
		{
			bool bEOF;
			unsigned nameSize;
			nameSize = readI2(bEOF);
			if (bEOF)
				break;
			unsigned char valueName[500];
			if (nameSize >= sizeof(valueName))
				break; //Some Mac produced K3000 files caused this - just skip value
			if (decRead(valueName, nameSize) != nameSize)
			{
				m_errorCode = PLERR_TRUNCATED; //leaf stream terminated too soon.
				return false;
			}
			if (valueName[nameSize-1] != '\0')
			{
				m_errorCode = PLERR_CORRUPTED; //Corrupt Data in stream.
				bSkipValue = AskIfSkip();
				if (!bSkipValue)
					return false;
				else
					continue;
			}
			short dataType;
			dataType = readI2(bEOF);
			if (bEOF)
			{
				m_errorCode = PLERR_TRUNCATED;
				return false;
			}
			if (dataType != CValue::CVT_I4 && dataType != CValue::CVT_STRING)
			{
				m_errorCode = PLERR_CORRUPTED; //Corrupt Data
				return false;
			}
			CValue *value = NULL;
			if (dataType == CValue::CVT_I4)
			{
				int data;
				data = readI4(bEOF);
				if (bEOF)
				{
					m_errorCode = PLERR_TRUNCATED; //terminated too early
					return false;
				}
				value = new CValue(data);
			}
			else if (dataType == CValue::CVT_STRING)
			{
				unsigned int strSize;
				strSize = readI4(bEOF);
				if (bEOF)
				{
					m_errorCode = PLERR_TRUNCATED; //terminated too early
					return false;
				}
				if (bufSize < strSize)
				{
					if (bDeleteStr)
						delete [] pStr;
					pStr = new unsigned char[strSize];
					bDeleteStr = true;
					bufSize = strSize;
				}
				if (decRead(pStr, strSize) != strSize)
				{
					m_errorCode = PLERR_TRUNCATED; //terminated too early
					return false;
				}
				if (pStr[strSize - 1] != '\0')
				{
					m_errorCode = PLERR_CORRUPTED; //Corrupt data
					return false;
				}
				std::string newS = (const char *) pStr;
				value = new CValue(newS);
			}
			if (value)
			{
				std::string sValueName = (const char *) valueName;
				CValue *pOldVal = 0;
				STRVALMAP::iterator valIter = m_valueMap.find(sValueName);
				if (valIter != m_valueMap.end())
				{
					STRVALPAIR valPair = *valIter;
					pOldVal = valPair.second;
				}
				if (pOldVal)
				{
					if (pOldVal->GetDataType() == CValue::CVT_I4)
						m_overriddenValueLen += 4;
					else if (pOldVal->GetDataType() == CValue::CVT_STRING)
						m_overriddenValueLen += (unsigned int) pOldVal->GetStringValue().size() + 1;
					delete pOldVal;
					m_valueMap[sValueName] = value;
				}
				else
					m_valueMap[sValueName] = value;
			}
		}
		if (bDeleteStr)
			delete [] pStr;
		return true;
	}
}

void CPoleLeaf::clearValues()
{
	STRVALMAP::iterator itr;
	for (itr = m_valueMap.begin(); itr != m_valueMap.end(); itr++)
	{
		CValue * pVal = itr->second;
		delete pVal;
	}
	m_valueMap.clear();
}

short CPoleLeaf::readI2(bool &bEOF)
{
	bEOF = true;
	unsigned char c1, c2;
	unsigned int rbytes = decRead(&c1, 1);
	if (rbytes != 1)
		return -1;
	rbytes = decRead(&c2, 1);
	if (rbytes != 1)
		return -1;
	short val = (short) c1 + (short) (c2 << 8); //data always stored little-endian in KES
	bEOF = false;
	return val;
}

int CPoleLeaf::readI4(bool &bEOF)
{
	unsigned char c1, c2, c3, c4;
	bEOF = true;
	unsigned int rbytes = decRead(&c1, 1);
	if (rbytes != 1)
		return -1;
	rbytes = decRead(&c2, 1);
	if (rbytes != 1)
		return -1;
	rbytes = decRead(&c3, 1);
	if (rbytes != 1)
		return -1;
	rbytes = decRead(&c4, 1);
	if (rbytes != 1)
		return -1;
	bEOF = false;
	int val = (int) c1 + (int) (c2 << 8) +
		(int) (c3 << 16) + (int) (c4 << 24); //data always stored little-endian in KES
	return val;
}

void CPoleLeaf::writeI2(unsigned short value)
{
	unsigned char writeBuff[2];
	writeBuff[0] = (unsigned char) (value & 0xFF);
	writeBuff[1] = (unsigned char) ((value >> 8) & 0xFF);
	encWrite(writeBuff, 2);
}

void CPoleLeaf::writeI4(unsigned int value)
{
	unsigned char writeBuff[4];
	writeBuff[0] = (unsigned char) (value & 0xFF);
	writeBuff[1] = (unsigned char) ((value >> 8) & 0xFF);
	writeBuff[2] = (unsigned char) ((value >> 16) & 0xFF);
	writeBuff[3] = (unsigned char) ((value >> 24) & 0xFF);
	encWrite(writeBuff, 4);
}

bool CPoleLeaf::IsEmpty()
{
	if (!bCheckedForValues)
	{
		if (!loadValues())
			bHasValues = false;
	}
	if (bHasValues)
		return false;
	if (pStream->size() != 0)
		return false;
	return true;
}

bool CPoleLeaf::IsValid()
{
	if ((pStream == 0) || (!pStream->isValid()))
		return false;
	return true;
}

long CPoleLeaf::GetInteger(const std::string &valueName, long defaultValue)
{
	return GetInteger(valueName.c_str(), defaultValue);
}

long CPoleLeaf::GetInteger(const char *valueName, long defaultValue)
{
	if (!bCheckedForValues)
	{
		if (!loadValues())
			bHasValues = false;
	}
	long rtn = defaultValue;
	if (bHasValues)
	{
		std::string sName = valueName;
		CValue *value = 0;
		STRVALMAP::iterator valIter = m_valueMap.find(sName);
		if (valIter != m_valueMap.end())
		{
			STRVALPAIR valPair = *valIter;
			value = valPair.second;
		}
		if (value)
		{
			if (value->GetDataType() == CValue::CVT_I4)
				rtn = value->GetLongValue();
			else
				m_errorCode = PLERR_BADVALUETYPE; //incorrect value specifier
		}
	}
	return rtn;
}

void CPoleLeaf::SetInteger(const std::string &valueName, long newValue)
{
	SetInteger(valueName.c_str(), newValue);
}

void CPoleLeaf::SetInteger(const char *valueName, long newValue)
{
	if (!bCheckedForValues)
	{
		if (!loadValues())
			bHasValues = false;
	}
	if (!bHasValues && !IsEmpty())
		m_errorCode = PLERR_NOTVALUESLEAF; // Not a values leaf
	else
	{
		std::string sName = valueName;
		CValue *value = 0;
		STRVALMAP::iterator valIter = m_valueMap.find(sName);
		if (valIter != m_valueMap.end())
		{
			STRVALPAIR valPair = *valIter;
			value = valPair.second;
		}
		if (value && value->GetDataType() == CValue::CVT_I4 && value->GetLongValue() == newValue)
			return; //Nothing to do!
		else if (value)
		{
			m_overriddenValueLen += 4;
			delete value;
			CValue *newCVal = new CValue(newValue);
			newCVal->SetModified(true);
			m_valueMap[sName] = newCVal;
		}
		else
		{
			STRVALPAIR svp;
			svp.first = sName;
			svp.second = new CValue(newValue);
			svp.second->SetModified(true);
			m_valueMap.insert(svp);
		}
		m_modified = true;
		bHasValues = true;
	}
}

char * CPoleLeaf::GetString(const std::string &valueName, const std::string &defaultValue)
{
	return GetString(valueName.c_str(), defaultValue.c_str());
}

char * CPoleLeaf::GetString(const char *valueName, const char *defaultValue)
{
	if (!bCheckedForValues)
	{
		if (!loadValues())
			bHasValues = false;
	}
	const char *rtn = defaultValue;
	if (bHasValues)
	{
		std::string sName = valueName;
		CValue *value = 0;
		STRVALMAP::iterator valIter = m_valueMap.find(sName);
		if (valIter != m_valueMap.end())
		{
			STRVALPAIR valPair = *valIter;
			value = valPair.second;
		}
		if (value)
		{
			if (value->GetDataType() == CValue::CVT_STRING)
				rtn = value->GetStringValue().c_str();
			else
				m_errorCode = PLERR_BADVALUETYPE; //incorrect value specifier
		}
	}
	return (char *) rtn;
}

void CPoleLeaf::SetString(const std::string &valueName, const std::string &newValue)
{
	SetString(valueName.c_str(), newValue.c_str());
}

void CPoleLeaf::SetString(const char *valueName, const char *newValue)
{
	if (!bCheckedForValues)
	{
		if (!loadValues())
			bHasValues = false;
	}
	if (!bHasValues && !IsEmpty())
		m_errorCode = PLERR_NOTVALUESLEAF; // Not a values leaf
	else
	{
		std::string sName = valueName;
		std::string sNewValue = newValue;
		CValue *value = 0;
		STRVALMAP::iterator valIter = m_valueMap.find(sName);
		if (valIter != m_valueMap.end())
		{
			STRVALPAIR valPair = *valIter;
			value = valPair.second;
		}
		if (value && value->GetDataType() == CValue::CVT_STRING && value->GetStringValue() == sNewValue)
			return; //Nothing to do!
		else if (value)
		{
			m_overriddenValueLen += (unsigned int) value->GetStringValue().size() + 1;
			delete value;
			CValue *newValue2 = new CValue(sNewValue);
			newValue2->SetModified(true);
			m_valueMap[sName] = newValue2;
		}
		else
		{
			STRVALPAIR svp;
			svp.first = sName;
			svp.second = new CValue(sNewValue);
			svp.second->SetModified(true);
			m_valueMap.insert(svp);
		}
		m_modified = true;
		bHasValues = true;
	}
}

void CPoleLeaf::DeleteValue(const std::string &valueName)
{
	DeleteValue(valueName.c_str());
}

void CPoleLeaf::DeleteValue(const char *valueName)
{
	if (!bCheckedForValues)
	{
		if (!loadValues())
			bHasValues = false;
	}
	if (!bHasValues)
		return;
	std::string sName = valueName;
	STRVALMAP::iterator valIter = m_valueMap.find(sName);
	if (valIter != m_valueMap.end())
	{
		CValue *pVal = valIter->second;
		delete pVal;
		m_valueMap.erase(sName);
		m_deletedValues = true;
		m_modified = true;
	}
}

void CPoleLeaf::flush()
{
	if (m_modified)
		saveValues();
	pStream->flush();
}

void CPoleLeaf::saveValues()
{
	bool rewriteAll = false;
	unsigned long streamSize = (unsigned long) pStream->size();
	if (streamSize == 0 || m_deletedValues)
		rewriteAll = true;
	else if (m_overriddenValueLen > streamSize / 10)
		rewriteAll = true;
	else if (pEncrypter)
		rewriteAll = true;
	if (rewriteAll)
	{
		m_deletedValues = false;
		m_overriddenValueLen = 0;
		encSeek(0);
		std::string strID = VALUE_STREAM_ID;
		encWrite((unsigned char *) strID.c_str(), (unsigned int) strID.size() + 1);
	}
	else
		encSeek((unsigned long)pStream->size());
	STRVALMAP::iterator itr;
	for (itr = m_valueMap.begin(); itr != m_valueMap.end(); itr++)
	{
		std::string valName = itr->first;
		CValue * pVal = itr->second;
		if (rewriteAll || pVal->IsModified())
		{
			unsigned short nameSize = (unsigned short) valName.size() + 1;
			writeI2(nameSize);
			encWrite((unsigned char *) valName.c_str(), nameSize);
			short dataType = (short) pVal->GetDataType();
			writeI2(dataType);
			if (dataType == CValue::CVT_I4)
				writeI4(pVal->GetLongValue());
			else
			{
				int valueSize = (int) pVal->GetStringValue().size() + 1;
				writeI4(valueSize);
				encWrite((unsigned char *) pVal->GetStringValue().c_str(), valueSize);
			}
		}
		pVal->SetModified(false);
	}
	if (rewriteAll)
		pStream->setSize(pStream->tell()); //truncate the leaf
}

#define BUFSIZE 5000 //important to be bigger than 4096, which is the threshold size for pole storage...

void CPoleLeaf::CopyFromFile(const char *fileName, bool bRefresh)
{
	if (bRefresh)
		Refresh(); //clear out whatever was already there
	std::ifstream file;
	file.open(fileName, std::ios::binary|std::ios::in);
	// find size of input file
	file.seekg( 0, std::ios::end );
	long filesize = (long) file.tellg();
	file.seekg( 0 );
	unsigned char buffer[BUFSIZE];
	int bytesRead = 0;
	int bytesToRead;
	for (;;)
	{
		bytesToRead = filesize - bytesRead;
		if (bytesToRead <= 0)
			break;
		if (bytesToRead > BUFSIZE)
			bytesToRead = BUFSIZE;
		file.read((char *)buffer, bytesToRead);
		encWrite(buffer, bytesToRead);
		bytesRead += bytesToRead;
	}
	pStream->flush();
	file.close();
}


void CPoleLeaf::CopyToFile(const char *fileName, bool bFromZero)
{
	if (bFromZero)
		encSeek(0);
	std::ofstream file;
	file.open( fileName, std::ios::binary|std::ios::out );

	unsigned char buffer[BUFSIZE];
	for( ;; )
	{
		unsigned read = decRead( buffer, sizeof( buffer ) );
		file.write( (const char*)buffer, read  );
		if( read < sizeof( buffer ) ) break;
	}
	file.close();
}

void CPoleLeaf::CopyFromBuffer(unsigned char *pbuf, long size)
{
	encWrite(pbuf, size);
}

long CPoleLeaf::CopyToBuffer(unsigned char *pbuf, long maxSize)
{
	return decRead(pbuf, maxSize);
}

unsigned int CPoleLeaf::decRead(unsigned char *buf, unsigned int maxLen)
{
	if (pEncrypter)
	{
		unsigned char *buf2 = new unsigned char[maxLen+1];
		unsigned long pos = (unsigned long) pStream->tell();
		unsigned int nBytes = (unsigned int) pStream->read(buf2, maxLen);
		nBytes = pEncrypter->Decrypt(buf2, nBytes, buf, maxLen, pos, false);
		delete[] buf2;
		return nBytes;
	}
	else
		return (unsigned int) pStream->read(buf, maxLen);	
}



unsigned int CPoleLeaf::encWrite(unsigned char *buf, unsigned int len)
{
	if (pEncrypter)
	{
		unsigned char *buf2 = new unsigned char[len+1];
		unsigned int nBytes = (unsigned int) pEncrypter->Encrypt(buf, len, buf2, len, (unsigned long) pStream->tell(), false);
		nBytes = (unsigned int) pStream->write(buf2, nBytes);
		delete[] buf2;
		return nBytes;
	}
	else
		return (unsigned int) pStream->write(buf, len);
}

void CPoleLeaf::encSeek(unsigned long pos)
{
	if (pEncrypter)
		pEncrypter->Reset();
	pStream->seek(pos);
}

void CPoleLeaf::dumpValues()
{
	STRVALMAP::iterator itr;
	for (itr = m_valueMap.begin(); itr != m_valueMap.end(); itr++)
	{
		//CValue * pVal = itr->second;
        printf("%s = (NYI)\n", itr->first.c_str());
	}

}

bool CPoleLeaf::AskIfSkip()
{
	//Something has gone very wrong while we were trying to load values in a leaf. Ask the user if they'd like to skip the value, and if so, where should
	//we start for the next value?
	std::string command;
	std::cout << "An error has occurred while parsing a value. Would you like to skip to the next one?";
	std::getline(std::cin, command);
	if (command[0] == 'y' || command[0] == 'Y')
	{
		long streamPos = pStream->tell();
		long blockIdx = streamPos / 512;
		long blockOff = streamPos % 512;
		long fileBlockNum = pStream->BlockIdx2FileBlockNum(blockIdx);
		std::string sVal = std::to_string((fileBlockNum + 1) * 512);
		std::string sVal2 = std::to_string(streamPos);
		std::string sVal3 = std::to_string(blockOff);
		std::cout << "The Stream Offset is " << sVal2 << std::endl;
		std::cout << "The File Offset is " << sVal << std::endl;
		std::cout << "The Offset in the block is " << sVal3 <<std::endl;
		std::cout << "Please enter a new Stream Offset: ";
		std::getline(std::cin, command);
		long newPos;
		char *endPtr;
		newPos = std::strtol(command.c_str(), &endPtr, 10);
		newPos += (blockIdx * 512);
		pStream->seek(newPos);
		return true;
	}
	return false;
}
