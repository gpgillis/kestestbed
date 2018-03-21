/**
 * \class CPoleLeaf
 *
 * \brief This reads and writes any node in a KESI file that can contains data.
 *
 * This class uses and, to a certain extent, isolates higher levels from POLE - Portable Structured Storage,
 * though direct access to a POLE Stream is made available, and is be necessary. It also handles KES values.
 * Any leaf can contain a collection of values - though it would then only contain values, not other
 * forms of data. Each value has a name that is unique to the collection, and it then has an associated
 * CVALUE, which can be a string or an integer.
 *
 */

#ifndef POLELEAF_H
#define POLELEAF_H

#include <string>
#include <map>
#include "pole.h"
#include "Encrypter.h"

class CPoleLeaf
{
public:
	/** \brief Create a new CPoleLeaf instance, using a POLE Stream.
	  * \param[in]  s A pointer to a POLE Stream.
	  * \param[in]  penc A pointer to a CEncrypter instance - NULL if the leaf is not encrypted.
	  *
	  * Note that this constructor is almost always called CPoleFile method.
	  */
	CPoleLeaf(POLE::Stream *s, CEncrypter *penc);
	~CPoleLeaf();
	/// Returns true if the leaf contains a set of KES values (named strings and/or integers)
	bool HasValues();
	/// Returns the number of named values found in this leaf - 0 if it is not a values leaf
	int ValueCount();
	/** \brief Returns the name of a value given its index (see ValueCount)
	  * \param[in]  index A zero based index into the collection of values - largest is ValueCount()-1
	  * \return A string pointer, containing the name for the value
	  */
	const char *GetValueName(long index);
	/** \brief Get a 4 byte integer given a value name
	  * \param[in]  valueName The name for the value
	  * \param[in]  defaultValue If the value isn't found in this leaf, defaultValue is returned
	  * \return the value associated with the name is returned, or defaultValue
	  */
	long GetInteger(const char *valueName, long defaultValue);
	/** \brief Get a 4 byte integer given a value name
	  * \param[in]  valueName The name for the value
	  * \param[in]  defaultValue If the value isn't found in this leaf, defaultValue is returned
	  * \return the value associated with the name is returned, or defaultValue
	  */
	long GetInteger(const std::string &valueName, long defaultValue);
	/** \brief Create or modify an 4 byte integer value
	  * \param[in]  valueName The name for the value
	  * \param[in]  newValue The new value itself.
	  */
	void SetInteger(const char *valueName, long newValue);
	/** \brief Create or modify an 4 byte integer value
	  * \param[in]  valueName The name for the value
	  * \param[in]  newValue The new value itself.
	  */
	void SetInteger(const std::string &valueName, long newValue);
	/** \brief Get a string given its value name
	  * \param[in]  valueName The name for the value
	  * \param[in]  defaultValue If the value isn't found in this leaf, defaultValue is returned
	  * \return the value associated with the name is returned, or defaultValue
	  */
	char * GetString(const char *valueName, const char *defaultValue);
	/** \brief Get a string given its value name
	  * \param[in]  valueName The name for the value
	  * \param[in]  defaultValue If the value isn't found in this leaf, defaultValue is returned
	  * \return the value associated with the name is returned, or defaultValue
	  */
	char * GetString(const std::string &valueName, const std::string &defaultValue);
	/** \brief Create or modify a string value
	  * \param[in]  valueName The name for the value
	  * \param[in]  newValue The new value itself.
	  */
	void SetString(const char *valueName, const char *newValue);
	/** \brief Create or modify a string value
	  * \param[in]  valueName The name for the value
	  * \param[in]  newValue The new value itself.
	  */
	void SetString(const std::string &valueName, const std::string &newValue);
	/** \brief Delete a value (either for an integer or a string)
	  * \param[in]  valueName the name for the value
	  */
	void DeleteValue(const char *valueName);
	/** \brief Delete a value (either for an integer or a string)
	  * \param[in]  valueName the name for the value
	  */
	void DeleteValue(const std::string &valueName);
	/// flush any changes made to this leaf (or, as it turns out, to anything in the KES file)
	void flush();
	/// return true if the leaf is currently empty (no values, and no other contents either)
	bool IsEmpty();
	/// return true if the leaf is valid, false if the attempt to open an existing leaf failed
	bool IsValid();
	/// seek to the beginning of the leaf, and set its size to 0
	void Refresh();
	/** \brief Write the contents of a file into the leaf, beginning at the current position.
	  * \param[in]  fileName the fully qualified name of the file.
	  * \param[in]  bRefresh Refresh the leaf, emptying its contents, if true.
	  */
	void CopyFromFile(const char *fileName, bool bRefresh);
	/** \brief Read the contents of leaf starting from the current position, and create a file with that contents.
	  * \param[in]  fileName the fully qualified name of the file.
	  * \param[in]  bFromZero If true, do a seek to make sure we start from the start of the leaf.
	  */
	void CopyToFile(const char *fileName, bool bFromZero);
	/** \brief Write the contents of a buffer into the leaf, beginning at its start.
	  * \param[in]  pbuf a pointer to the data which is to be written.
	  * \param[in]  size the size, in bytes, of the data.
	  */
	void CopyFromBuffer(unsigned char *pbuf, long size);
	/** \brief Read some of the contents of the leaf into a buffer
	  * \param[out]  pbuf a pointer to the data area which will contain the data.
	  * \param[in]  maxSize the maximum size, in bytes, which can be read.
	  * \return the actual number of bytes read.
	  */
	long CopyToBuffer(unsigned char *pbuf, long maxSize);
	/** \brief Read some of the contents of the leaf into a buffer.
	  * \param[out] buf a pointer to the data area which will contain the data.
	  * \param[in]  maxLen the maximum size, in bytes, which can be read.
	  * \return the actual number of bytes read.
	  */
	unsigned int decRead(unsigned char *buf, unsigned int maxLen);
	/** \brief Write the contents of a buffer into the leaf, beginning at the current position.
	  * \param[in]  buf a pointer to the data which is to be written.
	  * \param[in]  len the size, in bytes, of the data.
	  */
	unsigned int encWrite(unsigned char *buf, unsigned int len);
	/** \brief Seek to a particular position in a leaf's stream, and reset the encrypter.
	  * \param[in]  pos a position in the leaf's stream.
	  */
	void encSeek(unsigned long pos);
	/// return the POLE::Stream instance that is encapsulated by this object. See POLE::Stream documentation
	POLE::Stream *GetStream() {return pStream;}
	/// return the CEncrypter instance for this leaf. See CEncrypter documentation for usage information.
	CEncrypter *GetEncrypter() {return pEncrypter;}
	class CValue
	{
	public:
		enum CVTYPE //Needs to imitate values from WTypes.h - VT_ERROR, VT_I4, VT_BSTR
		{
			CVT_ERROR = 10,
			CVT_I4 = 3,
			CVT_STRING = 8
		};
		CValue() : m_strValue(), m_lValue(), m_type(CVT_ERROR), m_modified(false) {}
		CValue(const std::string& val) : m_strValue(val), m_lValue(), m_type(CVT_STRING), m_modified(false) {}
		CValue(long val) : m_strValue(), m_lValue(val), m_type(CVT_I4), m_modified(false) {}
		
		bool IsModified() const {return m_modified;}
		CVTYPE GetDataType() const {return m_type;}
		long GetLongValue() const {return m_lValue;}
		const std::string& GetStringValue() const {return m_strValue;}

		void SetModified(bool newVal) {m_modified = newVal;}

	private:
		std::string m_strValue;
		long m_lValue;
		CVTYPE m_type;
		bool m_modified;
	};

	/** \brief Get a values iterator, if possible, for the current leaf.
	  * \param[valIter] An iterator (use the STRVALMAP typedef here).
	  * \return true if an iterator was returned
	  */
	bool GetFirstValueIterator(std::map<std::string, CPoleLeaf::CValue *>::iterator &valIter);
	/** \brief Verify that an iterator (returned by GetFirstValueIterator and possibly iterated) is valid.
	  * \param[valIter] An iterator (use the STRVALMAP typedef here).
	  * \return true if the iterator has not reached the end of available values.
	  */
	bool IsValidIterator(std::map<std::string, CPoleLeaf::CValue *>::iterator &valIter);
    
    void dumpValues();
private:
	bool AskIfSkip();

	POLE::Stream *pStream;
	CEncrypter *pEncrypter;
	std::map<std::string, CPoleLeaf::CValue *> m_valueMap;
	unsigned int m_overriddenValueLen; //sum of length of overwritten values
	bool m_deletedValues; //If values are deleted, we'll do a full rewrite of those values
	bool bCheckedForValues;
	bool bHasValues;
	int m_errorCode;
	bool m_modified;

	bool loadValues();
	void clearValues();
	void saveValues();
	short readI2(bool &bEOF);
	int readI4(bool &bEOF);
	void writeI2(unsigned short value);
	void writeI4(unsigned int value);
    
private:    // define these as private to stop EffC++ warning about pointer members
    CPoleLeaf(const CPoleLeaf& rhs);
    CPoleLeaf& operator= (const CPoleLeaf& rhs);
};

typedef std::pair<std::string, CPoleLeaf::CValue *> STRVALPAIR;
typedef std::map<std::string, CPoleLeaf::CValue *> STRVALMAP;

#endif // POLELEAF_H
