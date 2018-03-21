/**
 * \class CEncrypter
 *
 * \brief This is a base class for encrypting and decrypting KESI files
 *
 * This class is not useful by itself, but should be inherited
 * by classes that provide various flavors for the encryption
 * of KES files. Two examples of such classes are CPlainTextEncrypter,
 * which itself doesn't actually encrypt at all, and CXOREncrypter,
 * which is the one portable encrypter that has been written thus
 * far.
 *
 *
 */

#if !defined(AFX_ENCRYPTER_H__DC4CF66B_F188_4420_9F79_8BFB11CFF79B__INCLUDED_)
#define AFX_ENCRYPTER_H__DC4CF66B_F188_4420_9F79_8BFB11CFF79B__INCLUDED_

#include <string>

class CEncrypter  
{
protected:
	CEncrypter();
	virtual ~CEncrypter();

public:
	/// A home made reference counter is used for allocation and freeing these objects - this is the Add
	void AddRef() {++m_ref;}
	/// A home made reference counter is used for allocation and freeing these objects - this removes a reference
	void Release() {if (this != NULL && --m_ref == 0) delete this;}

	/** \brief Encrypt a block of characters.
	  * \param[in]  src A pointer to the bytes to be encrypted.
	  * \param[in]  len The length, in bytes, of the data to be encrypted.
	  * \param[out] dst A pointer to the output area - at least len bytes should be reserved!
	  * \param[in]  maxLen The maximum length, in bytes, of the region pointed to by dst.
	  * \param[in]  posInStream The position in the output stream that the dst will be written to.
	  * \param[in]  bFinal Indicates whether or not this is the last chunk of data for this stream.
	  * \return     The number of characters actually encrypted and placed in *dst
	  *
	  * Please note that as currently written there is an expectation that the number of bytes
	  * in an encrypted string will match the number of bytes to be encrypted.
	  */
	virtual unsigned long Encrypt(unsigned char *src, unsigned long len, 
		unsigned char *dst, unsigned long maxLen, 
		unsigned long posInStream, bool bFinal) = 0;
	/** \brief Decrypt a block of characters that were once encrypted with the same method.
	  * \param[in]  src A pointer to the bytes to be decrypted.
	  * \param[in]  len The length, in bytes, of the data to be decrypted.
	  * \param[out] dst A pointer to the output area - at least len bytes should be reserved!
	  * \param[in]  maxLen The maximum length, in bytes, of the region pointed to by dst.
	  * \param[in]  posInStream The position in the input stream that the source was read from.
	  * \param[in]  bFinal Indicates whether or not this is the last chunk of data for this stream.
	  * \return     The number of characters actually decrypted and placed in *dst
	  *
	  * Please note that as currently written there is an expectation that the number of bytes
	  * in an encrypted string will match the number of bytes to be encrypted.
	  */
	virtual unsigned long Decrypt(unsigned char *src, unsigned long len,
		unsigned char *dst, unsigned long maxLen, 
		unsigned long posInStream, bool bFinal) = 0;
	/// Call this to indicate the end or beginning of an encryption or decryption for any one stream.  
	virtual void Reset() {}
	/// This should create and return an encrypted block of data containing password information.
	virtual std::string CreatePasswordVerificationData() = 0;
	/// Verify that a block of pasword data is correct. This is the consumer of the outpt from CreatePasswordVerificationData
	virtual bool VerifyPasswordData(unsigned char *buf, unsigned long len) = 0;

	/** \brief Encrypt the incoming string while keeping forward slash characters intact and in place.
	 * \param[in]  src - a pointer to the string to be encrypted
	 * \param[out] dst - a pointer to a buffer for the result, guaranteed to be at least as long as the source string
	 */
	void ScramblePath(unsigned char *src, unsigned char *dst);
	/** \brief Decrypt the incoming string while keeping forward slash characters intact and in place.
	 * \param[in]   src - a pointer to the string to be decrypted.
	 * \param[out]  dst - a pointer to a buffer for the result, guaranteed to be at least as long as the source string
	 *
	 * This will reverse the action of ScramblePath(). Note that a scrambled string is guaranteed to be
	 * the same length as an unscrambled string.
	 */
	void UnscramblePath(unsigned char *src, unsigned char *dst);

private:
	unsigned int m_ref;			 ///< A reference count
};

#endif // !defined(AFX_ENCRYPTER_H__DC4CF66B_F188_4420_9F79_8BFB11CFF79B__INCLUDED_)
