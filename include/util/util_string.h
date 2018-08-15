/*
 * gbm_string.h
 *
 *  Created on: Oct 21, 2013
 *      Author: zch
 */

#ifndef GBM_STRING_H_
#define GBM_STRING_H_


#include <string>
#include <stdint.h>
#include <vector>



std::string StrError(int gbmerrno);

bool 	WildcardMatch(const char * pattern, const char* str, bool case_sensitive);


/**
* A simple exception class that records a message for the user.
*/
class Error {
private:
std::string errstr;
public:

/**
 * Create an error object with the given message.
 */
Error(const std::string& msg);

/**
 * Construct an error object with the given message that was created on
 * the given file, line, and functino.
 */
Error(const std::string& msg,
	  const std::string& file, int line, const std::string& function);

/**
 * Get the error message.
 */
const std::string& getMessage() const;
};

/**
* Check to make sure that the condition is true, and throw an exception
* if it is not. The exception will contain the message and a description
* of the source location.
*/
#define ASSERT(CONDITION, MESSAGE) \
{ \
  if (!(CONDITION)) { \
	throw Error((MESSAGE), __FILE__, __LINE__, \
								__PRETTY_FUNCTION__); \
  } \
}


//bool EndsWith(const std::string& str, char c);
bool EndsWith(const std::string& str, const std::string endStr);
bool StartsWith(const std::string& str, const std::string startStr);

std::string Section(const std::string& str, char sep, int start, int end);

/**
 * Convert an integer to a string.
 */
std::string ToString(int32_t x);

std::string To32Str(int32_t x);

std::string To32Str(int64_t x);

/**
 * Convert an integer to a string.
 */
std::string Short2Str(short x);

/**
 * Convert an integer to a string.
 */
std::string Int2Str(int32_t x);


/**
 * Convert an float to a string.
 */
std::string Float2Str(float x);

/**
 * Convert an double to a string.
 */
std::string Double2Str(double x);


/**
 * Convert an long integer to a string.
 */
std::string Int642Str(int64_t x);


/**
 * Convert a string to an integer.
 * @throws Error if the string is not a valid integer
 */
short ToShort(const std::string& val);

/**
 * Convert a string to an integer.
 * @throws Error if the string is not a valid integer
 */
int32_t ToInt(const std::string& val);

int64_t ToInt64(const std::string& val);

/**
 * Convert the string to a float.
 * @throws Error if the string is not a valid float
 */
float ToFloat(const std::string& val);

/**
 * Convert the string to a double.
 * @throws Error if the string is not a valid float
 */
double ToDouble(const std::string& val);

/**
 * Convert the string to a boolean.
 * @throws Error if the string is not a valid boolean value
 */
bool ToBool(const std::string& val);

/**
 * Get the current time in the number of milliseconds since 1970.
 */
uint64_t GetCurrentMillis();

/**
 * Split a string into "words". Multiple deliminators are treated as a single
 * word break, so no zero-length words are returned.
 * @param str the string to split
 * @param separator a list of characters that divide words
 */
std::vector<std::string> SplitString(const std::string& str,
        const char* separator);

std::vector<std::string> SplitString(const std::string& str);

std::vector<std::string> SplitString(const std::string& str, const char delim, bool trimmed = true);

std::string Replace(std::string& str, const char oldChar, const char newChar);
/**
 * Quote a string to avoid "\", non-printable characters, and the
 * deliminators.
 * @param str the string to quote
 * @param deliminators the set of characters to always quote
 */
std::string QuoteString(const std::string& str,
        const char* deliminators);
std::string QuoteString(const std::string& str);
/**
 * Unquote the given string to return the original string.
 * @param str the string to unquote
 */
std::string UnquoteString(const std::string& str);

std::string Trimmed(const std::string& str);

std::string ToLower(const std::string& str);

std::string Chop(const std::string& str, int n);

bool FloatEqual(float a, float b);
bool DoubleEqual(double a, double b);

#endif /* GBM_STRING_H_ */
