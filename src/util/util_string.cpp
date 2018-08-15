/*
 * gbm_string.cpp
 *
 *  Created on: Oct 21, 2013
 *      Author: zch
 */
#include "util/util_string.h"
#include <ctype.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/time.h>
#include <math.h>


#if 0
__thread int gbm_errno = 0;


std::string StrError(int gbmerrno) {
	std::string errMsg;
	if(gbmerrno == GBM_ERR_IN_ERRNO) {
		errMsg = strerror(errno);
	} else {

		char errStr[32];
		sprintf(errStr, "errno=%d", gbmerrno);
		errMsg = errStr;
	}

	return errMsg;
}
#endif

bool WildcardMatch(const char * pat, const char* str, bool case_sensitive) {
	static char mapCaseTable[256];
	static char mapNoCaseTable[256];
	static bool first = true;

	if(first) {
		for(int i = 0; i < 256; i++) {
			mapCaseTable[i] = i;
			mapNoCaseTable[i] = (i >= 'a' && i <='z') ? i + 'A' - 'a' : i;
		}
		first = false;
	}

	const char* table = case_sensitive ? mapCaseTable : mapNoCaseTable;

	const char* s, *p;
	bool star = false;

loopStart:
	for (s = str, p = pat; *s; ++s, ++p) {
		switch(*p) {
		case '?':
			if (*s == '.') goto starCheck;
			break;
		case '*':
			star = true;
			str = s, pat = p;
			do { ++pat; } while(*pat == '*');
			if(!*pat) return true;
			goto loopStart;
		default:
			if( table[*s] != table[*p])
				goto starCheck;
			break;
		}/*switch*/
	}/*for*/

	while (*p == '*') ++p;
	return (!*p);

starCheck:
	if(!star) return false;
	str++;
	goto loopStart;
}


using std::string;
using std::vector;


Error::Error(const std::string& msg) : errstr(msg) {
}

Error::Error(const std::string& msg,
        const std::string& file, int line,
        const std::string& function) {
    char lineStr[64];
    sprintf(lineStr, "%d", line);

    errstr = msg + " at " + file + ":" + lineStr +
            " in " + function;
}

string ToString(int32_t x) {
	char str[32];
	sprintf(str, "%d", x);
	return str;
}

std::string To32Str(int32_t x) {
    char str[32];
    sprintf(str, "%032d", x);
    return str;
}

std::string To32Str(int64_t x) {
    char str[32];
    sprintf(str, "%032lld", x);
    return str;
}


std::string Short2Str(short x) {
	 char str[32];
	sprintf(str, "%hd", x);
	return str;
}

string int2Str(int32_t x) {
	char str[32];
	sprintf(str, "%d", x);
	return str;
}

string Float2Str(float x) {
	char str[100];
	sprintf(str, "%f", x);
	return str;
}

string Double2Str(double x) {
	char str[100];
	sprintf(str, "%lf", x);
	return str;
}

string Int642Str(int64_t x) {
	char str[100];
	sprintf(str, "%ld", x);
	return str;
}
/**
 * Convert a string to an integer.
 * @throws Error if the string is not a valid integer
 */
 short ToShort(const std::string& val) {
	short result;
	char trash;
	int num = sscanf(val.c_str(), "%hd%c", &result, &trash);
	ASSERT(num == 1,
			"Problem converting " + val + " to short integer.");
	return result;
}

 int ToInt(const string& val) {
	int result;
	char trash;
	int num = sscanf(val.c_str(), "%d%c", &result, &trash);
	ASSERT(num == 1,
			"Problem converting " + val + " to integer.");
	return result;
}

int64_t ToInt64(const string& val) {
	int64_t result = 0;
	char trash;
	int num = sscanf(val.c_str(), "%lld%c", &result, &trash);

	return result;
}


 float ToFloat(const string& val) {
	float result;
	char trash;
	int num = sscanf(val.c_str(), "%f%c", &result, &trash);
	ASSERT(num == 1,
			"Problem converting " + val + " to float.");
	return result;
}

  double ToDouble(const string& val) {
	double result;
	char trash;
	int num = sscanf(val.c_str(), "%lf%c", &result, &trash);
	ASSERT(num == 1,
			"Problem converting " + val + " to double.");
	return result;
}

bool ToBool(const string& val) {
	string newVal = ToLower(val);
	if (newVal == "true") {
		return true;
	} else if (newVal == "false") {
		return false;
	} else {
		ASSERT(false,
				"Problem converting " + val + " to boolean.");
		return false;
	}
}

/**
 * Get the current time in the number of milliseconds since 1970.
 */
uint64_t GetCurrentMillis() {
	struct timeval tv;
	struct timezone tz;
	int sys = gettimeofday(&tv, &tz);
	ASSERT(sys != -1, strerror(errno));
	return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

vector<string> SplitString(const std::string& str, const char* separator) {
	vector<string> result;

	string::size_type prev_pos = 0;
	string::size_type pos = 0;
	while ((pos = str.find_first_of(separator, prev_pos)) != string::npos) {
		if (prev_pos < pos) {
			result.push_back(str.substr(prev_pos, pos - prev_pos));
		}
		prev_pos = pos + 1;
	}

	if (prev_pos < str.size()) {
		result.push_back(str.substr(prev_pos));
	}
	return result;
}

vector<string> SplitString(const string& str) {
	vector<string> result;

	int prev = 0;
	//string result(str);
	int strLen = str.length();
	int i = 0;
	while(i < strLen) {
		const char ch = str[i];
		if (isspace(ch) ) {

			if( i > prev) {
				result.push_back(std::string(str.c_str() + prev, i - prev));
			}
			prev = i + 1;
		}
		i++;
	}
	if( i > prev) {
		result.push_back(std::string(str.c_str() + prev, i - prev));
	}

	return result;
}

std::vector<std::string> SplitString(const std::string& str, const char delim, bool trimmed) {
	vector<string> result;

	int prev = 0;
	int cur  = 0;
	//string result(str);
	int strLen = str.length();
	int i = 0;

	if(trimmed) {

		while(i < strLen) {
			const char ch = str[i];
			if ( (ch == delim) || isspace(ch) ) {

				if( i > prev) {
					result.push_back(std::string(str.c_str() + prev, i - prev));
				}
				prev = i + 1;
			}
			i++;
		}
		if( i > prev) {
			result.push_back(std::string(str.c_str() + prev, i - prev));
		}

	} else {
		while(i < strLen) {
			const char ch = str[i];
			if (ch == delim ) {

				if( i > prev) {
					result.push_back(std::string(str.c_str() + prev, i - prev));
				}
				prev = i + 1;
			}
			i++;
		}
		if( i > prev) {
			result.push_back(std::string(str.c_str() + prev, i - prev));
		}
	}

	return result;
}


std::string Replace(std::string& str, const char oldChar, const char newChar) {

	std::string newStr;
	int size = str.size();
	for (int i = 0; i < size; i++) {
		if (str[i] == oldChar) {
			newStr += newChar;

		} else
			newStr += str[i];
	}

	return newStr;
}

string QuoteString(const string& str,
		const char* deliminators) {

	string result(str);
	for (int i = result.length() - 1; i >= 0; --i) {
		char ch = result[i];
		if (!isprint(ch) ||
				ch == '\\' ||
				strchr(deliminators, ch)) {
			switch (ch) {
				case '\\':
					result.replace(i, 1, "\\\\");
					break;
				case '\t':
					result.replace(i, 1, "\\t");
					break;
				case '\n':
					result.replace(i, 1, "\\n");
					break;
				case ' ':
					result.replace(i, 1, "\\s");
					break;
				default:
					char buff[4];
					sprintf(buff, "\\%02x", static_cast<unsigned char> (result[i]));
					result.replace(i, 1, buff);
			}
		}
	}
	return result;
}

string QuoteString(const string& str) {

	string result(str);
	for (int i = result.length() - 1; i >= 0; --i) {
		char ch = result[i];
		if (isspace(ch) ||
				ch == '\\' ) {
			switch (ch) {
				case '\\':
					result.replace(i, 1, "\\\\");
					break;
				case '\t':
					result.replace(i, 1, "\\t");
					break;
				case '\n':
					result.replace(i, 1, "\\n");
					break;
				case ' ':
					result.replace(i, 1, "\\s");
					break;
				default:
					char buff[4];
					sprintf(buff, "\\%02x", static_cast<unsigned char> (result[i]));
					result.replace(i, 1, buff);
			}
		}
	}
	return result;
}

string UnquoteString(const string& str) {
	string result(str);
	string::size_type current = result.find('\\');
	while (current != string::npos) {
		if (current + 1 < result.size()) {
			char new_ch;
			int num_chars;
			if (isxdigit(result[current + 1])) {
				num_chars = 2;
				ASSERT(current + num_chars < result.size(),
						"escape pattern \\<hex><hex> is missing second digit in '"
						+ str + "'");
				char sub_str[3];
				sub_str[0] = result[current + 1];
				sub_str[1] = result[current + 2];
				sub_str[2] = '\0';
				char* end_ptr = NULL;
				long int int_val = strtol(sub_str, &end_ptr, 16);
				ASSERT(*end_ptr == '\0' && int_val >= 0,
						"escape pattern \\<hex><hex> is broken in '" + str + "'");
				new_ch = static_cast<char> (int_val);
			} else {
				num_chars = 1;
				switch (result[current + 1]) {
					case '\\':
						new_ch = '\\';
						break;
					case 't':
						new_ch = '\t';
						break;
					case 'n':
						new_ch = '\n';
						break;
					case 's':
						new_ch = ' ';
						break;
					default:
						string msg("unknow n escape character '");
						msg += result[current + 1];
						ASSERT(false, msg + "' found in '" + str + "'");
				}
			}
			result.replace(current, 1 + num_chars, 1, new_ch);
			current = result.find('\\', current + 1);
		} else {
			ASSERT(false, "trailing \\ in '" + str + "'");
		}
	}
	return result;
}
/*
bool EndsWith(const std::string& str, char c) {
	if (str.empty())
		return false;

	int size = str.size();
	char lastChar = str.at(size - 1);

	if (lastChar == c)
		return true;
	else
		return false;
}*/
bool EndsWith(const std::string& str, const std::string endStr) {
	int strLen = str.size();
	int endStrLen = endStr.size();

	if (strLen < endStrLen)
		return false;
	std::string tempStr = str.substr(strLen - endStrLen);
	return tempStr == endStr;
}

bool StartsWith(const std::string& str, const std::string startStr) {
	if (str.size() < startStr.size())
		return false;
	int startSize = startStr.size();
	std::string tempStr = str.substr(0, startSize);
	return tempStr == startStr;
}

std::string Section(const std::string& str, char sep, int start, int end = -1) {
	std::string newStr;
	if (start < 0)
		return newStr;

	//position the start
	size_t startPos = 0;
	size_t pos = -1;

	for (int i = 0; i < start; i++) {
		pos = str.find(sep, pos + 1);
		if (pos == std::string::npos) {
			return newStr;
		}
	}
	startPos = pos;

	//position the end
	size_t endPos = 0;
	if (end == -1) {
		endPos = str.size();
	} else {
		pos = -1;
		for (int i = 0; i <= end; i++) {
			pos = str.find(sep, pos + 1);
			if (pos == std::string::npos) {
				pos = str.size();
				break;
			}
		}
		endPos = pos;
	}

	int len = endPos - startPos - 1;


	newStr = str.substr(startPos + 1, len);

	return newStr;

}

std::string Trimmed(const std::string& str) {
	if (str.empty())
		return str;

	int size = str.size();
	int index = 0;

	//trim left
	int pos = index;
	do {
		index = pos;
		char c = str.at(pos);
		switch (c) {
			case ' ':
			case '\t':
			case '\n':
			case '\r':
				pos++;
				break;
		}

	} while(pos != index && pos < size);

	if (pos == size) {
			return "";
	}


	std::string newStr = str.substr(pos, size - pos);

	//trim right
	size = newStr.size();
	index = size - 1;

	pos = index;
	do {
		index = pos;
		char c = newStr.at(pos);
		switch (c) {
			case ' ':
			case '\t':
			case '\n':
			case '\r':
				pos--;
				break;
		}
	} while(pos != index);

	std::string newNewStr = newStr.substr(0, pos + 1);

	return newNewStr;

}

std::string ToLower(const std::string& str) {
	std::string newStr;

	int size = str.size();
	for (int i = 0; i < size; i++) {
		char c = str.at(i);
		newStr += tolower(c);
	}

	return newStr;
}

std::string Chop(const std::string& str, int n) {
	int size = str.size();
	if (size >= n)
		return std::string();

	return str.substr(0, size - n);
}


bool FloatEqual(float a, float b) {
    return fabs(a - b) < 0.000001 ? true : false;
}

bool DoubleEqual(double a, double b) {
	return fabs(a - b) < 0.000001 ? true : false;
}
