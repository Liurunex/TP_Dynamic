
/**
 *	  Copyright (c) Freewheel, 2007-2009. All rights reserved.
 *
 *	  @file
 *
 *	  @author
 *
 *	  @brief
 *
 *	  Revision History:
 *			  2007/09/24	  jack
 *					  Created.
 *
 */
//=============================================================================

#ifndef ADS_TYPES_H
#define ADS_TYPES_H

#include <string>
#include <sstream>
#include <list>
#include <set>
#include <map>
#include <vector>
#include <tuple>
#include <deque>
#include <algorithm>
#include <cstdlib>
#include <cmath>
#include <random>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <math.h>
#include <errno.h>

#include <arpa/inet.h>	// inet_ntoa
#include <sys/syscall.h>

#define ADS_INT64_CONST(i)		((int64_t)(i##LL))

typedef std::string Ads_String;
typedef std::pair<Ads_String, Ads_String> Ads_String_Pair;
typedef std::vector<Ads_String> Ads_String_List;
typedef std::set<Ads_String> Ads_String_Set;
typedef std::map<Ads_String, Ads_String> Ads_String_Map;
typedef std::multimap<Ads_String, Ads_String> Ads_String_Multi_Map;
typedef std::vector<Ads_String_Pair> Ads_String_Pair_Vector;
typedef std::list<Ads_String_Pair> Ads_String_Pair_List;

typedef int64_t Ads_GUID;
typedef std::pair<Ads_GUID, Ads_GUID> Ads_GUID_Pair;
typedef std::list<Ads_GUID> Ads_GUID_List;
typedef std::list<Ads_GUID_Pair> Ads_GUID_Pair_List;
typedef std::vector<Ads_GUID> Ads_GUID_Vector;
typedef std::set<Ads_GUID> Ads_GUID_Set;
typedef std::set<Ads_GUID_Pair> Ads_GUID_Pair_Set;
typedef std::map<Ads_GUID, Ads_GUID> Ads_GUID_Map;
typedef std::multimap<Ads_GUID, Ads_GUID> Ads_GUID_Multi_Map;

/// Compatibility

#ifndef RTLD_DEEPBIND
#define RTLD_DEEPBIND 0
#endif
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

#if __cplusplus >= 201103L || defined(__GXX_EXPERIMENTAL_CXX0X__)
#define compat std
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include <ext/hash_map>
namespace ads {
	using std::unordered_map;
	using std::unordered_multimap;
	using std::unordered_set;

	inline size_t __stl_hash_string(const char* __s)
	{
		#if __APPLE__
		return __stl_hash_string(__s);
		#else
		return __gnu_cxx::__stl_hash_string(__s);
		#endif
	}

	inline size_t __stl_hash_string(const Ads_String& s) { return __stl_hash_string(s.c_str()); }

}

#define hash_map unordered_map
#define hash_multimap unordered_multimap
#define hash_set unordered_set

#else

#define compat __gnu_cxx
#include <tr1/memory>		// tr1::shared_ptr
#define nullptr 0
namespace std {
	using namespace tr1;
};

#include <ext/hash_map>
#include <ext/hash_set>

namespace ads {
	using __gnu_cxx::hash_map;
	using __gnu_cxx::hash_multimap;
	using __gnu_cxx::hash_set;

	inline size_t __stl_hash_string(const char* __s)
	{
		return __gnu_cxx::__stl_hash_string(__s);
	}
}

namespace compat {
	template<> struct hash<std::string>
    {
        #if defined(__APPLE__)
        size_t operator()(const report::string& __x) const { return ads::__stl_hash_string(__x.c_str()); }
        #else
        size_t operator()(const std::string& __x) const { return __stl_hash_string(__x.c_str()); }
        #endif
    };
}

#endif

namespace ads {
	inline size_t djb2_hash(const char *str)
    {
        size_t hash = 5381;
        int c;
        while ((c = *str++))
		{
            hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
		}

        return hash;
    }

};

namespace compat {
	template<> struct hash<void *>
	{
		size_t operator()(void *__x) const
		{
			return (size_t)(__x);
		}
	};

	#if __cplusplus < 201103L
	#if !defined(__WORDSIZE) || __WORDSIZE != 64 || __APPLE__
	template<> struct hash<Ads_GUID>
	{
		 size_t operator()(Ads_GUID __x) const
		 {
			 return (size_t)(__x);
		 }
	};
	#endif
	#endif

	template<> struct hash<Ads_GUID_Pair >
	{
		   size_t operator()(const Ads_GUID_Pair& __x) const
		   {
			   return (size_t)(__x.first ^ __x.second);
		   }
	};
}

#define ADS_XID_HASH_COMPARE  compat::hash<Ads_XID>, std::equal_to<Ads_XID>

#define ADS_GUID_HASH_COMPARE  Ads_GUID_Hash, std::equal_to<Ads_GUID>
#define ADS_RSTRING_HASH_COMPARE Ads_RString_Hash, std::equal_to<Ads_RString>
#define ADS_GUID_PAIR_HASH_COMPARE Ads_GUID_Pair_Hash, std::equal_to<Ads_GUID_Pair>
typedef std::shared_ptr<Ads_String> Ads_Message_Block;

#include "User_State.pb.h"
typedef std::shared_ptr<ads::User_State> Ads_User_State_Ptr;

#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <dlfcn.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/file.h>

#define PROT_RDWR (PROT_READ|PROT_WRITE)

namespace ads {
	enum ENVIRONMENT
	{
		ENV_NONE = 0,
		ENV_PAGE = 1,
		ENV_PLAYER = 2,
		ENV_VIDEO = 3,
		ENV_LAST = 4
	};

	enum MEDIA_TYPE
	{
		MEDIA_NONE = 0,
		MEDIA_SITE_SECTION = 0x01,
		MEDIA_VIDEO = 0x02,
		MEDIA_ALL = MEDIA_SITE_SECTION | MEDIA_VIDEO
	};

	enum STANDARD_AD_UNIT
	{
		UNKNOWN_AD_UNIT = 0,
		PREROLL = 1,
		MIDROLL = 2,
		POSTROLL = 3,
		OVERLAY = 4,
		DISPLAY = 5,
		PAUSE_MIDROLL = 6,
		ANY_INTERRUPTIVE = 7,
		ANY_IN_VIDEO = 8
	};

	inline MEDIA_TYPE media_type(ENVIRONMENT env)
	{
		return (env == ENV_PAGE)? MEDIA_SITE_SECTION : MEDIA_VIDEO;
	}

	inline const char * env_name(ENVIRONMENT env)
	{
		static const char *dict[] = { "none", "page", "player", "video", "last" };
		return dict[env];
	}

	inline const char * media_name(MEDIA_TYPE mt)
	{
		static const char *dict[] = { "none", "site", "video", "site/video" };
		return dict[mt];
	}

	inline const char *standard_ad_unit_name(STANDARD_AD_UNIT au)
	{
		typedef std::pair<STANDARD_AD_UNIT, const char *> P;
		static const P _m[] =
		{
			P(PREROLL, "preroll"),
			P(MIDROLL, "midroll"),
			P(POSTROLL, "postroll"),
			P(OVERLAY, "overlay"),
			P(DISPLAY, "display"),
			P(PAUSE_MIDROLL, "pause_midroll")
		};

		typedef std::map<STANDARD_AD_UNIT, const char *> M;
		static const M m(_m, _m + sizeof(_m) / sizeof(_m[0]));

		M::const_iterator it = m.find(au);
		return it != m.end()? it->second: "unknown";
	}

	inline STANDARD_AD_UNIT standard_ad_unit(const Ads_String& name)
	{
		typedef std::pair<Ads_String, STANDARD_AD_UNIT> P;
		static const P _m[] =
		{
			P("preroll", PREROLL),
			P("midroll", MIDROLL),
			P("postroll", POSTROLL),
			P("overlay", OVERLAY),
			P("display", DISPLAY),
			P("pause_midroll", PAUSE_MIDROLL)
		};

		typedef std::map<Ads_String, STANDARD_AD_UNIT> M;
		static const M m(_m, _m + sizeof(_m) / sizeof(_m[0]));

		M::const_iterator it = m.find(name);
		return it != m.end()? it->second: UNKNOWN_AD_UNIT;
	}

	template <typename STRING> inline bool is_integer(const STRING& s)
	{
		return (! s.empty() && s.find_first_not_of("0123456789") == STRING::npos);
	}

	/// copy from multi_cpu_increment in ace/Atomic_Op.cpp
	inline long exchange(volatile long *value, long rhs)
	{
		unsigned long addr = reinterpret_cast<unsigned long>(value);
		#if defined(__GNUC__)
		// The XCHG instruction automatically follows LOCK semantics
		asm volatile ("xchg %0, (%1)" : "+r"(rhs) : "r"(addr));
		#endif
		return rhs;
	}

	void print_stacktrace();

	inline int64_t str_to_i64(const char* s) 		{ errno = 0; return strtoll(s, 0, 10); }
	inline int64_t str_to_i64(const Ads_String& s) 	{ return str_to_i64(s.c_str()); }
	inline uint64_t str_to_u64(const char* s)       { errno = 0; return strtoull(s, 0, 10); }
	inline uint64_t str_to_u64(const Ads_String& s)     { return str_to_u64(s.c_str()); }

	template<class T> inline void delete_ptr(T *ptr) { delete ptr; }

	inline Ads_String i64_to_str(int64_t i)
	{
		char buf[128], *b = buf;
		char out[0xff], *buffer = out;

		if(i < 0)
		{
			*(buffer++) = '-';
			i = -i;
		}
		if(i == 0) *(buffer++) = '0';
		else
		{
			while(i > 0)
			{
				*(b++) = '0' + ((char)(i%10));
				i /= 10;
			}
			while(b > buf) *(buffer++) = *(--b);
		}
		*buffer = 0;

		return Ads_String(out);
	}

	inline Ads_String
	u64_to_str(uint64_t i)
	{
		char buf[128], *b = buf;
		char out[0xff], *buffer = out;

		if(i == 0) *(buffer++) = '0';
		else
		{
			while(i > 0)
			{
				*(b++) = '0' + ((char)(i%10));
				i /= 10;
			}
			while(b > buf) *(buffer++) = *(--b);
		}
		*buffer = 0;

		return Ads_String(out);
	}

	inline std::string url_hash(const char* url)
	{
		return ads::u64_to_str(ads::__stl_hash_string(url)) + ads::u64_to_str(ads::djb2_hash(url));
	}

	inline bool is_prefix(const Ads_String &str, const Ads_String &substr)
	{
		return str.compare(0, substr.length(), substr) == 0;
	}

	inline bool is_suffix(const Ads_String &str, const Ads_String &substr)
	{
		return str.length() >= substr.length()
		       && str.compare(str.length() - substr.length(), substr.length(), substr) == 0;
	}

	inline const char * tolower(char *s, size_t length)
	{
		char *p = s, *end = p + length;
		for(; p < end && *p; ++p) *p = std::tolower(*p);
		return s;
	}

	template <typename STRING> inline const STRING& tolower(STRING& s)
	{
		std::transform(s.begin(), s.end(), s.begin(), (int(*)(int)) std::tolower);
		return s;
	}
	template <typename STRING> inline STRING tolower_copy(const STRING& s)
	{
		STRING result = s;
		tolower(result);
		return result;
	}

	template <typename STRING> inline const STRING& toupper(STRING & s)
	{
		std::transform(s.begin(), s.end(), s.begin(), (int(*)(int)) std::toupper);
		return s;
	}
};

#include "Ads_GUID.h"

#define ADS_CONST_STRING_LENGTH(x) (x ? sizeof(x) - 1 : 0)

#define ADS_PERCENTAGE_MAGNIFYING_FACTOR		10000
#define ADS_CURRENCY_MAGNIFYING_FACTOR			(1000 * 1000)

#define ADS_TO_MAGNIFIED_PERCENTAGE(a)				((int64_t)((a) * ADS_PERCENTAGE_MAGNIFYING_FACTOR))
#define ADS_TO_MAGNIFIED_CURRENCY(a)				((int64_t)((a) * ADS_CURRENCY_MAGNIFYING_FACTOR))
#define ADS_REVERSE_MAGNIFIED_CURRENCY(a)			((double)(a) / ADS_CURRENCY_MAGNIFYING_FACTOR)

#define ADS_TO_REAL_PERCENTAGE(a)					(double(a) / ADS_PERCENTAGE_MAGNIFYING_FACTOR )
#define ADS_TO_REAL_CURRENCY(a)						(double(a) / ADS_CURRENCY_MAGNIFYING_FACTOR )

#define ADS_APPLY_MAGNIFIED_PERCENTAGE(a, b)		((int64_t)((a) * ADS_TO_REAL_PERCENTAGE(b)))
#define ADS_APPLY_REVERSE_MAGNIFIED_PERCENTAGE(a, b)	((a) / ADS_TO_REAL_PERCENTAGE(b))

namespace std {
	template <>
	struct equal_to<const char *>: public binary_function<const char *, const char *, bool>
	{
		bool operator()(const char* p1, const char*p2) const
		{
			return strcmp(p1,p2)==0;
		}
	};
};

struct Ads_GUID_Hash {
	size_t
	operator()(Ads_GUID id) const
	{
		return (size_t)(ads::entity::id(id));
	}

	bool operator()(Ads_GUID k1, Ads_GUID k2) const
	{
		return k1 < k2;
	}
};

struct Ads_GUID_Pair_Hash {
	size_t operator()(const Ads_GUID_Pair& k) const
	{
		return (size_t)(ads::entity::id(k.first) << 16 | ads::entity::id(k.second));
	}

	bool operator()(const Ads_GUID_Pair& k1, const Ads_GUID_Pair& k2) const
	{
		return (k1.first < k2.first || (k1.first == k2.first && k1.second < k2.second));
	}
};

typedef ads::hash_set<Ads_GUID> Ads_GUID_HSet;

#define AVOIDANCE_NETWORK_DEFAULT (-2)
#define AVOIDANCE_MAX_LEVEL (3 + 1)

enum LOG_PRIORITY {
	LP_TRACE = 0x0001,
	LP_DEBUG = 0x0002,
	LP_INFO = 0x0004,
	LP_WARNING = 0x0008,
	LP_ERROR = 0x0010,
	LP_CRIT = 0x0020
};

namespace ads {
	inline const char * log_priority_name(int priority) {
		switch(priority)
		{
		case LP_TRACE:
			return "trace";
		case LP_DEBUG:
			return "debug";
		case LP_INFO:
			return "info ";
		case LP_WARNING:
			return "warn ";
		case LP_ERROR:
			return "error";
		case LP_CRIT:
			return "crit ";
		default:
			break;
		}

		return "unkn";
	}

	bool is_log_enabled(LOG_PRIORITY priority, const char* format, ...);
	void log_state(bool simple, const char *file, int line, const char *function);
	int log(LOG_PRIORITY priority, const char *format, ...);

	template<class _InIt, class _Fn1> inline _Fn1 for_each2nd(_InIt _First, _InIt _Last, _Fn1 _Func)
	{
		for(; _First != _Last; ++_First)
			_Func((*_First).second);
		return (_Func);
	}

	template<typename _InputIterator1, typename _InputIterator2> inline bool set_overlapped(_InputIterator1 __first1, _InputIterator1 __last1,
	        _InputIterator2 __first2, _InputIterator2 __last2)
	{
		while(__first1 != __last1 && __first2 != __last2)
			if(*__first1 < *__first2)
				++__first1;
			else if(*__first2 < *__first1)
				++__first2;
			else
				return true;

		return false;
	}

    template<typename K1, typename K2>
    static std::pair<K1, K2>& operator +=(std::pair<K1, K2>& p1, const std::pair<K1, K2>& p2)
    {
        p1.first += p2.first;
        p1.second += p2.second;
        return p1;
    }

	// require V operator +=
	template <typename M1, typename M2>
	static void merge_delta(M1& large, const M2& small)
	{
		for (typename M2::const_iterator it = small.begin(); it != small.end(); ++it)
			large[it->first] += it->second;
	}

	/*
	 * Split string <str> into tokens and insert each token into container <c>.
	 * The order of tokens is preserved, if posible.
	 * @param
	 *	 ANY of the characters in <delim> is used as delimiter.  (as std::string::find_first_of())
	 *	 Zero-width heads/tails are NOT ignored.  (":a:b::" => {"", "a", "b", "", ""})
	 *	 <str> is split into no more than max(1, <limit>) tokens.  ("a:b:c",2 => {"a", "b:c"})
	 * @return
	 *	  Number of split tokens.  It is always positive.
	 *	  Tokens are inserted into <c>.  Note that this function does NOT clear() <c>.
	 */
	template<class C, class D> size_t
	tokenize(/* OUT */ C &c, const Ads_String &str, const D &delim, size_t limit = (size_t)-1)
	{
		size_t n = 1;
		Ads_String::size_type last_ptr = 0, ptr = 0;
		while(limit > 1 && (ptr = str.find_first_of(delim, last_ptr)) != Ads_String::npos)
		{
			c.insert(c.end(), str.substr(last_ptr, ptr - last_ptr));
			last_ptr = ptr + 1;
			-- limit;
			++ n;
		}
		c.insert(c.end(), str.substr(last_ptr));
		return n;
	}

	template<class C, class S> size_t split(const S &s, C& c, char delim)
	{
		size_t i=0, j=0, n=0, len = s.length();

		while((j < len) && ((i = s.find(delim, j)) != S::npos))
		{
			c.push_back(s.substr(j, i - j));
			++n;
			++i;
			j = i;
		}

		if(j < s.length())
		{
			c.push_back(s.substr(j));
			++n;
		}

		return n;
	}

	template<class C, class S> size_t split(const S &s, C& c, const char *delim)
	{
		size_t i=0, j=0, n=0, len = s.length(), m = strlen(delim);

		while((j < len) && ((i = s.find(delim, j)) != S::npos))
		{
			c.push_back(s.substr(j, i - j));
			++n;
			i += m;
			j = i;
		}

		if(j < s.length())
		{
			c.push_back(s.substr(j));
			++n;
		}

		return n;
	}

	template <class C> size_t split(const char *s, C&c, const char *delim) { return split(Ads_String(s), c, delim); }

	template<class C, class S> size_t split_q(const S &s, C& c, char delim, char quote = '"')
	{
		size_t length = s.length();

		size_t start  = 0, n = 0;
		while(start < length)
		{
			size_t i = start;

			bool skipping = false;
			while (i < length)
			{
				if (s[i] == quote)
				{
					++i;
					skipping = true;
					while (i < length && s[i] != quote) ++i;
				}

				if (skipping)
				{
					if (i == length)
						break;

					++i;
					skipping = false;
					continue;
				}

				if (s[i] == delim)
					break;

				++i;
			}

			if (i >= length)
			{
				c.push_back(s.substr(start));
				start = length;
				++n;
			}
			else
			{
				c.push_back(s.substr(start, i -  start));
				++n;
				start = i + 1;
			}
		}

		return n;
	}

	template<class C>
	Ads_String
	concat(const C& c, char delim, typename C::const_iterator end)
	{
		Ads_String s;
		for(typename C::const_iterator it = c.begin(); it != end;)
		{
			s += *it;
			if(++it != end) s += delim;
		}

		return s;
	}

	template<class C> Ads_String concat(const C& c, char delim) { return concat(c, delim, c.end()); }

	template <class Container>
	inline void copy_if_not_found(const Container& src, Container& dst)
	{
		for(typename Container::const_iterator it = src.begin(); it != src.end(); ++it)
		{
			if(dst.find(it->first) == dst.end()) dst.insert(*it);
		}
	}

	inline
	Ads_String
	trim(const Ads_String& src, const Ads_String& delims = " \t\r\n")
	{
		Ads_String result(src);
		Ads_String::size_type i = result.find_last_not_of(delims);

		if(i != Ads_String::npos)
			result.erase(++i);

		i = result.find_first_not_of(delims);
		if(i != Ads_String::npos)
			result.erase(0, i);
		else
			result.erase();

		return result;
	}

	inline std::string
	replace(const std::string& search, const std::string& replace, const std::string& subject)
	{
		std::string res;
		size_t pos = 0, pos1, len = subject.length();
		res.reserve(len);
		while(pos < len)
		{
			pos1 = subject.find(search, pos);
			if(pos1 == std::string::npos)
			{
				break;
			}
			else
			{
				res.append(subject, pos, pos1 - pos);
				res.append(replace);
				pos = pos1 + search.length();
			}
		}
		res.append(subject, pos, std::string::npos);
		return res;
	}

	inline const Ads_String&
	replace(Ads_String& src, char r, char s)
	{
		for(Ads_String::iterator it = src.begin(); it != src.end(); ++it)
		{
			if(*it == r) *it = s;
		}
		return src;
	}

	inline Ads_String
	remove_any_of(Ads_String &src, const Ads_String& delims = " \t\r\n")
	{
		Ads_String::size_type begin = 0;
		Ads_String::size_type end = src.find_first_of(delims);
		if(end == Ads_String::npos) return Ads_String(src);

		Ads_String s;
		while(end != Ads_String::npos)
		{
			s += src.substr(begin, end - begin);
			begin = src.find_first_not_of(delims, end);
			end = src.find_first_of(delims, begin);
		}

		if(begin != Ads_String::npos)
			s += src.substr(begin);
		return s;

	}

	inline Ads_String
	to_hex_string(const char *buf, size_t length)
	{
		#define C2H(c) ((c) < 10)? (c) + '0' : ((c) <= 0x0F)? (c) - 10 + 'a' : '-'
		Ads_String s;
		s.resize(2 * length);
		for(size_t i=0; i<length; i++)
		{
			unsigned char c1 = ((unsigned char)(buf[i] & 0xf0)) >> 4;
			unsigned char c2 = (unsigned char)(buf[i] & 0x0f);
			s[i*2] = C2H(c1);
			s[i*2+1] = C2H(c2);
		}
		s[length * 2] = '\0';
		return s;
		#undef C2H
	}

	template <typename I>
	inline Ads_String to_bin_string(I v)
	{
		char buf[0xff] = {0};
		I j = 1;
		for(size_t i = 0; i < sizeof(v) * 8; ++i)
		{
			buf[i] = (v & j)? '1': '0';
			j <<= 1;
		}

		return buf;
	}

	inline size_t
	from_hex_string(const Ads_String &s, char *buf, size_t length)
	{
		#define H2C(c) ((c) >= '0' && (c) <= '9')? (c) - '0' : ((c) >= 'A' && (c) <= 'F')? (c) - 'A' + 10 : ((c) >= 'a' && (c) <= 'f')? (c) - 'a' + 10 : (-1)
		if(s.length() %2 != 0 || length < s.length() / 2)
			return -1;

		for(size_t i = 0; i<s.length(); i+= 2)
		{
			char c1 = H2C(s[i]);
			char c2 = H2C(s[i+1]);

			if(c1 < 0 || c2 < 0)
				return -1;

			buf[i/2] = (unsigned char)(c1 << 4) + c2;
		}

		return s.length() / 2;
		#undef H2C
	}


	template <class C>
	static Ads_String entities_to_pretty_str(const C& c, char delimeter = ',')//for debug
	{
		Ads_String s;
		for(typename C::const_iterator it=c.begin(); it!=c.end(); ++it)
		{
			s += ads::entity::pretty_str(*it);
			s += delimeter;
		}
		return s;
	}

	template <class C>
	static Ads_String entities_to_str(const C& c, char delimeter = ',')
	{
		Ads_String s;
		for(typename C::const_iterator it=c.begin(); it!=c.end(); ++it)
		{
			s += ads::entity::str(*it);
			s += delimeter;
		}
		return s;
	}

	template <class C>
	static Ads_String entitie_ptrs_to_str(const C& c, char delimeter = ',')
	{
		Ads_String s;
		for(typename C::const_iterator it=c.begin(); it!=c.end(); ++it)
		{
			if(*it == 0)
				continue;
			s += ads::entity::str((*it)->id_);
			s += delimeter;
		}
		return s;
	}

	template <class C>
	static Ads_String string_list_to_str(const C& c, char delimeter = ',')
	{
		Ads_String s;
		for(typename C::const_iterator it=c.begin(); it!=c.end(); ++it)
		{
			s += *it;
			s += delimeter;
		}
		return s;
	}


	template <typename OutputIterator>
	static size_t str_to_entities(const Ads_String& s, int type, int subtype, OutputIterator oi, char delimeter = ',')
	{
		std::list<Ads_String> items;
		ads::split(s, items, delimeter);

		size_t n = 0;
		for(std::list<Ads_String>::const_iterator it = items.begin(); it != items.end(); ++it, ++oi)
		{
			const Ads_String& item = *it;
			if(item.empty()) continue;
			Ads_GUID id = ads::entity::make_id(type, subtype, ads::str_to_i64(item));

			*oi = id;
			++n;
		}

		return n;
	}

	inline Ads_GUID str_to_asset_id(const Ads_String& s)
	{
		if(s.empty()) return ads::entity::invalid_id(ADS_ENTITY_TYPE_ASSET);
		if(s[0] == 'g')
			return ads::asset::make_id(ADS_ENTITY_TYPE_ASSET, 1, ads::str_to_i64(s.substr(1)));
		else
			return ads::entity::make_id(ADS_ENTITY_TYPE_ASSET, ads::str_to_i64(s));
	}

	inline Ads_GUID str_to_section_id(const Ads_String& s)
	{
		if(s.empty()) return ads::entity::invalid_id(ADS_ENTITY_TYPE_SECTION);
		if(s[0] == 'g')
			return ads::asset::make_id(ADS_ENTITY_TYPE_SECTION, 1, ads::str_to_i64(s.substr(1)));
		else
			return ads::entity::make_id(ADS_ENTITY_TYPE_SECTION, ads::str_to_i64(s));
	}

	inline Ads_GUID str_to_profile_id(const Ads_String& s)
	{
		if(s.empty()) return -1;
		if(s[0] == 'g')
			return ads::entity::make_id(1 /* Ads_Environment_Profile::COMPOUND */, ads::str_to_i64(s.substr(1)));
		else
			return ads::entity::make_id(0, ads::str_to_i64(s));
	}

	inline Ads_GUID str_to_entity_id(int type, const Ads_String& s)
	{
		if(s.empty()) return ads::entity::invalid_id(type);
		return ads::entity::make_id(type, ads::str_to_i64(s));
	}

	inline Ads_GUID str_to_asset_section_id(const Ads_String& s)
	{
		if(s.empty()) return ads::entity::invalid_id(ADS_ENTITY_TYPE_ASSET);

		if(s[0] == 'v')
			return str_to_asset_id(s.substr(1));
		else if(s[0] == 's')
			return str_to_section_id(s.substr(1));
		else if(s[0] == 'a')
			return str_to_entity_id(ADS_ENTITY_TYPE_AUDIENCE, s.substr(1));
		else
			return str_to_section_id(s);
	}

	inline Ads_GUID str_to_content_id(const Ads_String& s)
	{
		if (s.empty()) return ads::entity::invalid_id(ADS_ENTITY_TYPE_ASSET);
		if (s[0] == 'p')
			return str_to_entity_id(ADS_ENTITY_TYPE_CONTENT_PACKAGE, s.substr(1));
		else
			return str_to_asset_section_id(s);
	}

	inline Ads_String entity_id_to_str(Ads_GUID id)
	{
		if(!ads::entity::is_valid_id(id)) return "-1";
		return ads::entity::str(id);
	}

	inline Ads_String asset_id_to_str(Ads_GUID id)
	{
		if(!ads::entity::is_valid_id(id)) return "-1";

		if(ads::asset::is_group(id))
			return Ads_String("g") + ads::entity::str(id);
		else
			return ads::entity::str(id);
	}

	inline Ads_String asset_section_id_to_str(Ads_GUID id)
	{
		Ads_String s = "";
		if(!ads::entity::is_valid_id(id)) return "-1";

		if(ads::entity::type(id) == ADS_ENTITY_TYPE_SECTION)
			s += "s";
		else if (ads::entity::type(id) == ADS_ENTITY_TYPE_ASSET)
			s += "v";
		else
			s += "a"; // audience item

		if(ads::asset::is_group(id))
			s +=  "g";
		return s + ads::entity::str(id);
	}

	inline Ads_GUID str_to_term_id(const Ads_String& s)
	{
		if(s.empty()) return ads::entity::invalid_id(ADS_ENTITY_TYPE_TARGETING_TERM);

		size_t pos = s.find(":");
		if(pos == Ads_String::npos)
			return ads::entity::make_id(ADS_ENTITY_TYPE_TARGETING_TERM, ads::str_to_i64(s));
		else
			return ads::term::make_id(ads::str_to_i64(s.substr(0, pos)), ads::str_to_i64(s.substr(pos + 1)));
	}

	inline Ads_String entity_id_to_tag_str(Ads_GUID id)
	{
		return asset_section_id_to_str(id);
	}

	inline Ads_GUID tag_str_to_entity_id(const Ads_String& s)
	{
		return str_to_asset_section_id(s);
	}

	inline Ads_String term_id_to_str(Ads_GUID id)
	{
		if(!ads::entity::is_valid_id(id)) return "-1";
		return ads::term::str(id);
	}

	template<typename S>
	inline bool is_numeric(const S& s)
	{
		for(typename S::const_iterator c = s.begin(); c != s.end(); c++)
		{
			if(! ::isdigit(*c)) return false;
		}
		return true;
	}

	inline bool is_float(Ads_String & s)
	{
		std::istringstream iss(s.c_str());
		float f;
		iss >> std::noskipws >> f;
		return iss.eof() && !iss.fail();
	}

	int ensure_directory_for_file(const char * file);
	int ensure_directory(const char * file);
	int copy_file(const char *src, const char *dst);

	Ads_String string_date(time_t t);
	Ads_String string_date_(time_t t);
	Ads_String string_date_time(time_t t);
	Ads_String string_date_time_(time_t t);

	#define DEFAULT_TIME_FORMAT ("%Y%m%d%H%M%S")
	inline time_t str_to_time(const char *s, const char *format = DEFAULT_TIME_FORMAT)
	{
		struct tm tm = {0};
		char *result = strptime(s, format, &tm);
		if(!result || *result) return (time_t)-1;
		return mktime(&tm);
	}

	inline Ads_String time_to_str(const char *format, time_t t)
	{
    	char buf[0xff];
	    struct tm tm;
		struct tm* res = gmtime_r(&t, &tm);
		if (!res)
			return "";
		::strftime(buf, sizeof(buf), format, res);
	    return Ads_String(buf);
	}

	inline Ads_String string_date(time_t t)	{ return time_to_str("%Y%m%d", t); }
	inline Ads_String string_date_(time_t t) { return time_to_str("%Y-%m-%d", t); }
	inline Ads_String string_date_time(time_t t)	{ return time_to_str("%Y%m%d%H%M%S", t); }
	inline Ads_String string_date_time_(time_t t) { return time_to_str("%Y-%m-%d %H:%M:%S", t); }
}

namespace json {
	class Object;
	class ObjectP;
	class Array;
	class ArrayP;

	typedef std::shared_ptr<ObjectP> Object_Ptr;
}

#if defined(ADS_ENABLE_DEBUG) && defined(ADS_ENABLE_ASSERT)
#include <assert.h>
#define ADS_ASSERT(a) assert(a)
#else
#define ADS_ASSERT(a)
#endif

#define ADS_DIRECTORY_SEPARATOR_CHAR '/'
#define ADS_DIRECTORY_SEPARATOR_STR "/"

#ifdef __clang__
#include <pthread.h>
#endif

#if !defined(MAP_LOCKED)
#define MAP_LOCKED 0
#endif

namespace ads {
	struct Time_Value
	{
		Time_Value() { this->tv_.tv_sec = 0; this->tv_.tv_usec = 0; }
		explicit Time_Value(const timeval& tv) { this->tv_.tv_sec = tv.tv_sec; this->tv_.tv_usec = tv.tv_usec; }
		explicit Time_Value(time_t sec, suseconds_t usec = 0) { this->tv_.tv_sec = sec; this->tv_.tv_usec = usec; }

		void set (time_t sec, suseconds_t usec = 0) { this->tv_.tv_sec = sec; this->tv_.tv_usec = usec; }

		time_t sec() const { return this->tv_.tv_sec; }
		suseconds_t usec() const { return this->tv_.tv_usec; }
		time_t msec() const { return this->tv_.tv_sec * 1000 + this->tv_.tv_usec / 1000; }

		timeval tv_;

		const Time_Value& operator -=(const Time_Value& that)
		{
			this->tv_.tv_sec -= that.tv_.tv_sec;
			this->tv_.tv_usec -= that.tv_.tv_usec;
			return *this;
		}

		const Time_Value& operator +=(const Time_Value& that)
		{
			this->tv_.tv_sec += that.tv_.tv_sec;
			this->tv_.tv_usec += that.tv_.tv_usec;
			return *this;
		}
	};

	inline suseconds_t Time_Interval_usec(const Time_Value& t0, const Time_Value& t1)
	{
		return (t1.sec() - t0.sec()) * 1000000 + (t1.usec() - t0.usec());
	};

	inline suseconds_t Time_Interval_msec(const Time_Value& t0, const Time_Value& t1)
	{
		return t1.msec() - t0.msec();
	};

	struct Dirent
	{
		DIR *dir_;

		Dirent() : dir_(NULL) {}
		~Dirent() { close(); }

		int open(const Ads_String& dir)
		{
			dir_ = ::opendir(dir.c_str());
			return dir_? 0: -1;
		}

		void close()
		{
			if (dir_) ::closedir(dir_);
			dir_ = 0;
		}

		struct dirent *read()
		{
			if (!dir_) return 0;
			return ::readdir(dir_);
		}

		static int children(const Ads_String& dir, size_t flag, Ads_String_List &files)
		{
			Dirent d;
			if (d.open(dir) < 0) return -1;

			struct dirent *dp;
			while ( (dp = d.read()) )
			{
				if (dp->d_name[0] == '.') continue;
				if (dp->d_type == DT_UNKNOWN)
				{
					struct stat statbuf;
					if (stat((dir + ADS_DIRECTORY_SEPARATOR_CHAR + dp->d_name).c_str(), &statbuf) == 0)
					{
						size_t type = 0;
						if (S_ISREG(statbuf.st_mode))
						{
							type = DT_REG;
						}
						else if (S_ISDIR(statbuf.st_mode))
						{
							type = DT_DIR;
						}
						if (type & flag)
							files.push_back(dp->d_name);
						continue;
					}
				}
				if (dp->d_name[0] == '.' || !(dp->d_type & flag)) continue;
				files.push_back(dp->d_name);
			}

			d.close();
			return 0;
		}
	};

	class Mem_Map {
		public:
		Mem_Map() : base_addr_(MAP_FAILED), length_(0), handle_(-1) {}
		~Mem_Map() { close(); }

		int map(const char *filename,
               size_t length = static_cast<size_t> (-1),
               int flags = O_RDWR | O_CREAT,
               mode_t mode = 0644,
               int prot = PROT_RDWR,
               int share = MAP_PRIVATE,
               void *addr = 0,
               size_t offset = 0, bool throttle = false)
		{
			close();

			handle_ = -1;
			if (filename[0])
			{
				handle_ = ::open(filename, flags, mode);
				if (handle_ == -1) return -1;

			size_t file_size = ::lseek(handle_, 0, SEEK_END);
			::lseek(handle_, 0, SEEK_SET);

			if (length == size_t(-1))
			{
				this->length_ = file_size - offset;
			}
			else
			{
				size_t requested_file_length = length + offset;

				/// need to resize file
				if (requested_file_length > file_size)
				{
					if (::pwrite(handle_, "", 1, requested_file_length - 1) == -1)
						return -1;
				}

				this->length_ = length;
			}
			}
			else
			{
				this->length_ = length;
			}

			if (!throttle || addr == 0)
			{
				this->base_addr_ = ::mmap(addr, length_, prot, share, handle_, offset);
			}
			else
			{
				size_t trunk = (1<<20);
				int pace_us = (1*1000);
				char *ptr = (char *)addr, *limit = ptr + length_;
				while (ptr < limit)
				{
					size_t remain = limit - ptr;
					size_t map_size = std::min(remain, trunk);

					mmap(ptr, map_size, prot, share, handle_, offset);
					offset += map_size;
					ptr += map_size;

					if (pace_us > 0)
						usleep(pace_us);
				}

				this->base_addr_ = addr;
			}
			return this->base_addr_ == MAP_FAILED? -1: 0;
		}

		void set_mapping(void* addr, size_t length)
		{
			this->base_addr_ = addr;
			this->length_ = length;
		}

		int sync(int flags = MS_SYNC)
		{
			if (this->base_addr_ == MAP_FAILED) return 0;
			return ::msync(this->base_addr_, this->length_, flags);
		}

		int unmap()
		{
			if (base_addr_ != MAP_FAILED) ::munmap(this->base_addr_, this->length_);
			this->base_addr_ = MAP_FAILED;
			this->length_ = 0;
			return 0;
		}

		// default unmap speed limit: 1GB/s
		int unmap_throttle(size_t length, size_t trunk=(4<<20), int pace_us=(4*1000))
		{
			if (base_addr_ == MAP_FAILED) return 0;

			char *ptr = (char *)this->base_addr_;
			char *limit = ptr + (length < this->length_ ? length : this->length_);

			while (ptr < limit)
			{
				size_t remain = limit - ptr;
				size_t unmap_size = std::min(remain, trunk);

				munmap(ptr, unmap_size);
				ptr += unmap_size;
				this->length_ -= unmap_size;

				if (pace_us > 0)
					usleep(pace_us);
			}

			this->base_addr_ = this->length_ > 0 ? ptr : MAP_FAILED;
			return 0;
		}

		int close()
		{
			unmap();
			if (handle_ != -1) ::close(handle_);
			handle_ = -1;
			return 0;
		}

		void *addr() const 		{ return this->base_addr_; }
		size_t size() const 	{ return this->length_; }
		int handle() const		{ return this->handle_; }

		private:
		void *base_addr_;
		size_t length_;

		int handle_;
	};

	class DLL {
		public:
		DLL() : handle_(NULL) {}
		~DLL() { close(); }

		int open(const char *filename, int flags)
		{
			handle_ = ::dlopen(filename, flags);
			return handle_ != NULL? 0: -1;
		}

		int close()
		{
			if (handle_) ::dlclose(handle_);
			handle_ = NULL;
			return 0;
		}

		void *symbol(const char *name)
		{
			return  handle_? ::dlsym(handle_, name): NULL;
		}

		private:
		void *handle_;
	};

	inline ads::Time_Value
	gettimeofday() {
		timeval tv;
		return ::gettimeofday(&tv, 0) < 0? ads::Time_Value(time_t(0)): ads::Time_Value(tv);
	}

	class Mutex {
		public:
			Mutex()
			{
				::pthread_mutexattr_init(&ma_);
				::pthread_mutexattr_settype(&ma_, PTHREAD_MUTEX_RECURSIVE);
				::pthread_mutex_init(&mu_, &ma_);
			}
			~Mutex()
			{
				::pthread_mutex_destroy(&mu_);
				::pthread_mutexattr_destroy(&ma_);
			}

			int acquire()
			{
				//time_t t0 = ads::gettimeofday().msec();
				::pthread_mutex_lock(&mu_);
				//time_t t1 = ads::gettimeofday().msec();
				//if (t1 - t0 > 300)
				//ads::print_stacktrace();

				return 0;
			}
			int release() 	{ ::pthread_mutex_unlock(&mu_); return 0; }

		private:
			pthread_mutexattr_t ma_;
			pthread_mutex_t mu_;

			Mutex(const Mutex&);
			const Mutex& operator = (const Mutex&);

			friend class Condition_Var;
	};

	class Condition_Var {
		public:
			explicit Condition_Var(Mutex& m): mu_(m) { ::pthread_cond_init(&cv_, NULL); }
			~Condition_Var() { ::pthread_cond_destroy(&cv_); }

			int wait() 		{ ::pthread_cond_wait(&cv_, &mu_.mu_); return 0; }
			int signal()	{ ::pthread_cond_signal(&cv_);  return 0; }
			int broadcast() { ::pthread_cond_broadcast(&cv_); return 0; }

		private:
			pthread_cond_t cv_;
			Mutex& mu_;

			Condition_Var(const Condition_Var&);
			const Condition_Var& operator = (const Condition_Var&);
	};

	class Guard {
		public:
			explicit Guard(Mutex& m): mu_(m) { mu_.acquire(); }
			~Guard() 				{ mu_.release(); }

		private:
			Mutex& mu_;

			Guard(const Guard&);
			const Guard& operator = (const Guard&);
		};

	class Guard_Pointer {
		public:
			explicit Guard_Pointer(Mutex *m): mu_(m) { mu_->acquire(); }
			void change_mutex(Mutex* m)
			{
				mu_->release();
				mu_ = m;
				mu_->acquire();
			}
			~Guard_Pointer() 				{ mu_->release(); }

		private:
			Mutex* mu_;

			Guard_Pointer(const Guard_Pointer&);
			const Guard_Pointer& operator = (const Guard_Pointer&);
		};

	class Event {
		public:
		Event(): set_(false), mu_(), cv_(mu_) {}

		int wait()
		{
			mu_.acquire();
			while(! set_) cv_.wait();
			set_ = false;
			mu_.release();
			return 0;
		}

		int signal()
		{
			mu_.acquire();
			set_ = true;
			cv_.signal();
			mu_.release();
			return 0;
		}

		private:
		bool set_;
		Mutex mu_;
		Condition_Var cv_;

		Event(const Event&);
		const Event& operator = (const Event&);
	};

	template <typename T> class TSS {
	public:
		TSS() { ::pthread_key_create(&key_, cleanup); }
		~TSS() { }

		T *get() const
		{
			void *data = ::pthread_getspecific(key_);
			if(data) return (T *)data;

			T *t = new T();
			::pthread_setspecific(key_, t);
			return t;
		}

		T* operator->() {  return get();  }
		operator T *(void) const { return get(); }

		static void cleanup(void *p) { delete(T *)p; }

	private:
		pthread_key_t key_;
	};

	// non-protected simple version
	template <class T> class Singleton {
	public:
		static inline T* instance();

	private:
		Singleton(void);
		~Singleton(void);
		Singleton(const Singleton&);
		Singleton & operator= (const Singleton &);
		static std::auto_ptr<T> _instance;
	};

	template <class T> std::auto_ptr<T> Singleton<T>::_instance;
	template <class T> inline T* Singleton<T>::instance() {
		if(0 == _instance.get())
			_instance.reset(new T());
		return _instance.get();
	}

	struct Error_Info {
		typedef enum
		{
			SEVERITY_INFO = 1,
			SEVERITY_WARN = 2,
			SEVERITY_NORMAL = 3,
			SEVERITY_CRITICAL = 4
		} SEVERITY;

		int64_t id_;
		SEVERITY severity_;
		Ads_String name_;
		Ads_String message_;
		Ads_String context_;

		Error_Info(int64_t id, SEVERITY severity, const Ads_String& name, const Ads_String& message, const Ads_String& context)
			: id_(id), severity_(severity), name_(name), message_(message), context_(context)
		{}

		void destroy() { delete this; }
	};

	template <typename T>

	class Message_Queue {
		public:
		Message_Queue(int capacity=-1)
			: mutex_(), not_full_cond_(mutex_), not_empty_cond_(mutex_), empty_cond_(mutex_),  count_(0), capacity_(size_t(capacity)), active_(true) {}

		int enqueue(T *t, bool front = false, bool wait = true)
		{
			mutex_.acquire();
			if (! active_)
			{
				mutex_.release();
				return -1;
			}

			while (count_ >= capacity_)
			{
				if (! wait || !active_)
				{
					mutex_.release();
					return -1;
				}

				not_full_cond_.wait();
			}
			if (front) items_.push_front(t);
			else items_.push_back(t);
			++count_;
			not_empty_cond_.signal();
			mutex_.release();
			return 0;
		}

		int dequeue(T *&t, bool front = true, bool wait = true, bool active_only = true)
		{
			mutex_.acquire();

			if (active_only)
			{
				if (! active_)
				{
					mutex_.release();
					return -1;
				}
			}

			while (items_.empty())
			{
				empty_cond_.signal();
				if (! wait || !active_)
				{
					mutex_.release();
					return -1;
				}

				not_empty_cond_.wait();
			}

			if (front) { t = items_.front(); items_.pop_front(); }
			else { t = items_.back(); items_.pop_back(); }
			--count_;
			not_full_cond_.signal();
			mutex_.release();
			return 0;
		}

		void wait_for_empty()
		{
			ads::Guard __g(mutex_);
			if(!items_.empty())
			{
				empty_cond_.wait();
			}
		}

		size_t message_count() const { return count_; }

		bool is_empty()
		{
			ads::Guard __g(mutex_);
			return items_.empty();
		}

		void deactivate()
		{
			mutex_.acquire();
			active_ = false;
			not_full_cond_.broadcast();
			not_empty_cond_.broadcast();
			mutex_.release();
		}

		void capacity(size_t cap) { this->capacity_ = cap; }

		private:
		Mutex mutex_;
		Condition_Var not_full_cond_, not_empty_cond_, empty_cond_;

		std::list<T *> items_;
		size_t count_;
		size_t capacity_;
		bool active_;
	};

	struct Error_List: public std::list<Error_Info *> {
		~Error_List() 	{ std::for_each(begin(), end(), std::mem_fun(&Error_Info::destroy)); }
		void reset()	{ std::for_each(begin(), end(), std::mem_fun(&Error_Info::destroy)); this->clear(); }
	};

	inline pthread_t thr_self() { return ::pthread_self(); }

	/**
	 * Get thread id of specific pthread_t.
	 * @param  thread optional pthread_t. By default it will be current thread.
	 * @return        returns current thread id
	 */
	inline uint64_t thr_id(pthread_t thread = thr_self()) {
		#ifdef __APPLE__
		uint64_t tid;
		pthread_threadid_np(thread, &tid);
		return tid;
		#else
		//return thread;
		return syscall(SYS_gettid);
		#endif
	}

	inline const char * basename(const char *pathname, char delim = '/')
	{
		const char *temp = ::strrchr(pathname, delim);
		return ! temp ? pathname: temp + 1;
	}

	inline size_t
	filesize(const char *path)
	{
		struct stat st;
		if(::stat(path, &st) < 0) return -1;
		return st.st_size;
	}

	inline Ads_String path_join(const Ads_String& s1, const Ads_String& s2)
	{
		return s1 + ADS_DIRECTORY_SEPARATOR_CHAR + s2;
	}

	inline Ads_String path_join(const Ads_String& s1, const Ads_String& s2, const Ads_String& s3)
	{
		return s1 + ADS_DIRECTORY_SEPARATOR_CHAR + s2 + ADS_DIRECTORY_SEPARATOR_CHAR + s3;
	}

	inline Ads_String path_join(const Ads_String& s1, const Ads_String& s2, const Ads_String& s3, const Ads_String& s4)
	{
		return s1 + ADS_DIRECTORY_SEPARATOR_CHAR + s2 + ADS_DIRECTORY_SEPARATOR_CHAR + s3 + ADS_DIRECTORY_SEPARATOR_CHAR + s4;
	}

	inline Ads_String path_join(const Ads_String& s1, const Ads_String& s2, const Ads_String& s3, const Ads_String& s4, const Ads_String& s5)
	{
		return s1 + ADS_DIRECTORY_SEPARATOR_CHAR + s2 + ADS_DIRECTORY_SEPARATOR_CHAR + s3 + ADS_DIRECTORY_SEPARATOR_CHAR + s4 + ADS_DIRECTORY_SEPARATOR_CHAR + s5;
	}

	inline size_t log_priority_masks(const Ads_String& ms)
	{
		typedef std::pair<Ads_String, size_t> P;
		static const P _m[] =
		{
			P("trace", LP_TRACE),
			P("debug", LP_DEBUG),
			P("info", LP_INFO),
			P("warning", LP_WARNING),
			P("error", LP_ERROR),
			P("all", LP_TRACE|LP_DEBUG|LP_INFO|LP_WARNING|LP_ERROR)
		};

		typedef std::map<Ads_String, size_t> M;
		static const M m(_m, _m + sizeof(_m) / sizeof(_m[0]));

		Ads_String_List sl;
		ads::split(ms, sl, ',');

		size_t masks = 0;
		for(Ads_String_List::const_iterator it = sl.begin(); it != sl.end(); ++it)
		{
			Ads_String s = *it;
			if(s.empty()) continue;

			bool positive = true;
			if(s[0] == '-')
			{
				s = s.substr(1);
				positive = false;
			}

			M::const_iterator mit = m.find(s);
			if(mit == m.end()) continue;

			size_t n = mit->second;
			if(positive) masks |= n;
			else masks &= (~n);
		}

		return masks;
	}

	Ads_String to_string(const json::ObjectP& o);
	Ads_String to_string(const json::Object& o);
	Ads_String obj_attr_string(const json::Object& o, const Ads_String& name);
	int obj_attr_int(const json::Object& o, const Ads_String& name);
	void obj_flatten(const json::Object& from, json::Object& to, const Ads_String& prefix = "");

	Ads_String to_string(const json::ArrayP& arr);

	void set_thread_name(const char *fmt, ...);
	inline Ads_String dir(const char *f, char delim='/')
	{
		const char *p = strrchr(f, delim);
		return (p == nullptr) ? "." : Ads_String(f, p-f);
	}

	enum DATA_TYPE
	{
		DATA_IPV4 = 0,
		DATA_IPV4_ISP = 1,
		DATA_IPV6 = 2,
		DATA_LAST = 3
	};
	static const Ads_String data_file_prefix[ads::DATA_LAST]={"na-", "na_isp-", "na_ipv6-"};
	static const Ads_String data_file_suffix[ads::DATA_LAST]={"dat", "dat", "csv"};

	template <typename T>
	class dual_backup
	{
	private:
		T* ctx_[2] = {nullptr};
		int active_ctx_ = 0;
		ads::Mutex ctx_mutex_;

	public:
		void switch_ctx(T* new_ctx)
		{
			if (new_ctx == nullptr)
				return;

			ads::Guard __g(this->ctx_mutex_);

			int next_active_ctx = (!this->active_ctx_);
			T* old_ctx = this->ctx_[next_active_ctx];
			if (this->ctx_[next_active_ctx])
				delete static_cast<T*>(old_ctx);

			this->ctx_[next_active_ctx] = new_ctx;
			this->active_ctx_ = next_active_ctx;
		}

		T* get_ctx()
		{
			ads::Guard __g(ctx_mutex_);
			return ctx_[active_ctx_];
		}

		~dual_backup()
		{
			for (int i=0; i<2; i++)
			{
				delete static_cast<T*>(this->ctx_[i]);
			}
		}
	};

	inline Ads_String kafka_key(const Ads_String& transaction_id, const Ads_String& server_id)
	{
		ads::Time_Value now = ads::gettimeofday();
		char buf[0xff];
		snprintf(buf, sizeof buf, "%s-%s-%ld-%lu-%06d", transaction_id.c_str(), server_id.c_str(), now.msec(), ads::thr_id(), ::rand()%1000000);
		return buf;
	}
};

#define GET_METHOD_NAME(method_name)                            \
do                                                              \
{                                                               \
    method_name = __FUNCTION__;                                 \
    std::string pretty_function(__PRETTY_FUNCTION__);           \
    std::string::size_type p1 = pretty_function.find("(");      \
    if (p1 == std::string::npos)                                \
    {                                                           \
        break;                                                  \
    }                                                           \
                                                                \
    std::string::size_type p2 = pretty_function.rfind(' ', p1); \
    if (p2 == std::string::npos)                                \
    {                                                           \
        break;                                                  \
    }                                                           \
                                                                \
    method_name = pretty_function.substr(p2 + 1, p1 - p2 - 1);  \
} while(0)

// forward declarations
class Ads_XML_Node;
class Ads_Request;
class Ads_Response;
class Ads_Repository;

class Ads_User_Info;
typedef std::shared_ptr<Ads_User_Info> Ads_User_Info_Ptr;

class Ads_Slot_Base;
typedef std::deque<Ads_Slot_Base *> Ads_Slot_List;

#define ADS_LOG(X) \
	do { \
		if(!ads::is_log_enabled X) break; \
		ads::log_state(false, __FILE__, __LINE__, __PRETTY_FUNCTION__); \
		ads::log X; \
	} while (0)

#define ADS_LOG_RETURN(X, Y) \
	do { \
		if(!ads::is_log_enabled X) return Y; \
		ads::log_state(false, __FILE__, __LINE__, __PRETTY_FUNCTION__); \
		ads::log X; \
		return Y; \
	} while (0)

#define ADS_LOG0(X) \
	do { \
		if(!ads::is_log_enabled X) break; \
		ads::log_state(true, __FILE__, __LINE__, __PRETTY_FUNCTION__); \
		ads::log X; \
	} while (0)

#define ADS_ERROR_RETURN	ADS_LOG_RETURN

#if !defined (ADS_ENABLE_DEBUG)

#define ADS_DEBUG(X) do {} while (0)
#define ADS_DEBUG0(X) do {} while (0)

#else

#if !defined(__GNUC__)
#define __PRETTY_FUNCTION__ ""
#endif

#define ADS_DEBUG	ADS_LOG
#define ADS_DEBUG0 ADS_LOG0

#endif

#endif /* ADS_TYPES_H */
