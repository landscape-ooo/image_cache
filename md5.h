#ifndef Md5_H
#define Md5_H

#include <string>
#include <fstream>
#include <string.h>

/* Type define */
typedef unsigned char md5byte;
typedef unsigned int uint32;

using std::string;
using std::ifstream;

///Md5存储结构体,128位
struct Md5Data
{
public: 
  unsigned int data[4];
public:
  bool operator < (const Md5Data& p) const
  {
    return memcmp(data,p.data,4*sizeof(int))<0;
  }   
  bool operator == (const Md5Data& p) const
  {
    return memcmp(data,p.data,4*sizeof(int))==0;
  }   
  bool operator <= (const Md5Data& p) const
  {
    return memcmp(data,p.data,4*sizeof(int))<=0;
  }   
};

/* Md5 declaration. */
class Md5 {
public:
  Md5();
  Md5(const void* input, size_t length);
  Md5(const string& str);
  Md5(ifstream& in);
  void update(const void* input, size_t length);
  void update(const string& str);
  void update(ifstream& in);
  const md5byte* digest();
  string toString();
  void reset();
  static void GetMd5(const string & str, Md5Data& md5data);   
  static string  GetMd5Str(const string &str);

private:
  void update(const md5byte* input, size_t length);
  void final();
  void transform(const md5byte block[64]);
  void encode(const uint32* input, md5byte* output, size_t length);
  void decode(const md5byte* input, uint32* output, size_t length);
  string md5bytesToHexString(const md5byte* input, size_t length);

  /* class uncopyable */
  Md5(const Md5&);
  Md5& operator=(const Md5&);

private:
  uint32 _state[4]; /* state (ABCD) */
  uint32 _count[2]; /* number of bits, modulo 2^64 (low-order word first) */
  md5byte _buffer[64]; /* input buffer */
  md5byte _digest[16]; /* message digest */
  bool _finished;   /* calculate finished ? */

  static const md5byte PADDING[64];  /* padding for calculate */
  static const char HEX[16];
  enum { BUFFER_SIZE = 1024 };
};

#endif /*Md5_H*/
