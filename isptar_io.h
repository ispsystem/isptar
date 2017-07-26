#ifndef __ISPTAR_IO_H__
#define __ISPTAR_IO_H__
#include "isptar_misc.h"
#define	CHUNK	4096

namespace io {
using namespace misc;

class IStream {
public:
	virtual ~IStream() {}
	virtual int Read(char *buf, int size) = 0;
};

class OStream {
public:
	virtual ~OStream() {}
	virtual void Write(const char *buf, int size) = 0;

	void WriteStream(IStream &in);
	void WriteStr(const std::string &str);
};

class FileIStream : public IStream {
public:
	FileIStream();
	FileIStream(const string &name);
	FileIStream(ResHandle fd);
	void Reset(ResHandle fd);
	ResHandle fd();

	virtual int Read(char *buf, int size);
	virtual int64_t Seek(int64_t pos, int whence);
private:
	ResHandle m_fd;
};

class FileOStream : public OStream {
public:
	FileOStream();
	FileOStream(const string &name);
	FileOStream(ResHandle fd);
	void Reset(ResHandle fd);
	ResHandle fd();

	virtual void Write(const char *buf, int size);
	virtual int64_t Offset() const;
private:
	ResHandle m_fd;
};
} // end of misc namespace

#endif
