#include <zlib.h>
#include "isptar_io.h"
#include "isptar_slice.h"
#include <map>

namespace gzip {
using std::string;

class IStream : public io::IStream {
public:
	IStream(io::IStream &in, int64_t limit = -1);
	~IStream();
	void Reset(int64_t limit = -1);

	virtual int Read(char *buf, int size);
	virtual void Seek(int64_t pos);
private:
	io::IStream &m_in;
	int64_t m_limit;
	int64_t m_current_pos;
	z_stream m_strm;
	unsigned char m_buf[CHUNK];

	void Init();
};

class OStream : public io::OStream {
public:
	OStream(io::OStream &out, int level = 9);
	~OStream();
	virtual void Write(const char *buf, int size);
	virtual int64_t Offset();

	void Flush(bool finish);
	void SetLevel(int level, int strategy = Z_DEFAULT_STRATEGY);
	int64_t TotalOut() const;
private:
	io::OStream &m_out;
	z_stream m_strm;
	int64_t m_offset;
	int64_t m_total_out;
	bool m_empty;

	void Pack(const char *buf, int size, int flush);
};

string Pack(const string &data);
std::map<string, string> GetHeader(slice::IStream &in);
} // end of gzip namespace
