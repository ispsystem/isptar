#include "isptar_gzip.h"
#include <stdexcept>
#include <assert.h>
#define	MIN_TAILSIZE	20
#define	MAX_TAILSIZE	39

namespace gzip {
IStream::IStream(io::IStream &in, int64_t limit)
	: m_in(in)
	, m_limit(limit)
	, m_current_pos(0) {
	m_strm.zalloc = Z_NULL;
	m_strm.zfree = Z_NULL;
	m_strm.opaque = Z_NULL;
	if (inflateInit2(&m_strm, 15 + 16) != Z_OK)
		throw std::runtime_error("Failed to init zlib");
	m_strm.avail_in = 0;
}

IStream::~IStream() { inflateEnd(&m_strm); }

void IStream::Reset(int64_t limit) {
	m_limit = limit;
	m_current_pos = 0;
	m_strm.avail_in = 0;
	if (inflateReset(&m_strm) != Z_OK)
		throw std::runtime_error("Failed to reset zlib state");
}

int IStream::Read(char *buf, int size) {
	m_strm.avail_out = size;
	m_strm.next_out = (unsigned char *)buf;

	while (m_strm.avail_out > 0) {
		if (!m_strm.avail_in) {
			m_strm.next_in = m_buf;
			int len = m_limit == -1 || (int)m_limit > (int)sizeof(m_buf)
				? sizeof(m_buf)
				: m_limit;
			int have = m_in.Read((char *)m_buf, len);
			m_strm.avail_in = have;
			if (have == -1)
				throw std::runtime_error("Failed to get input");
			if (m_strm.avail_in == 0)
				break;// size - m_strm.avail_out;
			if (m_limit != -1)
				m_limit -= m_strm.avail_in;
		}
		int res = inflate(&m_strm, Z_NO_FLUSH);
		if (res > Z_OK)
			break;
		if (res < Z_OK)
			throw std::runtime_error("Failed to extract");
	}
	int res = size - m_strm.avail_out;
	m_current_pos += res;
	return res;
}

void IStream::Seek(int64_t pos) {
	if (m_current_pos > pos)
		throw std::runtime_error("Try to seek backward in gzip stream");
	char buf[CHUNK];
	int64_t left = pos - m_current_pos;
	while (left > 0) {
		int len = left > (int64_t)sizeof(buf) ? sizeof(buf) : left;
		int res = Read(buf, len);
		if (res == 0)
			throw std::runtime_error("Unexpected end of stream");
		left -= res;
	}
}

OStream::OStream(io::OStream &out, int level)
	: m_out(out)
	, m_offset(0)
	, m_total_out(0)
	, m_empty(true) {
	m_strm.zalloc = Z_NULL;
	m_strm.zfree = Z_NULL;
	m_strm.opaque = Z_NULL;
	if (deflateInit2(&m_strm, level, Z_DEFLATED, 31, 9, Z_DEFAULT_STRATEGY) != Z_OK)
		throw std::runtime_error("Failed to init zlib");
}

OStream::~OStream() { deflateEnd(&m_strm); }

void OStream::Write(const char *buf, int size) {
	m_total_out += size;
	Pack(buf, size, Z_NO_FLUSH);
}

int64_t OStream::Offset() {
	Flush(false);
	return m_offset;
}

void OStream::Flush(bool finish) {
	if (finish) {
		Pack(NULL, 0, Z_FINISH);
		if (deflateReset(&m_strm) != Z_OK)
			throw std::runtime_error("Failed to reset zlib state");
		m_offset = 0;
	} else {
		Pack(NULL, 0, Z_SYNC_FLUSH); // записать данные и выровнить по границе байта
	}
}

void OStream::SetLevel(int level, int strategy) {
	Flush(true);
	if (deflateParams(&m_strm, level, strategy) == Z_STREAM_ERROR)
		throw std::runtime_error("Failed to set compressing level");
}

int64_t OStream::TotalOut() const { return m_total_out; }

void OStream::Pack(const char *in, int size, int flush) {
	if (size == 0) {
		if (m_empty)
			return;
		m_empty = true;
	} else
		m_empty = false;

	m_strm.avail_in = size;
	m_strm.next_in = (unsigned char *)in;
	unsigned char buf[CHUNK];
	do {
		m_strm.avail_out = sizeof(buf);
		m_strm.next_out = buf;
		if (deflate(&m_strm, flush) == Z_STREAM_ERROR)
			throw std::runtime_error("Failed to compress");
		int have = sizeof(buf) - m_strm.avail_out;
		m_out.Write((char *)buf, have);
		m_offset += have;
	} while (m_strm.avail_out == 0);
	assert(m_strm.avail_in == 0);
}

string Pack(const string &data) {
	z_stream strm;
	strm.zalloc = Z_NULL;
	strm.zfree = Z_NULL;
	strm.opaque = Z_NULL;
	if (deflateInit2(&strm, 9, Z_DEFLATED, 31, 9, Z_DEFAULT_STRATEGY) != Z_OK)
		throw std::runtime_error("Failed to init zlib");
	strm.avail_in = data.size();
	strm.next_in = (unsigned char *)data.data();
	unsigned char buf[CHUNK];
	string res;
	do {
		strm.avail_out = sizeof(buf);
		strm.next_out = buf;
		if (deflate(&strm, Z_FINISH) == Z_STREAM_ERROR)
			throw std::runtime_error("Failed to compress");
		int have = sizeof(buf) - strm.avail_out;
		res.append((char *)buf, have);
	} while (strm.avail_out == 0);
	assert(strm.avail_in == 0);
	return res;
}

std::map<std::string, std::string> GetHeader(slice::IStream &in) {
	std::map<std::string, std::string> result;
	unsigned char inbuf[CHUNK];
	in.Seek(0, -MAX_TAILSIZE, SEEK_END);
	int size = in.Read((char *)inbuf, MAX_TAILSIZE);
	if (size < MIN_TAILSIZE)
		throw std::runtime_error("Failed to get header size");
	z_stream strm;
	strm.zalloc = Z_NULL;
	strm.zfree = Z_NULL;
	strm.opaque = Z_NULL;
	if (inflateInit2(&strm, 16 + 15) != Z_OK)
		throw std::runtime_error("Failed to init zlib");
	unsigned char outbuf[512 * 4];	// tar добавит килобайт лишних нулей
	for (int i = size - MIN_TAILSIZE; i >= 0; --i) {
		strm.avail_in = size - i;
		strm.next_in = inbuf + i;
		strm.avail_out = sizeof(outbuf);
		strm.next_out = outbuf;
		if (inflateReset(&strm) != Z_OK)
			throw std::runtime_error("Failed to reset zlib state");
		int res = inflate(&strm, Z_FINISH);
		if (res == Z_STREAM_END || res == Z_BUF_ERROR) {
			int header_size = atoi((char *)outbuf);
			int real_header_size = size - i + header_size;

			in.Seek(0, -real_header_size, SEEK_END);
			if (inflateReset(&strm) != Z_OK)
				throw std::runtime_error("Failed to reset zlib state");
			std::string header;
			for (int64_t size = header_size; size > 0; ) {
				int len = std::min((int)sizeof(inbuf), (int)size);
				int res = in.Read((char *)inbuf, len);
				if (res != len)
					throw std::runtime_error("Failed to read header");
				size -= len;
				strm.avail_in = len;
				strm.next_in = inbuf;
				strm.avail_out = sizeof(outbuf);
				strm.next_out = outbuf;
				res = inflate(&strm, size ? Z_NO_FLUSH : Z_FINISH);
				if (res == Z_STREAM_ERROR)
					throw std::runtime_error("Bad header");
				header.append((char *)outbuf, sizeof(outbuf) - strm.avail_out);
			}
			std::string::size_type start = 0;
			for (auto pos = header.find('='); pos != std::string::npos; pos = header.find('=', start)) {
				std::string name = header.substr(start, pos - start);
				start = pos + 1;
				pos = header.find('\n', start);
				if (pos == std::string::npos) {
					if (name == "header_size") {
						char buf[16];
						snprintf(buf, sizeof(buf), "%d", real_header_size);
						result[name] = buf;
						return result;
					}
				}
				result[name] = header.substr(start, pos - start);
				start = pos + 1;
			}
			throw std::runtime_error("Bad header");
		}
	}
	return result;
}


} // end of misc namespace
