#include "isptar_io.h"
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>

namespace io {
void OStream::WriteStream(IStream &in) {
	int size;
	char buf[CHUNK];
	while ((size = in.Read(buf, sizeof(buf))) > 0)
		Write(buf, size);
}

void OStream::WriteStr(const string &str) { Write(str.data(), str.size()); }

FileIStream::FileIStream() {}
FileIStream::FileIStream(ResHandle fd) : m_fd(fd) {}
FileIStream::FileIStream(const string &name)
	: m_fd(open(name.c_str(), O_RDONLY|O_LARGEFILE)) { }
void FileIStream::Reset(ResHandle fd) { m_fd = fd; }
ResHandle FileIStream::fd() { return m_fd; }

int FileIStream::Read(char *buf, int size) {
	int res = read(m_fd, buf, size);
	if (res == -1)
		throw std::runtime_error("Failed to read data from file");
	return res;
}

int64_t FileIStream::Seek(int64_t pos, int whence) {
	int64_t res =lseek64(m_fd, pos, whence);
	if (res == -1)
		throw std::runtime_error("Failed to set file position");
	return res;
}

FileOStream::FileOStream() {}
FileOStream::FileOStream(ResHandle fd) : m_fd(fd) {}
FileOStream::FileOStream(const string &name) {
	misc::Su su;
	m_fd = open(name.c_str(), O_WRONLY|O_CREAT|O_TRUNC|O_LARGEFILE, 0666);
	if (!m_fd)
		throw std::runtime_error("Failed to create file " + name);
}

void FileOStream::Reset(ResHandle fd) { m_fd = fd; }
ResHandle FileOStream::fd() { return m_fd; }

void FileOStream::Write(const char *buf, int size) {
	if (write(m_fd, buf, size) != size)
		throw std::runtime_error("Failed to write data to stream");
}

int64_t FileOStream::Offset() const {
	int64_t res = lseek64(m_fd, 0, SEEK_CUR);
	if (res == -1)
		throw std::runtime_error("Failed to get file position");
	return res;
}
} // end of io namespace

