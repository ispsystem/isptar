#include "isptar_slice.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <dirent.h>
#include <string.h>
#include <unistd.h>
#include <iostream>

namespace slice {
using misc::ResHandle;
error::error(const string &what) : std::runtime_error(what) {}

static bool Execute(string cmd, const string &filename, const string &context) {
	misc::Script script(cmd);
	auto pos = filename.rfind('/');
	script.AddParam('p', (pos == string::npos) ? "." : filename.substr(0, pos));
	const string name = (pos == string::npos) ? filename : filename.substr(pos + 1);
	script.AddParam('f', name);
	pos = name.rfind(SLICE_SEP);
	script.AddParam('n', (pos == string::npos) ? "" : name.substr(pos + strlen(SLICE_SEP)));
	script.AddParam('e', (pos == string::npos) ? "" : SLICE_SEP);
	script.AddParam('c', context);
	script.AddParam('b', (pos == string::npos) ? name : name.substr(0, pos));
	return script.Do();
}

static ResHandle LockSlice(const string &filename) {
	misc::Su su;
	ResHandle fd = open(filename.c_str(), O_RDONLY|O_LARGEFILE|O_NOFOLLOW);
	if (fd) {
		if (flock(fd, LOCK_SH) != 0)
			throw error("Failed to lock downloaded file");
	}
	return fd;
}

OStream::OStream(const string &name, int64_t slice_size)
	: m_file(name)
	, m_filename(name)
	, m_slice_size(slice_size)
	, m_slice_id(1) { }

void OStream::Finish() {
	if (!m_command.empty())
		if (!Execute(m_command, m_slice_id > 1
			? m_filename + SLICE_SEP + misc::Str(m_slice_id)
			: m_filename, "last_slice"))
			throw error("Failed to upload data");
}

void OStream::Write(const char *buf, int size) {
	int64_t left = m_slice_size - m_file.Offset();
	while (left < size) {
		m_file.Write(buf, left);
		misc::Su su;
		if (m_slice_id == 1)
			rename(m_filename.c_str(), (m_filename + SLICE_SEP "1").c_str());
		if (!m_command.empty()) {
			if (!Execute(m_command, m_filename + SLICE_SEP + misc::Str(m_slice_id), "operation"))
				throw error("Failed to upload data");
		}
		const std::string filename = m_filename + SLICE_SEP + misc::Str(++m_slice_id);
		m_file.Reset(open(filename.c_str(), O_CREAT|O_TRUNC|O_LARGEFILE|O_WRONLY, 0666));
		size -= left;
		buf += left;
		left = m_slice_size;
	}
	m_file.Write(buf, size);
}

Offs OStream::Offset() const {
	return std::make_pair(m_slice_id, m_file.Offset());
}

int64_t OStream::Size(Offs start) {
	auto end = Offset();
	int64_t res = end.first - start.first;
	return res * m_slice_size + end.second - start.second;
}

void OStream::SetUpload(const string &command) { m_command = command; }

IStream::IStream(const string &name)
	: m_filename(name)
	, m_slice_id(0) { }

IStream::~IStream() { DeleteLast(); }

void IStream::DeleteLast() {
	if (!m_last.empty())
		unlink(m_last.c_str());
}

void IStream::SetDownload(const string &command) { m_command = command; }

misc::ResHandle IStream::Open(const string &filename) {
	misc::ResHandle fd = LockSlice(filename);
	if (!fd && errno == ENOENT) {
		if (m_command.empty())
			return misc::ResHandle();
		DeleteLast();
		Execute(m_command, filename, "operation");
		fd = LockSlice(filename);
		if (!fd)
			return misc::ResHandle();
		m_last = filename;
	}
	return fd;
}

int64_t IStream::LookupLastSlice(const string &folder, const string &name) {
	int64_t res = -1;
	struct dirent entry, *result;
	if (DIR *dir = opendir(folder.c_str())) {
		string partname = name + SLICE_SEP;
		while (readdir_r(dir, &entry, &result) == 0 && result)
			if (strncmp(entry.d_name, partname.c_str(), partname.size()) == 0)
				res = std::max(res, misc::Int(entry.d_name + partname.size()));
			else if (name == entry.d_name)
				return 0;
		closedir(dir);
	}
	return res;
}

void IStream::OpenLast() {
	m_slice_id = 1;
	misc::ResHandle fd = LockSlice(m_filename);
	if (!fd) {
		auto pos = m_filename.rfind('/');
		const string folder = (pos == string::npos) ? "." : m_filename.substr(0, pos);
		const string name = (pos == string::npos) ? m_filename : m_filename.substr(pos + 1);
		m_slice_id = LookupLastSlice(folder, name);
		if (m_slice_id == -1) {
			if (m_command.empty())
				throw error("File not found cmd empty");
			Execute(m_command, m_filename + SLICE_SEP "0", "init");
			m_slice_id = LookupLastSlice(folder, name);
			if (m_slice_id == -1)
				throw error("File not found slice -1 folder: " + folder + "name: " + name);
			m_last = m_slice_id > 0 ? m_filename + SLICE_SEP + misc::Str(m_slice_id) : m_filename;
			fd = LockSlice(m_last);
			if (!fd)
				throw error("File not found no fd");
		} else {
			const string filename = m_filename + SLICE_SEP + misc::Str(m_slice_id);
			fd = LockSlice(filename);
		}
	}
	m_file.Reset(fd);
}

int IStream::Read(char *buf, int size) {
	int have = m_file.Read(buf, size);
	if (have != 0)
		return have;
	auto fd = Open(m_filename + SLICE_SEP + misc::Str(++m_slice_id));
	if (!fd)
		return 0;
	m_file.Reset(fd);
	return m_file.Read(buf, size);
}

Offs IStream::Seek(int64_t file, int64_t pos, int whence) {
	if (whence == SEEK_SET) {
		if (file != m_slice_id) {
			m_slice_id = file;
			ResHandle fd;
			if (file == 1)
				fd = Open(m_filename);
			if (!fd)
				fd = Open(m_filename + SLICE_SEP + misc::Str(m_slice_id));
			if (!fd)
				throw error("Failed to get slice");
			m_file.Reset(fd);
		}
		return std::make_pair(file, m_file.Seek(pos, whence));
	}
	if (whence == SEEK_END)
		OpenLast();
	auto len = m_file.Seek(0, whence);
	while (len < -pos) {
		auto fd = Open(m_filename + SLICE_SEP + misc::Str(--m_slice_id));
		if (!fd)
			throw error("Failed to get data");
		m_file.Reset(fd);
		pos += len;
		len = m_file.Seek(0, whence);
	}
	return std::make_pair(m_slice_id, m_file.Seek(pos, whence));
}
} // end of slice namespace

