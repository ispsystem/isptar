#ifndef __ISPTAR_TAR_H__
#define __ISPTAR_TAR_H__
#include "isptar_io.h"
#include <sys/stat.h>
#include <tar.h>
#define LONGLINK_LINKTYPE	'K'
#define LONGLINK_FILETYPE	'L'
#define LONGLINK	"././@LongLink"
#include <string>
#include <map>
#include <vector>

namespace tar {
using std::string;
typedef uint64_t FileSizeType;

class FileInfo {
public:
	string filename;
	string linkname;
	string user;
	string group;

	FileSizeType size;
	time_t time;
	int mode;
	int type;
	int uid;
	int gid;
	int devmajor;
	int devminor;

	FileInfo();
	FileInfo & Set(const string &name, struct stat &sb);
	FileInfo & Set(string &line);
	string Str() const;
	io::ResHandle Create(const string &prefix);
	io::ResHandle Create(const string &prefix, io::IStream &in);

	string GetUserName();
	string GetGroupName();
	static string DecodeFileName(string name);
	static string EncodeFileName(string name);
	bool operator == (const FileInfo &info) const;
private:
	class DirDesc;
	typedef std::shared_ptr<DirDesc> DirDescPtr;
	std::map<int, string> m_user;
	std::map<int, string> m_group;
	std::vector< std::pair<string, DirDescPtr> > m_dir_fd;

	io::ResHandle Create(const io::ResHandle &fd, const string &name) const;
	void Remove(const io::ResHandle &fd, const string &name) const;
};

class Writer {
public:
	Writer(io::OStream &out);
	~Writer();

	void Add(const FileInfo &info);
	void WriteData(const string &value);
	void WriteData(io::IStream &in);
	void WriteTail(bool finish = false);
	int64_t DataLeft(int buffer_size = -1) const;
	void AddDone(int64_t done);

private:
	io::OStream &m_out;
	int64_t m_left;
	int m_tail;

	void LongLink(const FileInfo &info, string value, char type);
};
} // end of mgr_tar namespace

#endif
