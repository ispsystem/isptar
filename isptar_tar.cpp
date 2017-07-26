#include "isptar_tar.h"
#include <string.h>
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <iostream>
#include <stdexcept>
#include <dirent.h>

namespace tar {
class FileInfo::DirDesc {
public:
	static DirDescPtr Create(io::ResHandle fd) { return DirDescPtr(new DirDesc(fd)); }
	DirDesc(io::ResHandle fd) : m_fd(fd), m_access(0) {}
	io::ResHandle fd() const { return m_fd; }
	void GrantWrite() {
		if (!m_access) {
			struct stat sb;
			if (fstat(m_fd, &sb) == -1)
				throw std::runtime_error("Failed to get folder perms");
			m_access = sb.st_mode;
			if ((m_access & 0700) != 0700)
				if (fchmod(m_fd, 07777 & (m_access | 0700)) == -1)
					throw std::runtime_error("Failed to grant perms for folder");
		}
	}
	~DirDesc() {
		if (m_access && (m_access & 0700) != 0700)
			if (fchmod(m_fd, 07777 & m_access) == -1)
				throw std::runtime_error("Failed to revoke perms for folder");
	}
private:
	io::ResHandle m_fd;
	int m_access;
};

FileInfo::FileInfo()
	: size(0)
	, time(0)
	, mode(0)
	, type(AREGTYPE)
	, uid(0)
	, gid(0)
	, devmajor(0)
	, devminor(0) {}

string FileInfo::GetUserName() {
	auto pos = m_user.find(uid);
	if (pos != m_user.end())
		return pos->second;
	string name;
	if (auto pwd = getpwuid(uid)) {
		name = pwd->pw_name;
		m_user[uid] = name;
	}
	return name;
}

string FileInfo::GetGroupName() {
	auto pos = m_group.find(gid);
	if (pos != m_group.end())
		return pos->second;
	string name;
	if (auto grp = getgrgid(gid)) {
		name = grp->gr_name;
		m_group[gid] = name;
	}
	return name;
}

FileInfo & FileInfo::Set(const string &name, struct stat &sb) {
	filename = name;
	mode = sb.st_mode & 07777;
	uid = sb.st_uid;
	user = GetUserName();
	gid = sb.st_gid;
	group = GetGroupName();
	time = sb.st_mtim.tv_sec;
	size = 0;
	devmajor = 0;
	devminor = 0;
	linkname.clear();
	if (S_ISDIR(sb.st_mode)) {
		type = DIRTYPE;
	} else if (S_ISLNK(sb.st_mode)) {
		type = SYMTYPE;
	} else if (S_ISCHR(sb.st_mode)) {
		type = CHRTYPE;
		devmajor = major(sb.st_dev);
		devminor = minor(sb.st_dev);
	} else if (S_ISBLK(sb.st_mode)) {
		type = BLKTYPE;
		devmajor = major(sb.st_dev);
		devminor = minor(sb.st_dev);
	} else if (S_ISFIFO(sb.st_mode)) {
		type = FIFOTYPE;
	} else if (S_ISREG(sb.st_mode)) {
		type = REGTYPE;
		size = sb.st_size;
	} else {
		type = AREGTYPE;
	}
	return *this;
}

FileInfo & FileInfo::Set(string &line) {
	filename = DecodeFileName(misc::GetWord(line, '\t'));
	user = misc::GetWord(line, '\t');
	uid = misc::Int(misc::RGetWord(user, '#'));
	group = misc::GetWord(line, '\t');
	gid = misc::Int(misc::RGetWord(group, '#'));
	mode = misc::Int(misc::GetWord(line, '\t'));
	size = 0;
	linkname.clear();
	devmajor = 0;
	devminor = 0;
	time = 0;

	string stype = misc::GetWord(line, '\t');
	if (stype == "file") {
		type = REGTYPE;
		time = misc::Int(misc::GetWord(line, '\t'));
		size = misc::Int(misc::GetWord(line, '\t'));
	} else if (stype == "dir") {
		type = DIRTYPE;
	} else if (stype == "link") {
		type = SYMTYPE;
		linkname = DecodeFileName(misc::GetWord(line, '\t'));
	} else if (stype == "hard") {
		type = LNKTYPE;
		linkname = DecodeFileName(misc::GetWord(line, '\t'));
	} else if (stype == "char") {
		type = CHRTYPE;
		devmajor = misc::Int(misc::GetWord(line, '\t'));
		devminor = misc::Int(misc::GetWord(line, '\t'));
	} else if (stype == "block") {
		type = BLKTYPE;
		devmajor = misc::Int(misc::GetWord(line, '\t'));
		devminor = misc::Int(misc::GetWord(line, '\t'));
	} else if (stype == "fifo") {
		type = FIFOTYPE;
	} else {
		type = AREGTYPE;
	}
	return *this;
}

string FileInfo::Str() const {
	string res = EncodeFileName(filename) + '\t';
	res += user + '#' + misc::Str(uid) + '\t';
	res += group + '#' + misc::Str(gid) + '\t';
	res += misc::Str(mode) + '\t';
	switch (type) {
		case REGTYPE:
			res += "file\t" + misc::Str(time) + '\t' + misc::Str(size);
			break;
		case LNKTYPE:
			res += "hard\t" + EncodeFileName(linkname);
			break;
		case SYMTYPE:
			res += "link\t" + EncodeFileName(linkname);
			break;
		case CHRTYPE:
			res += "char\t" + misc::Str(devmajor) + '\t' + misc::Str(devminor);
			break;
		case BLKTYPE:
			res += "block\t" + misc::Str(devmajor) + '\t' + misc::Str(devminor);
			break;
		case DIRTYPE:
			res += "dir";
			break;
		case FIFOTYPE:
			res += "fifo";
			break;
	}
	return res;
}

static void SetOwnerMode(int fd, int uid, int gid, int mode, const string &name) {
	if (fchmod(fd, mode))
		throw std::runtime_error("Failed to set file mode " + name);
	if (geteuid() == 0 && fchown(fd, uid, gid))
		throw std::runtime_error("Failed to set file owner " + name);
}

misc::ResHandle FileInfo::Create(const string &prefix, io::IStream &in) {
	auto fd = Create(prefix);
	char buf[CHUNK];
	auto left = size;
	io::FileOStream out(fd);
	while (left > 0) {
		int len = left > sizeof(buf) ? sizeof(buf) : left;
		int size = in.Read(buf, len);
		if (size <= 0)
			throw std::runtime_error("Failed to get data");
		out.Write(buf, size);
		left -= size;
	}
	if (fd) {
		struct timeval tv[2];
		tv[0].tv_sec = ::time(NULL);
		tv[0].tv_usec = 0;
		tv[1].tv_sec = time;
		tv[1].tv_usec = 0;
		if (futimes(fd, tv))
			throw std::runtime_error("Failed to set utimes");
	}
	return fd;
}

misc::ResHandle FileInfo::Create(const string &prefix) {
	if (m_dir_fd.empty() || m_dir_fd[0].first != "./" + prefix) {
		// может быть несколько префиксов при смене префикса все сбросить
		m_dir_fd.clear();
		misc::ResHandle fd = open(prefix.c_str(), O_RDONLY|O_DIRECTORY);
		if (!fd)
			throw std::runtime_error("Failed to open " + prefix);
		m_dir_fd.push_back(std::make_pair("./" + prefix, DirDesc::Create(fd)));
	}

	auto fd = m_dir_fd[0].second->fd();
	string::size_type start = 0;
	string::size_type pos = filename.find('/');
	int i = 1;
	while (pos != string::npos && i < (int)m_dir_fd.size()) {
		if (filename.compare(start, pos - start, m_dir_fd[i].first) != 0)
			break;
		start = pos + 1;
		pos = filename.find('/', start);
		fd = m_dir_fd[i].second->fd();
		++i;
	}
	m_dir_fd.resize(i);

	while (pos != string::npos) {
		string folder = filename.substr(start, pos - start);
		misc::ResHandle tmp_fd = openat(fd, folder.c_str(), O_RDONLY|O_DIRECTORY);
		if (!tmp_fd) {
			if (errno == ENOENT) {
				if (mkdirat(fd, folder.c_str(), 0777))
					throw std::runtime_error("Failed to create folder " + folder);
				tmp_fd = openat(fd, folder.c_str(), O_RDONLY|O_DIRECTORY);
				if (!tmp_fd)
					throw std::runtime_error("Failed to open folder " + folder);
			// TODO } else if (errno == EACCES) {
			// можно поднять права на каталог, а потом убрать
			} else
				throw std::runtime_error("Failed to open folder " + folder);
		}
		fd = tmp_fd;
		m_dir_fd.push_back(std::make_pair(filename.substr(start, pos), DirDesc::Create(fd)));
		start = pos + 1;
		pos = filename.find('/', start);
	}

	// поднять доступ до u+w
	m_dir_fd.rbegin()->second->GrantWrite();
	fd = Create(fd, filename.substr(start));
	if (type == DIRTYPE) {
		m_dir_fd.push_back(std::make_pair(filename, DirDesc::Create(fd)));
	}
	return fd;
}

void FileInfo::Remove(const io::ResHandle &fd, const string &name) const {
	struct stat sb;
	if (fstatat(fd, name.c_str(), &sb, AT_SYMLINK_NOFOLLOW/*|AT_NO_AUTOMOUNT*/))
		return;
	if (!S_ISDIR(sb.st_mode)) {
		unlinkat(fd, name.c_str(), 0);
	} else {
		int tmp = openat(fd, name.c_str(), O_RDONLY|O_DIRECTORY);
		if (!tmp)
			throw std::runtime_error("Failed to open folder " + name + " for reading");
		io::ResHandle dfd = dup(tmp);
		std::shared_ptr<DIR> dir(fdopendir(tmp), closedir);
		while (auto ent = readdir(dir.get()))
			if (strcmp(ent->d_name, ".") != 0 && strcmp(ent->d_name, "..") != 0)
				Remove(dfd, ent->d_name);
	}
}

misc::ResHandle FileInfo::Create(const io::ResHandle &fd, const string &name) const {
	struct stat sb;
	misc::ResHandle res;
	if (type == DIRTYPE) {
		bool exists = fstatat(fd, name.c_str(), &sb,
			AT_SYMLINK_NOFOLLOW/*|AT_NO_AUTOMOUNT*/) == 0;
		if (exists && !S_ISDIR(sb.st_mode)) {
			unlinkat(fd, name.c_str(), 0);
			exists = false;
		}
		if (!exists)
			mkdirat(fd, name.c_str(), mode);
		res = openat(fd, name.c_str(), O_RDONLY|O_DIRECTORY);
		if (!res || fstat(res, &sb) != 0 || !S_ISDIR(sb.st_mode))
			throw std::runtime_error("Failed to create dir " + name);
		SetOwnerMode(res, uid, gid, mode, name);
	} else {
		Remove(fd, name);
		switch (type) {
			case REGTYPE:
				res = openat(fd, name.c_str(), O_CREAT|O_EXCL|O_WRONLY|O_LARGEFILE, mode);
				if (!res)
					throw std::runtime_error("Failed to create file " + name);
				SetOwnerMode(res, uid, gid, mode, name);
				break;
			case SYMTYPE:
				if (symlinkat(linkname.c_str(), fd, name.c_str()))
					throw std::runtime_error("Failed to create symlink " + name);
				fchmodat(fd, name.c_str(), mode, AT_SYMLINK_NOFOLLOW);
				if (geteuid() == 0 && fchownat(fd, name.c_str(), uid, gid, AT_SYMLINK_NOFOLLOW))
					throw std::runtime_error("Failed to set file owner");
				break;
			case LNKTYPE:
				if (linkat(m_dir_fd[0].second->fd(), linkname.c_str(), fd, name.c_str(), 0))
					throw std::runtime_error("Failed to create hard link " + name);
				break;
			case CHRTYPE:
			case BLKTYPE:
				if (mknodat(fd, name.c_str(), mode, makedev(devmajor, devminor)))
					throw std::runtime_error("Failed to create device");
				res = openat(fd, name.c_str(), O_RDONLY);
				if (!res || fstat(res, &sb) != 0 ||
					(type == BLKTYPE ? !S_ISBLK(sb.st_mode) : !S_ISBLK(sb.st_mode)))
					throw std::runtime_error("Failed to create nod");
				SetOwnerMode(res, uid, gid, mode, name);
				break;
			case FIFOTYPE:
				if (mkfifoat(fd, name.c_str(), mode))
					throw std::runtime_error("Failed to create fifo file " + name);
				res = openat(fd, name.c_str(), O_RDONLY);
				if (!res || fstat(res, &sb) != 0 || !S_ISFIFO(sb.st_mode))
					throw std::runtime_error("Failed to create nod");
				SetOwnerMode(res, uid, gid, mode, name);
				break;
		}
	}
	return res;
}

bool FileInfo::operator == (const FileInfo &info) const {
	return filename == info.filename &&
		type == info.type &&
		uid == info.uid &&
		gid == info.gid &&
		(type != REGTYPE || (time == info.time && size == info.size)) &&
		devmajor == info.devmajor &&
		devminor == info.devminor &&
		linkname == info.linkname;
}

string FileInfo::DecodeFileName(string file) {
	auto pos = file.find('\\');
	while (pos != string::npos) {
		if (file[pos + 1] == '\\')
			file.erase(pos, 1);
		else if (file[pos + 1] == 't')
			file.replace(pos, 2, "\t");
		else if (file[pos + 1] == 'n')
			file.replace(pos, 2, "\n");
		else
			throw std::runtime_error("Bad encoded filename '" + file + "'");
		pos = file.find('\\', pos + 1);
	}
	return file;
}

string FileInfo::EncodeFileName(string file) {
	auto pos = file.find_first_of("\t\n\\");
	while (pos != string::npos) {
		if (file[pos] == '\\')
			file.insert(pos, "\\");
		else if (file[pos] == '\t')
			file.replace(pos, 1, "\\t");
		else
			file.replace(pos, 1, "\\n");
		pos = file.find_first_of("\t\n\\", pos + 2);
	}
	return file;
}

struct TarHeader {
	char name[100];
	char mode[8];
	char uid[8];
	char gid[8];
	char size[12];
	char mtime[12];
	char chksum[8];
	char typeflag[1];
	char linkname[100];
	char magic[6];
	char version[2];
	char uname[32];
	char gname[32];
	char devmajor[8];
	char devminor[8];
	char prefix[155];
	char unused[12];
};

Writer::Writer(io::OStream &out) : m_out(out), m_left(0), m_tail(0) {}

Writer::~Writer() {
	//char buf[1024];
	//bzero(buf, sizeof(buf));
	//m_out.Write(buf, sizeof(buf));
}

void Writer::Add(const FileInfo &info) {
	WriteTail();
	TarHeader header;
	bzero(&header, sizeof(header));
	memcpy(header.magic, TMAGIC, TMAGLEN);
	memcpy(header.version, TVERSION, TVERSLEN);
	memset(header.chksum, ' ', sizeof(header.chksum));

	// длинные ссылки должны идти раньше длинных имен
	if (info.type == CHRTYPE || info.type == BLKTYPE) {
		snprintf(header.devmajor, sizeof(header.devmajor), "%07o ", info.devmajor);
		snprintf(header.devminor, sizeof(header.devminor), "%07o ", info.devminor);
	} else if (info.type == SYMTYPE || info.type == LNKTYPE) {
		if (info.linkname.size() <= sizeof(header.linkname)) {
			// короткая ссылка
			memcpy(header.linkname, info.linkname.data(), info.linkname.size());
		} else {
			// длинная ссылка
			memcpy(header.linkname, info.linkname.data(), sizeof(header.linkname));
			LongLink(info, info.linkname, LONGLINK_LINKTYPE);
		}
	}

	// имя
	if (info.filename.size() <= sizeof(header.name)) {
		// нечего делить, имя помещается в name полностью
		memcpy(header.name, info.filename.data(), info.filename.size());
	} else {
		auto pos = info.filename.find('/', info.filename.size() - sizeof(header.name));
		if (pos != string::npos && pos <= sizeof(header.prefix)) {
			// можно разделить на префикс и имя
			memcpy(header.name, info.filename.c_str() + pos + 1,
				info.filename.size() - pos - 1);
			memcpy(header.prefix, info.filename.c_str(), pos);
		} else {
			memcpy(header.name, info.filename.data(), sizeof(header.name));
			// создаем отдельный файл для имени файла
			LongLink(info, info.filename, LONGLINK_FILETYPE);
		}
	}

	// размер
	int64_t size = (info.type == REGTYPE || info.type == LONGLINK_LINKTYPE ||
		info.type == LONGLINK_FILETYPE) ? info.size : 0;
	if (size < 0100000000000ll) {
		snprintf(header.size, sizeof(header.size), "%011zo", size);
	} else {
		header.size[0] = (char)0x80;
		unsigned char *ptr = (unsigned char *)header.size + sizeof(header.size);
		for (size_t data = size; data; data >>= 8)
			*--ptr = data & 0xFF;
	}

	snprintf(header.mode, sizeof(header.mode), "%07o ", info.mode);
	snprintf(header.uid, sizeof(header.uid), "%07o ", info.uid);
	snprintf(header.gid, sizeof(header.gid), "%07o ", info.gid);
	snprintf(header.uname, sizeof(header.uname), "%s", info.user.c_str());
	snprintf(header.gname, sizeof(header.gname), "%s", info.group.c_str());

	header.typeflag[0] = info.type;
	snprintf(header.mtime, sizeof(header.mtime), "%011lo", info.time);

	// вычисляем контрольную сумму
	unsigned char * data = ((unsigned char *) &header) + sizeof(header);
	int sum = 0;
	while (data > (void *)&header)
		sum += *--data;
	snprintf(header.chksum, sizeof(header.chksum), "%06o", sum);
	m_out.Write((const char *)&header, sizeof(header));

	m_left = size;
	m_tail = size % 512;
	if (m_tail)
		m_tail = 512 - m_tail;
}

void Writer::WriteData(const string &value) {
	int len = DataLeft(value.size());
	m_left -= len;
	m_out.Write(value.data(), len);
}

void Writer::WriteTail(bool finish) {
	m_left += m_tail;
	if (m_left) {
		char buf[CHUNK];
		bzero(buf, sizeof(buf));
		while (int len = DataLeft(sizeof(buf))) {
			m_out.Write(buf, len);
			m_left -= len;
		}
		m_tail = 0;
	}
	if (finish) {
		char buf[1024];
		bzero(buf, sizeof(buf));
		m_out.Write(buf, sizeof(buf));
	}
}

void Writer::WriteData(io::IStream &in) {
	char buf[1024];
	while (int len = DataLeft(sizeof(buf))) {
		len = in.Read(buf, len);
		if (len <= 0)
			break;
		m_out.Write(buf, len);
		m_left -= len;
	}
}

int64_t Writer::DataLeft(int buffer_size) const {
	return (buffer_size != -1 && buffer_size < m_left) ? buffer_size : m_left;
}

void Writer::AddDone(int64_t done) {
	m_left -= done;
}

void Writer::LongLink(const FileInfo &info, string value, char type) {
	value.push_back('\0');
	FileInfo longlink;
	longlink.filename = LONGLINK;
	longlink.mode = info.mode;
	longlink.size = value.size();
	longlink.type = type;
	longlink.uid = info.uid;
	longlink.gid = info.gid;
	Add(longlink);
	WriteData(value);
	WriteTail();
}
} // end of mgr_tar namespace

