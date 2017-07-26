#include <sys/stat.h>
#include <zlib.h>
#include <pwd.h>
#include <grp.h>
#include <map>
#include <string>
#include <fcntl.h>
#include <fnmatch.h>
#include <iostream>
#include <memory>
#include "isptar_misc.h"
#include "isptar_tar.h"
#include "isptar_file.h"
#include "isptar_gzip.h"
#include "isptar_args.h"
#include "isptar_slice.h"
#include <string.h>
#include <stdexcept>

#define	PART_NAME_PREFIX	".partname."
#define	PART_DEST_PREFIX	"."
#define	EXCLUDE				"--exclude-compression "

class TarReader;

class Copy : public io::OStream {
public:
	Copy(io::OStream &first, io::OStream &second) : m_first(first), m_second(second) {}
	void Write(const char *buf, int size) {
		m_first.Write(buf, size);
		m_second.Write(buf, size);
	}
private:
	io::OStream &m_first;
	io::OStream &m_second;
};

class LIStream : public io::IStream {
public:
	LIStream(io::IStream &in, int64_t limit = 0) : m_in(in), m_limit(limit) {}
	void Reset(int64_t limit) { m_limit = limit; }
	int Read(char *buf, int size) {
		int res = m_in.Read(buf, std::min(m_limit, (int64_t)size));
		if (res > 0)
			m_limit -= res;
		return res;
	}
private:
	io::IStream &m_in;
	int64_t m_limit;
};

class Sender {
public:
	Sender() : m_source(0) {}
	virtual ~Sender() {}
	virtual bool SendInfo(const tar::FileInfo &info) {
		std::cout << info.Str() << std::endl;
		return false;
	}
	virtual void SendData(io::IStream &in) {}

	void SetSource(TarReader *reader, bool reference) {
		m_source = reader;
		m_reference = reference;
	}

protected:
	struct PrevInfo {
		bool found;
		std::string file_offs;
		io::IStream *file_data;
		PrevInfo() : found(false), file_data(NULL) {}
	};

	PrevInfo GetPrevInfo(const tar::FileInfo &info);

private:
	TarReader *m_source;
	bool m_reference;
};

class PipeSender : public Sender {
public:
	virtual bool SendInfo(const tar::FileInfo &info) {
		std::string line = info.Str();
		int16_t len = line.size();
		if (write(1, &len, sizeof(len)) != sizeof(len))
			throw std::runtime_error("Failed to send line size");
		if (write(1, line.data(), len) != len)
			throw std::runtime_error("Failed to send info");
		if (read(0, &len, sizeof(len)) != sizeof(len))
			throw std::runtime_error("Failed to get answer");
		m_size = info.size;
		return len;
	}

	void Finish() {
		int16_t len = 0;
		if (write(1, &len, sizeof(len)) != sizeof(len))
			throw std::runtime_error("Failed to send eof");
	}

	virtual void SendData(io::IStream &in) {
		char buf[CHUNK];
		while (m_size) {
			int len = m_size > (int64_t)sizeof(buf) ? sizeof(buf) : (int)m_size;
			int16_t res = in.Read(buf, len);
			////std::cerr << '\t' << len << " res: " << res << std::endl;
			if (res < 0)
				throw std::runtime_error("Failed to get data");
			if (write(1, &res, sizeof(res)) != sizeof(res))
				throw std::runtime_error("Failed to send chunk size");
			if (res == 0)
				return;
			if (write(1, buf, res) != res)
				throw std::runtime_error("Failed to send data");
			m_size -= res;
		}
	}
private:
	int64_t m_size;
};

void MakeIsolated(io::IStream &in, std::map<std::string, std::string> &head, io::OStream &out) {
	gzip::OStream gz_out(out);
	tar::Writer tar(gz_out);
	auto list_real_size = misc::Int(head["listing_real_size"]);
	std::string header;
	head.erase("header_size");
	for (auto ptr = head.begin(); ptr != head.end(); ++ptr)
		header += ptr->first + '=' + ptr->second + '\n';
	header += "header_size=";
	std::string packed_header = gzip::Pack(header);
	std::string header_size = misc::Str(packed_header.size());
	tar::FileInfo info;
	info.filename = ".backup.info";
	info.type = REGTYPE;
	info.uid = getuid();
	info.user = info.GetUserName();
	info.gid = getgid();
	info.group = info.GetGroupName();
	info.mode = 0400;
	info.time = time(NULL);
	info.size = list_real_size + header.size() + header_size.size();
	tar.Add(info);
	gz_out.Flush(true);
	char buf[CHUNK];
	int64_t listing_size = misc::Int(head["listing_size"]);
	while (listing_size) {
		int len = listing_size > (int)sizeof(buf) ? sizeof(buf) : listing_size;
		auto size = in.Read(buf, len);
		out.Write(buf, size);
		listing_size -= size;
	}
	out.WriteStr(packed_header);
	tar.AddDone(list_real_size + header.size());
	//std::cout << "Header size: " << header_size << std::endl;
	tar.WriteData(header_size);
	tar.WriteTail(true);
	gz_out.Flush(true);
}


class TarSender : public Sender {
public:
	TarSender(const std::string &name, slice::OStream &out, const std::string &lname)
		: m_filename(name)
		, m_out(out)
		, m_gz_out(m_out)
		, m_tar(m_gz_out)
		, m_listing_name(lname)
		, m_gz_listing(m_listing)
		, m_compress(true) {
		char path[128];
		strncpy(path, "/tmp/backup.XXXXXX", sizeof(path));
		misc::ResHandle fd = mkostemps(path, 0, O_LARGEFILE);
		if (!fd)
			throw std::runtime_error("Failed to create tempfile");
		unlink(path);
		m_listing.Reset(fd);
		LoadCompressedList("etc/isptar.conf");
	}

	void LoadCompressedList(const std::string &filename) {
		io::ResHandle fd(open(filename.c_str(), O_RDONLY|O_LARGEFILE));
		if (!fd)
			return;
		char buf[512];
		std::string buffer;
		while (true) {
			auto pos = buffer.find('\n');
			while (pos == std::string::npos) {
				int size = read(fd, buf, sizeof(buf));
				if (size <= 0) {
					if (!buffer.empty())
						if (buffer.size() > sizeof(EXCLUDE) && buffer.compare(0, sizeof(EXCLUDE) - 1, EXCLUDE) == 0)
							m_compressed.push_back(buffer.substr(sizeof(EXCLUDE) - 1));
					return;
				}
				auto offs = buffer.size();
				buffer.append(buf, size);
				pos = buffer.find('\n', offs);
			}
			if (pos > 0)
				if (pos > sizeof(EXCLUDE) && buffer.compare(0, sizeof(EXCLUDE) - 1, EXCLUDE) == 0)
					m_compressed.push_back(buffer.substr(sizeof(EXCLUDE) - 1, pos - (sizeof(EXCLUDE) - 1)));
			buffer.erase(0, pos + 1);
		}
	}

	void WriteFooter(const std::string &parts = "") {
		m_gz_out.Flush(true);
		m_gz_listing.WriteStr("\n");
		m_gz_listing.Flush(true);
		auto list_size = m_listing.Offset();
		auto list_real_size = m_gz_listing.TotalOut();
		std::map<std::string, std::string> head;
		head["listing_header"] = "512";
		head["listing_size"] = misc::Str(list_size);
		head["listing_real_size"] = misc::Str(list_real_size);
		if (!parts.empty())
			head["parts"] = parts;
		lseek64(m_listing.fd(), 0, SEEK_SET);
		io::FileIStream in(m_listing.fd());
		if (!m_listing_name.empty()) {
			io::FileOStream lst(m_listing_name);
			Copy copy(m_out, lst);
			MakeIsolated(in, head, copy);
		} else
			MakeIsolated(in, head, m_out);
	}

	bool IsNeedCompress(const tar::FileInfo &info) {
		ForEachI(m_compressed, it)
			if (info.filename.size() >= it->size() &&
				info.filename.compare(info.filename.size() - it->size(), it->size(), *it) == 0)
				return false;
		return true;
	}

	void SetCompress(bool compress) {
		if (compress) {
			if (!m_compress) {
				m_compress = true;
				m_gz_out.SetLevel(9, Z_DEFAULT_STRATEGY);
			}
		} else {
			if (m_compress) {
				m_compress = false;
				m_gz_out.SetLevel(0, Z_DEFAULT_STRATEGY);
			}
		}
	}

	virtual bool SendInfo(const tar::FileInfo &info) {
		auto prev = GetPrevInfo(info);
		m_gz_listing.WriteStr(info.Str());
		bool save_data = !prev.found || prev.file_data;
		//std::cerr << info.Str() << (save_data ? " save " : " not save ") << std::endl;
		if (save_data) {
			SetCompress(true);
			m_tar.Add(info);
		}
		if (info.type == REGTYPE) {
			save_data &= info.size > 0;
			if (save_data) {
				SetCompress(IsNeedCompress(info));
				m_gz_out.Flush(true);
				auto fpos = m_out.Offset();
				auto zpos = m_gz_out.Offset();
				m_gz_listing.WriteStr("\t0:" + misc::Str(fpos.first) + ':' +
					misc::Str(fpos.second) + ':' + misc::Str(zpos));
				if (prev.file_data) {
					// берем файл из архива
					SendData(*prev.file_data);
					save_data = false;
				}
			} else if (!prev.file_offs.empty()) // ссылка на предыдущий архив
				m_gz_listing.WriteStr('\t' + prev.file_offs);
		} else
			save_data = false;
		m_gz_listing.WriteStr("\n");
		return save_data;
	}

	virtual void SendData(io::IStream &in) {
		m_tar.WriteData(in);
		m_tar.WriteTail();
	}
private:
	const std::string m_filename;
	slice::OStream &m_out;
	gzip::OStream m_gz_out;
	tar::Writer m_tar;
	const std::string m_listing_name;
	io::FileOStream m_listing;
	gzip::OStream m_gz_listing;
	std::vector<std::string> m_compressed;
	bool m_compress;
};

class Reader {
public:
	Reader(Sender &send, const args::StringVector &exclude) : m_send(send), m_exclude(exclude) { }

	void Read(const std::string &_root, const std::string &folder) {
		std::string root = _root;
		if (!root.empty() && root[root.size() - 1] != '/')
			root.push_back('/');
		file::DirTree dir(root + folder);
		tar::FileInfo info;
		misc::Script script(m_hook);
		while (dir.Read()) {
			if (dir.type() != file::DirTree::dttDir && dir.IsDir())
				continue;
			const std::string filename = dir.RealPath();
			const std::string arch_filename = filename.substr(root.size());
			if (Exclude(arch_filename)) {
				dir.Set(file::DirTree::dtSkip);
				continue;
			}
			bool hook = !m_hook_name.empty() && arch_filename.compare(0, m_hook_name.size(), m_hook_name) == 0;
			if (hook) {
				auto pos = filename.rfind('/');
				script.AddParam('p', pos == std::string::npos ? "" : filename.substr(0, pos));
				script.AddParam('f', pos == std::string::npos ? filename : filename.substr(pos + 1));
				script.AddParam('c', "start");
				if (!script.Do())
					throw std::runtime_error("Failed to execute backup hook");
			}
			struct stat sb;
			if (lstat(dir.RealPath().c_str(), &sb)) {
				//Warning("Failed to stat. Skip '%s'", dir.FullName().c_str());
				continue;
			}
			if (info.Set(arch_filename, sb).type == AREGTYPE) {
				//Warning();
				continue;
			}
			//std::cerr << "Pack " << dir.RealPath() << '\t' << info.size << std::endl;
			io::FileIStream data; // убедиться что файл можно прочитать, иначе пропустить файл
			if (info.type == SYMTYPE) {
				char buf[sb.st_size + 1];
				int size = readlink(dir.RealPath().c_str(), buf, sizeof(buf));
				if (size == -1 || size >= (int)sizeof(buf)) {
					// Warning();
					continue;
				}
				info.linkname.assign(buf, size);
			} else if (info.type == REGTYPE) {
				if (sb.st_nlink > 1) {
					auto res = m_hardlinks.insert(
						std::make_pair(sb.st_ino, info.filename)
					);
					if (!res.second) {
						info.type = LNKTYPE;
						info.linkname = res.first->second;
					}
				}
				if (info.type == REGTYPE) {
					data = io::FileIStream(dir.RealPath());
					if (!data.fd())
						continue;
				}
			}
			if (m_send.SendInfo(info)) {
				m_send.SendData(data);
			}
			if (hook) {
				script.AddParam('c', "end");
				if (!script.Do())
					throw std::runtime_error("Failed to execute backup hook");
			}
		}
	}

	void SetBackupHook(const std::string &prefix, const std::string &command) {
		m_hook = command;
		m_hook_name = prefix;
	}

private:
	Sender &m_send;
	args::StringVector m_exclude;
	std::string m_hook;
	std::string m_hook_name;
	std::map<ino_t, std::string> m_hardlinks;

	bool Exclude(const std::string &filename) const {
		ForEachI(m_exclude, exclude) {
			//fnmatch не находит если слэш в конце
			std::string _exclude = *exclude;
			std::string::size_type size =_exclude.size() - 1;
			if (_exclude[size] == '/')
				_exclude.resize(size);
			if (fnmatch(_exclude.c_str(), filename.c_str(), FNM_LEADING_DIR|FNM_EXTMATCH) == 0)
				return true;
		}
		return false;
	}
};

class TarReader {
public:
	TarReader(const std::string &data, const std::string &list,
		const std::string &download)
		: m_in(list.empty() ? data : list)
		, m_listing(m_in)
		, m_file(data)
		, m_file_data(m_file)
		, m_file_limited_data(m_file_data)
		, m_base(0)
		, m_download(download) {
		m_in.SetDownload(m_download);
		m_file.SetDownload(m_download);
		m_head = gzip::GetHeader(m_in);
		if (m_head.empty())
			throw std::runtime_error("No header found");
		int64_t listing_size = misc::Int(m_head["listing_size"]);
		m_in.Seek(0, -(listing_size + misc::Int(m_head["header_size"])), SEEK_END);
		m_listing.Reset(listing_size);
	}

	~TarReader() {
		if (m_base)
			delete m_base;
	}

	void AddBase(const std::string &filename) {
		if (m_base)
			m_base->AddBase(filename);
		else
			m_base = new TarReader(filename, "", m_download);
	}

	bool Read() {
		auto pos = m_data.find('\n');
		while (pos == std::string::npos) {
			char buf[CHUNK];
			int size = m_listing.Read(buf, sizeof(buf));
			if (size <= 0)
				return false;
			m_data.append(buf, size);
			pos = m_data.find('\n');
		}
		if (pos == 0)
			return false;
		m_line = m_data.substr(0, pos);
		m_info.Set(m_line);
		m_data.erase(0, pos + 1);
		return true;
	}

	tar::FileInfo & info() { return m_info; }
	std::string Offset() const { return m_line; }
	io::IStream & data() {
		std::string tmp = m_line;
		int depth = misc::Int(misc::GetWord(tmp, ':'));
		return GetData(depth, tmp, m_info.size);
	}

	std::string Header(const std::string &name) {
		auto pos = m_head.find(name);
		return pos == m_head.end() ? "" : pos->second;
	}

private:
	slice::IStream m_in;
	gzip::IStream m_listing;
	tar::FileInfo m_info;
	std::string m_data;
	std::string m_line;
	slice::IStream m_file;
	gzip::IStream m_file_data;
	LIStream m_file_limited_data;
	TarReader *m_base;
	const std::string m_download;
	std::map<std::string, std::string> m_head;

	/**
	 * Для того, чтобы прочитать отдельный файл используется класс LIStream
	 * (limited input stream), который ограничивает максимальное количество
	 * байт которое может отдать поток. За счет того, что slice::IStream
	 * читает данные до границы текущего slice и переходит к следующему только
	 * если в текущем данных не осталось, мы избегаем ситуации, когда при
	 * чтении пограничного блока может возникать переключение между слайсами
	 * (в обратную сторону)
	 */
	io::IStream & GetData(int depth, std::string tmp, tar::FileSizeType size) {
		if (depth) {
			if (!m_base)
				throw std::runtime_error("Failed to get file from base");
			return m_base->GetData(depth - 1, tmp, size);
		}
		int64_t file = misc::Int(misc::GetWord(tmp, ':'));
		int64_t pos = misc::Int(misc::GetWord(tmp, ':'));
		/*int gz_offs = misc::Int(misc::GetWord(tmp, '\t'))*/;
		m_file.Seek(file, pos, SEEK_SET);
		m_file_data.Reset();
		//m_file_data.Seek(offs, SEEK_CUR);
		m_file_limited_data.Reset(size);
		return m_file_limited_data;
	}
};

Sender::PrevInfo Sender::GetPrevInfo(const tar::FileInfo &info) {
	PrevInfo res;
	if (m_source) {
		while (file::DirTree::AlphaSort(m_source->info().filename.c_str(), info.filename .c_str()) < 0 && m_source->Read()) {
			//std::cout << "Skip " << m_source->info().filename << std::endl;
		}
		if (m_source->info() == info) {
			res.found = true;
			if (info.type == REGTYPE) {
				if (info.size > 0) {
					if (m_reference) {
						std::string offs = m_source->Offset();
						int backup = misc::Int(misc::GetWord(offs, ':'));
						offs = misc::Str(backup + 1) + ':' + offs;
						res.file_offs = offs;
					} else
						res.file_data = &m_source->data();
				} else if (!m_reference)
					res.found = false;
			}
		}
	}
	return res;
}

bool GetInfoFromStdin(tar::FileInfo &info) {
	int16_t size;
	if (read(0, &size, sizeof(size)) != sizeof(size))
		throw std::runtime_error("Failed to get info size");
	if (size == 0) {
		//std::cerr << "GOT eof" << std::endl;
		return false;
	}
	int left = size;
	char buf[CHUNK];
	std::string line;
	while (left > 0) {
		int len = (int)left > (int)sizeof(buf) ? sizeof(buf) : left;
		int res = read(0, buf, len);
		if (res <= 0)
			throw std::runtime_error("Failed to get info");
		line.append(buf, res);
		left -= res;
	}
	info.Set(line);
	//std::cerr << "GOT " << info.filename << std::endl;
	return true;
}

class StdinIStream : public io::IStream {
public:
	StdinIStream() : m_chunk_size(0) {}
	int Read(char *buf, int size) {
		if (!m_chunk_size) {
			////std::cerr << "READ " << sizeof(m_chunk_size) << std::endl;
			if (read(0, &m_chunk_size, sizeof(m_chunk_size)) != sizeof(m_chunk_size))
				throw std::runtime_error("Failed to get next chunk size");
			if (m_chunk_size == 0)
				return 0;
		}
		int len = size > m_chunk_size ? m_chunk_size : size;
		int res = read(0, buf, len);
		////std::cerr << "READ " << len << std::endl;
		if (res > 0)
			m_chunk_size -= res;
		return res;
	}
private:
	int16_t m_chunk_size;
};

bool CheckName(const args::StringVector &args, const std::string &name) {
	if (name[0] == '/' || name.find("/../") != std::string::npos || name.compare(0, 3, "../") == 0) {
		std::cerr << "Ignoring bad path " << name << std::endl;
		return false;
	}
	if (args.empty())
		return true;
	ForEachI(args, arg)
		if (name.compare(0, arg->size(), *arg) == 0 &&
			(name.size() == arg->size() || name[arg->size()] == '/' || arg->at(arg->size() - 1) == '/'))
			return true;
	return false;
}

bool ValidSize(std::string &str) {
	if (str.empty())
		return false;
	auto pos = str.find_first_not_of("0123456789");
	if (pos == std::string::npos)
		return true;
	std::string suffix = str.substr(pos);
	str.resize(pos);
	int64_t value = misc::Int(str);
	if (suffix == "K")
		value *= 1024ll;
	else if (suffix == "M")
		value *= 1024ll * 1024;
	else if (suffix == "G")
		value *= 1024ll * 1024 * 1024;
	else if (suffix == "T")
		value *= 1024ll * 1024 * 1024 * 1024;
	else
		return false;
	str = misc::Str(value);
	return true;
}

static std::string GetPart(std::string &parts) {
	std::string name = misc::GetWord(parts, ' ');
	if (name.compare(0, sizeof(PART_DEST_PREFIX) - 1, PART_DEST_PREFIX) == 0)
		name.erase(0, sizeof(PART_DEST_PREFIX) - 1);
	else
		std::cerr << "Part name prefix missed for part '" << name << "'\n";
	return name;
}

static void SetEUid(const std::string &username) {
	struct passwd *pw;
	if (username.find_first_not_of("0123456789") == std::string::npos)
		pw = getpwuid(misc::Int(username));
	else
		pw = getpwnam(username.c_str());
	if (!pw)
		throw std::runtime_error("Unknown user '" + username + "'");

	std::vector<gid_t> groups(2);
	int ngroups = groups.size();
	getgrouplist(pw->pw_name, pw->pw_gid, &groups[0], &ngroups);
	while (ngroups > (int)groups.size()) {
		groups.resize(ngroups);
		getgrouplist(pw->pw_name, pw->pw_gid, &groups[0], &ngroups);
	}
	setgroups(ngroups, &groups[0]);
	setegid(pw->pw_gid);
	seteuid(pw->pw_uid);
}

int main(int argc, const char *argv[]) {
	try {
		args::Args args("ISPsystem backup tool");
		args
			.AddOption("execute", 'E', "execute command to get slice if it missed or upload after it was created").SetParam()
			.AddOption("extract", 'x', "extract files from backup")
				.SetGroup("command").SetParam().SetRequired()
				.AddSuboption("base", 'B', "path to base archive for difencial backup")
					.SetParam().SetMultiple()
				.AddOption("listing", 'L', "Get file list from specified file").SetParam()
				.AddOption("root", 'R', "extract files to specified folder")
					.SetDefault(get_current_dir_name()).SetGroup("dest")
					.AddSuboption("user", 'U', "act as specified user").SetParam()
					.Last()
				.AddOption("tar", 'T', "extract files to tar archive").SetParam().SetGroup("dest")
					.AddSuboption("plain-file", 'P', "write single file content to stream").SetParam()
					.Last()
				.AddOption("list-only", 'D', "list files without extracting data").SetGroup("dest")
				.Last()
			.AddOption("list", 'l', "get backup listing").SetGroup("command").SetParam()
			.AddOption("create", 'c', "create new backup").SetGroup("command").SetParam()
				.AddSuboption("slice", 'S', "set slice size").SetDefault("100M").SetValidator(ValidSize)
				.AddOption("user", 'U', "act as specified user").SetParam()
				.AddOption("base", 'B', "path to prev backup").SetParam()
					.AddSuboption("listing", 'L', "Get file list from specified file").SetParam()
					.Last()
				.AddOption("copy-data", 'C', "copy data from prev backup into new")
				.AddOption("ref-execute", 'F', "execute command to get base slice if it missed")
				.AddOption("save-listing", 'S', "keep new listing file").SetParam()
				.AddOption("exclude", 'X', "exclude files from backup").SetMultiple().SetParam()
				.AddOption("root", 'R', "search files starting from this folder").SetDefault(get_current_dir_name())
				.AddOption("backup-hook", '<', "execute script before and after backup following files").SetParam()
					.AddSuboption("backup-hook-execute", '>', "script name to execute").SetRequired()
					.Last()
				.Last()
			.AddOption("client", 'n', "start backup client. All data will be send to stdout").SetGroup("command")
				.AddSuboption("root", 'R', "search files starting from this folder").SetDefault(get_current_dir_name())
				.AddOption("user", 'U', "act as specified user").SetParam()
				.AddOption("backup-hook", '<', "execute script before and after backup following files").SetParam()
					.AddSuboption("backup-hook-execute", '>', "script name to execute").SetRequired()
					.Last()
				.AddOption("exclude", 'X', "exclude files from backup").SetMultiple().SetParam()
				.Last()
			.AddOption("server", 's', "start backup server. All data will be got from stdin").SetGroup("command").SetParam()
				.AddSuboption("slice", 'S', "set slice size").SetDefault("100M").SetValidator(ValidSize)
				.AddOption("base", 'B', "path to prev backup").SetParam()
					.AddSuboption("listing", 'L', "Get file list from specified file").SetParam()
					.Last()
				.AddOption("copy-data", 'C', "copy data from prev backup into new")
				.AddOption("ref-execute", 'F', "execute command to get base slice if it missed")
				.AddOption("save-listing", 'S', "keep new listing file").SetParam()
				.Last()
			.AddOption("isolate", 'i', "extract cataloge from archive").SetParam().SetGroup("command")
			.AddOption("merge", 'm', "merge archives into one file").SetParam().SetGroup("command")
				.AddSuboption("slice", 'S', "set slice size").SetDefault("1T").SetValidator(ValidSize)
				.AddOption("ref-execute", 'F', "execute command to get base slice if it missed").SetParam()
				.AddOption("save-listing", 'S', "keep new listing file").SetParam()
				.Last()
			.AddOption("split", 'p', "split archive merged by '--merge'. You can specify new archive name prefix as last command line argument").SetParam().SetGroup("command")
				.AddSuboption("slice", 'S', "set slice size").SetDefault("1T").SetValidator(ValidSize)
				.AddOption("ref-execute", 'F', "execute command to get base slice if it missed").SetParam()
				.AddOption("save-listing", 'S', "keep new listing file").SetParam()
				.AddOption("single-part", '1', "save new arhive in single part")
				.Last();

		args.Parse(argc, argv);

		const std::string command = args["command"];
		if (command == "merge") {
			if (args->ArgsCount() < 1)
				args.Usage();

			slice::OStream out(args["merge"], misc::Int(args["slice"]));
			if (args->Has("execute"))
				out.SetUpload(args["execute"]);
			TarSender sender(args["merge"], out, args->Param("save-listing"));
			unsigned int arg = 0;
			std::string parts;
			int id = 1;
			while (arg < args->ArgsCount()) {
				TarReader reader(args->Args(arg), "", args->Param("ref-execute"));
				for (++arg; arg < args->ArgsCount() && args->Args(arg)[0] != ':'; ++arg)
					reader.AddBase(args->Args(arg));
				while (reader.Read())
					if (sender.SendInfo(reader.info()))
						sender.SendData(reader.data());
				if (arg < args->ArgsCount()) {
					tar::FileInfo info;
					info.filename = PART_NAME_PREFIX + misc::Str(id++);
					info.type = SYMTYPE;
					info.linkname = PART_DEST_PREFIX + args->Args(arg).substr(1);
					info.mode = 0600;
					info.uid = getuid();
					info.gid = getgid();
					info.time = time(0);
					sender.SendInfo(info);
					parts += info.linkname;
					parts.push_back(' ');
					++arg;
				}
			}
			sender.WriteFooter(parts);
			out.Finish();
		} else if (command == "split") {
			TarReader reader(args["split"], "", args->Param("ref-execute"));
			std::string parts = reader.Header("parts");
			if (parts.empty())
				throw std::runtime_error("No parts found");
			while (!parts.empty()) {
				std::string name = GetPart(parts);
				std::string filename;
				if (args->Has("single-part")) {
					filename = args->ArgsCount() ? args->Args(0) : "master";
				} else {
					filename = (args->ArgsCount() || !name.empty())
						? args->Args(0) + name
						: "master";
				}
				slice::OStream out(filename, misc::Int(args["slice"]));
				if (args->Has("execute"))
					out.SetUpload(args["execute"]);
				TarSender sender(filename, out,
					args->Has("save-listing")
						? args->Has("single-part")
							? args->Param("save-listing")
							: args->Param("save-listing") + name
						: "");
				while (reader.Read())
					if (reader.info().filename.compare(0, sizeof(PART_NAME_PREFIX) - 1, PART_NAME_PREFIX) == 0) {
						if (reader.info().linkname != PART_DEST_PREFIX + name)
							std::cerr << "Warning: bad part name '" <<
								reader.info().linkname << "'. '" << PART_DEST_PREFIX << name << "' expected\n";
						if (args->Has("single-part"))
							name = GetPart(parts);
						else
							break;
					} else if (sender.SendInfo(reader.info()))
						sender.SendData(reader.data());
				sender.WriteFooter();
				out.Finish();
			}
		} else if (command == "extract") {
			TarReader reader(args["extract"],
				args->Has("listing") ? args["listing"] : "",
				args->Has("execute") ? args["execute"] : ""
			);
			for (size_t i = 0; i < args->ParamCount("base"); ++i)
				reader.AddBase(args->Param("base", i));
			const std::string root = args["root"];
			if (args["dest"] == "root") {
				if (args->Has("user"))
					SetEUid(args["user"]);
				while (reader.Read())
					if (CheckName(args->Args(), reader.info().filename)) try {
						if (reader.info().type == REGTYPE && reader.info().size > 0)
							reader.info().Create(root, reader.data());
						else
							reader.info().Create(root);
					} catch (const slice::error &) {
						throw;
					} catch (const std::exception &e) {
						std::cerr << reader.info().filename << '\t' << e.what() << std::endl;
					}
			} else if (args["dest"] == "list-only") {
				while (reader.Read())
					if (CheckName(args->Args(), reader.info().filename))
						std::cout << reader.info().Str() << std::endl;
			} else {
				io::FileOStream out(args["tar"]);
				gzip::OStream gz_out(out);
				tar::Writer tar(gz_out);
				bool plain_done = false;
				std::string plain_file = args->Param("plain-file");
				while (reader.Read())
					if (CheckName(args->Args(), reader.info().filename)) {
						if (plain_done) {
							plain_done = false;
							io::FileIStream in(args["plain-file"]);
							tar.WriteData(in);
							unlink(args["plain-file"].c_str());
						}
						tar.Add(reader.info());
						if (reader.info().type == REGTYPE) {
							if (!plain_file.empty()) {
								io::FileOStream out(plain_file);
								if (reader.info().size > 0)
									out.WriteStream(reader.data());
								plain_done = true;
							} else if (reader.info().size > 0)
								tar.WriteData(reader.data());
						}
						plain_file.clear();
					}
				if (plain_done) {
					unlink(args["tar"].c_str());
				} else {
					tar.WriteTail(true);
					gz_out.Flush(true);
				}
			}
		} else if (command == "isolate") {
			if (args->ArgsCount() != 1)
				args.Usage();
			slice::IStream in(args["isolate"]);
			if (args->Has("execute"))
				in.SetDownload(args["execute"]);
			auto head = gzip::GetHeader(in);
			if (head.empty())
				throw std::runtime_error("No header found");
			int64_t listing_size = misc::Int(head["listing_size"]);
			in.Seek(0, -(listing_size + misc::Int(head["header_size"])), SEEK_END);

			io::FileOStream out(args->Args(0));
			MakeIsolated(in, head, out);

		} else if (command == "list") {
			slice::IStream in(args["list"]);
			if (args->Has("execute"))
				in.SetDownload(args["execute"]);
			auto head = gzip::GetHeader(in);
			if (head.empty())
				throw std::runtime_error("No header found");
			int64_t listing_size = misc::Int(head["listing_size"]);
			in.Seek(0, -(listing_size + misc::Int(head["header_size"])), SEEK_END);
			gzip::IStream listing(in, listing_size);
			int size;
			char buf[CHUNK];
			while ((size = listing.Read(buf, sizeof(buf))) > 0)
				std::cout.write(buf, size);
		} else if (command == "client") {
			if (!args->ArgsCount())
				args.Usage();
			PipeSender sender;
			Reader reader(sender, args->Params("exclude"));
			if (args->Has("backup-hook"))
				reader.SetBackupHook(args["backup-hook"], args["backup-hook-execute"]);
			const std::string root = args["root"];
			if (args->Has("user"))
				SetEUid(args["user"]);
			ForEachI(args->Args(), arg)
				reader.Read(root, *arg);
			sender.Finish();
		} else if (command == "server") {
			slice::OStream out(args["server"], misc::Int(args["slice"]));
			if (args->Has("execute"))
				out.SetUpload(args["execute"]);
			TarSender sender(args["server"], out,
				args->Has("save-listing") ? args["save-listing"] : "");
			if (args->Has("base")) {
				auto base = new TarReader(args["base"],
					args->Has("listing") ? args["listing"] : "",
					args->Has("ref-execute") ? args["ref-execute"] : ""
				);
				sender.SetSource(base, !args->Has("copy-data"));
			}
			tar::FileInfo info;
			while (GetInfoFromStdin(info)) {
				////std::cerr << info.Str() << std::endl;
				int16_t response = sender.SendInfo(info) ? 1 : 0;
				if (write(1, &response, sizeof(response)) != sizeof(response))
					throw std::runtime_error("Failed to send response " + info.filename);
				if (response) {
					StdinIStream in;
					sender.SendData(in);
				}
			}
			sender.WriteFooter();
			out.Finish();
		} else if (command == "create") {
			if (!args->ArgsCount())
				args.Usage();

			slice::OStream out(args["create"], misc::Int(args["slice"]));
			if (args->Has("execute"))
				out.SetUpload(args["execute"]);
			TarSender sender(args["create"], out,
				args->Has("save-listing") ? args["save-listing"] : "");
			if (args->Has("base")) {
				auto base = new TarReader(args["base"],
					args->Has("listing") ? args["listing"] : "",
					args->Has("ref-execute") ? args["ref-execute"] : ""
				);
				sender.SetSource(base, !args->Has("copy-data"));
			}
			Reader reader(sender, args->Params("exclude"));
			if (args->Has("backup-hook"))
				reader.SetBackupHook(args["backup-hook"], args["backup-hook-execute"]);
			const std::string root = args["root"];
			if (args->Has("user"))
				SetEUid(args["user"]);
			ForEachI(args->Args(), arg)
				reader.Read(root, *arg);
			seteuid(getuid());
			sender.WriteFooter();
			out.Finish();
		}
		return 0;
	} catch (const std::exception &e) {
		std::cerr << e.what() << std::endl;
		return 1;
	}
}

