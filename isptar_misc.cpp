#include "isptar_misc.h"
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <stdexcept>

namespace misc {
Su::Su() : m_uid(-1), m_gid(-1) {
	if (getuid() == 0) {
		m_uid = geteuid();
		if (m_uid)
			seteuid(0);
		m_gid = getegid();
		if (m_gid)
			setegid(0);
	}
}
Su::~Su() { Release(); }
void Su::Release() {
	if (m_gid > 0)
		setegid(m_gid);
	if (m_uid > 0)
		seteuid(m_uid);
	m_uid = -1;
	m_gid = -1;
}

Script::Script(const string &command) : m_command(command) {}
void Script::AddParam(char ch, const string &value) { m_replace[ch] = value; }
bool Script::Do() {
	string cmd = m_command;
	for (auto pos = cmd.find('%'); pos != string::npos; pos = cmd.find('%', pos)) {
		auto ptr = m_replace.find(cmd[pos + 1]);
		string value = (ptr == m_replace.end()) ? cmd.substr(pos + 1, 1) : ptr->second;
		cmd.replace(pos, 2, value);
		pos += value.size();
	}

	int pfd[2];
	if (pipe(pfd))
		throw std::runtime_error("Failed to open pipe");
	auto pid = fork();
	if (pid == -1) {
		close(pfd[0]);
		close(pfd[1]);
		throw std::runtime_error("Failed to fork");
	}
	if (pid == 0) {
		if (dup2(open("/dev/null", O_WRONLY), 0) == -1)
			_exit(1);
		if (dup2(pfd[1], 1) == -1)
			_exit(1);
		if (dup2(pfd[1], 2) == -1)
			_exit(1);
		for (int i = getdtablesize(); i > 2; --i)
			close(i);
		setegid(getgid());
		seteuid(getuid());
		execl("/bin/sh", "/bin/sh", "-c", cmd.c_str(), (char *)0);
		_exit(1);
	}
	close(pfd[1]);
	int size;
	char buf[1024];
	while ((size = read(pfd[0], buf, sizeof(buf))) > 0)
		write(2, buf, size);
	close(pfd[0]);
	int status;
	if (waitpid(pid, &status, 0) != pid)
		throw std::runtime_error("Waitpid failed");
	return (WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

static void Close(int *fd) {
	close(*fd);
	delete fd;
}

ResHandle::ResHandle() { }
ResHandle::ResHandle(int fd) {
	if (fd != -1)
		m_fd = std::shared_ptr<int>(new int(fd), &Close);
}
ResHandle::operator int() const { return *m_fd.get(); }
ResHandle::operator bool() const { return (bool)m_fd; }

string GetWord(string &str, char ch) {
	string res = str;
	auto pos = str.find(ch);
	if (pos == string::npos) {
		str.clear();
	} else {
		res.resize(pos);
		str.erase(0, pos + 1);
	}
	return res;
}

string RGetWord(string &str, char ch) {
	string res = str;
	auto pos = str.rfind(ch);
	if (pos == string::npos) {
		str.clear();
	} else {
		res.erase(0, pos + 1);
		str.erase(pos);
	}
	return res;
}

string Str(int64_t val) {
	char buf[32];
	snprintf(buf, sizeof(buf), "%lld", (long long)val);
	return buf;
}

int64_t Int(const string &val) {
	char *end;
	return strtoll(val.c_str(), &end, 0);
}
} // end of misc namespace

