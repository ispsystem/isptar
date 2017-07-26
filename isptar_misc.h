#ifndef __ISPTAR_MISC_H__
#define __ISPTAR_MISC_H__
#include <string>
#include <memory>
#include <map>
#define	ForEachI(L, item) for (auto item = (L).begin(); item != (L).end(); ++item)

namespace misc {
using std::string;
class ResHandle {
public:
	ResHandle();
	ResHandle(int fd);
	operator int() const;
	operator bool() const;
private:
	std::shared_ptr<int> m_fd;
};

class Script {
public:
	Script(const string &command);
	void AddParam(char, const string &value);
	bool Do();
private:
	const string m_command;
	std::map<char, string> m_replace;
};

string GetWord(string &str, char ch);
string RGetWord(string &str, char ch);
string Str(int64_t val);
int64_t Int(const string &str);

class Su {
public:
	Su();
	~Su();
	void Release();
private:
	int m_uid;
	int m_gid;
};
} // end of misc namespace

#endif
