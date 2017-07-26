#ifndef __ISPTAR_FILE_H__
#define __ISPTAR_FILE_H__
#include <string>
#include <memory>

namespace file {
using std::string;
class DirTree {
public:
	enum Option {
	};

	//ReadOption
	static const int dtSkip;
	static const int dtAgain;
	static const int dtFollow;
	static const int dtNext;

	//Type
	static const int dttDir;
	static const int dttDirPost;
	static const int dttDirCircle;
	static const int dttDirNoRead;
	static const int dttDirDot;
	static const int dttErr;
	static const int dttOther;
	static const int dttFile;
	static const int dttFileFailStat;
	static const int dttFileNoStat;
	static const int dttSymLink;
	static const int dttSymLinkNone;

	static const int DefaultFlags;

	DirTree(const string &name, int flags =
			DefaultFlags);
	~DirTree();

	bool Read();
	void Set(int option);

	string name() const;
	string FullName() const;
	string RealPath() const;
	int type() const;
	bool IsDir() const;
	bool IsFile() const;
	bool IsSymLink() const;

	static int AlphaSort(const char *A, const char *B);
private:
	struct Data;
	std::shared_ptr<Data> m_data;

	void Init(int flags);
};
} // end of file namespace

#endif
