#undef _FILE_OFFSET_BITS
#include <fts.h>
#include <sys/types.h>
#include "isptar_file.h"
#include <stdexcept>

namespace file {
const int DirTree::dtSkip = FTS_SKIP;
const int DirTree:: dtAgain = FTS_AGAIN;
const int DirTree:: dtFollow = FTS_FOLLOW;
const int DirTree::dtNext = FTS_NOINSTR;

const int DirTree::dttDir = FTS_D;
const int DirTree::dttDirPost = FTS_DP;
const int DirTree::dttDirCircle = FTS_DC;
const int DirTree::dttDirNoRead = FTS_DNR;
const int DirTree::dttDirDot = FTS_DOT;
const int DirTree::dttErr = FTS_ERR;
const int DirTree::dttOther = FTS_DEFAULT;
const int DirTree::dttFile = FTS_F;
const int DirTree::dttFileFailStat = FTS_NS;
const int DirTree::dttFileNoStat = FTS_NSOK;
const int DirTree::dttSymLink = FTS_SL;
const int DirTree::dttSymLinkNone = FTS_SLNONE;

struct DirTree::Data {
	string m_root;
	int m_flags;
	FTS *m_fts;
	FTSENT *m_ent;
};

int _AlphaSort(const FTSENT **a, const FTSENT **b) {
	if ((*a)->fts_info == FTS_ERR || (*b)->fts_info == FTS_ERR)
		return 0;
	return DirTree::AlphaSort((*a)->fts_name, (*b)->fts_name);
}

void DirTree::Init(int flags) {
	const char *data[2] = { m_data->m_root.c_str(), NULL };
	if (!(m_data->m_fts = fts_open((char **)data, flags, &_AlphaSort)))
		throw std::runtime_error("Failed to open FTS");

	if (!(m_data->m_ent = fts_read(m_data->m_fts)))
		throw std::runtime_error("Failed to read");
		/*
	std::cout << "root " << m_root << std::endl;
	if (m_ent->fts_pathlen < m_root.size())
		throw std::runtime_error("Failed to get first entry");
	m_root.assign(m_ent->fts_path, m_ent->fts_pathlen - m_root.size());
	std::cout << "root " << m_root.size() << std::endl;
	*/
}

DirTree::DirTree(const string &name, int flags) : m_data(new DirTree::Data) {
	m_data->m_root = name;
	m_data->m_flags = flags;
	Init(flags);
}

DirTree::~DirTree() { fts_close(m_data->m_fts); }

bool DirTree::Read() {
	m_data->m_ent = fts_read(m_data->m_fts);
	return m_data->m_ent && m_data->m_ent->fts_pathlen > m_data->m_root.size();
}

void DirTree::Set(int option) {
	if (option != dtNext)
		fts_set(m_data->m_fts, m_data->m_ent, option);
}

string DirTree::name() const { return string(m_data->m_ent->fts_name, m_data->m_ent->fts_namelen); }
string DirTree::FullName() const {
	int size = m_data->m_root.size() + 1;
	return string(m_data->m_ent->fts_path + size, m_data->m_ent->fts_pathlen - size);
}
string DirTree::RealPath() const {
	return string(m_data->m_ent->fts_path, m_data->m_ent->fts_pathlen);
}
int DirTree::type() const { return m_data->m_ent->fts_info; }

bool DirTree::IsDir() const {
	auto t = type();
	return t == dttDir || t == dttDirPost || t == dttDirCircle || t == dttDirNoRead || t == dttDirDot;
}

bool DirTree::IsFile() const {
	auto t = type();
	return t == dttFile || t == dttFileFailStat || t == dttFileNoStat;
}

bool DirTree::IsSymLink() const {
	auto t = type();
	return t == dttSymLink || t == dttSymLinkNone;
}

const int DirTree::DefaultFlags = FTS_XDEV|FTS_COMFOLLOW|FTS_PHYSICAL|FTS_NOCHDIR;

int DirTree::AlphaSort(const char * A, const char * B) {
	while (*A && *A==*B) {
		A++;
		B++;
	}
	if (!*A && !*B)
		return 0;
	if (*A=='/')
		return *B ? -1 : 1;
	if (*B=='/')
		return *A ? 1 : -1;
	return *A-*B;
}

} // end of file namespace
