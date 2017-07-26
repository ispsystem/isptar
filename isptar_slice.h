#ifndef __ISPTAR_SLICE_H__
#define __ISPTAR_SLICE_H__
#include "isptar_io.h"
#include <stdexcept>
#define	SLICE_SEP	".part"

namespace slice {
using std::string;
typedef std::pair<int64_t, int64_t> Offs;
class error : public std::runtime_error {
public:
	error(const string &what);
};

class OStream : public io::OStream {
public:
	OStream(const string &name, int64_t slice_size);
	void Finish();

	virtual void Write(const char *buf, int size);
	Offs Offset() const;
	void SetUpload(const string &script);
	int64_t Size(Offs start);

private:
	io::FileOStream m_file;
	const string m_filename;
	int64_t m_slice_size;
	int64_t m_slice_id;
	string m_command;
};

class IStream : public io::IStream {
public:
	IStream(const string &name);
	virtual ~IStream();

	virtual int Read(char *buf, int size);
	Offs Seek(int64_t file, int64_t pos, int whence);
	void SetDownload(const string &script);

private:
	io::FileIStream m_file;
	const string m_filename;
	int64_t m_slice_id;
	string m_command;
	string m_last;

	io::ResHandle Open(const string &filename);
	void OpenLast();
	void DeleteLast();
	static int64_t LookupLastSlice(const string &folder, const string &name);
};
} // end of slice namespace

#endif

