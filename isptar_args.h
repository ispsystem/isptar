#ifndef	__ISPTAR_ARGS_H__
#define	__ISPTAR_ARGS_H__
#include <string>
#include <vector>
#include <list>
#include <map>
#include <memory>

//! Классы для разбора параметров командной строки
namespace args {
using std::string;
typedef std::vector<string> StringVector;
typedef std::map<string,string> StringMap;
typedef bool (*Valid)(string &data);

class Result {
public:
	string operator[] (const string &name) const;
	string Param(const string &name, int index = -1) const;
	StringVector Params(const string &name) const;
	size_t ParamCount(const string &name) const;
	bool Has(const string &name) const;
	const StringVector& Args() const;
	string Args(int index) const;
	size_t ArgsCount() const;
	void SetParam(const string &name, const string &value);
private:
	struct ParamValue {
		StringVector values;
		bool has;
	};
	StringVector m_args;
	std::map<string, ParamValue> m_values;
	StringMap m_groups;
	friend class Args;
};

typedef std::shared_ptr<Result> ResultPtr;

class Args {
public:
	typedef int size_type;
	typedef const char **value_type;
	class UsageError;
	class Arg {
	public:
		Arg& AddOption(const string &name, char short_name, const string &help);
		Arg& AddSuboption(const string &name, char short_name, const string &help);
		Arg& SetValidator(Valid valid);
		Arg& SetDefault(const string &def_value);
		Arg& SetGroup(const string &group);
		Arg& SetRequired();
		Arg& SetParam();
		Arg& SetMultiple();
		Arg& Last() const;
	private:
		Arg *m_parent;
		string m_name;
		string m_group;
		string m_help;
		string m_def_value;
		Valid m_valid;
		std::list<Arg *> m_child;
		int m_params;
		char m_short_name;
		bool m_required;
		bool m_multiple;

		Arg(Arg *parent, const string &name, char short_name, const string &help);
		~Arg();
		string Usage(int level = 0);
		friend class Args;
	};
	Args(const string &desc);
	Arg& AddOption(const string &name, char short_name, const string &help);
	int Parse(size_type argc, value_type argv);
	string operator[] (const string &name) const;
	Result * operator -> () const;
	ResultPtr GetResult() const;
	void Usage(const string &error = "") const;
private:
	const string m_desc;
	Arg m_args;
	ResultPtr m_result;
	bool Parse(Arg *, size_type argc, value_type argv, size_type &i);
	void SetValue(Args::Arg *args, const string &name, size_type &i, size_type argc, value_type argv);
	bool ParseLong(Args::Arg *args, const string &name, size_type &i, size_type argc, value_type argv);
	bool ParseShort(Args::Arg *args, char name, size_type &i, size_type argc, value_type argv);
	void SetDefaults(Args::Arg *args);
};
} // end of args namespace
#endif
