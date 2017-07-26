#include "isptar_args.h"
#include "isptar_misc.h"
#include <string.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <assert.h>
#define	TABSTOP	2
#define	FIRSTSEP	'\t'

#define	ASSERT(A,B)	assert(A)

namespace str {
using std::string;
using namespace misc;
string Append(const string &a, const string &b, const string &d) {
	if (a.empty())
		return b;
	if (b.empty())
		return a;
	return a + d + b;
}

void Trim(string &str) {
	string res;
	auto pos = str.find_first_not_of(" \t\n\r");
	if (pos != string::npos) {
		res = str.substr(pos);
		pos = res.find_last_not_of(" \t\n\r");
		res.resize(pos + 1);
	}
	str = res;
}

string Replace(string data, const string &from, const string &to) {
	for (string::size_type pos = data.find(from); pos != string::npos; pos = data.find(from, pos)) {
		data.replace(pos, from.size(), to);
		pos += to.size();
	}
	return data;
}
}

namespace args {
const StringVector& Result::Args() const { return m_args; }
size_t Result::ArgsCount() const { return m_args.size(); }
string Result::Args(int index) const { return m_args[index]; }
string Result::operator[](const string &name) const {
	auto ptr = m_values.find(name);
	if (ptr == m_values.end()) {
		auto grp = m_groups.find(name);
		ASSERT(grp != m_groups.end(), "Try to get unknown value '" + name + "'");
		return grp->second;
	}
	return ptr->second.values.empty() ? "" : *ptr->second.values.rbegin();
}

size_t Result::ParamCount(const string &name) const {
	auto ptr = m_values.find(name);
	return ptr == m_values.end() ? 0 : ptr->second.values.size();
}

StringVector Result::Params(const string &name) const {
	auto ptr = m_values.find(name);
	return (ptr == m_values.end()) ? StringVector() : ptr->second.values;
}

string Result::Param(const string &name, int index) const {
	auto ptr = m_values.find(name);
	if (ptr == m_values.end()) {
		auto grp = m_groups.find(name);
		return grp == m_groups.end() ? "" : grp->second;
	}
	if (ptr->second.values.empty())
		return "";
	if (index == -1)
		return *ptr->second.values.rbegin();
	return index >= (int)ptr->second.values.size() ? "" : ptr->second.values[index];
}

bool Result::Has(const string &name) const {
	auto ptr = m_values.find(name);
	if (ptr == m_values.end())
		return m_groups.find(name) != m_groups.end();
	return ptr->second.has;
}

void Result::SetParam(const string &name, const string &value) {
	auto &param = m_values[name];
	param.has = true;
	param.values.clear();
	param.values.push_back(value);
}

Args::Arg& Args::Arg::AddOption(const string &name, char sname, const string &help) {
	auto res = new Arg(m_parent, name, sname, help);
	m_parent->m_child.push_back(res);
	return *res;
}

Args::Arg& Args::Arg::AddSuboption(const string &name, char sname, const string &help) {
	auto res = new Arg(this, name, sname, help);
	m_child.push_back(res);
	return *res;
}

Args::Arg& Args::Arg::SetValidator(Valid valid) {
	m_valid = valid;
	if (!m_params)
		m_params = 1;
	return *this;
}

Args::Arg& Args::Arg::SetDefault(const string &def_value) {
	ASSERT(!def_value.empty(), "Empty default values not supported");
	m_def_value = def_value;
	if (!m_params)
		m_params = 1;
	return *this;
}

Args::Arg& Args::Arg::SetGroup(const string &group) {
	m_group = group;
	return *this;
}

Args::Arg& Args::Arg::SetRequired() {
	m_required = true;
	if (!m_params && m_group.empty())
		m_params = 1;
	return *this;
}

Args::Arg& Args::Arg::SetParam() {
	m_params = 1;
	return *this;
}

Args::Arg& Args::Arg::SetMultiple() {
	m_multiple = true;
	return *this;
}

Args::Arg& Args::Arg::Last() const {
	ASSERT(m_parent, "Try to get parent of top option");
	return *m_parent;
}

Args::Arg::Arg(Arg *parent, const string &name, char short_name, const string &help)
	: m_parent(parent)
	, m_name(name)
	, m_help(help)
	, m_valid(NULL)
	, m_params(0)
	, m_short_name(short_name)
	, m_required(false)
	, m_multiple(false)
	{}

Args::Arg::~Arg() {
	ForEachI(m_child, child)
		delete *child;
}

string Args::Arg::Usage(int level) {
	string res(level * TABSTOP, ' ');
	res += "--";
	res += m_name;
	if (m_params)
		res +=  " <value>";
	if (m_multiple)
		res += m_params ? "..." : " <value>...";
	res += "\t(-";
	res.push_back(m_short_name);
	res += ") ";
	if (m_required && m_group.empty())
		res += "*";
	res += m_help;
	if (!m_def_value.empty())
		res += " (default: " + m_def_value + ")";
	ForEachI(m_child, child)
		res = str::Append(res, (*child)->Usage(level + 1), "\n");
	return res;
}

Args::Args(const string &desc) : m_desc(desc), m_args(NULL, "", 0, "") {}

Args::Arg& Args::AddOption(const string &name, char short_name, const string &help) {
	return m_args.AddSuboption(name, short_name, help);
}

string Args::operator[] (const string &name) const { return (*m_result)[name]; }
Result * Args::operator -> () const { return m_result.get(); }
ResultPtr Args::GetResult() const { return m_result; }

class Args::UsageError : public std::exception {
public:
	UsageError(const string &desc): m_desc(desc) {}
	const char * what() const throw() { return m_desc.c_str(); }
	virtual ~UsageError() throw() {}
private:
	string m_desc;
};

void Args::Usage(const string &error) const {
	struct winsize w;
	ioctl(STDOUT_FILENO, TIOCGWINSZ, &w);
	std::vector< std::pair<string, string> > usage_entries;
	string res;
	string::size_type max_length = 0;
	ForEachI(m_args.m_child, child) {
		string usage = (*child)->Usage();
		bool group_head = true;
		while(!usage.empty()) {
			string child_usage = str::GetWord(usage, '\n');
			string left_col = str::GetWord(child_usage, '\t');
			str::Trim(left_col);
			if (!group_head)
				left_col = string(TABSTOP, ' ') + left_col;
			if (left_col.size() > max_length)
				max_length = left_col.size();
			usage_entries.push_back(std::make_pair(left_col, child_usage));
			group_head = false;
		}
	}
	size_t cols = w.ws_col;
	cols -= max_length;
	ForEachI(usage_entries, entry) {
		res = str::Append(res, entry->first + string(max_length - entry->first.size(), ' '), "");
		int len_left = cols;
		while (!entry->second.empty()) {
			string word = str::GetWord(entry->second, ' ');
			int word_len = word.size() + 1;
			if (len_left < word_len) {
				res = str::Append(res, "\n" + string(max_length + 6, ' ') + word, "");
				len_left = cols;
			} else
				res = str::Append(res, ' ' + word, "");
			len_left -= word_len;
		}
		res = str::Append(res, "\n", "");
	}
	res = str::Replace(res, "(- )", string(4, ' '));
	string _error = error;
	if (error.empty() && !m_result->m_args.empty())
		_error = "Unknown parameter '" + m_result->m_args[0] + "'";
	throw UsageError(str::Append(str::Append(m_desc, res, "\n\n"), _error, "\n"));

}

int Args::Parse(Args::size_type argc, Args::value_type argv) {
	m_result.reset(new Result);
	for (int i = 1; i < argc;) {
		const char *arg = argv[i++];
		if (strcmp(arg, "--help") == 0) {
			Usage("");
		} else if (strcmp(arg, "--") == 0) {
			for(; i < argc; ++i)
				m_result->m_args.push_back(argv[i]);
			return m_result->ArgsCount();
		}
		if (arg[0] != '-') {
			m_result->m_args.push_back(arg);
		} else if (strncmp(arg, "--", 2) == 0) {
			//Debug("Lookup long param '%s'", arg);
			if (!ParseLong(&m_args, arg + 2, i, argc, argv)) {
				//Warning("Unknown param '%s'", arg);
				m_result->m_args.push_back(arg);
			}
		} else {
			for (int j = 1; arg[j]; ++j) {
				//Debug("Lookup short param '%c'", arg[j]);
				if (!ParseShort(&m_args, arg[j], i, argc, argv)) {
					//Warning("Unknown param '-%c'", arg[j]);
					m_result->m_args.push_back(string("-") + arg[j]);
				}
			}
		}
	}
	SetDefaults(&m_args);
	return m_result->ArgsCount();
}

void Args::SetValue(Args::Arg *args, const string &name, size_type &i, size_type argc, value_type argv) {
	if (!args->m_group.empty()) {
		auto res = m_result->m_groups.insert(std::make_pair(args->m_group, args->m_name));
		if (!res.second && res.first->second != args->m_name)
			Usage("Duplicate parameter group '" + args->m_group + "' value '" +
				args->m_name + "'. First value was '" + res.first->second + "'");
	}
	auto &value = m_result->m_values[name];
	value.has = true;
	if (!args->m_multiple && !value.values.empty()) {
		//Warning("Rewrite value for param '%s'", name.c_str());
		value.values.clear();
	}
	for (int param = args->m_params; param; --param) {
		if (i >= argc)
			Usage("Not anought parameter count for option '" + name + "'");
		string val = argv[i++];
		if (args->m_valid && !args->m_valid(val))
			Usage("Invalid value '" + val + "' for option '" + name + "'");
		//LogExtInfo("Command line argument '%s' add value '%s'", name.c_str(), val.c_str());
		value.values.push_back(val);
	}
	//ForEachI((*child)->m_child, tmp)
		//SetDefaults(*tmp);
}

bool Args::ParseLong(Args::Arg *args, const string &name, size_type &i, size_type argc, value_type argv) {
	ForEachI(args->m_child, child)
		if ((*child)->m_name == name) {
			SetValue(*child, name, i, argc, argv);
			return true;
		} else if (m_result->Has((*child)->m_name) && ParseLong(*child, name, i, argc, argv))
			return true;
	return false;
}

bool Args::ParseShort(Args::Arg *args, char name, size_type &i, size_type argc, value_type argv) {
	ForEachI(args->m_child, child)
		if ((*child)->m_short_name == name) {
			SetValue(*child, (*child)->m_name, i, argc, argv);
			return true;
		} else if (m_result->Has((*child)->m_name) && ParseShort(*child, name, i, argc, argv))
			return true;
	return false;
}

void Args::SetDefaults(Args::Arg *args) {
	ForEachI(args->m_child, child) {
		auto ptr = m_result->m_values.find((*child)->m_name);
		if (ptr == m_result->m_values.end()) {
			if (!(*child)->m_def_value.empty()) {
				auto &value = m_result->m_values[(*child)->m_name];
				value.has = false;
				string val = (*child)->m_def_value;
				if ((*child)->m_valid)
					(*child)->m_valid(val);
				value.values.push_back(val);
				ptr = m_result->m_values.find((*child)->m_name);
			}
		}
		if (ptr != m_result->m_values.end())
			SetDefaults(*child);
		else if ((*child)->m_required && m_result->m_groups.find((*child)->m_group) == m_result->m_groups.end()) {
			string params;
			ForEachI(args->m_child, group)
				if((*group)->m_group == (*child)->m_group)
					params = str::Append(params, "--" + (*group)->m_name, ", ");
			Usage("Need to use one of parameters: " + params);
		}
	}
}
} // end of args namespace
