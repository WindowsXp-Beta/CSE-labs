#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <algorithm>


#include <mutex>
#include <string>
#include <vector>
#include <map>

#include "rpc.h"
#include "mr_protocol.h"
#define DEBUG 1

// flag = true means INFO
// flag = false means ERROR
#define debug_log(flag, ...) do{ \
    if(DEBUG){ \
      if(flag) printf("[INFO]File: %s line: %d: ", __FILE__, __LINE__); \
      else printf("[ERROR]File: %s line: %d: ", __FILE__, __LINE__); \
      printf(__VA_ARGS__); \
      fflush(stdout); \
    } }while(0);

using namespace std;

struct KeyVal {
    string key;
    string val;
	KeyVal(string key = string(), string val = string()):key(key), val(val){}
};

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
vector<KeyVal> Map(const string &filename, const string &content)
{
	// Copy your code from mr_sequential.cc here.
	vector<KeyVal> intermediate_file;
    size_t size = content.size();
    for(int i = 0; i < size; i++){
        int start = i;
        while(isalpha(content[i])) i++;
        if(i > start)intermediate_file.emplace_back(content.substr(start, i - start), "1");
    }
    return intermediate_file;
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
string Reduce(const string &key, const vector < string > &values)
{
    // Copy your code from mr_sequential.cc here.
	unsigned int value = 0;
    for(auto &p : values){
        value += stoul(p);
    }
    return to_string(value);
}


typedef vector<KeyVal> (*MAPF)(const string &key, const string &value);
typedef string (*REDUCEF)(const string &key, const vector<string> &values);

class Worker {
public:
	Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf);

	void doWork();

private:
	void doMap(int index, const vector<string> &filenames);
	// void doMap(int index, string &filenames);
	void doReduce(int index);
	void doSubmit(mr_tasktype taskType, int index);

	mutex mtx;
	int id;

	rpcc *cl;
	std::string basedir;
	MAPF mapf;
	REDUCEF reducef;
};


Worker::Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf)
{
	this->basedir = dir;
	this->mapf = mf;
	this->reducef = rf;

	sockaddr_in dstsock;
	make_sockaddr(dst.c_str(), &dstsock);
	this->cl = new rpcc(dstsock);
	if (this->cl->bind() < 0) {
		printf("mr worker: call bind error\n");
	}
}

void Worker::doMap(int index, const vector<string> &filenames)
{
	// Lab2: Your code goes here.
	string filename = filenames[index];
	string content;
	debug_log(true, "read file %s\n", filename.data())
	getline(ifstream(filename), content, '\0');

	vector<KeyVal> intermediate_content = mapf(filename, content);
	vector<ofstream> intermediate_files(REDUCER_COUNT);
	vector<string> intermediate_file_content(REDUCER_COUNT);

	stringstream string_format;

	for(auto &p : intermediate_content){
		// intermediate_files[(p.key[0]%4)] << p.key << ' ' << p.val << endl;
		intermediate_file_content[(p.key[0]%4)] += p.key + ' ' + p.val + '\n';
		// intermediate_files[(p.key[0]%4)].write(p.key.c_str(), p.key.size());
		// string space(" ");
		// intermediate_files[(p.key[0]%4)].write(space.c_str(), 1);
		// intermediate_files[(p.key[0]%4)].write(p.val.c_str(), p.val.size());
		// string end_line("\n");
		// intermediate_files[(p.key[0]%4)].write(end_line.c_str(), 1);
	}
	for(int i = 0; i < REDUCER_COUNT; i++){
		string_format.str(string());//clear the content of stringstream
		string_format << this->basedir << "mr-" << index << "-" << i;
		intermediate_files[i].open(string_format.str());
		debug_log(true, "intermediate file name is %s\n", string_format.str().data());
		intermediate_files[i].write(intermediate_file_content[i].c_str(), intermediate_file_content[i].size());
		intermediate_files[i].close();
	}
	// for(auto &p : intermediate_files){
	// 	p.close();
	// }
}

void Worker::doReduce(int index)
{
	// Lab2: Your code goes here.
	vector<string> files_end_in_index;
	string index_str = to_string(index);
	debug_log(true, "index_str is %s\n", index_str.data());

	DIR *dir;
    struct dirent *rent;
    dir = opendir(basedir.c_str());
	while((rent = readdir(dir))){
		string filename(rent->d_name);
		// debug_log(true, "filename is %s\n", filename.data());
		size_t pos = filename.rfind(index_str);
		if(pos != -1 && (pos == filename.size() - index_str.size()) && filename[pos - 1] == '-'){
			debug_log(true, "filename found is %s\n", filename.data())
			files_end_in_index.push_back(filename);
		}
	}
	map<string, vector<string>> intermediate_content;
	for(auto &p : files_end_in_index){
		string if_name = basedir + p;
		debug_log(true, "intermediate file is %s\n", if_name.data());
		ifstream intermediate_file(basedir + p);
		string key, val;
		while(intermediate_file >> key >> val){
			// debug_log(true, "key is %s\tval is %s\n", key.data(), val.data());
			intermediate_content[key].push_back(val);
		}
	}
	string content;
	for(auto &p : intermediate_content){
		string result = reducef(p.first, p.second);
		content += p.first + " " + result + '\n';
		// mr_out_file << p.first << ' ' << result << endl;//use stream is too slow!!!
	}
	// stringstream string_format;
	// string_format << basedir << "mr-out" << index;
	// ofstream mr_out_file(string_format.str());
	string out_filename = basedir + "mr-out" + to_string(index);
	ofstream mr_out_file(out_filename);
	mr_out_file.write(content.data(), content.size());
	mr_out_file.close();
}

void Worker::doSubmit(mr_tasktype taskType, int index)
{
	bool b;
	mr_protocol::status ret = this->cl->call(mr_protocol::submittask, taskType, index, b);
	if (ret != mr_protocol::OK) {
		fprintf(stderr, "submit task failed\n");
		exit(-1);
	}
}

void Worker::doWork()
{
	for (;;) {

		//
		// Lab2: Your code goes here.
		// Hints: send asktask RPC call to coordinator
		// if mr_tasktype::MAP, then doMap and doSubmit
		// if mr_tasktype::REDUCE, then doReduce and doSubmit
		// if mr_tasktype::NONE, meaning currently no work is needed, then sleep
		//
		mr_protocol::AskTaskResponse reply;
		int r;
		cl->call(mr_protocol::asktask, r, reply);
		switch (reply.taskType)
		{
		case MAP:
			doMap(reply.index, reply.filenames);
			doSubmit(MAP, reply.index);
			break;
		case REDUCE:
			doReduce(reply.index);
			doSubmit(REDUCE, reply.index);
			break;
		case NONE:
			sleep(1);
			break;
		default:
			debug_log(false, "unsupproted tast type %u\n", reply.taskType);
			break;
		}
	}
}

int main(int argc, char **argv)
{
	if (argc != 3) {
		fprintf(stderr, "Usage: %s <coordinator_listen_port> <intermediate_file_dir> \n", argv[0]);
		exit(1);
	}

	MAPF mf = Map;
	REDUCEF rf = Reduce;
	
	Worker w(argv[1], argv[2], mf, rf);
	w.doWork();

	return 0;
}

