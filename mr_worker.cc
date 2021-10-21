#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>

#include <mutex>
#include <string>
#include <vector>
#include <map>

#include "rpc.h"
#include "mr_protocol.h"
#define DEBUG 0

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
	KeyVal(string key, string val):key(key), val(val){}
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
	getline(ifstream(filename), content, '\0');

	vector<KeyVal> intermediate_content = mapf(filename, content);
	vector<ofstream> intermediate_files(REDUCER_COUNT);

	stringstream string_format;
	for(int i = 0; i < REDUCER_COUNT; i++){
		string_format << this->basedir << "mr-" << index << "-" << i;
		intermediate_files[i].open(string_format.str());
	}
	for(auto &p : intermediate_content){
		intermediate_files[(p.key[0]%4)] << p.key << ' ' << p.val << endl;
	}
	for(auto &p : intermediate_files){
		p.close();
	}
}

void Worker::doReduce(int index)
{
	// Lab2: Your code goes here.
	vector<string> files_end_in_index;
	string index_str = to_string(index);

	DIR *dir;
    struct dirent *rent;
    dir = opendir(basedir.c_str());
	while((rent = readdir(dir))){
		string filename(rent->d_name);
		size_t pos = filename.rfind(index_str);
		if(pos != -1 && (pos == filename.size() - index_str.size() + 1)){
			files_end_in_index.push_back(filename);
		}
	}
	map<string, vector<string>> intermediate_content;
	for(auto &p : files_end_in_index){
		ifstream intermediate_file(basedir + p);
		string key, val;
		char space;
		intermediate_file >> key >> space >> val;
		intermediate_content[key].push_back(val);
	}
	stringstream string_format;
	string_format << "mr-out" << index;
	ofstream mr_out_file(string_format.str());
	for(auto &p : intermediate_content){
		string result = reducef(p.first, p.second);
		mr_out_file << p.first << ' ' << result;
	}
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

