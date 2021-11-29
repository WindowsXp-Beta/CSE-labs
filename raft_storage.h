#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string& file_dir);
    ~raft_storage();
    // Your code here
    int cterm;
    int vote_for;
    std::vector<log_entry<command>> llist;
    int my_log_size;
    int meta_fd;
    int log_fd;
    int snapshot_fd;

    int last_include_index;
    int last_include_term;
    std::vector<char> stm_snapshot;

    int __lastlogsize;

    bool has_snapshot;
    bool recovered;

    void persistmeta(int &current_term, int &vote_for);
    void persistlog(std::vector<log_entry<command>> log_list, int mIndex);
    void persistsnapshot(std::vector<char> stm_snapshot, int last_include_index, int last_include_term);
    void recover();
private:
    std::mutex mtx;
};

template<typename command>
raft_storage<command>::raft_storage(const std::string& dir){
    // Your code here
    meta_fd = open((dir + "/meta.txt").c_str(), O_CREAT|O_RDWR, S_IRUSR | S_IWUSR);
    log_fd = open((dir + "/log.txt").c_str(), O_CREAT|O_RDWR, S_IRUSR | S_IWUSR);
    // snapshot 
    snapshot_fd = open((dir + "/snapshot.txt").c_str(), O_CREAT|O_RDWR, S_IRUSR | S_IWUSR);

    __lastlogsize = 1;
    if (meta_fd == -1 || log_fd == -1) {
        printf("open failed! %s", strerror(errno));
        exit(0);
    }
}

template<typename command>
raft_storage<command>::~raft_storage() {
   // Your code here
   close(meta_fd);
   close(log_fd);
}

template<typename command>
void raft_storage<command>::persistmeta(int &current_term, int &vote_for) {
    mtx.lock();

    lseek(meta_fd, 0, SEEK_SET);
    void *buf = &current_term;
    write(meta_fd, (char *)buf, sizeof(int));
    buf = &vote_for;
    write(meta_fd, (char *)buf, sizeof(int));

    mtx.unlock();
}

template<typename command>
void raft_storage<command>::persistlog(std::vector<log_entry<command>> log_list, int mIndex) {
    mtx.lock();
    if (mIndex == 0 || __lastlogsize == 1 || __lastlogsize < mIndex + 1) {
        int total_size = 8;
        lseek(log_fd, 4, SEEK_SET);
        int log_list_size = log_list.size();
        __lastlogsize = log_list_size;
        void *buf;
        buf = &log_list_size;
        write(log_fd, (char *)buf, sizeof(int));
        for (int i = 0;i < log_list_size;i++) {
            int one_log_size = 16;
            buf = &log_list[i].index;
            write(log_fd, (char *)buf, sizeof(int));
            buf = &log_list[i].term;
            write(log_fd, (char *)buf, sizeof(int));
            int lsize = log_list[i].cmd.size();
            buf = &lsize;
            write(log_fd, (char *)buf, sizeof(int));
            buf = new char [log_list[i].cmd.size()];
            log_list[i].cmd.serialize((char *)buf, log_list[i].cmd.size());
            write(log_fd, (char *)buf, log_list[i].cmd.size());

            one_log_size += log_list[i].cmd.size();
            buf = &one_log_size;
            write(log_fd, (char *)buf, sizeof(int));

            total_size += one_log_size;
        }
        lseek(log_fd, 0, SEEK_SET);
        buf = &total_size;
        write(log_fd, (char *)buf, sizeof(int));

    } else {
        int log_list_size = log_list.size();
        int nop;
        void *buf = &nop;
        lseek(log_fd, 0, SEEK_SET);
        int lastsize = 0;
        read(log_fd, buf, sizeof(int));
        lastsize = *(int *)buf;

        int last_log_size = 0;
        read(log_fd, buf, sizeof(int));
        last_log_size = *(int *)buf;
        assert(last_log_size == __lastlogsize);
        assert(mIndex < log_list.size());
        lseek(log_fd, lastsize - 4, SEEK_SET);
        int lostsize = 0;
        for (int i = last_log_size - 1; i > mIndex; i--) {
            int logsize = 0;
            read(log_fd, buf, sizeof(int));
            logsize = *(int *)buf;
            lostsize += logsize;
            lseek(log_fd, -(logsize + 4), SEEK_CUR);
        }

        lastsize -= lostsize;
        lseek(log_fd, 4, SEEK_CUR);
        for (int i = mIndex + 1; i < log_list.size(); i++) {
            int one_log_size = 16;
            buf = &log_list[i].index;
            write(log_fd, (char *)buf, sizeof(int));
            buf = &log_list[i].term;
            write(log_fd, (char *)buf, sizeof(int));
            int lsize = log_list[i].cmd.size();
            buf = &lsize;
            write(log_fd, (char *)buf, sizeof(int));
            buf = new char [log_list[i].cmd.size()];
            log_list[i].cmd.serialize((char *)buf, log_list[i].cmd.size());
            write(log_fd, (char *)buf, log_list[i].cmd.size());
            one_log_size += log_list[i].cmd.size();

            buf = &one_log_size;
            write(log_fd, (char *)buf, sizeof(int));

            lastsize += one_log_size;
        }

        lseek(log_fd, 0, SEEK_SET);
        buf = &lastsize;
        write(log_fd, (char *)buf, sizeof(int));
        buf = &log_list_size;
        write(log_fd, (char *)buf, sizeof(int));
        __lastlogsize = log_list_size;
    }
    mtx.unlock();
}

template<typename command>
void raft_storage<command>::persistsnapshot(std::vector<char> stm_snapshot, int  last_include_index, int  last_include_term) {
    mtx.lock();
    int nop = 0;
    void * buf = &nop;
    buf = &last_include_index;
    write(snapshot_fd, (char*)buf, sizeof(int));
    buf = &last_include_term;
    write(snapshot_fd, (char*)buf, sizeof(int));
    int snapshot_size = stm_snapshot.size();
    buf = &snapshot_size;
    write(snapshot_fd, (char*)buf, sizeof(int));
    for (int i = 0; i < snapshot_size; i++) {
        buf = &stm_snapshot[i];
        write(snapshot_fd, (char *)buf, sizeof(char));
    }
    mtx.unlock();
}


template<typename command>
void raft_storage<command>::recover() {
    int tmp;
    void *buf = &tmp;
    lseek(meta_fd, 0, SEEK_SET);
    lseek(log_fd, 4, SEEK_SET);
    if(!read(meta_fd, buf, sizeof(int))) {
        recovered = false;
        return;
    }
    cterm = *((int*) buf);
    read(meta_fd, buf, sizeof(int));
    vote_for = *((int *) buf);

    read(log_fd, buf, sizeof(int));
    my_log_size = *((int *) buf);

    for (int i = 0; i < my_log_size; i++) {
        read(log_fd, buf, sizeof(int));
        log_entry<command> tmp;
        tmp.index = *((int *) buf);
        read(log_fd, buf, sizeof(int));
        tmp.term = *((int *) buf);
        read(log_fd, buf, sizeof(int));
        int lsize = *((int *)  buf);
        buf = new char [lsize];
        read(log_fd, buf, lsize);
        tmp.cmd.deserialize((char *)buf, lsize);

        read(log_fd, buf, sizeof(int));
        llist.push_back(tmp);
    }
    recovered = true;
    has_snapshot = true;
    // snapshot
    lseek(snapshot_fd, 0, SEEK_SET);

    if(!read(snapshot_fd, (char *)buf, sizeof(int))) {
        has_snapshot = false;
        return;
    }

    last_include_index = *(int *)buf;
    read(snapshot_fd, (char *)buf, sizeof(int));
    last_include_term = *(int *)buf;
    read(snapshot_fd, (char *)buf, sizeof(int));
    int snapshot_size = 0;
    snapshot_size = *(int *) buf;
    for (int i = 0; i < snapshot_size; i++) {
        read(snapshot_fd, (char *)buf, sizeof(char));
        char state_s = *(char *)buf;
        stm_snapshot.push_back(state_s);
    }

}

#endif // raft_storage_h