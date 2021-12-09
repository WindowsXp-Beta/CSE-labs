#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

class ballotCounter{
    public:
    int term;
    int ballots;
};

template<typename state_machine, typename command>
class raft {

static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");


friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do { \
        auto now = \
        std::chrono::duration_cast<std::chrono::milliseconds>(\
            std::chrono::system_clock::now().time_since_epoch()\
        ).count();\
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while(0);

public:
    raft(
        rpcs* rpc_server,
        std::vector<rpcc*> rpc_clients,
        int idx, 
        raft_storage<command>* storage,
        state_machine* state    
    );
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node. 
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped(). 
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false. 
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx;                     // A big lock to protect the whole data structure
    ThrPool* thread_pool;
    raft_storage<command>* storage;              // To persist the raft log
    state_machine* state;  // The state machine that applies the raft log, e.g. a kv store

    rpcs* rpc_server;               // RPC server to recieve and handle the RPC requests
    std::vector<rpcc*> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                     // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;

    std::thread* background_election;
    std::thread* background_ping;
    std::thread* background_commit;
    std::thread* background_apply;

    // Your code here:
    int vote_for;
    int commit_index;
    int last_applied;
    std::vector<log_entry<command>> log_list;
    
    ballotCounter counter;
    unsigned long last_received_RPC_time;
    unsigned long getTime();

    int snapshot_index;

    int *next_index;
    int *match_index;

    // snapshot
    std::vector<char> last_snapshot;


private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply& reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply& reply);

    int install_snapshot(install_snapshot_args<command> arg, install_snapshot_reply& reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply);

    void send_install_snapshot(int target, install_snapshot_args<command> arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args<command>& arg, const install_snapshot_reply& reply);


private:
    bool is_stopped();
    int num_nodes() {return rpc_clients.size();}

    // background workers    
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:


};

template<typename state_machine, typename command>
unsigned long raft<state_machine, command>::getTime() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
}

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs* server, std::vector<rpcc*> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    storage(storage),
    state(state),   
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    stopped(false),
    role(follower),
    current_term(0),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr)
{
    mtx.lock();
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here: 
    // Do the initialization
    vote_for = -1;
    last_received_RPC_time = getTime();
    last_applied = 0;
    commit_index = 0;
    snapshot_index = 0;
    log_entry<command> tmp;
    tmp.term = 0;
    tmp.index = 0;
    // snapshot
    tmp.logic_index = 0;
    log_list.push_back(tmp);
    next_index = new int[rpc_clients.size()];
    match_index = new int[rpc_clients.size()];
    unsigned long a = getTime();
    storage->recover();
    unsigned long b = getTime();
    RAFT_LOG("recover time:%d", b - a);
    if (storage->recovered) {
        current_term = storage->cterm;
        vote_for = storage->vote_for;
        storage->llist[0].logic_index = 0;
        log_list.assign(storage->llist.begin(), storage->llist.end());
        // snapshot
        if (storage->has_snapshot) {
            RAFT_LOG("recover success with snapshot!");
            state->apply_snapshot(storage->stm_snapshot);
            last_snapshot.assign(storage->stm_snapshot.begin(), storage->stm_snapshot.end());
            log_list[0].logic_index = storage->last_include_index;
            log_list[0].term = storage->last_include_term;
            snapshot_index = storage->last_include_index;
            last_applied = storage->last_include_index;
            commit_index = storage->last_include_index;
        }
        RAFT_LOG("recover success!");
    }
    mtx.unlock();
}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;    
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Your code here:
    
    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);

    bool log_change = false;
    if (role == leader) {
        RAFT_LOG("leader %d new cmd:%d %d", my_id, current_term, log_list.size());
        log_entry<command> tmp;
        tmp.cmd = cmd;
        tmp.index = log_list.size();
        // snapshot
        tmp.logic_index = tmp.index + log_list[0].logic_index;
        tmp.term = current_term;
        log_list.push_back(tmp);
        log_change = true;
    } else {
        // forward the cmd
        return false;
    }
    // snapshot
    index = log_list.size() - 1 + log_list[0].logic_index;
    term = current_term;
    if (log_change) storage->persistlog(log_list, log_list.size() - 2);
    return true;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    auto snapshot = state->snapshot();
    last_snapshot = snapshot;
    std::vector<log_entry<command>> tmp;
    log_entry<command> first_log;
    first_log.index = 0;
    first_log.logic_index = log_list[last_applied - log_list[0].logic_index].logic_index;
    first_log.term = log_list[last_applied - log_list[0].logic_index].term;
    tmp.push_back(first_log);

    for (int i = last_applied + 1; i < log_list.size(); i++) {
        log_list[i].index = i - last_applied;
        tmp.push_back(log_list[i]);
    }
    log_list.assign(tmp.begin(), tmp.end());
    storage->persistsnapshot(snapshot, first_log.logic_index, first_log.term);
    storage->persistlog(log_list, 0);
    return true;
}



/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply& reply) {
    // Your code here:
    bool meta_change = false;
    bool log_change = false;
    RAFT_LOG("received vote request %d", my_id);
    std::unique_lock<std::mutex> lock(mtx);
    if (vote_for != -1 && args.candidate_term == current_term) {
        reply.voter_index = my_id;
        reply.vote = false;
        reply.voter_term = current_term;
        last_received_RPC_time = getTime();
        return 0;
    }
     // snapshot
     if (args.candidate_term < current_term || 
         (args.last_log_term < log_list[log_list.size() - 1].term) ||
          (args.last_log_term == log_list[log_list.size() - 1].term &&
           args.last_log_index < log_list.size() - 1 + log_list[0].logic_index)) {
        if (args.candidate_term > current_term) {
            current_term = args.candidate_term;
            vote_for = -1;
            role = follower;
            meta_change = true;
        }
        reply.voter_index = my_id; 
        reply.vote = false;
        reply.voter_term = current_term;
        if (meta_change) storage->persistmeta(current_term, vote_for);
        return 0;
    }
    else {
        reply.voter_index = my_id;
        reply.vote = true;
        current_term = args.candidate_term;
        vote_for = args.candidate_index;
        
        role = follower;
        reply.voter_term = current_term;
        meta_change = true;
    }
    last_received_RPC_time = getTime();
    RAFT_LOG("%d vote given %d %d", my_id, args.last_log_index, log_list.size() - 1);
    if (meta_change) storage->persistmeta(current_term, vote_for);
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply) {
    // Your code here:
    bool meta_change = false;
    bool log_change = false;
    RAFT_LOG("received vote reply %d", target);
    std::unique_lock<std::mutex> lock(mtx);
    if (role == leader || role == follower) {
        if (reply.voter_term > current_term) {
            current_term = reply.voter_term;
            vote_for = -1;
            role = follower;
            meta_change = true;
        }
        if (meta_change) storage->persistmeta(current_term, vote_for);
        return;
    }
    last_received_RPC_time = getTime();
    if (reply.vote) {
        counter.ballots++;
        if (counter.ballots >= (rpc_clients.size() / 2 + 1)) {
            RAFT_LOG("new leader! %d", my_id);
            role = leader;
            for (int i = 0; i < rpc_clients.size(); i++) {
                // snapshot
                next_index[i] = log_list.size() + log_list[0].logic_index;
                match_index[i] = -1;
            }
        }
    }
    else {
        if (reply.voter_term > current_term) {
            current_term = reply.voter_term;
            vote_for = -1;
            role = follower;
            meta_change = true;
        }
    }
    if (meta_change) storage->persistmeta(current_term, vote_for);
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Your code here:
    bool meta_change = false;
    bool log_change = false;
    std::unique_lock<std::mutex> lock(mtx);
    if (arg.heartbeat) {
        if (arg.term >= current_term) {
            if (arg.term == current_term && arg.leader_commit > commit_index &&
                arg.leader_commit < log_list.size() + log_list[0].logic_index) {
                commit_index = arg.leader_commit;
                RAFT_LOG("%d commit idx %d", my_id, commit_index);
            }
            current_term = arg.term;

            role = follower;
            reply.term = current_term;
            reply.success = true;
            reply.heartbeat = true;
            last_received_RPC_time = getTime();
            meta_change = true;
        } else {
            reply.success = false;
            reply.term = current_term;
            reply.heartbeat = true;
            last_received_RPC_time = getTime();
        }
    } else {
        reply.heartbeat = false;
        if (arg.term >= current_term) {
            current_term = arg.term;
            role = follower;
            meta_change = true;
            // snapshot
            if (arg.prev_log_index <= log_list.size() + log_list[0].logic_index - 1 &&
                arg.prev_log_term == log_list[arg.prev_log_index - log_list[0].logic_index].term) {
                log_list.resize(arg.prev_log_index - log_list[0].logic_index + arg.entry_size + 1);
                for (int i = arg.prev_log_index + 1 - log_list[0].logic_index; 
                    i <= arg.prev_log_index + arg.entry_size - log_list[0].logic_index; i++) {
                if (i < log_list.size()) {
                    log_list[i] = arg.entries[i - arg.prev_log_index - 1 + log_list[0].logic_index];
                } else {
                    log_list.push_back(arg.entries[i - arg.prev_log_index - 1 + log_list[0].logic_index]);
                }
            }
                log_change = true;
                if (arg.leader_commit > commit_index &&
                    arg.leader_commit < log_list.size() + log_list[0].logic_index) {
                    commit_index = arg.leader_commit;
                    RAFT_LOG("%d commit idx %d", my_id, commit_index);
                }

                reply.match_index = arg.prev_log_index + arg.entry_size;
                reply.term = current_term;
                reply.success = true;
                RAFT_LOG("%d receive log communication match_index:%d", my_id, reply.match_index);
            } else {
                // snapshot
                reply.term = current_term;
                reply.success = false;
            }
        } else {
            reply.term = current_term;
            reply.success = false;
        }
    }
    last_received_RPC_time = getTime();
    if (meta_change) storage->persistmeta(current_term, vote_for);
    if (log_change) {
        int tm = arg.prev_log_index;
        if (tm < log_list[0].logic_index) printf("tm:%d logic:%d", tm, log_list[0].logic_index);
        storage->persistlog(log_list, tm - log_list[0].logic_index);
    }
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    bool meta_change = false;
    bool log_change = false;
    last_received_RPC_time = getTime();
    if (role != leader) {
        return;
    }
    if (reply.term > current_term) {
        vote_for = -1;
        current_term = reply.term;
        meta_change = true;
        role = follower;
        if (meta_change) storage->persistmeta(current_term, vote_for);
        return;
    }
    if (reply.heartbeat) {
        if (meta_change) storage->persistmeta(current_term, vote_for);
        return;
    } else {
        if (reply.success) {
            // snapshot
            if (reply.match_index > match_index[target]) match_index[target] = reply.match_index;
            if (next_index[target] < log_list.size() + log_list[0].logic_index) {
                match_index[target] = reply.match_index;
                next_index[target] = match_index[target] + 1;
                if (next_index[target] <= log_list.size() - 1 + log_list[0].logic_index)
                        next_index[target] = log_list.size() - 1 + log_list[0].logic_index;
                else 
                    next_index[target] = log_list.size() + log_list[0].logic_index;
            }
            RAFT_LOG("%d replicate success! %d %d %d", target, match_index[target], next_index[target], log_list.size() + log_list[0].logic_index);
            // snapshot
            for (int i = commit_index + 1 - log_list[0].logic_index; i < log_list.size(); i++) {
                int cnt = 0;
                if (log_list[i].term != current_term) continue;
                for(int j = 0; j < rpc_clients.size(); j++) {
                    if (j == my_id){
                        cnt++;
                        continue;
                    }
                    // snapshot
                    if (match_index[j] >= i + log_list[0].logic_index) cnt++;
                }
                if (cnt >= (rpc_clients.size() / 2) + 1) {
                    RAFT_LOG("leader commit idx %d", commit_index);
                    //add snapshot
                    commit_index = i + log_list[0].logic_index;
                }
                else
                    continue;
            }
        } else
            //add snapshot
            next_index[target]--;
    }
    if (meta_change) storage->persistmeta(current_term, vote_for);
}


template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args<command> args, install_snapshot_reply& reply) {
    // Your code here:
    printf("last include is %d\t", args.last_include_index);
    std::unique_lock<std::mutex> lock(mtx);
    bool meta_change = false;
    bool log_change = false;
    last_received_RPC_time = getTime();
    if (current_term > args.term) {
        reply.term = current_term;
        return 0;
    } else {
        if (args.term > current_term) {
            current_term = args.term;
            role = follower;
            vote_for = -1;
            meta_change = true;
        }
        if (args.last_include_term == log_list[0].term &&
            args.last_include_index == log_list[0].logic_index &&
            log_list.size() >= args.entries.size()) {
            reply.term = current_term;
            reply.match_index = args.last_include_index + args.entries.size() - 1;
            if (meta_change) storage->persistmeta(current_term, vote_for);
            return 0;
        }
        state->apply_snapshot(args.stm_snapshot);
        last_applied = args.last_include_index;
        commit_index = args.last_include_index;
        log_list[0].term = args.last_include_term;
        log_list[0].logic_index = args.last_include_index;
        last_snapshot.assign(args.stm_snapshot.begin(), args.stm_snapshot.end());
        log_list.resize(args.entries.size());
        assert(args.entries.size() != 0);
        for (int i = 0; i < args.entries.size(); i++) log_list[i] = args.entries[i];

        if (meta_change) storage->persistmeta(current_term, vote_for);
        storage->persistsnapshot(args.stm_snapshot, args.last_include_index, args.last_include_term);
        storage->persistlog(log_list, 0);
        reply.term = current_term;
        reply.match_index = args.last_include_index + args.entries.size() - 1;
        RAFT_LOG("%d install snapshot! matcheidx:%d,lastinclude:%d", my_id, reply.match_index, args.last_include_index);
    }
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args<command>& arg, const install_snapshot_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    bool meta_change = false;
    last_received_RPC_time = getTime();
    if (reply.term > current_term) {
        current_term = reply.term;
        role = follower;
        vote_for = -1;
        meta_change = false;
    } else {
        match_index[target] = reply.match_index;
        next_index[target] = log_list.size() - 1 + log_list[0].logic_index;
        if (next_index[target] <= match_index[target])
            next_index[target] = match_index[target] + 1;
        for (int i = commit_index + 1 - log_list[0].logic_index; i < log_list.size(); i++) {
            int cnt = 0;

            if (log_list[i].term != current_term) continue;
            for(int j = 0; j < rpc_clients.size(); j++) {
                if (j == my_id) {
                    cnt++;
                    continue;
                }
                // snapshot
                if (match_index[j] >= i + log_list[0].logic_index) cnt++;
            }
            if (cnt >= (rpc_clients.size() / 2) + 1) {
                RAFT_LOG("leader commit idx %d", commit_index);
                // snapshot
                commit_index = i + log_list[0].logic_index;
            } else 
                continue;
        }
    }
    if (meta_change) storage->persistmeta(current_term, vote_for);

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args<command> arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Check the liveness of the leader.
    // Work for followers and candidates.

    // Hints: You should record the time you received the last RPC.
    //        And in this function, you can compare the current time with it.
    //        For example:
    //        if (current_time - last_received_RPC_time > timeout) start_election();
    //        Actually, the timeout should be different between the follower (e.g. 300-500ms) and the candidate (e.g. 1s).

    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        mtx.lock();
        bool meta_change = false;
        bool log_change = false;
        if ((role == follower && (getTime() - last_received_RPC_time) > (300 + 200 * my_id / rpc_clients.size()))||
            (role == candidate && (getTime() - last_received_RPC_time) > 800 + 200 * my_id / rpc_clients.size())) {
            RAFT_LOG("begin election");
            role = candidate;
            current_term++;
            counter.ballots = 1;
            vote_for = my_id;
            meta_change = true;
            counter.term = current_term;
            last_received_RPC_time = getTime();
            for (int i = 0; i < rpc_clients.size(); i++) {
                if (i == my_id) continue;
                request_vote_args tmp;
                tmp.candidate_index = my_id;
                tmp.candidate_term = current_term;
                // snapshot
                tmp.last_log_index = log_list.size() - 1 + log_list[0].logic_index;
                tmp.last_log_term = log_list[log_list.size() - 1].term;
                thread_pool->addObjJob(this, &raft::send_request_vote, i, tmp);
            }
        }
        
        if (meta_change) storage->persistmeta(current_term, vote_for);
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    // Hints: You should check the leader's last log index and the follower's next log index.        
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        mtx.lock();
        if (role == leader) {
            for (int i = 0; i < rpc_clients.size(); i++) {
                if (i == my_id) continue;
                // snapshot
                if ((log_list.size() + log_list[0].logic_index > next_index[i])||
                    (match_index[i] < static_cast<int>(log_list.size() - 1) + log_list[0].logic_index) && (log_list.size() && log_list[0].logic_index)) {
                    append_entries_args<command> tmp;
                    tmp.term = current_term;
                    tmp.index = my_id;
                    // snapshot
                    if ((next_index[i] - log_list[0].logic_index <= 1 && log_list[0].logic_index) ||
                        (match_index[i] == -1 && log_list[0].logic_index)) {

                        install_snapshot_args<command> arg;
                        arg.term = current_term;
                        arg.leader_id = my_id;
                        arg.last_include_index = log_list[0].logic_index;
                        printf("logic index is %d", log_list[0].logic_index);
                        arg.last_include_term = log_list[0].term;
                        arg.stm_snapshot.assign(last_snapshot.begin(), last_snapshot.end());
                        for (auto log_item : log_list)
                            arg.entries.push_back(log_item);

                        thread_pool->addObjJob(this, &raft::send_install_snapshot, i, arg);
                    } else {
                        tmp.prev_log_index = next_index[i] - 1;
                        tmp.prev_log_term = log_list[next_index[i] - log_list[0].logic_index - 1].term;
                        tmp.heartbeat = false;
                        tmp.leader_commit = commit_index;
                        tmp.entry_size = log_list.size() + log_list[0].logic_index - next_index[i];
                        for (int j = 0; j < tmp.entry_size; j++)
                            tmp.entries.push_back(log_list[next_index[i] - log_list[0].logic_index + j]);
                        
                        thread_pool->addObjJob(this, &raft::send_append_entries, i, tmp);
                    }
                }
            }
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    // Hints: You should check the commit index and the apply index.
    //        Update the apply index and apply the log if commit_index > apply_index


    while (true) {
        if (is_stopped()) return;
        // Your code here:
        mtx.lock();
        if (last_applied < commit_index) {
            for (int i = last_applied + 1; i <= commit_index; i++) {
                // snapshot
                assert(i - log_list[0].logic_index >= 0);
                state->apply_log(log_list[i-log_list[0].logic_index].cmd);
            }
            RAFT_LOG("%d %d applied", last_applied + 1, commit_index);
            last_applied = commit_index;
        }

        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.

    // Only work for the leader.
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        mtx.lock();
         
        if (role == leader) {
            for (int i = 0; i < rpc_clients.size(); i++) {
                if (i == my_id) continue;
                append_entries_args<command> tmp;
                tmp.term = current_term;
                tmp.index = my_id;
                tmp.heartbeat = true;
                tmp.entry_size = 0;
                // snapshot
                tmp.leader_commit = 0;
                if (match_index[i] >= commit_index)
                    tmp.leader_commit = commit_index;
                
                thread_pool->addObjJob(this, &raft::send_append_entries, i, tmp);
            }
            last_received_RPC_time = getTime();
        }
        mtx.unlock();
        
        std::this_thread::sleep_for(std::chrono::milliseconds(150)); // Change the timeout here!
    }    
    return;
}


/******************************************************************

                        Other functions

*******************************************************************/



#endif // raft_h