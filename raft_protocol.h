#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
   OK,
   RETRY,
   RPCERR,
   NOENT,
   IOERR
};

class request_vote_args {
public:
    // Your code here
    int candidate_index;
    int candidate_term;
    int last_log_index;
    int last_log_term;
};

marshall& operator<<(marshall &m, const request_vote_args& args);
unmarshall& operator>>(unmarshall &u, request_vote_args& args);


class request_vote_reply {
public:
    // Your code here
    int voter_index;
    bool vote;
    int voter_term;
};

marshall& operator<<(marshall &m, const request_vote_reply& reply);
unmarshall& operator>>(unmarshall &u, request_vote_reply& reply);

template<typename command>
class log_entry {
public:
    // Your code here
    int index;
    int term;
    // snapshot
    int logic_index;

    command cmd;
};

template<typename command>
marshall& operator<<(marshall &m, const log_entry<command>& entry) {
    // Your code here
    m << entry.index << entry.term << entry.logic_index << entry.cmd;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, log_entry<command>& entry) {
    // Your code here
    u >> entry.index >> entry.term >> entry.logic_index >> entry.cmd;
    return u;
}

template<typename command>
class append_entries_args {
public:
    // Your code here
    int term;
    int index;
    int prev_log_index;
    int prev_log_term;
    int leader_commit;
    std::vector<log_entry<command>> entries;
    int entry_size;
    bool heartbeat;
};

template<typename command>
marshall& operator<<(marshall &m, const append_entries_args<command>& args) {
    // Your code here
    m << args.term << args.index << args.prev_log_index
      << args.prev_log_term << args.leader_commit << args.entry_size;
    for (auto entry_item : args.entries) {
        m << entry_item;
    }
    m << args.heartbeat;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, append_entries_args<command>& args) {
    // Your code here
    int tmp;
    u >> args.term >> args.index >> args.prev_log_index
      >> args.prev_log_term >> args.leader_commit >> tmp;
    args.entry_size = tmp;
    for (int i = 0; i < tmp; i++) {
        log_entry<command> log;
        u >> log;
        args.entries.push_back(log);
    }
    u >> args.heartbeat;
    return u;
}

class append_entries_reply {
public:
    // Your code here
    int term;
    bool success;
    bool heartbeat;
    int match_index;
    // snapshot
    int receiver_conflict_index;
    int receiver_conflict_term;
};

marshall& operator<<(marshall &m, const append_entries_reply& reply);
unmarshall& operator>>(unmarshall &m, append_entries_reply& reply);

template<typename command>
class install_snapshot_args {
public:
    // Your code here
    int term;
    int leader_id;
    int last_include_index;
    int last_include_term;

    std::vector<char>  stm_snapshot;
    std::vector<log_entry<command>> entries;
};

template<typename command>
marshall& operator<<(marshall &m, const install_snapshot_args<command>& args)
{
    m << args.term << args.leader_id
      << args.last_include_index << args.last_include_term;
    m << static_cast<int>(args.stm_snapshot.size());
    for (auto snapshot_item : args.stm_snapshot) {
        m << snapshot_item;
    }
    m << static_cast<int>(args.entries.size());
    for (auto entry_item : args.entries) {
        m << entry_item;
    }
    return m;
}
template<typename command>
unmarshall& operator>>(unmarshall &m, install_snapshot_args<command>& args)
{
    m >> args.term >> args.leader_id 
      >> args.last_include_index >> args.last_include_term;
    int snapshot_size;
    m >> snapshot_size;
    args.stm_snapshot.resize(snapshot_size);
    for (auto &snapshot_item : args.stm_snapshot) {
        m >> snapshot_item;
    }
    int entries_size;
    m >> entries_size;
    args.entries.resize(entries_size);
    for (auto &entry_item : args.entries) {
        m >> entry_item;
    }
    return m;
}


class install_snapshot_reply {
public:
    // Your code here
    int term;
    int match_index;
};

marshall& operator<<(marshall &m, const install_snapshot_reply& reply);
unmarshall& operator>>(unmarshall &m, install_snapshot_reply& reply);


#endif // raft_protocol_h