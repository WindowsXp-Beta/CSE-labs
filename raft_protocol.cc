#include "raft_protocol.h"

marshall& operator<<(marshall &m, const request_vote_args& args) {
    // Your code here
    m << args.candidate_index << args.candidate_term
      << args.last_log_index << args.last_log_term;
    return m;

}
unmarshall& operator>>(unmarshall &u, request_vote_args& args) {
    // Your code here
    u >> args.candidate_index >> args.candidate_term 
      >> args.last_log_index >> args.last_log_term;
    return u;
}

marshall& operator<<(marshall &m, const request_vote_reply& reply) {
    // Your code here
    m << reply.voter_index << reply.vote << reply.voter_term;
    return m;
}

unmarshall& operator>>(unmarshall &u, request_vote_reply& reply) {
    // Your code here
    u >> reply.voter_index >> reply.vote >> reply.voter_term;
    return u;
}

marshall& operator<<(marshall &m, const append_entries_reply& reply) {
    // Your code here
    m << reply.term << reply.success << reply.heartbeat << reply.match_index
      << reply.receiver_conflict_index << reply.receiver_conflict_term;
    return m;
}
unmarshall& operator>>(unmarshall &m, append_entries_reply& reply) {
    // Your code here
    m >> reply.term >> reply.success >> reply.heartbeat >> reply.match_index
      >> reply.receiver_conflict_index >> reply.receiver_conflict_term;
    return m;
}


marshall& operator<<(marshall &m, const install_snapshot_reply& reply) {
    // Your code here
    m << reply.term << reply.match_index;
    return m;
}

unmarshall& operator>>(unmarshall &u, install_snapshot_reply& reply) {
    // Your code here
    u >> reply.term >> reply.match_index;
    return u;
}