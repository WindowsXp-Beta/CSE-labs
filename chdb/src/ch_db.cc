#include "ch_db.h"

bool put_command(chdb_raft_group* group, int key, int val,int tx_id) {
    auto start = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start + std::chrono::seconds(10)) {
        int leader = group->check_exact_one_leader();
        int term, index;
        chdb_command command(chdb_command::CMD_PUT, key, val,tx_id);
        ASSERT(group->nodes[leader]->new_command(command, term, index), "Leader cannot change");
        {
            std::unique_lock<std::mutex> lock(command.res->mtx);
            if (!command.res->done) {
                if (command.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2500))
                    == std::cv_status::no_timeout) {
                    return true;
                }
            }
        }
    }
    return false;
}

bool get_command(chdb_raft_group* group, int key,int tx_id) {
    auto start = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start + std::chrono::seconds(10)) {
        int leader = group->check_exact_one_leader();
        int term, index;
        chdb_command command(chdb_command::CMD_GET, key, 0, tx_id);
        ASSERT(group->nodes[leader]->new_command(command, term, index), "Leader cannot change");
        {
            std::unique_lock<std::mutex> lock(command.res->mtx);
            if (!command.res->done) {
                if (command.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2500))
                    == std::cv_status::no_timeout) {
                    return true;
                }
            }
        }
    }
    return false;
}

bool none_command(chdb_raft_group* group,int tx_id) {
    auto start = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start + std::chrono::seconds(10)) {
        int leader = group->check_exact_one_leader();
        int term, index;
        chdb_command command(chdb_command::CMD_NONE, 0, 0, tx_id);
        ASSERT(group->nodes[leader]->new_command(command, term, index), "Leader should not change");
        {
            std::unique_lock<std::mutex> lock(command.res->mtx);
            if (!command.res->done) {
                if (command.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2500)) == std::cv_status::no_timeout){
                    return true;
                }
            }
        }
    }
    return false;
}

int view_server::broad_cast(unsigned int proc, const int tx_id, int &r) {
    if (none_command(raft_group, tx_id)) {
        switch (proc) {
            case chdb_protocol::Commit: {
                for (auto shard_rpc_node : node->rpc_clients) {
                    node->call(
                        shard_rpc_node.first, proc,
                        chdb_protocol::commit_var{.tx_id = tx_id}, r
                    );
                }
            } break;
            case chdb_protocol::Rollback: {
                for (auto shard_rpc_node : node->rpc_clients) {
                    node->call(
                        shard_rpc_node.first, proc,
                        chdb_protocol::rollback_var{.tx_id = tx_id}, r
                    );
                }
            } break;
            case chdb_protocol::Prepare: {
                for (auto shard_rpc_node : node->rpc_clients) {
                    node->call(
                        shard_rpc_node.first, proc,
                        chdb_protocol::prepare_var{.tx_id = tx_id}, r
                    );
                    if (r == 0) return chdb_protocol::prepare_not_ok;
                }
            } break;
            default: {
                fprintf(stderr, "Unsupported Operation\n");
                assert(false);
            } break;
        }
        return 1;
    } else {
        fprintf(stderr, "No consensus!\n");
        return -1;
    }
}

int view_server::execute(unsigned int query_key, unsigned int proc, const chdb_protocol::operation_var &var, int &r) {
    // TODO: Your code here
    bool raft_ok = false;
    switch (proc) {
        case chdb_protocol::Get:
            raft_ok = get_command(raft_group, var.key, var.tx_id);
            break;
        case chdb_protocol::Put:
            raft_ok = put_command(raft_group, var.key, var.value, var.tx_id);
            break;
        default:
            fprintf(stderr, "Unsupported Operations!\n");
            assert(false);
    }
    if (!raft_ok) {
        fprintf(stderr, "No consensus!\n");
        return -1;
    }
    int base_port = this->node->port();
    int shard_offset = this->dispatch(query_key, shard_num());

    return this->node->template call(base_port + shard_offset, proc, var, r);
}

view_server::~view_server() {
#if RAFT_GROUP
    delete this->raft_group;
#endif
    delete this->node;
}