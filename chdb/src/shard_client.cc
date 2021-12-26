#include "shard_client.h"


int shard_client::put(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    has_put = true;
    int old_value;
    try {
        old_value = get_store().at(var.key).value;
    } catch(const std::exception& e){
        old_value = -1;
    }
    undo_log[var.tx_id].emplace_back(var.key, value_entry(old_value));
    for (auto &replica : store) {
        replica[var.key] = value_entry(var.value);
    }
    return 0;
}

int shard_client::get(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    try {
        r = get_store().at(var.key).value;
    } catch(const std::exception& e){
        r = -1;
    }
    return 0;
}

int shard_client::commit(chdb_protocol::commit_var var, int &r) {
    // TODO: Your code here
    printf("current tx_id is %d\n", var.tx_id);
    undo_log[var.tx_id].clear();
    has_put = false;
    return 0;
}

int shard_client::rollback(chdb_protocol::rollback_var var, int &r) {
    // TODO: Your code here
    auto &tx_undo_log = undo_log[var.tx_id];
    for (auto undo_item : tx_undo_log) {
        for (auto &replica : store) {
            replica[undo_item.first] = undo_item.second;
        }
    }
    has_put = false;
    return 0;
}

int shard_client::check_prepare_state(chdb_protocol::check_prepare_state_var var, int &r) {
    // TODO: Your code here
    r = active || !has_put;
    return 0;
}

int shard_client::prepare(chdb_protocol::prepare_var var, int &r) {
    // TODO: Your code here
    r = active || !has_put;
    return 0;
}

shard_client::~shard_client() {
    delete node;
}