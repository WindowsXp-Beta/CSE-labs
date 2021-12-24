#include "tx_region.h"
#define GLOBAL_LOCKx
#define TWO_PL

int tx_region::put(const int key, const int val) {
    // TODO: Your code here
#ifdef TWO_PL
    if (data_mtx.count(key) == 0) {
        data_mtx.emplace(key, db->data_mtx[key]);
    }
#endif
    int r;
    this->db->vserver->execute(
        key, chdb_protocol::Put,
        chdb_protocol::operation_var{
            .tx_id = tx_id,
            .key = key,
            .value = val
        }, r
    );
    return r;
}

int tx_region::get(const int key) {
    // TODO: Your code here
#ifdef TWO_PL
    if (data_mtx.count(key) == 0) {
        data_mtx.emplace(key, db->data_mtx.at(key));
    }
#endif
    int r;
    this->db->vserver->execute(
        key, chdb_protocol::Get,
        chdb_protocol::operation_var{
            .tx_id = tx_id,
            .key = key,
        }, r
    );
    return r;
}

int tx_region::tx_can_commit() {
    // TODO: Your code here
    auto vserver_rpc_node = this->db->vserver->node;
    for (auto shard_rpc_node : vserver_rpc_node->rpc_clients) {
        int r;
        vserver_rpc_node->call(
            shard_rpc_node.first,
            chdb_protocol::Prepare,
            chdb_protocol::prepare_var{.tx_id = tx_id},
            r
        );
        if (r == 0) return chdb_protocol::prepare_not_ok;
    }
    printf("tx[%d] can commit\n", tx_id);
    return chdb_protocol::prepare_ok;
}

int tx_region::tx_begin() {
    // TODO: Your code here
#ifdef GLOBAL_LOCK
    this->db->tx_id_mtx.lock();
#endif
    printf("tx[%d] begin\n", tx_id);
    return 0;
}

int tx_region::tx_commit() {
    // TODO: Your code here
    auto vserver_rpc_node = this->db->vserver->node;
    for (auto shard_rpc_node : vserver_rpc_node->rpc_clients) {
        int r;
        vserver_rpc_node->call(
            shard_rpc_node.first, chdb_protocol::Commit,
            chdb_protocol::commit_var{.tx_id = tx_id}, r
        );
    }
#ifdef GLOBAL_LOCK
    this->db->tx_id_mtx.unlock();
#endif
#ifdef TWO_PL
    data_mtx.clear();
#endif
    printf("tx[%d] commit\n", tx_id);
    return 0;
}

int tx_region::tx_abort() {
    // TODO: Your code here
    auto vserver_rpc_node = this->db->vserver->node;
    for (auto shard_rpc_node : vserver_rpc_node->rpc_clients) {
        int r;
        vserver_rpc_node->call(
            shard_rpc_node.first, chdb_protocol::Rollback,
            chdb_protocol::rollback_var{.tx_id = tx_id}, r
        );
    }
    printf("tx[%d] abort\n", tx_id);
#ifdef GLOBAL_LOCK
    this->db->tx_id_mtx.unlock();
#endif
#ifdef TWO_PL
    data_mtx.clear();
#endif
    return 0;
}
