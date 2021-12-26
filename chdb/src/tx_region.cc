#include "tx_region.h"
#include <thread>
#include <chrono>
#define GLOBAL_LOCKx
#define TWO_PL

operation::~operation() {}

int tx_region::put(const int key, const int val) {
    // TODO: Your code here
START:
#ifdef TWO_PL
    if (data_mtx.count(key) == 0) {
        // begin to get the mutex
        db->tx_id_mtx.lock();
        // while (db->data_tx_id[key] > tx_id) {
        //     db->tx_id_mtx.unlock();
        //     std::this_thread::sleep_for(std::chrono::milliseconds(20));
        //     db->tx_id_mtx.lock();
        // }
        // if (db->data_mtx[key])
        // db->tx_id_mtx.unlock();
        // retry();

        if (db->data_tx_id.count(key) != 0) {
            int owner_tx = db->data_tx_id.at(key);
            if (tx_id < owner_tx) {
                // can wait
                db->tx_id_mtx.unlock();
                data_mtx.emplace(key, db->data_mtx[key]);
            } else {
                db->tx_id_mtx.unlock();
                retry();
                goto START;
            }
        } else {
            db->data_tx_id[key] = tx_id;
            db->tx_id_mtx.unlock();
            data_mtx.emplace(key, db->data_mtx[key]);
        }
    }
    retry_log[tx_id].push_back(new put_operation(key, val));
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
START:
#ifdef TWO_PL
    if (data_mtx.count(key) == 0) {
        // begin to get the mutex
        db->tx_id_mtx.lock();
        if (db->data_tx_id.count(key) != 0) {
            int owner_tx = db->data_tx_id.at(key);
            if (tx_id < owner_tx) {
                // can wait
                db->tx_id_mtx.unlock();
                data_mtx.emplace(key, db->data_mtx[key]);
            } else {
                db->tx_id_mtx.unlock();
                retry();
                goto START;
            }
        } else {
            db->data_tx_id[key] = tx_id;
            db->tx_id_mtx.unlock();
            data_mtx.emplace(key, db->data_mtx[key]);
        }
    }
    retry_log[tx_id].push_back(new get_operation(key));
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
    int r;
    this->db->vserver->broad_cast(
        chdb_protocol::Prepare,
        tx_id, r
    );
    printf("tx[%d] can commit\n", tx_id);
    return static_cast<chdb_protocol::prepare_state>(r);
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
    int r;
    this->db->vserver->broad_cast(
        chdb_protocol::Commit,
        tx_id, r
    );
#ifdef GLOBAL_LOCK
    this->db->tx_id_mtx.unlock();
#endif
#ifdef TWO_PL
    clear();
#endif
    printf("tx[%d] commit\n", tx_id);
    return 0;
}

int tx_region::tx_abort() {
    // TODO: Your code here
    int r;
    this->db->vserver->broad_cast(
        chdb_protocol::Rollback,
        tx_id, r
    );
    printf("tx[%d] abort\n", tx_id);
#ifdef GLOBAL_LOCK
    this->db->tx_id_mtx.unlock();
#endif
#ifdef TWO_PL
    clear();
#endif
    return 0;
}

void tx_region::retry() {
#ifdef TWO_PL
    tx_abort();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    for (auto oper : retry_log[tx_id]) {
        if (typeid(*oper) == typeid(put_operation)) {
            auto put_oper = static_cast<put_operation *>(oper);
            put(put_oper->key_, put_oper->val_);
        } else if (typeid(*oper) == typeid(get_operation)) {
            auto get_oper = static_cast<get_operation *>(oper);
            get(get_oper->key_);
        } else {
            assert(false);
        }
    }
#endif
}

void tx_region::clear() {
#ifdef TWO_PL
    db->tx_id_mtx.lock();
    for (auto &key : data_mtx) {
        db->data_tx_id.erase(key.first);
    }
    db->tx_id_mtx.unlock();
    data_mtx.clear();
    for (auto oper : retry_log[tx_id]) {
        delete oper;
    }
    retry_log.erase(tx_id);
#endif
}