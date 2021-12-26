#include "../src/tx_region.h"

#define DEBUG 0
// flag = true means INFO
// flag = false means ERROR
#define debug_log(flag, ...) do{ \
    if (DEBUG) { \
      if (flag) printf("[INFO]File: %s line: %d: ", __FILE__, __LINE__); \
      else printf("[ERROR]File: %s line: %d: ", __FILE__, __LINE__); \
      printf(__VA_ARGS__); \
      fflush(stdout); \
    } }while(0);

void tx_put_and_check(chdb *store) {
    int r;
    int write_val = 15, key_upper_bound = 10;
    {
        tx_region db_client(store);
        for (int key = 0; key < key_upper_bound; ++key) {
            db_client.put(key, write_val + key);
        }

        for (int key = 0; key < key_upper_bound; ++key) {
            r = db_client.get(key);
            ASSERT(r == write_val + key, "local transaction read bad result");
        }

        ASSERT(db_client.tx_can_commit() == chdb_protocol::prepare_ok, "Transaction should commit");

    }

    {
        tx_region db_client(store);
        for (int key = 0; key < key_upper_bound; ++key) {
            r = db_client.get(key);
            ASSERT(r == write_val + key, "Transaction read bad result");
        }
        ASSERT(db_client.tx_can_commit() == chdb_protocol::prepare_ok, "Transaction should commit");
    }
}

void tx_incr(chdb *store) {
    std::vector<int> keys = {1, 5, 10};

    {
        tx_region db_client(store);

        for (auto &key: keys) {
            db_client.put(key, key << 2);
        }

        for (auto &key: keys) {
            int r = db_client.get(key);
            ASSERT(r == key << 2, "[tx incr] Local transaction read bad");
        }
        ASSERT(db_client.tx_can_commit() == chdb_protocol::prepare_ok, "[tx incr] Transaction should commit");
    }

    {
        tx_region db_client(store);
        for (auto &key: keys) {
            int r = db_client.get(key);
            debug_log(true, "key is %d\tvalue is %d\n", key, r);
            db_client.put(key, ++r);
        }
        ASSERT(db_client.tx_can_commit() == chdb_protocol::prepare_ok, "[tx incr] Transaction should commit");

        for (auto &key: keys) {
            int r = db_client.get(key);
            debug_log(true, "key is %d\tvalue is %d\n", key, r);
            ASSERT(r == (key << 2) + 1, "[tx incr] Local transaction read bad");
        }
        ASSERT(db_client.tx_can_commit() == chdb_protocol::prepare_ok, "[tx incr] Transaction should commit");

    }

    {
        tx_region db_client(store);
        for (auto &key: keys) {
            int r = db_client.get(key);
            ASSERT(r == (key << 2) + 1, "[tx incr] Transaction read bad");
        }
        ASSERT(db_client.tx_can_commit() == chdb_protocol::prepare_ok, "[tx incr] Transaction should commit");

    }
}

static int test_dispatch_target_one(const int key, int shard_num) {
    return 1;
}

static int test_dispatch_in_static_range(const int key, int shard_num) {
    std::vector<int> limits = {10, 20, 40, 60, 100};
    int shard_offset = 1;

    for (auto limit: limits) {
        if (key < limit) {
            break;
        }
        ++shard_offset;
    }
    return shard_offset;
}
