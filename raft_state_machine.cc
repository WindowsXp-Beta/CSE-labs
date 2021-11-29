#include "raft_state_machine.h"


kv_command::kv_command() : kv_command(CMD_NONE, "", "") { }

kv_command::kv_command(command_type tp, const std::string &key, const std::string &value) : 
    cmd_tp(tp), key(key), value(value), res(std::make_shared<result>())
{
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

kv_command::kv_command(const kv_command &cmd) :
    cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), res(cmd.res) {}

kv_command::~kv_command() { }

int kv_command::size() const {
    // Your code here:
    return 1 + key.length() + value.length() + 8;
}


void kv_command::serialize(char* buf, int size) const {
    // Your code here:
    switch (cmd_tp) {
        case CMD_NONE:
            buf[0] = '1';
            break;
        case CMD_GET:
            buf[0] = '2';
            break;
        case CMD_PUT:
            buf[0] = '3';
            break;
        case CMD_DEL:
            buf[0] = '4';
            break;
    }
    int key_size = key.size();
    int value_size = value.size();
    memcpy(buf + 1, &key_size, 4);
    memcpy(buf + 5, key.c_str(), key_size);
    memcpy(buf + 5 + key_size, &value_size, 4);
    memcpy(buf + 9 + key_size, value.c_str(), value_size);
    return;
}

void kv_command::deserialize(const char* buf, int size) {
    // Your code here:
    switch (buf[0]) {
        case '1':
            cmd_tp = CMD_NONE;
            break;
        case '2':
            cmd_tp=CMD_GET;
            break;
        case '3':
            cmd_tp = CMD_PUT;
            break;
        case '4':
            cmd_tp = CMD_DEL;
            break;
    }
    int key_size;
    int value_size;
    memcpy(&key_size, buf + 1, 4);
    key.resize(key_size);
    memcpy(&key[0], buf + 5, key_size);
    memcpy(&value_size, buf + 5 + key_size, 4);
    value.resize(value_size);
    memcpy(&value[0], buf + 9 + key_size, value_size);
    return;
}

marshall& operator<<(marshall &m, const kv_command& cmd) {
    // Your code here:
    m << static_cast<int>(cmd.cmd_tp) << cmd.key << cmd.value;
    return m;
}

unmarshall& operator>>(unmarshall &u, kv_command& cmd) {
    // Your code here:
    int enum2int;
    u >> enum2int >> cmd.key >> cmd.value;
    cmd.cmd_tp = static_cast<kv_command::command_type>(enum2int);
    return u;
}

kv_state_machine::~kv_state_machine() {

}

void kv_state_machine::apply_log(raft_command &cmd) {
    kv_command &kv_cmd = dynamic_cast<kv_command&>(cmd);
    std::unique_lock<std::mutex> lock(kv_cmd.res->mtx);
    // Your code here:
    switch (kv_cmd.cmd_tp) {
        case kv_command::CMD_NONE:
            kv_cmd.res -> succ = true;
            return;
        case kv_command::CMD_GET: {
            auto get_iterator = data_storage.find(kv_cmd.key);
            if (get_iterator == data_storage.end()) {
                kv_cmd.res->succ = false;
                kv_cmd.res->key = kv_cmd.key;
                kv_cmd.res->value = std::string();
            } else {
                kv_cmd.res->succ = true;
                kv_cmd.res->key = kv_cmd.key;
                kv_cmd.res->value = get_iterator->second;
            }
            break;
        }
        case kv_command::CMD_PUT: {
            auto put_iterator = data_storage.find(kv_cmd.key);
            if (put_iterator == data_storage.end()) {
                kv_cmd.res->succ = true;
                kv_cmd.res->key = kv_cmd.key;
                kv_cmd.res->value = kv_cmd.value;
                data_storage.insert(std::pair<std::string, std::string>(kv_cmd.key, kv_cmd.value));
            } else {
                kv_cmd.res->succ = false;
                kv_cmd.res->key = kv_cmd.key;
                kv_cmd.res->value = put_iterator->second;
                data_storage.insert(std::pair<std::string, std::string>(kv_cmd.key, kv_cmd.value));
            }
            break;
        }
        case kv_command::CMD_DEL: {
            auto del_iterator = data_storage.find(kv_cmd.key);
            if (del_iterator == data_storage.end()) {
                kv_cmd.res->succ = false;
                kv_cmd.res->key = kv_cmd.key;
                kv_cmd.res->value = std::string();
            } else {
                kv_cmd.res->succ = true;
                kv_cmd.res->key = kv_cmd.key;
                kv_cmd.res->value = del_iterator->second;
                data_storage.erase(kv_cmd.res->key);
            }
            break;
        }
    }
    kv_cmd.res->done = true;
    kv_cmd.res->cv.notify_all();
    return;
}

std::vector<char> kv_state_machine::snapshot() {
    // Your code here:
    std::vector<char> snapshot;
    for (auto data_storage_item : data_storage) {
        std::string key = data_storage_item.first;
        std::string value = data_storage_item.second;
        for (auto key_char : key) snapshot.push_back(key_char);
        // use # as delimeter
        snapshot.push_back('#');
        for (auto value_char : value) snapshot.push_back(value_char);
        snapshot.push_back('#');
    }
    return std::vector<char>();
}

void kv_state_machine::apply_snapshot(const std::vector<char>& snapshot) {
    // Your code here:
    data_storage.clear();
    int count = 0;
    int index = 0;
    int snapshot_size = snapshot.size();
    while (index < snapshot_size) {
        std::string key;
        std::string value;
        while (snapshot[index] != '#') {
            key += snapshot[index];
            index++;
        }
        index++;
        while (snapshot[index] != '#') {
            value += snapshot[index];
            index++;
        }
        index++;
        data_storage.insert(std::pair<std::string, std::string>(key, value));
    }
    return;
}
