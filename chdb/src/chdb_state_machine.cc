#include "chdb_state_machine.h"

chdb_command::chdb_command() : chdb_command(CMD_NONE, -1, -1, -1) {
    // TODO: Your code here
}

chdb_command::chdb_command(command_type tp, const int &key, const int &value, const int &tx_id)
        : cmd_tp(tp), key(key), value(value), tx_id(tx_id), res(std::make_shared<result>()) {
    // TODO: Your code here
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

chdb_command::chdb_command(const chdb_command &cmd) :
        cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), tx_id(cmd.tx_id), res(cmd.res) {
    // TODO: Your code here
}


void chdb_command::serialize(char *buf, int size) const {
    // TODO: Your code here
    switch(cmd_tp) {
        case CMD_NONE: 
            buf[0] = '1';
            break;
        case CMD_GET:
            buf[0] = '2';
            break;
        case CMD_PUT:
            buf[0]='3';
            break;
    }
    int tmp_key = key;
    int tmp_value = value;
    int tmp_tx_id = tx_id;
    memcpy(buf + 1, &tmp_key, 4);
    memcpy(buf + 5, &tmp_value, 4);
    memcpy(buf + 9, &tmp_tx_id, 4);
}

void chdb_command::deserialize(const char *buf, int size) {
    // TODO: Your code here
    switch(buf[0])
    {
        case '1':
            cmd_tp = CMD_NONE;
            break;
        case '2':
            cmd_tp = CMD_GET;
            break;
        case '3':
            cmd_tp = CMD_PUT;
            break;
    }
    int tmp_key;
    int tmp_value;
    int tmp_tx_id;
    memcpy(&tmp_key, buf + 1, 4);
    memcpy(&tmp_value, buf + 5, 4);
    memcpy(&tmp_tx_id, buf + 9, 4);
    key = tmp_key;
    value = tmp_value;
    tx_id = tmp_tx_id;
    return;
}

marshall &operator<<(marshall &m, const chdb_command &cmd) {
    // TODO: Your code here
    int tmp = cmd.cmd_tp;
    m << tmp;
    m << cmd.key;
    m << cmd.value;
    m << cmd.tx_id;
    return m;
}

unmarshall &operator>>(unmarshall &u, chdb_command &cmd) {
    // TODO: Your code here
    int tmp;
    u >> tmp;
    u >> cmd.key;
    u >> cmd.value;
    u >> cmd.tx_id;
    cmd.cmd_tp = (chdb_command::command_type) tmp;
    return u;
}

void chdb_state_machine::apply_log(raft_command &cmd) {
    // TODO: Your code here
    chdb_command &kv_cmd = dynamic_cast<chdb_command&>(cmd);
    switch(kv_cmd.cmd_tp) {
        case chdb_command::CMD_NONE:{
            kv_cmd.res->done=true;
        } break;
        case chdb_command::CMD_GET:{
            kv_cmd.res->done=true;
        } break;
        case chdb_command::CMD_PUT:{
            kv_cmd.res->done=true;
        } break;
    }
    kv_cmd.res->done = true;
    kv_cmd.res->cv.notify_all();
    return;
}