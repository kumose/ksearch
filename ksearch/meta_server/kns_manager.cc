// Copyright (C) 2026 Kumo inc. and its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include <ksearch/meta_server/kns_manager.h>
#include <ksearch/meta_server/meta_rocksdb.h>
#include <ksearch/meta_server/meta_util.h>

namespace ksearch {
    void KnsManager::create_kns(
        const pb::MetaManagerRequest &request,
        braft::Closure *done) {
        auto &kns_info = const_cast<pb::KnsInfo &>(request.kns_info());
        std::string kns_name = kns_info.kns_name();
        if (_kns_id_map.find(kns_name) != _kns_id_map.end()) {
            DB_WARNING("request kns:%s has been existed", kns_name.c_str());
            if (kns_info.if_exist()) {
                IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "kns already exist");
            } else {
                IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
            }
            return;
        }
        std::vector<std::string> rocksdb_keys;
        std::vector<std::string> rocksdb_values;

        //准备 kns 信息
        int64_t tmp_kns_id = _max_kns_id + 1;
        kns_info.set_kns_id(tmp_kns_id);
        kns_info.set_version(1);
        kns_info.set_status(pb::KNS_NORMAL);

        std::string kns_value;
        if (!kns_info.SerializeToString(&kns_value)) {
            DB_WARNING("request serializeToArray fail, request:%s", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return;
        }
        rocksdb_keys.push_back(construct_kns_key(tmp_kns_id));
        rocksdb_values.push_back(kns_value);

        //持久化分配出去id的信息
        std::string max_kns_id_value;
        max_kns_id_value.append((char *) &tmp_kns_id, sizeof(int64_t));
        rocksdb_keys.push_back(construct_max_kns_id_key());
        rocksdb_values.push_back(max_kns_id_value);

        int ret = MetaRocksdb::get_instance()->put_meta_info(rocksdb_keys, rocksdb_values);
        if (ret < 0) {
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
            return;
        }
        //更新内存值
        set_kns_info(kns_info);
        set_max_kns_id(tmp_kns_id);
        IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        DB_NOTICE("create kns success, request:%s", request.ShortDebugString().c_str());
    }

    void KnsManager::drop_kns(const pb::MetaManagerRequest &request, braft::Closure *done) {
        auto &kns_info = request.kns_info();
        const std::string &kns_name = kns_info.kns_name();
        if (_kns_id_map.find(kns_name) == _kns_id_map.end()) {
            DB_WARNING("request kns:%s not exist", kns_name.c_str());
            if (!kns_info.if_exist()) {
                IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "kns not exist");
            } else {
                IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
            }
            return;
        }

        //判断 kns 下是否存在database，存在则不能删除 kns
        int64_t kns_id = _kns_id_map[kns_name];
        if (!_peer_ids[kns_id].empty()) {
            DB_WARNING("request kns:%s has peer", kns_name.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "kns has table");
            return;
        }

        //持久化删除数据
        std::string kns_key = construct_kns_key(kns_id);

        int ret = MetaRocksdb::get_instance()->delete_meta_info(std::vector<std::string>{kns_key});
        if (ret < 0) {
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
            return;
        }

        //更新内存值
        erase_kns_info(kns_name);
        IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        DB_NOTICE("drop kns success, request:%s", request.ShortDebugString().c_str());
    }

    void KnsManager::modify_kns(const pb::MetaManagerRequest &request, braft::Closure *done) {
        auto &kns_info = request.kns_info();
        const std::string &kns_name = kns_info.kns_name();
        if (_kns_id_map.find(kns_name) == _kns_id_map.end()) {
            DB_WARNING("request kns:%s not exist", kns_name.c_str());
            if (!kns_info.if_exist()) {
                IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "kns not exist");
            } else {
                IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
            }
            return;
        }

        int64_t kns_id = _kns_id_map[kns_name];
        pb::KnsInfo tmp_info;

        tmp_info = _kns_info_map[kns_id];
        if (kns_info.has_status()) {
            tmp_info.set_status(kns_info.status());
        }

        tmp_info.set_version(_kns_info_map[kns_id].version() + 1);

        //持久化新的 kns 信息
        std::string kns_value;
        if (!tmp_info.SerializeToString(&kns_value)) {
            DB_WARNING("request serializeToArray fail, request:%s", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return;
        }

        int ret = MetaRocksdb::get_instance()->put_meta_info(construct_kns_key(kns_id), kns_value);
        if (ret < 0) {
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
            return;
        }

        // update memory

        set_kns_info(tmp_info);
        IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        DB_NOTICE("modify kns success, request:%s", request.ShortDebugString().c_str());
    }

    int KnsManager::load_kns_snapshot(const std::string &value) {
        pb::KnsInfo kns_pb;
        if (!kns_pb.ParseFromString(value)) {
            DB_FATAL("parse from pb fail when load kns snapshot, value: %s", value.c_str());
            return -1;
        }
        DB_WARNING("kns snapshot:%s", kns_pb.ShortDebugString().c_str());
        set_kns_info(kns_pb);
        return 0;
    }
} // namespace ksearch
