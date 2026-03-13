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


#include <ksearch/meta_server/kns_peer_manager.h>
#include <ksearch/meta_server/meta_util.h>
#include <ksearch/meta_server/meta_rocksdb.h>
#include <ksearch/meta_server/kns_manager.h>
#include  <turbo/times/time.h>

namespace ksearch {
    void KnsPeerManager::create_peer(const pb::MetaManagerRequest &request, braft::Closure *done) {
        //校验合法性
        auto peer_info = request.kns_peer_info();
        std::string kns_name = peer_info.kns_name();
        std::string peer_name = peer_info.peer_name();
        int64_t kns_id = KnsManager::get_instance()->get_kns_id(kns_name);
        if (kns_id == 0) {
            DB_WARNING("request kns:%s not exist", kns_name.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "kns not exist");
            return;
        }
        if (_peer_id_map.find(peer_name) != _peer_id_map.end()) {
            DB_WARNING("request peer:%s already exist", peer_name.c_str());
            IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "peer already exist");
            return;
        }

        std::vector<std::string> rocksdb_keys;
        std::vector<std::string> rocksdb_values;

        //准备peer_info信息
        auto ms_now = turbo::Time::current_milliseconds();
        int64_t tmp_peer_id = _max_peer_id + 1;
        peer_info.set_peer_id(tmp_peer_id);
        peer_info.set_kns_id(kns_id);
        peer_info.set_version(1);
        peer_info.set_status(pb::KNS_PEER_NORMAL);
        peer_info.set_timestamp(ms_now);

        std::string peer_value;
        if (!peer_info.SerializeToString(&peer_value)) {
            DB_WARNING("request serializeToArray fail, request:%s", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return;
        }
        rocksdb_keys.push_back(construct_peer_key(tmp_peer_id));
        rocksdb_values.push_back(peer_value);

        //持久化分配出去peer_id
        std::string max_peer_id_value;
        max_peer_id_value.append((char *) &tmp_peer_id, sizeof(int64_t));
        rocksdb_keys.push_back(construct_max_peer_id_key());
        rocksdb_values.push_back(max_peer_id_value);

        int ret = MetaRocksdb::get_instance()->put_meta_info(rocksdb_keys, rocksdb_values);
        if (ret < 0) {
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
            return;
        }
        //更新内存值
        set_peer_info(peer_info);
        set_max_peer_id(tmp_peer_id);
        KnsManager::get_instance()->add_peer_id(kns_id, tmp_peer_id);
        IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        DB_NOTICE("create peer success, request:%s", request.ShortDebugString().c_str());
    }


    void KnsPeerManager::drop_peer(const pb::MetaManagerRequest &request, braft::Closure *done) {
        //合法性检查
        auto &peer_info = request.kns_peer_info();
        std::string kns_name = peer_info.kns_name();
        std::string peer_name = peer_info.peer_name();
        int64_t kns_id = KnsManager::get_instance()->get_kns_id(kns_name);
        if (kns_id == 0) {
            DB_WARNING("request kns:%s not exist", kns_name.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "kns not exist");
            return;
        }
        if (_peer_id_map.find(peer_name) == _peer_id_map.end()) {
            DB_WARNING("request peer:%s not exist", peer_name.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "peer not exist");
            return;
        }
        int64_t peer_id = _peer_id_map[peer_name];
        
        //持久化数据
        int ret = MetaRocksdb::get_instance()->delete_meta_info(
            std::vector<std::string>{construct_peer_key(peer_id)});
        if (ret < 0) {
            DB_WARNING("drop peer:%s to rocksdb fail", peer_name.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
            return;
        }
        //更新内存中peer信息
        erase_peer_info(peer_name);
        //更新内存中 kns 信息
        KnsManager::get_instance()->delete_peer_id(kns_id, peer_id);
        IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        DB_NOTICE("drop peer success, request:%s", request.ShortDebugString().c_str());
    }

    void KnsPeerManager::modify_peer(const pb::MetaManagerRequest &request, braft::Closure *done) {
        auto &peer_info = request.kns_peer_info();
        std::string kns_name = peer_info.kns_name();
        std::string peer_name = peer_info.peer_name();
        int64_t kns_id = KnsManager::get_instance()->get_kns_id(kns_name);
        if (kns_id == 0) {
            DB_WARNING("request kns:%s not exist", kns_name.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "kns not exist");
            return;
        }
        if (_peer_id_map.find(peer_name) == _peer_id_map.end()) {
            DB_WARNING("request peer:%s not exist", peer_name.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "peer not exist");
            return;
        }
        int64_t peer_id = _peer_id_map[peer_name];

        pb::KnsPeerInfo tmp_peer_info = _peer_info_map[peer_id];
        if (peer_info.has_status()) {
            tmp_peer_info.set_status(peer_info.status());
        }
        tmp_peer_info.set_version(tmp_peer_info.version() + 1);

        std::string peer_value;
        if (!tmp_peer_info.SerializeToString(&peer_value)) {
            DB_WARNING("request serializeToArray fail, request:%s", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return;
        }
        int ret = MetaRocksdb::get_instance()->put_meta_info(construct_peer_key(peer_id), peer_value);
        if (ret < 0) {
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
            return;
        }
        //更新内存值
        set_peer_info(tmp_peer_info);
        IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        DB_NOTICE("modify peer success, request:%s", request.ShortDebugString().c_str());
    }

    void KnsPeerManager::peer_update(const pb::MetaManagerRequest &request, braft::Closure *done) {
        auto &peer_info = request.kns_peer_info();
        std::string kns_name = peer_info.kns_name();
        std::string peer_name = peer_info.peer_name();
        int64_t kns_id = KnsManager::get_instance()->get_kns_id(kns_name);
        if (kns_id == 0) {
            DB_WARNING("request kns:%s not exist", kns_name.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "kns not exist");
            return;
        }
        if (_peer_id_map.find(peer_name) == _peer_id_map.end()) {
            DB_WARNING("request peer:%s not exist", peer_name.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "peer not exist");
            return;
        }
        int64_t peer_id = _peer_id_map[peer_name];

        pb::KnsPeerInfo tmp_peer_info = _peer_info_map[peer_id];
        auto ms_now = turbo::Time::current_milliseconds();
        tmp_peer_info.set_timestamp(ms_now);

        std::string peer_value;
        if (!tmp_peer_info.SerializeToString(&peer_value)) {
            DB_WARNING("request serializeToArray fail, request:%s", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return;
        }
        int ret = MetaRocksdb::get_instance()->put_meta_info(construct_peer_key(peer_id), peer_value);
        if (ret < 0) {
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
            return;
        }
        //更新内存值
        set_peer_info(tmp_peer_info);
        IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        DB_NOTICE("peer update success, request:%s", request.ShortDebugString().c_str());
    }

    int KnsPeerManager::load_peer_snapshot(const std::string &value) {
        pb::KnsPeerInfo peer_pb;
        if (!peer_pb.ParseFromString(value)) {
            DB_FATAL("parse from pb fail when load peer snapshot, key:%s", value.c_str());
            return -1;
        }
        DB_WARNING("peer snapshot:%s", peer_pb.ShortDebugString().c_str());
        set_peer_info(peer_pb);
        //更新内存中 kns 的值
        KnsManager::get_instance()->add_peer_id(
            peer_pb.kns_id(),
            peer_pb.peer_id());
        return 0;
    }

}  // namespace ksearch
