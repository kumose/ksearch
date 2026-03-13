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

#pragma once

#include <unordered_map>
#include <set>
#include <mutex>
#include <ksearch/proto/meta_interface.pb.h>
#include <ksearch/meta_server/meta_server.h>
#include <ksearch/meta_server/schema_manager.h>

namespace ksearch {
    class KnsManager {
    public:
        friend class QueryKnsManager;
        friend class QueryKnsPeerManager;

        ~KnsManager() {
            bthread_mutex_destroy(&_kns_mutex);
        }

        static KnsManager *get_instance() {
            static KnsManager instance;
            return &instance;
        }

        //raft串行调用写入类接口, 但与查询接口并发访问
        void create_kns(const pb::MetaManagerRequest &request, braft::Closure *done);

        void drop_kns(const pb::MetaManagerRequest &request, braft::Closure *done);

        void modify_kns(const pb::MetaManagerRequest &request, braft::Closure *done);

        int load_kns_snapshot(const std::string &value);

        void set_max_kns_id(int64_t max_kns_id) {
            BAIDU_SCOPED_LOCK(_kns_mutex);
            _max_kns_id = max_kns_id;
        }

        int64_t get_max_kns_id() {
            BAIDU_SCOPED_LOCK(_kns_mutex);
            return _max_kns_id;
        }

        void set_kns_info(const pb::KnsInfo &kns_info) {
            BAIDU_SCOPED_LOCK(_kns_mutex);
            _kns_id_map[kns_info.kns_name()] = kns_info.kns_id();
            _kns_info_map[kns_info.kns_id()] = kns_info;
        }

        void erase_kns_info(const std::string &kns_name) {
            BAIDU_SCOPED_LOCK(_kns_mutex);
            int64_t kns_id = _kns_id_map[kns_name];
            _kns_id_map.erase(kns_name);
            _kns_info_map.erase(kns_id);
            _peer_ids.erase(kns_id);
        }

        void add_peer_id(int64_t kns_id, int64_t peer_id) {
            BAIDU_SCOPED_LOCK(_kns_mutex);
            _peer_ids[kns_id].insert(peer_id);
        }

        void delete_peer_id(int64_t kns_id, int64_t peer_id) {
            BAIDU_SCOPED_LOCK(_kns_mutex);
            if (_peer_ids.find(kns_id) != _peer_ids.end()) {
                _peer_ids[kns_id].erase(peer_id);
            }
        }

        int64_t get_kns_id(const std::string &kns_name) {
            BAIDU_SCOPED_LOCK(_kns_mutex);
            if (_kns_id_map.find(kns_name) == _kns_id_map.end()) {
                return 0;
            }
            return _kns_id_map[kns_name];
        }


        int get_kns_info(const int64_t &kns_id, pb::KnsInfo &kns_info) {
            BAIDU_SCOPED_LOCK(_kns_mutex);
            if (_kns_info_map.find(kns_id) == _kns_info_map.end()) {
                return -1;
            }
            kns_info = _kns_info_map[kns_id];
            return 0;
        }

        void clear() {
            _kns_id_map.clear();
            _kns_info_map.clear();
            _peer_ids.clear();
        }

    private:
        KnsManager() : _max_kns_id(0) {
            bthread_mutex_init(&_kns_mutex, nullptr);
        }

        static std::string construct_kns_key(int64_t kns_id) {
            std::string kns_key = MetaServer::SCHEMA_IDENTIFY
                                  + MetaServer::KNS_SCHEMA_IDENTIFY;
            kns_key.append((char *) &kns_id, sizeof(int64_t));
            return kns_key;
        }

        static std::string construct_max_kns_id_key() {
            std::string max_kns_id_key = MetaServer::SCHEMA_IDENTIFY
                                         + MetaServer::MAX_ID_SCHEMA_IDENTIFY
                                         + SchemaManager::MAX_KNS_ID_KEY;
            return max_kns_id_key;
        }

    private:
        //std::mutex                                          _kns_mutex;
        bthread_mutex_t _kns_mutex;

        int64_t _max_kns_id;
        // kns name -> id的映射关系
        std::unordered_map<std::string, int64_t> _kns_id_map;
        // ks -> id与info的映射关系
        std::unordered_map<int64_t, pb::KnsInfo> _kns_info_map;
        std::unordered_map<int64_t, std::set<int64_t> > _peer_ids; //only in memory, not in rocksdb
    };
} // namespace ksearch
