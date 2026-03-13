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
#include <ksearch/meta_server/meta_server.h>
#include <ksearch/meta_server/schema_manager.h>
#include <ksearch/proto/meta_interface.pb.h>

namespace ksearch {
    class KnsPeerManager {
    public:
        friend class QueryKnsPeerManager;

        ~KnsPeerManager() {
            bthread_mutex_destroy(&_peer_mutex);
        }

        static KnsPeerManager *get_instance() {
            static KnsPeerManager instance;
            return &instance;
        }

        void create_peer(const pb::MetaManagerRequest &request, braft::Closure *done);

        void drop_peer(const pb::MetaManagerRequest &request, braft::Closure *done);

        void modify_peer(const pb::MetaManagerRequest &request, braft::Closure *done);

        void peer_update(const pb::MetaManagerRequest &request, braft::Closure *done);

        int load_peer_snapshot(const std::string &value);

    public:
        void set_max_peer_id(int64_t max_peer_id) {
            BAIDU_SCOPED_LOCK(_peer_mutex);
            _max_peer_id = max_peer_id;
        }

        int64_t get_max_peer_id() {
            BAIDU_SCOPED_LOCK(_peer_mutex);
            return _max_peer_id;
        }

        void set_peer_info(const pb::KnsPeerInfo &peer_info) {
            BAIDU_SCOPED_LOCK(_peer_mutex);
            std::string peer_name = peer_info.peer_name();
            _peer_id_map[peer_name] = peer_info.peer_id();
            _peer_info_map[peer_info.peer_id()] = peer_info;
        }

        void erase_peer_info(const std::string &peer_name) {
            BAIDU_SCOPED_LOCK(_peer_mutex);
            int64_t peer_id = _peer_id_map[peer_name];
            _peer_id_map.erase(peer_name);
            _peer_info_map.erase(peer_id);
        }

        int64_t get_peer_id(const std::string &peer_name) {
            BAIDU_SCOPED_LOCK(_peer_mutex);
            if (_peer_id_map.find(peer_name) != _peer_id_map.end()) {
                return _peer_id_map[peer_name];
            }
            return 0;
        }

        int get_peer_info(const int64_t &peer_id, pb::KnsPeerInfo &peer_info) {
            BAIDU_SCOPED_LOCK(_peer_mutex);
            if (_peer_info_map.find(peer_id) == _peer_info_map.end()) {
                return -1;
            }
            peer_info = _peer_info_map[peer_id];
            return 0;
        }


        void clear() {
            _peer_id_map.clear();
            _peer_info_map.clear();
        }

    private:
        KnsPeerManager() : _max_peer_id(0) {
            bthread_mutex_init(&_peer_mutex, nullptr);
        }

        static std::string construct_peer_key(int64_t peer_id) {
            std::string peer_key = MetaServer::SCHEMA_IDENTIFY
                                   + MetaServer::KNS_PEER_SCHEMA_IDENTIFY;
            peer_key.append((char *) &peer_id, sizeof(int64_t));
            return peer_key;
        }

        static std::string construct_max_peer_id_key() {
            std::string max_peer_id_key = MetaServer::SCHEMA_IDENTIFY
                                          + MetaServer::MAX_ID_SCHEMA_IDENTIFY
                                          + SchemaManager::MAX_KNS_PEER_ID_KEY;
            return max_peer_id_key;
        }

    private:
        //std::mutex                                          _peer_mutex;
        bthread_mutex_t _peer_mutex;
        int64_t _max_peer_id;
        // peer name 与id 映射关系，name: ip:port
        std::unordered_map<std::string, int64_t> _peer_id_map;
        std::unordered_map<int64_t, pb::KnsPeerInfo> _peer_info_map;
    }; //class
} // namespace
