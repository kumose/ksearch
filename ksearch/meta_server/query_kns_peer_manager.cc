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


#include <ksearch/meta_server/query_kns_peer_manager.h>
#include <ksearch/meta_server/kns_manager.h>

namespace ksearch {

    ///
    /// response->set_errmsg("success");
    /// response->set_errcode(pb::SUCCESS);
    /// no need to set, already default setting by caller.
    void QueryKnsPeerManager::get_peer_info(const pb::QueryRequest *request,
                                            pb::QueryResponse *response) {
        auto manager = KnsPeerManager::get_instance();
        BAIDU_SCOPED_LOCK(manager->_peer_mutex);

        if (request->has_kns_peer()) {
            auto idit = manager->_peer_id_map.find(request->kns_peer());

            if (idit == manager->_peer_id_map.end()) {
                response->set_errmsg("peer not exist");
                response->set_errcode(pb::INPUT_PARAM_ERROR);
                return;
            }

            auto id = idit->second;

            auto info = manager->_peer_info_map.find(id);
            if (info == manager->_peer_info_map.end()) {
                response->set_errmsg("peer not exist");
                response->set_errcode(pb::INPUT_PARAM_ERROR);
                return;
            }

            *(response->add_kns_peers()) = info->second;
            return;
        }

        if (!request->has_kns()) {
            for (auto &peer_info: manager->_peer_info_map) {
                *(response->add_kns_peers()) = peer_info.second;
            }
            return;
        }
        std::set<int64_t> ids;
        {
            auto kns_manager = KnsManager::get_instance();
            BAIDU_SCOPED_LOCK(kns_manager->_kns_mutex);
            auto id = kns_manager->_kns_id_map.find(request->kns());
            if (id == kns_manager->_kns_id_map.end()) {
                response->set_errmsg("kns not exist");
                response->set_errcode(pb::INPUT_PARAM_ERROR);
                return;
            }
            auto it = kns_manager->_peer_ids.find(id->second);
            if (it != kns_manager->_peer_ids.end()) {
                ids = it->second;
            }
        }

        for (auto id: ids) {
            auto it = manager->_peer_info_map.find(id);
            if (it == manager->_peer_info_map.end()) {
                continue;
            }
            *(response->add_kns_peers()) = it->second;
        }
    }
} //namespace
