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

#include <ksearch/meta_server/query_kns_manager.h>

namespace ksearch {

    ///
    /// response->set_errmsg("success");
    /// response->set_errcode(pb::SUCCESS);
    /// no need to set, already default setting by caller.
    void QueryKnsManager::get_kns_info(const pb::QueryRequest *request,
                                                   pb::QueryResponse *response) {
        auto manager = KnsManager::get_instance();
        BAIDU_SCOPED_LOCK(manager->_kns_mutex);
        if (!request->has_kns()) {
            for (auto &kns_info: manager->_kns_info_map) {
                *(response->add_kns_infos()) = kns_info.second;
            }
        } else {
            const std::string &kns_name = request->kns();
            if (manager->_kns_id_map.find(kns_name) != manager->_kns_id_map.end()) {
                int64_t id = manager->_kns_id_map[kns_name];
                *(response->add_kns_infos()) = manager->_kns_info_map[id];
            } else {
                response->set_errcode(pb::INPUT_PARAM_ERROR);
                response->set_errmsg("kns not exist");
                DB_FATAL("kns: %s  not exist", kns_name.c_str());
            }
        }
    }
}  // namespace
