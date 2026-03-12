// Copyright (c) 2018-2025 Baidu, Inc. All Rights Reserved.
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

#include <ksearch/common/data_stream_manager.h>

namespace ksearch {
    std::shared_ptr<DataStreamReceiver> DataStreamManager::create_receiver(
        const uint64_t log_id, const uint64_t fragment_instance_id, const int32_t node_id,
        RuntimeState *state, ExchangeReceiverNode *node) {
        ReceiverKey receiver_key = {log_id, fragment_instance_id, node_id};
        std::shared_ptr<DataStreamReceiver> receiver =
                std::make_shared<DataStreamReceiver>(log_id, fragment_instance_id, node_id);
        if (receiver == nullptr) {
            DB_WARNING("receiver is nullptr");
            return nullptr;
        }
        if (receiver->init(state, node) != 0) {
            DB_WARNING("receiver init failed");
            return nullptr;
        }
        BAIDU_SCOPED_LOCK(_mtx);
        _receiver_map[receiver_key] = receiver;
        return receiver;
    }

    std::shared_ptr<DataStreamReceiver> DataStreamManager::get_receiver(
        const uint64_t log_id, const uint64_t fragment_instance_id, const int32_t node_id) {
        ReceiverKey receiver_key = {log_id, fragment_instance_id, node_id};
        BAIDU_SCOPED_LOCK(_mtx);
        if (_receiver_map.find(receiver_key) == _receiver_map.end()) {
            return nullptr;
        }
        return _receiver_map[receiver_key];
    }

    int DataStreamManager::remove_receiver(
        const uint64_t log_id, const uint64_t fragment_instance_id, const int32_t node_id) {
        ReceiverKey receiver_key = {log_id, fragment_instance_id, node_id};
        BAIDU_SCOPED_LOCK(_mtx);
        _receiver_map.erase(receiver_key);
        return 0;
    }
} // namespace ksearch