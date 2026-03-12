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

#pragma once
#include <ksearch/proto/compaction_interface.pb.h>
#include <brpc/server.h>
#include <ksearch/engine/rocks_wrapper.h>
#include <ksearch/engine/rocksdb_filesystem.h>

namespace ksearch {
    class CompactionServer : public pb::CompactionService {
    public:
        virtual ~CompactionServer() {
        }

        static CompactionServer *get_instance() {
            static CompactionServer _instance;
            return &_instance;
        }

        virtual void do_compaction(google::protobuf::RpcController *controller,
                                   const pb::RemoteCompactionRequest *request,
                                   pb::RemoteCompactionResponse *response,
                                   google::protobuf::Closure *done);

        void set_options_override(const std::string &cf_name,
                                  const pb::RocksdbGFLAGS &rocksdb_gflags,
                                  rocksdb::CompactionServiceOptionsOverride &options_override);

    private:
        CompactionServer() : remote_compaction_total_latency("remote_compaction_total_latency"),
                             remote_compaction_file_read_latency("remote_compaction_file_read_latency"),
                             remote_compaction_copy_file_latency("remote_compaction_copy_file_latency"),
                             remote_compaction_cache_rate("remote_compaction_cache_rate") {
        }

        std::atomic_bool canceled_{false};
        bvar::LatencyRecorder remote_compaction_total_latency;
        bvar::LatencyRecorder remote_compaction_file_read_latency;
        bvar::LatencyRecorder remote_compaction_copy_file_latency;
        bvar::IntRecorder remote_compaction_cache_rate;
    };
} // namespace ksearch