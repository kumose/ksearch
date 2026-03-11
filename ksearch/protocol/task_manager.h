// Copyright (C) 2026 Kumo inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2018-2025 Baidu, Inc. All Rights Reserved.
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

#include <ksearch/common/common.h>
#include <ksearch/common/task_fetcher.h>

namespace ksearch {
    DECLARE_int32(worker_number);

    class TaskManager : public Singleton<TaskManager> {
    public:
        int init();

        void fetch_thread();

        void process_ddl_work(pb::RegionDdlWork work);

        void process_txn_ddl_work(pb::DdlWorkInfo work);

    private:
        ConcurrencyBthread _workers{FLAGS_worker_number};
    };
} // namespace ksearch
