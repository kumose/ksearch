// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
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

#pragma once

#include <ksearch/common/common.h>

namespace ksearch {
    inline std::string get_task_id(const pb::RegionDdlWork &work) {
        return std::to_string(work.table_id()) + "_" + std::to_string(work.region_id());
    }

    inline std::string get_task_id(const pb::DdlWorkInfo &work) {
        return std::to_string(work.table_id());
    }

    template<typename TaskType>
    class TaskFactory : public Singleton<TaskFactory<TaskType> > {
    public:
        int fetch_task(TaskType &task);

        int finish_task(const TaskType &task);

        int process_ddl_work(const TaskType &work);

        int process_heartbeat(const pb::KsHeartBeatResponse &response,
                              const google::protobuf::RepeatedPtrField<TaskType> & (pb::KsHeartBeatResponse::*
                                  method)() const);

        int construct_heartbeat(pb::KsHeartBeatRequest &request, TaskType * (pb::KsHeartBeatRequest::*method)());

    private:
        std::unordered_map<std::string, TaskType> _todo_tasks;
        std::unordered_map<std::string, TaskType> _doing_tasks;
        std::unordered_map<std::string, TaskType> _done_tasks;
        bthread::Mutex _mutex;
    };
}

#include <ksearch/common/task_fetcher.hpp>