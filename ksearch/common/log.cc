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
//

#include <ksearch/common/log.h>

namespace ksearch {

    int init_log(const char *bin_name) {
        if (FLAGS_log_dir.empty()) {
            FLAGS_log_dir = "log";
        }
        ::google::InitGoogleLogging(bin_name);

        /*
        ::google::SetLogDestination(google::GLOG_INFO, "log/task_info_log.");
        ::google::SetLogDestination(google::GLOG_WARNING, "log/task_warning_log.");
        ::google::SetLogDestination(google::GLOG_ERROR, "log/task_error_log.");

        if (FLAGS_servitysinglelog) {
            auto old_logger1 = google::base::GetLogger(google::GLOG_INFO);
            auto my_logger1 = new SingleLogFileObject(old_logger1, google::GLOG_INFO);
            google::base::SetLogger(google::GLOG_INFO, my_logger1);

            auto old_logger2 = google::base::GetLogger(google::GLOG_WARNING);
            auto my_logger2 = new SingleLogFileObject(old_logger2, google::GLOG_WARNING);
            google::base::SetLogger(google::GLOG_WARNING, my_logger2);

            auto old_logger3 = google::base::GetLogger(google::GLOG_ERROR);
            auto my_logger3 = new SingleLogFileObject(old_logger3, google::GLOG_ERROR);
            google::base::SetLogger(google::GLOG_ERROR, my_logger3);
        }*/
        return 0;
    }
}  // namespace ksearch

