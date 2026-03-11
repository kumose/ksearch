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

#include <vector>
#include <string>
#include <atomic>
#include <fstream>
#include <unordered_set>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <baidu/rpc/channel.h>
#include <json/json.h>

#include <ksearch/common/meta_server_interact.hpp>

namespace ksearch {
    DECLARE_string (meta_server_bns);

    int64_t get_tso() {
        pb::TsoRequest request;
        pb::TsoResponse response;
        request.set_op_type(pb::OP_GEN_TSO);
        request.set_count(1);
        MetaServerInteract msi;
        msi.init_internal(FLAGS_meta_server_bns);
        //发送请求，收到响应
        if (msi.send_request("tso_service", request, response) == 0) {
            //处理响应
            if (response.errcode() != pb::SUCCESS) {
                std::cout << "faield line:" << __LINE__ << std::endl;
                return -1;
            }
        } else {
            std::cout << "faield line:" << __LINE__ << std::endl;
            return -1;
        }

        int64_t tso_physical = response.start_timestamp().physical();
        int64_t tso_logical = response.start_timestamp().logical();
        std::cout << "physical: " << tso_physical << " logical:" << tso_logical << std::endl;
        return tso_physical << 18 + tso_logical;
    }
} //namespace ksearch

int main(int argc, char **argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    int64_t tso = ksearch::get_tso();
    std::cout << "tso:" << tso << std::endl;

    return 0;
}