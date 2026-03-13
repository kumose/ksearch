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

#include <string>
#include <fstream>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

#include <brpc/server.h>
#include <gflags/gflags.h>
#include <ksearch/raft/my_raft_log.h>
#include <ksearch/common/common.h>
#include <ksearch/meta_server/meta_util.h>
#include <ksearch/meta_server/meta_server.h>
#include <ksearch/engine/rocks_wrapper.h>
#include <ksearch/common/memory_profile.h>

namespace ksearch {
    DECLARE_int32 (meta_port);
    DECLARE_string (meta_server_bns);
    DECLARE_int32 (meta_replica_number);
}

int main(int argc, char **argv) {
#ifdef KSEARCH_REVISION
    google::SetVersionString(KSEARCH_REVISION);
    static bvar::Status<std::string> ksearch_version("ksearch_version", "");
    ksearch_version.set_value(KSEARCH_REVISION);
#endif
    google::SetCommandLineOption("flagfile", "conf/ksmeta.conf");
    google::ParseCommandLineFlags(&argc, &argv, true);
    boost::filesystem::path remove_path("init.success");
    boost::filesystem::remove_all(remove_path);
    // Initail log
    if (ksearch::init_log(argv[0]) != 0) {
        fprintf(stderr, "log init failed.");
        return -1;
    }
    DB_WARNING("log file load success");

    // 注册自定义的raft log的存储方式
    ksearch::register_myraft_extension();

    //add service
    brpc::Server server;
    butil::EndPoint addr;
    addr.ip = butil::IP_ANY;
    addr.port = ksearch::FLAGS_meta_port;
    //将raft加入到baidu-rpc server中
    if (0 != braft::add_service(&server, addr)) {
        DB_FATAL("Fail to init raft");
        return -1;
    }
    DB_WARNING("add raft to baidu-rpc server success");

    int ret = 0;
    //this step must be before server.Start
    std::vector<braft::PeerId> peers;
    std::vector<std::string> instances;
    bool completely_deploy = false;
    bool use_bns = false;

    //指定的是ip:port的形式
    std::vector<std::string> list_raft_peers;
    boost::split(list_raft_peers, ksearch::FLAGS_meta_server_bns, boost::is_any_of(","));
    for (auto &raft_peer: list_raft_peers) {
        DB_WARNING("raft_peer:%s", raft_peer.c_str());
        braft::PeerId peer(raft_peer);
        peers.push_back(peer);
    }


    ksearch::MetaServer *meta_server = ksearch::MetaServer::get_instance();
    //注册处理meta逻辑的service服务
    if (0 != server.AddService(meta_server, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        DB_FATAL("Fail to Add idonlyeService");
        return -1;
    }
    //启动端口
    if (server.Start(addr, NULL) != 0) {
        DB_FATAL("Fail to start server");
        return -1;
    }
    DB_WARNING("baidu-rpc server start");

    if (meta_server->init(peers) != 0) {
        DB_FATAL("meta server init fail");
        return -1;
    }
    ksearch::MemoryGCHandler::get_instance()->init();
    if (!completely_deploy && use_bns) {
        // 循环等待数据加载成功, ip_list配置区分不了全新/更新部署
        while (!meta_server->have_data()) {
            bthread_usleep(1000 * 1000);
        }
        std::ofstream init_fs("init.success", std::ofstream::out | std::ofstream::trunc);
    }
    DB_WARNING("meta server init success");
    //server.RunUntilAskedToQuit(); 这个方法会先把端口关了，导致丢请求
    while (!brpc::IsAskedToQuit()) {
        bthread_usleep(1000000L);
    }
    DB_WARNING("recevie kill signal, begin to quit");
    meta_server->shutdown_raft();
    meta_server->close();
    ksearch::MemoryGCHandler::get_instance()->close();
    ksearch::RocksWrapper::get_instance()->close();
    DB_WARNING("raft shut down, rocksdb close");
    server.Stop(0);
    server.Join();
    DB_WARNING("meta server quit success");
    return 0;
}