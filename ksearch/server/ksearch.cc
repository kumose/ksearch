// Copyright (C) Kumo inc. and its affiliates.
// Copyright (C) Kumo inc. and its affiliates.
// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
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

#include <net/if.h>
#include <sys/ioctl.h>
#include <signal.h>
#include <stdio.h>
#include <string>
#include <gflags/gflags.h>
#include <boost/filesystem.hpp>
//#include <gperftools/malloc_extension.h>
#include <ksearch/common/common.h>
#include <ksearch/protocol/network_server.h>
#include <ksearch/expr/fn_manager.h>
#include <ksearch/common/task_fetcher.h>
#include <ksearch/protocol/task_manager.h>
#include <ksearch/common/schema_factory.h>
#include <ksearch/common/information_schema.h>
#include <ksearch/common/memory_profile.h>
#include <ksearch/expr/arrow_function.h>
#include <ksearch/runtime/arrow_io_excutor.h>
#include <ksearch/exec/arrow_exec_node.h>
#include <ksearch/protocol/db_service.h>
#include <ksearch/common/tso_proxy.h>

namespace ksearch {
    // Signal handlers.
    void handle_exit_signal(int sig) {
        NetworkServer::get_instance()->graceful_shutdown();
    }

    void crash_handler(int sig) {
        NetworkServer::get_instance()->fast_stop();
        NetworkServer::get_instance()->graceful_shutdown();
        signal(sig, SIG_DFL);
        kill(getpid(), sig);
    }
} // namespace ksearch

int main(int argc, char **argv) {
    // Initail signal handlers.
    signal(SIGPIPE, SIG_IGN);
    //signal(SIGSEGV, (sighandler_t)ksearch::crash_handler);
    signal(SIGINT, (sighandler_t) ksearch::handle_exit_signal);
    signal(SIGTERM, (sighandler_t) ksearch::handle_exit_signal);
#ifdef KSEARCH_REVISION
    google::SetVersionString(KSEARCH_REVISION);
    static bvar::Status<std::string> ksearch_version("ksearch_version", "");
    ksearch_version.set_value(KSEARCH_REVISION);
#endif
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
    google::ParseCommandLineFlags(&argc, &argv, true);
    boost::filesystem::path remove_path("init.success");
    boost::filesystem::remove_all(remove_path);
    // Initail log
    if (ksearch::init_log(argv[0]) != 0) {
        fprintf(stderr, "log init failed.");
        return -1;
    }
    DB_NOTICE("ksearch starting");
    //    DB_WARNING("log file load success; GetMemoryReleaseRate:%f", 
    //            MallocExtension::instance()->GetMemoryReleaseRate());

    // init singleton
    ksearch::FunctionManager::instance()->init();
    ksearch::ToSqlFunctionManager::instance()->init();
    if (ksearch::SchemaFactory::get_instance()->init(true, false) != 0) {
        DB_FATAL("SchemaFactory init failed");
        return -1;
    }
    if (ksearch::ArrowFunctionManager::instance()->RegisterAllArrowFunction() != 0) {
        DB_FATAL("RegisterAllArrowFunction failed");
        return -1;
    }
    if (ksearch::ArrowExecNodeManager::RegisterAllArrowExecNode() != 0) {
        DB_FATAL("RegisterAllArrowExecNode failed");
        return -1;
    }
    if (ksearch::GlobalArrowExecutor::init() != 0) {
        DB_FATAL("GlobalArrowExecutor init failed");
        return -1;
    }
    if (ksearch::InformationSchema::get_instance()->init() != 0) {
        DB_FATAL("InformationSchema init failed");
        return -1;
    }
    if (ksearch::MetaServerInteract::get_instance()->init() != 0) {
        DB_FATAL("meta server interact init failed");
        return -1;
    }
    if (ksearch::MetaServerInteract::get_auto_incr_instance()->init() != 0) {
        DB_FATAL("meta server interact init failed");
        return -1;
    }
    if (ksearch::MetaServerInteract::get_tso_instance()->init() != 0) {
        DB_FATAL("meta server interact init failed");
        return -1;
    }
    if (ksearch::TsoProxy::get_instance()->init() != 0) {
        DB_FATAL("tso proxy init failed");
        return -1;
    }
    if (ksearch::TsoFetcher::get_instance()->init() != 0) {
        DB_FATAL("TsoFetcher init failed");
        return -1;
    }
    // 可以没有backup
    if (ksearch::MetaServerInteract::get_backup_instance()->init(true) != 0) {
        DB_FATAL("meta server interact backup init failed");
        return -1;
    }
    if (ksearch::MetaServerInteract::get_backup_instance()->is_inited()) {
        if (ksearch::SchemaFactory::get_backup_instance()->init(true, true) != 0) {
            DB_FATAL("SchemaFactory init failed");
            return -1;
        }
    }

    if (ksearch::TaskManager::get_instance()->init() != 0) {
        DB_FATAL("init task manager error.");
        return -1;
    }
    ksearch::HandleHelper::get_instance()->init();
    ksearch::ShowHelper::get_instance()->init();
    ksearch::MemoryGCHandler::get_instance()->init();
    ksearch::MemTrackerPool::get_instance()->init();
    brpc::Server dummy_server;
    ksearch::DbService *db_server = ksearch::DbService::get_instance();
    if (0 != dummy_server.AddService(db_server, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        DB_FATAL("Fail to Add idonlyeService");
        return -1;
    }
    ksearch::RuntimeState::localhost_address = ksearch::SchemaFactory::get_instance()->get_address();
    butil::EndPoint addr;
    addr.ip = butil::IP_ANY;
    addr.port = ksearch::FLAGS_db_port;
    //启动端口
    if (dummy_server.Start(addr, NULL) != 0) {
        DB_FATAL("Fail to start server");
        return -1;
    }
    DB_NOTICE("dummy_server start");

    if (0 != db_server->init_after_listen()) {
        DB_FATAL("db dummy_server instance init_after_listen fail");
        return -1;
    }

    // Initail server.
    ksearch::NetworkServer *server = ksearch::NetworkServer::get_instance();
    if (!server->init()) {
        DB_FATAL("Failed to initail network server.");
        return 1;
    }
    std::ofstream init_fs("init.success", std::ofstream::out | std::ofstream::trunc);
    if (!server->start()) {
        DB_FATAL("Failed to start server.");
    }
    DB_NOTICE("Server shutdown gracefully.");

    // Stop server.
    server->stop();
    ksearch::MemoryGCHandler::get_instance()->close();
    ksearch::MemTrackerPool::get_instance()->close();
    ksearch::GlobalArrowExecutor::shutdown();

    dummy_server.Stop(0);
    dummy_server.Join();

    DB_NOTICE("Server stopped.");
    return 0;
}