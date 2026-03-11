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
#pragma once
#include <iostream>
#include <istream>
#include <streambuf>
#include <string>
#include <vector>
#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <arrow/memory_pool.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/controller.h>
#include <ksearch/common/common.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <ksearch/common/lru_cache.h>

namespace ksearch {
    DECLARE_int64(compaction_sst_cache_max_block);

    int get_size_by_external_file_name(uint64_t *size, uint64_t *lines, const std::string &external_file);

    struct AfsStatis {
        explicit AfsStatis(const std::string &cluster_name) : afs_cluster(cluster_name),
                                                              read_time_cost(afs_cluster + "_afs_read_time_cost", 60),
                                                              open_time_cost(afs_cluster + "_afs_open_time_cost", 60),
                                                              close_time_cost(afs_cluster + "_afs_close_time_cost", 60),
                                                              read_bytes_per_second(
                                                                  afs_cluster + "_afs_read_bytes_per_second",
                                                                  &read_bytes),
                                                              write_bytes_per_second(
                                                                  afs_cluster + "_afs_write_bytes_per_second",
                                                                  &write_bytes),
                                                              read_fail_count(afs_cluster + "_afs_read_fail_count"),
                                                              write_fail_count(afs_cluster + "_afs_write_fail_count"),
                                                              reader_open_count(
                                                                  afs_cluster + "_afs_reader_open_count") {
        }

        std::string afs_cluster;
        bvar::Adder<int64_t> read_bytes;
        bvar::Adder<int64_t> write_bytes;
        bvar::LatencyRecorder read_time_cost;
        bvar::LatencyRecorder open_time_cost;
        bvar::LatencyRecorder close_time_cost;
        bvar::PerSecond<bvar::Adder<int64_t> > read_bytes_per_second;
        bvar::PerSecond<bvar::Adder<int64_t> > write_bytes_per_second;
        bvar::Adder<int64_t> read_fail_count;
        bvar::Adder<int64_t> write_fail_count;
        bvar::Adder<int64_t> reader_open_count;
    };

    class ExtFileReader {
    public:
        virtual ~ExtFileReader() {
        }

        virtual int64_t read(char *buf, uint32_t count, uint32_t offset, bool *eof) = 0;

        virtual int64_t skip(uint32_t n, bool *eof) {
            DB_FATAL("ExtFileReader::skip not implemented");
            return -1;
        }

        // Close the descriptor of this file adaptor
        virtual bool close() { return true; }

        virtual std::string file_name() { return ""; }

    protected:
        ExtFileReader() {
        }

    private:
        DISALLOW_COPY_AND_ASSIGN(ExtFileReader);
    };

    class ExtFileWriter {
    public:
        virtual ~ExtFileWriter() {
        }

        // Return |data.size()| if successful, -1 otherwise.
        virtual int64_t append(const char *buf, uint32_t count) = 0;

        virtual int64_t tell() = 0;

        // Sync data of the file to disk device
        virtual bool sync() = 0;

        // Close the descriptor of this file adaptor
        virtual bool close() { return true; }

        virtual std::string file_name() { return ""; }

    protected:
        ExtFileWriter() {
        }

    private:
        DISALLOW_COPY_AND_ASSIGN(ExtFileWriter);
    };

    class CompactionSstCache {
    public:
        static CompactionSstCache *get_instance() {
            static CompactionSstCache _instance;
            return &_instance;
        }

        virtual ~CompactionSstCache() {
        }

        void init(int64_t len_threshold) {
            _cache.init(len_threshold);
        }

        std::string get_info() {
            return _cache.get_info();
        }

        void add(const std::string &key, const std::string &value) {
            _cache.add(key, value);
        }

        int find(const std::string &key, std::string *value) {
            return _cache.find(key, value);
        }

        size_t size() {
            return _cache.size();
        }

    private:
        CompactionSstCache() {
        }

        Cache<std::string, std::string> _cache;
    };


    class CompactionExtFileReader : public ExtFileReader {
    public:
        explicit CompactionExtFileReader(const std::string &file_name, const std::string &server_address,
                                         const std::string &remote_compaction_id)
            : _file_name(file_name),
              _server_address(server_address),
              _remote_compaction_id(remote_compaction_id) {
            brpc::ChannelOptions channel_opt;
            channel_opt.timeout_ms = FLAGS_remote_compaction_request_file_timeout;
            channel_opt.connect_timeout_ms = FLAGS_remote_compaction_connect_timeout;

            if (_channel.Init(server_address.c_str(), &channel_opt) != 0) {
                DB_FATAL("Failed to initialize channel to %s", server_address.c_str());
                _is_open = false;
            } else {
                _is_open = true;
            }
            cache = false;
        }

        virtual ~CompactionExtFileReader() {
            // DB_WARNING("【COMPACTION_DEBUG】close remote_compaction_id: %s, file_name %s",
            //         _remote_compaction_id.c_str(), _file_name.c_str());
            if (_is_open) {
                close();
            }
        }

        virtual int64_t read(char *buf, uint32_t count, uint32_t offset, bool *eof) override;

        virtual int64_t skip(uint32_t n, bool *eof) override;

        virtual bool close() override;

        std::string file_name() { return _file_name; }

    private:
        std::string _file_name;
        brpc::Channel _channel;
        bool _is_open;
        std::string _server_address;
        std::string _remote_compaction_id; // TODO 新建赋值
        int64_t total_time = 0;
        bool cache;
    };

    class CompactionExtFileWriter : public ExtFileWriter {
    public:
        explicit CompactionExtFileWriter(const std::string &file_name, const std::string &server_address,
                                         const std::string &remote_compaction_id)
            : _file_name(file_name),
              _server_address(server_address),
              _remote_compaction_id(remote_compaction_id),
              _offset(0) {
            brpc::ChannelOptions channel_opt;
            channel_opt.timeout_ms = FLAGS_remote_compaction_request_file_timeout;
            channel_opt.connect_timeout_ms = FLAGS_remote_compaction_connect_timeout;

            if (_channel.Init(server_address.c_str(), &channel_opt) != 0) {
                DB_FATAL("Failed to initialize channel to %s", server_address.c_str());
                _is_open = false;
            } else {
                _is_open = true;
            }
        }

        virtual ~CompactionExtFileWriter() {
            if (_is_open) {
                close();
            }
        }

        virtual int64_t append(const char *buf, uint32_t count) override;

        virtual int64_t tell() override;

        virtual bool sync() override;

        virtual bool close() override;

    private:
        std::string _file_name;
        brpc::Channel _channel;
        bool _is_open;
        std::string _server_address;
        std::string _remote_compaction_id;
        int64_t _offset;
    };

    class ExtFileSystem {
    public:
        ExtFileSystem() {
        }

        virtual ~ExtFileSystem() {
        }

        virtual int init() = 0;

        // cluster : 指定集群
        // force   : 是否强制使用指定的cluster
        virtual std::string make_full_name(const std::string &cluster, bool force, const std::string &user_define_path)
        = 0;

        virtual int open_reader(const std::string &full_name, std::shared_ptr<ExtFileReader> *reader) = 0;

        virtual int open_writer(const std::string &full_name, std::unique_ptr<ExtFileWriter> *writer) = 0;

        // Deletes the given path, whether it's a file or a directory. If it's a directory,
        // it's perfectly happy to delete all of the directory's contents. Passing true to
        // recursive deletes subdirectories and their contents as well.
        // Returns true if successful, false otherwise. It is considered successful
        // to attempt to delete a file that does not exist.
        virtual int delete_path(const std::string &full_name, bool recursive) = 0;

        // 创建文件，需要支持递归创建路径上不存在的目录
        virtual int create(const std::string &full_name) = 0;

        // Creates a directory. If create_parent_directories is true, parent directories
        // will be created if not exist, otherwise, the create operation will fail.
        // Returns 'true' on successful creation, or if the directory already exists.
        virtual int create_directory(const std::string &full_name,
                                     bool create_parent_directories) {
            // create接口支持递归创建path中不存在的目录，暂时不使用create_directory
            DB_FATAL("not support");
            return -1;
        }

        // virtual int file_size(const std::string& path, int64_t* size) = 0;

        // Returns -1:failed; 0: not exists; 1: exists
        virtual int path_exists(const std::string &full_name) = 0;

        virtual int readdir(const std::string &full_name, std::set<std::string> &sub_files) = 0;

    private:
        DISALLOW_COPY_AND_ASSIGN(ExtFileSystem);
    };

    class CompactionExtFileSystem : public ExtFileSystem {
    public:
        explicit CompactionExtFileSystem(const std::string &address,
                                         const std::string &remote_compaction_id)
            : _address(address),
              _remote_compaction_id(remote_compaction_id) {
            brpc::ChannelOptions channel_opt;
            channel_opt.timeout_ms = FLAGS_remote_compaction_request_file_timeout;
            channel_opt.connect_timeout_ms = FLAGS_remote_compaction_connect_timeout;

            if (_channel.Init(_address.c_str(), &channel_opt) != 0) {
                DB_FATAL("Failed to initialize channel to %s", _address.c_str());
                _is_open = false;
            } else {
                _is_open = true;
            }
        }

        virtual ~CompactionExtFileSystem() {
        }

        virtual int init() override;

        virtual int open_reader(const std::string &full_name, std::shared_ptr<ExtFileReader> *reader) override;

        virtual int open_writer(const std::string &full_name, std::unique_ptr<ExtFileWriter> *writer) override;

        virtual int delete_path(const std::string &full_name, bool recursive) {
            DB_FATAL("CompactionExtFileSystem not implement delete_path, name: %s", full_name.c_str());
            return -1;
        }

        virtual int create(const std::string &full_name) override;

        virtual int path_exists(const std::string &full_name) override;

        virtual int readdir(const std::string &full_name, std::set<std::string> &file_list) override;

        int get_file_info_list(std::vector<pb::CompactionFileInfo> &file_info_list);

        int rename_file(const std::string &src_file_name, const std::string &dst_file_name);

        std::string make_full_name(const std::string &cluster, bool force, const std::string &user_define_path);

        int external_send_request(pb::CompactionOpType op_type,
                                  const std::string &full_name,
                                  bool recursive,
                                  pb::CompactionFileResponse &response);

        bool is_sst(const std::string &full_name);

        int delete_remote_copy_file_path();

    private:
        std::string _address;
        brpc::Channel _channel;
        bool _is_open;
        std::string _remote_compaction_id;
    };
} // namespace ksearch
