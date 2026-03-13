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
// #include <string>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <ksearch/common/store_interact.hpp>
#include <ksearch/common/schema_factory.h>
#include <ksearch/engine/external_filesystem.h>
#include <ksearch/engine/rocksdb_filesystem.h>

namespace ksearch {
    DECLARE_string(meta_server_bns);
    DECLARE_string(cold_rocksdb_afs_infos);
    DEFINE_int64(afs_double_read_interval_us, 1000 * 1000LL, "afs_double_read_interval_us");
    DEFINE_bool(afs_open_reader_async_switch, true, "Enable async AFS reader, default: true");
    DEFINE_int64(afs_gc_interval_s, 24 * 3600LL, "default 1 day");
    DEFINE_int64(afs_gc_count, 10, "afs_gc_count");
    DEFINE_int64(afs_gc_delay_days, 30, "afs_gc_delay_days");
    DEFINE_int64(afs_gc_allow_dead_store_count, 3, "afs_gc_allow_dead_store_count");
    DEFINE_bool(afs_gc_enable, false, "Enable AFS garbage collection, default: false");
    DEFINE_bool(need_ext_fs_gc, false, "Need external filesystem garbage collection, default: false");
    DEFINE_int64(compaction_sst_cache_max_block, 8192, "compaction_sst_cache_max_block");

    int get_size_by_external_file_name(uint64_t *size, uint64_t *lines, const std::string &external_file) {
        std::vector<std::string> split_vec;
        boost::split(split_vec, external_file, boost::is_any_of("/"));
        if (split_vec.empty()) {
            DB_FATAL("split %s failed", external_file.c_str());
            return -1;
        }

        std::vector<std::string> vec;
        vec.reserve(4);
        boost::split(vec, split_vec.back(), boost::is_any_of("._"));
        if (vec.empty()) {
            DB_FATAL("split %s failed", external_file.c_str());
            return -1;
        }
        if (vec.back() == "extsst") {
            // olap sst: regionID_lines_size_time.extsst
            if (vec.size() != 5) {
                DB_FATAL("split %s failed", external_file.c_str());
                return -1;
            }
            if (size != nullptr) {
                *size = boost::lexical_cast<uint64_t>(vec[2]);
            }
            if (lines != nullptr) {
                *lines = boost::lexical_cast<uint64_t>(vec[1]);
            }
        } else if (vec.back() == "binlogsst" || vec.back() == "datasst") {
            // backup binlog sst: regionID_startTS_endTS_idx_now()_size_lines.(binlogsst/datasst)
            if (vec.size() != 8) {
                DB_FATAL("split %s failed", external_file.c_str());
                return -1;
            }
            if (size != nullptr) {
                *size = boost::lexical_cast<uint64_t>(vec[5]);
            }
            if (lines != nullptr) {
                *lines = boost::lexical_cast<uint64_t>(vec[6]);
            }
        } else if (vec.back() == "parquet") {
            // parquet: regionID_tableID_startIndex_endIndex_Idx_size_lines.parquet
            if (vec.size() != 8) {
                DB_FATAL("split %s failed", external_file.c_str());
                return -1;
            }
            if (size != nullptr) {
                *size = boost::lexical_cast<uint64_t>(vec[5]);
            }
            if (lines != nullptr) {
                *lines = boost::lexical_cast<uint64_t>(vec[6]);
            }
        } else {
            DB_FATAL("external file: %s with abnormal file type: %s", external_file.c_str(), vec.back().c_str());
            return -1;
        }
        return 0;
    }

    int64_t CompactionExtFileReader::read(char *buf, uint32_t count, uint32_t offset, bool *eof) {
        if (!_is_open) {
            DB_FATAL("File: %s is not open for reading", _file_name.c_str());
            return -1;
        }
        TimeCost time;
        if (_file_name.find(".log") != std::string::npos) {
            *eof = true;
            return 0;
        }
        std::shared_ptr<CompactionSstExtLinkerData> linker_data;
        if (CompactionSstExtLinker::get_instance()->get_linker_data(_remote_compaction_id, linker_data) != 0 ||
            linker_data == nullptr) {
            DB_FATAL("remote compaction id: %s not found", _remote_compaction_id.c_str());
            return -1;
        }

        std::string cache_key = _server_address + "_" + _file_name + "_" + std::to_string(offset) + "_" +
                                std::to_string(count);
        std::string cache_value;
        if (_file_name.find(".sst") != std::string::npos && count < FLAGS_compaction_sst_cache_max_block
            && CompactionSstCache::get_instance()->find(cache_key, &cache_value) == 0) {
            memcpy(buf, cache_value.c_str(), cache_value.size());
            if (count != cache_value.size()) {
                *eof = true;
            }
            linker_data->cache_size++;
            linker_data->read_file_time += time.get_time();
            // cache = true;
            // DB_WARNING("cache remote_compaction_id: %s, cache_key:%s, time:%ld",
            //         _remote_compaction_id.c_str(), cache_key.c_str(), time.get_time());
            return cache_value.size();
        }
        linker_data->not_cache_size++;
        // Prepare the request
        pb::CompactionFileRequest request;
        request.set_remote_compaction_id(_remote_compaction_id);
        request.set_op_type(pb::OP_READ);
        request.set_file_name(_file_name);
        request.set_offset(offset);
        request.set_count(count);

        // Prepare the response and controller
        pb::CompactionFileResponse response;
        brpc::Controller cntl;

        // Call the remote procedure
        pb::StoreService_Stub stub(&_channel);
        stub.query_file_system(&cntl, &request, &response, nullptr);

        if (cntl.Failed()) {
            DB_FATAL("connect with server: %s fail, error:%s, remote_compaction_id:%s, file_name: %s",
                     _server_address.c_str(), cntl.ErrorText().c_str(), _remote_compaction_id.c_str(),
                     _file_name.c_str());
            return -1;
        }

        if (response.errcode() != pb::SUCCESS) {
            DB_WARNING("send read request to %s fail, error:%s, remote_compaction_id:%s, file_name: %s",
                       _server_address.c_str(), pb::ErrCode_Name(response.errcode()).c_str(),
                       _remote_compaction_id.c_str(),
                       _file_name.c_str());
            *eof = true;
            return 0;
        }

        if (_file_name.find(".sst") != std::string::npos
            && linker_data->input_file_set.count(_file_name) == 0
            && count < FLAGS_compaction_sst_cache_max_block) {
            CompactionSstCache::get_instance()->add(cache_key, response.data());
        }
        // Copy the data to the buffer
        memcpy(buf, response.data().c_str(), response.data().size());
        if (count != response.data().size()) {
            *eof = true;
        }
        linker_data->read_file_time += time.get_time();
        // if (linker_data->input_file_set.count(_file_name) == 0) {
        //     DB_WARNING("cache_not remote_compaction_id: %s, cache_key:%s, time:%ld",
        //                 _remote_compaction_id.c_str(), cache_key.c_str(), time.get_time());
        // }
        return response.data().size();
    }

    int64_t CompactionExtFileReader::skip(uint32_t n, bool *eof) {
        // TODO 实现skip功能
        return n;
    }

    bool CompactionExtFileReader::close() {
        _is_open = false;
        return true;
    }

    int64_t CompactionExtFileWriter::append(const char *buf, uint32_t count) {
        if (!_is_open) {
            DB_FATAL("File: %s is not open for writing", _file_name.c_str());
            return -1;
        }
        // Prepare the request
        pb::CompactionFileRequest request;
        request.set_op_type(pb::OP_WRITE);
        request.set_file_name(_file_name);
        request.set_data(std::string(buf, count));
        request.set_remote_compaction_id(_remote_compaction_id);
        request.set_offset(_offset);

        // Prepare the response and controller
        pb::CompactionFileResponse response;
        brpc::Controller cntl;

        // Call the remote procedure
        pb::StoreService_Stub stub(&_channel);
        stub.query_file_system(&cntl, &request, &response, nullptr);

        if (cntl.Failed()) {
            DB_FATAL("connect with server: %s fail, error:%s, remote_compaction_id:%s",
                     _server_address.c_str(), cntl.ErrorText().c_str(), _remote_compaction_id.c_str());
            return -1;
        }
        if (response.errcode() != pb::SUCCESS) {
            DB_FATAL("send read request to %s fail, error:%s, remote_compaction_id:%s",
                     _server_address.c_str(), pb::ErrCode_Name(response.errcode()).c_str(),
                     _remote_compaction_id.c_str());
            return -1;
        }
        _offset += count;
        return count;
    }

    int64_t CompactionExtFileWriter::tell() {
        // Assuming tell functionality is handled remotely
        return _offset;
    }

    bool CompactionExtFileWriter::sync() {
        return true;
    }

    bool CompactionExtFileWriter::close() {
        _is_open = false;
        return true;
    }

    int CompactionExtFileSystem::init() {
        return 0;
    }

    int CompactionExtFileSystem::open_reader(const std::string &full_name, std::shared_ptr<ExtFileReader> *reader) {
        reader->reset(new CompactionExtFileReader(full_name, _address, _remote_compaction_id));
        return 0;
    }

    int CompactionExtFileSystem::open_writer(const std::string &full_name, std::unique_ptr<ExtFileWriter> *writer) {
        writer->reset(new CompactionExtFileWriter(full_name, _address, _remote_compaction_id));
        return 0;
    }

    // -1：失败；0：不存在；大于0：存在
    int CompactionExtFileSystem::path_exists(const std::string &full_name) {
        pb::CompactionFileResponse response;
        if (0 != external_send_request(pb::OP_PATH_EXISTS, full_name, false, response)) {
            return -1;
        }
        return response.file_info_size();
    }

    int CompactionExtFileSystem::create(const std::string &full_name) {
        pb::CompactionFileResponse response;
        if (0 != external_send_request(pb::OP_CREATE_DIR, full_name, true, response)) {
            return -1;
        }
        return 0;
    }

    int CompactionExtFileSystem::get_file_info_list(std::vector<pb::CompactionFileInfo> &file_info_list) {
        pb::CompactionFileResponse response;
        if (0 != external_send_request(pb::OP_GET_FILE_INFO_LIST, "", false, response)) {
            return -1;
        }
        for (const auto &file: response.file_info()) {
            file_info_list.push_back(file);
        }
        return 0;
    }

    int CompactionExtFileSystem::readdir(const std::string &full_name, std::set<std::string> &file_list) {
        pb::CompactionFileResponse response;
        if (0 != external_send_request(pb::OP_READ_DIR, full_name, false, response)) {
            return -1;
        }
        for (auto &file: response.file_info()) {
            file_list.insert(file.file_path());
        }
        return file_list.size();
    }

    int CompactionExtFileSystem::delete_remote_copy_file_path() {
        pb::CompactionFileResponse response;
        if (0 != external_send_request(pb::OP_DELETE_PATH, "", true, response)) {
            return -1;
        }
        return 0;
    }

    int CompactionExtFileSystem::external_send_request(pb::CompactionOpType op_type,
                                                       const std::string &full_name,
                                                       bool recursive,
                                                       pb::CompactionFileResponse &response) {
        pb::CompactionFileRequest request;
        request.set_op_type(op_type);
        request.set_remote_compaction_id(_remote_compaction_id);
        request.set_file_name(full_name);
        request.set_recursive(recursive);

        brpc::Controller cntl;
        pb::StoreService_Stub stub(&_channel);
        stub.query_file_system(&cntl, &request, &response, nullptr);

        if (cntl.Failed()) {
            DB_FATAL("send store fail, error:%s, remote_compaction_id:%s, path:%s",
                     cntl.ErrorText().c_str(), _remote_compaction_id.c_str(), full_name.c_str());
            return -1;
        }
        if (response.errcode() == pb::COMPACTION_FILE_NOT_EXIST) {
            // DB_WARNING("file not exist, remote_compaction_id:%s, path:%s",
            //                 _remote_compaction_id.c_str(), full_name.c_str());
            return -1;
        } else if (response.errcode() != pb::SUCCESS) {
            DB_FATAL("send store fail, error:%s, remote_compaction_id:%s, path:%s",
                     pb::ErrCode_Name(response.errcode()).c_str(), _remote_compaction_id.c_str(), full_name.c_str());
            return -1;
        }
        return 0;
    }

    std::string CompactionExtFileSystem::make_full_name(const std::string &cluster, bool force,
                                                        const std::string &user_define_path) {
        return "";
    }

    int CompactionExtFileSystem::rename_file(const std::string &src_file_name, const std::string &dst_file_name) {
        DB_FATAL("rename_file not support src:%s, dst:%s", src_file_name.c_str(), dst_file_name.c_str());
        return -1;
    }
} // namespace ksearch
