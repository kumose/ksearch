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

#include <ksearch/common/mysql_interact.h>
#include <ksearch/common/common.h>
#include <boost/algorithm/string.hpp>

namespace ksearch {
    DEFINE_int32(mysql_retry_times, 1, "mysql query retry times");
    DEFINE_int32(mysql_connect_timeout_s, 10, "mysql connect timeout");
    DEFINE_int32(mysql_read_timeout_s, 60, "mysql read timeout");
    DEFINE_int32(mysql_write_timeout_s, 60, "mysql write timeout");
    DEFINE_int32(mysql_no_permission_wait_s, 60, "mysql wait time for data source no permission when connecting ");

    int MysqlInteract::query(const std::string &sql, ks::client::ResultSet *result, bool store) {
        int ret = -1;
        int retry_times = 0;
        if (_mysql_conn == nullptr) {
            _mysql_conn = fetch_connection();
        }
        do {
            // 重试时，重新获取连接
            if (retry_times > 0 && ret != 0) {
                bthread_usleep(1000000);
                _mysql_conn = fetch_connection();
            }
            if (_mysql_conn == nullptr) {
                continue;
            }
            ret = _mysql_conn->execute(sql, store, result);
        } while (++retry_times < FLAGS_mysql_retry_times && ret != 0);
        if (ret != 0) {
            int error_code = -1;
            std::string error_des;
            if (_mysql_conn != nullptr) {
                _mysql_conn->get_error_code(&error_code);
                error_des = _mysql_conn->get_error_des();
            }
            DB_WARNING("Fail to query, ret: %d, error_code: %d, error_des: %s, mysql_info: %s, sql: %s",
                       ret, error_code, error_des.c_str(), _mysql_info.ShortDebugString().c_str(), sql.c_str());
            return -1;
        }
        return 0;
    }

    int MysqlInteract::get_error_code() {
        if (_mysql_conn == nullptr) {
            DB_WARNING("_mysql_conn is nullptr");
            return -1;
        }
        int error_code = 0;
        _mysql_conn->get_error_code(&error_code);
        return error_code;
    }

    ks::client::MysqlShortConnection *MysqlInteract::get_connection() {
        if (_mysql_conn == nullptr) {
            _mysql_conn = fetch_connection();
        }
        return _mysql_conn.get();
    }

    std::string MysqlInteract::mysql_escape_string(ks::client::MysqlShortConnection *conn, const std::string &value) {
        char *str = new char[value.size() * 2 + 1];
        std::string escape_value;
        if (conn != nullptr) {
            MYSQL *RES = conn->get_mysql_handle();
            escape_value = str;
        } else {
            delete[] str;
            return boost::replace_all_copy(value, "\"", "\\\"");
        }
        delete[] str;
        return escape_value;
    }

    std::unique_ptr<ks::client::MysqlShortConnection> MysqlInteract::fetch_connection() {
        std::unique_ptr<ks::client::MysqlShortConnection> conn(new ks::client::MysqlShortConnection());
        if (conn == nullptr) {
            DB_WARNING("Fail to new MysqlShortConnection");
            return nullptr;
        }
        return conn;
    }
} // namespace ksearch