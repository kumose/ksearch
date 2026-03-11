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

#include <ksearch/expr/literal.h>
#include <ksearch/common/mysql_interact.h>

namespace ksearch {
    std::string Literal::to_sql(const std::unordered_map<int32_t, std::string> &slotid_fieldname_map,
                                ks::client::MysqlShortConnection *conn) {
        if (_node_type == pb::NULL_LITERAL) {
            return "NULL";
        }
        std::string res;
        if (_node_type == pb::STRING_LITERAL
            || _node_type == pb::DATE_LITERAL
            || _node_type == pb::DATETIME_LITERAL
            || _node_type == pb::TIME_LITERAL
            || _node_type == pb::TIMESTAMP_LITERAL) {
            res = "\"" + MysqlInteract::mysql_escape_string(conn, _value.get_string()) + "\"";
        } else {
            res = _value.get_string();
        }
        return res;
    }
} // namespace ksearch