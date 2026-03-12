// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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

#include <ksearch/mem_row/mem_row_descriptor.h>
#include <ksearch/mem_row/mem_row.h>
#include <ksearch/engine/table_iterator.h>
#include <vector>

int main(int argc, char *argv[]) {
    ksearch::MemRowDescriptor *desc = new ksearch::MemRowDescriptor;

    std::vector<ksearch::pb::TupleDescriptor> tuple_desc;
    for (int idx = 0; idx < 10; idx++) {
        ksearch::pb::TupleDescriptor tuple;
        tuple.set_tuple_id(idx);
        tuple.set_table_id(idx);

        for (int jdx = 1; jdx <= 8; ++jdx) {
            ksearch::pb::SlotDescriptor *slot = tuple.add_slots();
            slot->set_slot_id(jdx);
            slot->set_slot_type(ksearch::pb::UINT16);
            slot->set_tuple_id(idx);
        }
        tuple_desc.push_back(tuple);
    }

    if (0 != desc->init(tuple_desc)) {
        DB_WARNING("init failed");
        return -1;
    }

    std::vector<google::protobuf::Message *> messages;
    for (int idx = 0; idx < 10000000; ++idx) {
        auto msg = desc->new_tuple_message(idx % 10);
        messages.push_back(msg);
    }

    DB_WARNING("create message success");

    sleep(20);

    for (auto msg: messages) {
        delete msg;
    }
    delete desc;

    DB_WARNING("delete message success");

    sleep(30);

    return 0;
}