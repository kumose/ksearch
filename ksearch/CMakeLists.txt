# Copyright (C) Kumo inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
###########################################################################
# below are examples, after initialization, modify it to your own.
###########################################################################
###kmcmake_example

add_definitions(-DGLOG_USE_GLOG_EXPORT)

set(PROTO_FILES
        proto/binlog.proto
        proto/column.proto
        proto/common.proto
        proto/compaction_interface.proto
        proto/console.proto
        proto/db_interface.proto
        proto/dms.proto
        proto/expr.proto
        proto/fc.proto
        proto/meta_interface.proto
        proto/olap.proto
        proto/optype.proto
        proto/plan.proto
        proto/raft.proto
        proto/reverse.proto
        proto/statistics.proto
        proto/store_interface.proto
        proto/test_decode.proto
)

kmcmake_cc_proto(
        NAME proto_obj
        PROTOS
        ${PROTO_FILES}
        OUTDIR
        ${PROJECT_SOURCE_DIR}
)

kmcmake_cc_object(
        NAMESPACE ksearch
        NAME proto_obj
        SOURCES
        ${proto_obj_SRCS}
        CXXOPTS
        ${KMCMAKE_CXX_OPTIONS}
)

bison_target(
        TypeCalculationParser ${PROJECT_SOURCE_DIR}/ksearch/sqlparser/sql_parse.y
        ${PROJECT_SOURCE_DIR}/ksearch/sqlparser/sql_parse.yacc.cc
        DEFINES_FILE  ${PROJECT_SOURCE_DIR}/ksearch/sqlparser/sql_parse.yacc.hh
        COMPILE_FLAGS "")

flex_target(
        TypeCalculationScanner ${PROJECT_SOURCE_DIR}/ksearch/sqlparser/sql_lex.l
        ${PROJECT_SOURCE_DIR}/ksearch/sqlparser/sql_lex.flex.cc
        DEFINES_FILE ${PROJECT_SOURCE_DIR}/ksearch/sqlparser/sql_lex.flex.h
        COMPILE_FLAGS "")

add_flex_bison_dependency(TypeCalculationScanner TypeCalculationParser)

kmcmake_cc_object(
        NAMESPACE ksearch
        NAME parse_obj
        SOURCES
        ${BISON_TypeCalculationParser_OUTPUTS}
        ${FLEX_TypeCalculationScanner_OUTPUTS}
        CXXOPTS
        ${KMCMAKE_CXX_OPTIONS}
)


set(LIB_SRC
        #####################################################
        # common
        common/backup_stream.cc
        common/ks_heartbeat.cc
        common/common.cc
        common/data_stream_manager.cc
        common/datetime.cc
        common/default_room_define.cc
        common/expr_value.cc
        common/file_system.cc
        common/heartbeat_interval_define.cc
        common/histogram.cc
        common/hll_common.cc
        common/information_schema.cc
        common/log.cc
        common/memory_profile.cc
        common/meta_server_interact.cc
        common/mut_table_key.cc
        common/mysql_interact.cc
        common/password.cc
        common/proto_process.cc
        common/schema_factory.cc
        common/store_interact.cc
        common/statistics.cc
        common/table_key.cc
        common/table_record.cc
        common/tdigest.cc
        common/tso_proxy.cc
        common/tuple_record.cc
        #####################################################
        # engine
        engine/external_filesystem.cc
        engine/my_rocksdb.cc
        engine/olap_pre_split.cc
        engine/qos.cc
        engine/rocksdb_compaction_service.cc
        engine/rocksdb_filesystem.cc
        engine/rocksdb_merge_operator.cc
        engine/rocks_wrapper.cc
        engine/table_iterator.cc
        engine/transaction.cc
        engine/transaction_db_bthread_mutex.cc
        engine/transaction_pool.cc
        #####################################################
        # column
        column/column_record.cc
        column/file_manager.cc
        column/parquet_cache.cc
        column/parquet_writer.cc
        column/region_column.cc
        column/row2column.cc
        column/sort_merge.cc
        #####################################################
        # exec
        exec/access_path.cc
        exec/acero_base_node.cc
        exec/agg_node.cc
        exec/apply_node.cc
        exec/arrow_exec_node.cc
        exec/delete_manager_node.cc
        exec/delete_node.cc
        exec/dml_manager_node.cc
        exec/dml_node.cc
        exec/dual_scan_node.cc
        exec/exchange_receiver_node.cc
        exec/exchange_sender_node.cc
        exec/exec_node.cc
        exec/fetcher_store.cc
        exec/file_scan_node.cc
        exec/filter_node.cc
        exec/full_export_node.cc
        exec/index_ddl_manager_node.cc
        exec/insert_manager_node.cc
        exec/insert_node.cc
        exec/joiner.cc
        exec/join_node.cc
        exec/kill_node.cc
        exec/limit_node.cc
        exec/load_node.cc
        exec/lock_primary_node.cc
        exec/lock_secondary_node.cc
        exec/mysql_scan_node.cc
        exec/packet_node.cc
        exec/parquet_scan_node.cc
        exec/redis_scan_node.cc
        exec/rocksdb_scan_node.cc
        exec/scan_node.cc
        exec/select_manager_node.cc
        exec/single_txn_manager_node.cc
        exec/sort_node.cc
        exec/transaction_manager_node.cc
        exec/transaction_node.cc
        exec/truncate_node.cc
        exec/ttl_delete_node.cc
        exec/union_node.cc
        exec/update_manager_node.cc
        exec/update_node.cc
        exec/window_node.cc
        expr/agg_fn_call.cc
        #####################################################
        # expr
        expr/arrow_agg_function.cc
        expr/arrow_function.cc
        expr/arrow_string_function.cc
        expr/arrow_time_function.cc
        expr/expr_node.cc
        expr/fn_manager.cc
        expr/internal_functions.cc
        expr/literal.cc
        expr/operators.cc
        expr/predicate.cc
        expr/scalar_fn_call.cc
        expr/window_fn_call.cc
        #####################################################
        # logical_plan
        logical_plan/ddl_planner.cc
        logical_plan/ddl_work_planner.cc
        logical_plan/delete_planner.cc
        logical_plan/insert_planner.cc
        logical_plan/kill_planner.cc
        logical_plan/load_planner.cc
        logical_plan/logical_planner.cc
        logical_plan/prepare_planner.cc
        logical_plan/query_context.cc
        logical_plan/select_planner.cc
        logical_plan/setkv_planner.cc
        logical_plan/transaction_planner.cc
        logical_plan/union_planner.cc
        logical_plan/update_planner.cc
        #####################################################
        # mem_row
        mem_row/mem_row.cc
        mem_row/mem_row_compare.cc
        mem_row/mem_row_descriptor.cc
        #####################################################
        # physical_plan
        physical_plan/auto_inc.cc
        physical_plan/condition_optimizer.cc
        physical_plan/exec_type_analyzer.cc
        physical_plan/expr_optimizer.cc
        physical_plan/index_selector.cc
        physical_plan/join_reorder.cc
        physical_plan/join_type_analyzer.cc
        physical_plan/limit_calc.cc
        physical_plan/mpp_analyzer.cc
        physical_plan/physical_planner.cc
        physical_plan/plan_router.cc
        physical_plan/separate.cc
        #####################################################
        # protocol
        protocol/data_buffer.cc
        protocol/db_service.cc
        protocol/epoll_info.cc
        protocol/handle_helper.cc
        protocol/machine_driver.cc
        protocol/mysql_err_handler.cc
        protocol/mysql_wrapper.cc
        protocol/network_server.cc
        protocol/show_helper.cc
        protocol/state_machine.cc
        protocol/task_manager.cc
        #####################################################
        # raft
        raft/log_entry_reader.cc
        raft/my_raft_log.cc
        raft/my_raft_log_storage.cc
        raft/my_raft_meta_storage.cc
        raft/raft_control.cc
        raft/rocksdb_file_system_adaptor.cc
        raft_meta/can_add_peer_setter.cc
        raft_meta/split_index_getter.cc
        raft_meta/update_region_status.cc
        #####################################################
        # reverse
        reverse/boolean_executor.cc
        reverse/reverse_common.cc
        reverse/reverse_index.cc
        reverse/reverse_interface.cc
        #####################################################
        # runtime
        runtime/arrow_io_excutor.cc
        runtime/chunk.cc
        runtime/runtime_state.cc
        runtime/sorter.cc
        runtime/topn_sorter.cc
        #####################################################
        # session
        session/binlog_context.cc
        session/network_socket.cc
        session/user_info.cc
        #####################################################
        # sqlparser
        sqlparser/expr.cc
        sqlparser/parser.cc
        #####################################################
        # store
        store/backup.cc
        store/closure.cc
        store/compaction_server.cc
        store/meta_writer.cc
        store/region_binlog.cc
        store/region.cc
        store/region_control.cc
        store/region_olap.cc
        store/rpc_sender.cc
        store/store.cc
        #####################################################
        # vector_index
        vector_index/vector_index.cc
)

kmcmake_cc_library(
        NAMESPACE ${PROJECT_NAME}
        NAME common
        SOURCES
        ${LIB_SRC}
        OBJECTS
        proto_obj
        parse_obj
        DEPS
        proto_obj
        parse_obj
        CXXOPTS
        ${KMCMAKE_CXX_OPTIONS}
        PLINKS
        ${KMCMAKE_DEPS_LINK}
)

set(META_SERVER
        meta_server/auto_incr_state_machine.cc
        meta_server/cluster_manager.cc
        meta_server/common_state_machine.cc
        meta_server/database_manager.cc
        meta_server/ddl_manager.cc
        meta_server/kns_manager.cc
        meta_server/kns_peer_manager.cc
        meta_server/meta_rocksdb.cc
        meta_server/meta_server.cc
        meta_server/meta_state_machine.cc
        meta_server/meta_util.cc
        meta_server/namespace_manager.cc
        meta_server/privilege_manager.cc
        meta_server/query_cluster_manager.cc
        meta_server/query_database_manager.cc
        meta_server/query_namespace_manager.cc
        meta_server/query_privilege_manager.cc
        meta_server/query_region_manager.cc
        meta_server/query_table_manager.cc
        meta_server/query_kns_manager.cc
        meta_server/query_kns_peer_manager.cc
        meta_server/region_manager.cc
        meta_server/schema_manager.cc
        meta_server/table_manager.cc
        meta_server/tso_state_machine.cc
)


kmcmake_cc_binary(
        NAMESPACE ${PROJECT_NAME}
        NAME ksmeta
        SOURCES
        ${META_SERVER}
        server/meta.cc
        CXXOPTS
        ${KMCMAKE_CXX_OPTIONS}
        LINKS
        ${PROJECT_NAME}::common_static
        ${KMCMAKE_DEPS_LINK}
        LINKOPTS
        ${KMCMAKE_STATIC_BIN_OPTION}
        PUBLIC
)

kmcmake_cc_binary(
        NAMESPACE ${PROJECT_NAME}
        NAME kns
        SOURCES
        ${META_SERVER}
        server/kns.cc
        CXXOPTS
        ${KMCMAKE_CXX_OPTIONS}
        LINKS
        ${PROJECT_NAME}::common_static
        ${KMCMAKE_DEPS_LINK}
        LINKOPTS
        ${KMCMAKE_STATIC_BIN_OPTION}
        PUBLIC
)

kmcmake_cc_binary(
        NAMESPACE ${PROJECT_NAME}
        NAME ksstore
        SOURCES
        server/store.cc
        raft_store/can_add_peer_setter.cc
        raft_store/split_index_getter.cc
        raft_store/update_region_status.cc
        CXXOPTS
        ${KMCMAKE_CXX_OPTIONS}
        LINKS
        ${PROJECT_NAME}::common_static
        ${KMCMAKE_DEPS_LINK}
        LINKOPTS
        ${KMCMAKE_STATIC_BIN_OPTION}
        PUBLIC
)

kmcmake_cc_binary(
        NAMESPACE ${PROJECT_NAME}
        NAME kmars
        SOURCES
        server/kmars.cc
        raft_dummy/dummy_setter.cc
        CXXOPTS
        ${KMCMAKE_CXX_OPTIONS}
        LINKS
        ${PROJECT_NAME}::common_static
        ${KMCMAKE_DEPS_LINK}
        LINKOPTS
        ${KMCMAKE_STATIC_BIN_OPTION}
        PUBLIC
)

kmcmake_cc_binary(
        NAMESPACE ${PROJECT_NAME}
        NAME tlog
        SOURCES
        server/tglog.cc
        CXXOPTS
        ${KMCMAKE_CXX_OPTIONS}
        LINKS
        ${KMCMAKE_DEPS_LINK}
        LINKOPTS
        ${KMCMAKE_STATIC_BIN_OPTION}
        PUBLIC
)