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
############################################################
# system pthread and rt, dl
############################################################
set(KMCMAKE_SYSTEM_DYLINK)
if (APPLE)
    find_library(CoreFoundation CoreFoundation)
    list(APPEND KMCMAKE_SYSTEM_DYLINK ${CoreFoundation} pthread)
elseif (${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
    list(APPEND KMCMAKE_SYSTEM_DYLINK rt dl pthread)
endif ()

if (KMCMAKE_BUILD_TEST)
    enable_testing()
    #include(require_gtest)
    #include(require_gmock)
    #include(require_doctest)
endif (KMCMAKE_BUILD_TEST)

if (KMCMAKE_BUILD_BENCHMARK)
    #include(require_benchmark)
endif ()


find_package(Threads REQUIRED)
find_package(turbo REQUIRED)
find_package(BISON 3.0.4 REQUIRED)
find_package(FLEX 2.5.13 REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
find_package(unofficial-brpc CONFIG REQUIRED)
find_package(unofficial-braft CONFIG REQUIRED)
find_package(faiss CONFIG REQUIRED)
find_package(RocksDB CONFIG REQUIRED)
find_package(lz4 REQUIRED)
find_package(roaring REQUIRED)
find_package(leveldb REQUIRED)
find_package(Snappy REQUIRED)
find_package(BZip2 REQUIRED)
find_package(zstd CONFIG REQUIRED)
find_package(unofficial-brotli CONFIG REQUIRED)
find_package(re2 REQUIRED)
find_package(unofficial-libmariadb CONFIG REQUIRED)
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)
find_package(ArrowDataset CONFIG REQUIRED)
find_package(ArrowAcero CONFIG REQUIRED)
find_package(BLAS REQUIRED)
find_package(LAPACK REQUIRED)
find_package(OpenMP REQUIRED)
find_package(Boost REQUIRED COMPONENTS thread filesystem REQUIRED)
find_package(RocksDB REQUIRED)
find_package(Protobuf REQUIRED)
find_package(turbo REQUIRED)

if (Protobuf_VERSION GREATER 4.21)
    # required by absl
    find_package(absl REQUIRED CONFIG)
    set(protobuf_ABSL_USED_TARGETS
            absl::absl_check
            absl::absl_log
            absl::algorithm
            absl::base
            absl::bind_front
            absl::bits
            absl::btree
            absl::cleanup
            absl::cord
            absl::core_headers
            absl::debugging
            absl::die_if_null
            absl::dynamic_annotations
            absl::flags
            absl::flat_hash_map
            absl::flat_hash_set
            absl::function_ref
            absl::hash
            absl::layout
            absl::log_initialize
            absl::log_severity
            absl::memory
            absl::node_hash_map
            absl::node_hash_set
            absl::optional
            absl::span
            absl::status
            absl::statusor
            absl::strings
            absl::synchronization
            absl::time
            absl::type_traits
            absl::utility
            absl::variant
    )
endif ()

############################################################
#
# add you libs to the KMCMAKE_DEPS_LINK variable eg as turbo
# so you can and system pthread and rt, dl already add to
# KMCMAKE_SYSTEM_DYLINK, using it for fun.
##########################################################
set(KMCMAKE_DEPS_LINK
        #${TURBO_LIB}
        unofficial::braft::braft-static
        unofficial::brpc::brpc
        faiss
        Arrow::arrow_static
        Parquet::parquet_static
        ArrowDataset::arrow_dataset_static
        ArrowAcero::arrow_acero_static
        RocksDB::rocksdb
        leveldb::leveldb
        roaring::roaring
        zstd::libzstd
        Snappy::snappy
        turbo::turbo_static
        BZip2::BZip2
        ${LAPACK_LIBRARIES}
        ${BLAS_LIBRARIES}
        protobuf::libprotobuf
        ${protobuf_ABSL_USED_TARGETS}
        protobuf::libprotoc
        re2::re2
        ZLIB::ZLIB
        lz4::lz4
        turbo::turbo_static
        ${Boost_LIBRARIES}
        ${OPENSSL_SSL_LIBRARY}
        ${OPENSSL_CRYPTO_LIBRARY}
        ${KMCMAKE_SYSTEM_DYLINK}
)
list(REMOVE_DUPLICATES KMCMAKE_DEPS_LINK)
kmcmake_print_list_label("Denpendcies:" KMCMAKE_DEPS_LINK)

############################################################
# for binary
############################################################
set(KMCMAKE_STATIC_BIN_OPTION -static-libgcc -static-libstdc++)



