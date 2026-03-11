// 
#include <gtest/gtest.h>
#include <climits>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <ksearch/common/expr_value.h>
#include <ksearch/expr/fn_manager.h>
#include <ksearch/proto/expr.pb.h>
#include <ksearch/sqlparser/parser.h>
#include <ksearch/engine/qos.h>
#include <vector>
#include "ks_client.h"
DEFINE_int64(qos_rate, 100, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_burst, 100000, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_count, 100000, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_bthread_count, 10, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_committed_rate, 100, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_extended_rate, 100, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_globle_rate, 100, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_sum, 60, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_get_value, 60, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_sleep_us, 1000, "max_tokens_per_second, default: 10w");
DEFINE_int64(peer_thread_us, 1000 * 1000, "max_tokens_per_second, default: 10w");


namespace ksearch {
    void test_func() {
        bvar::Adder<int64_t> test_sum;
        bvar::PerSecond<bvar::Adder<int64_t> > test_sum_per_second(&test_sum, FLAGS_qos_sum);
        int i = 1;
        int count = 0;
        int count1 = 0;
        TimeCost cost;
        for (;;) {
            test_sum << i;
            count += i;
            if (cost.get_time() > 1000 * 1000) {
                cost.reset();
                i = i * 2;

                DB_WARNING("adder:%d, qps:%ld", count - count1, test_sum_per_second.get_value(FLAGS_qos_get_value));
                count1 = count;
            }
            bthread_usleep(100 * 1000);
        }
    }

    int qos_test1(ks::client::Service *ksearch) {
        TimeCost time_cost;
        BthreadCond concurrency_cond(-FLAGS_qos_bthread_count);
        std::atomic<int> count = {0};
        while (true) {
            auto func = [ksearch, &time_cost, &concurrency_cond, &count]() {
                std::shared_ptr<BthreadCond> auto_decrease(&concurrency_cond,
                                                           [](BthreadCond *cond) { cond->decrease_signal(); });
                std::string sql = "INSERT INTO TEST.qos_test values ";
                for (int i = 0; i < 100; i++) {
                    int c = ++count;
                    if (c > 1000000) { exit(0); }
                    sql += "(" + std::to_string(c) + ",1," + std::to_string(c) + ",1,1),";
                }

                sql.pop_back();

                ks::client::ResultSet result_set;
                int ret = ksearch->query(0, sql, &result_set);
                if (ret != 0) {
                    DB_FATAL("atom_test failed");
                } else {
                    DB_WARNING("atom_test succ");
                }
            };

            Bthread bth;
            concurrency_cond.increase_wait();
            bth.run(func);
        }
        concurrency_cond.wait(-FLAGS_qos_bthread_count);
        return 0;
    }

    int qos_test2(ks::client::Service *ksearch) {
        for (int i = 0; i < FLAGS_qos_bthread_count; i++) {
            auto func = [ksearch]() {
                static std::vector<std::string> f = {"id4", "id5"};
                int i = butil::fast_rand() % 2;

                std::string sql = "select " + f[i] + " from TEST.qos_test where id2 = 1 and id3= 22";

                ks::client::ResultSet result_set;
                int ret = ksearch->query(0, sql, &result_set);
                if (ret != 0) {
                    DB_FATAL("qos_test failed");
                } else {
                    DB_WARNING("qos_test succ");
                }
            };

            Bthread bth;
            bth.run(func);
        }
        return 0;
    }
} // namespace ksearch

using namespace ksearch;

int main(int argc, char *argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    ks::client::Manager tmp_manager;
    int ret = tmp_manager.init("conf", "ks_client.conf");
    if (ret != 0) {
        DB_FATAL("ks client init fail:%d", ret);
        return 0;
    }

    auto ksearch = tmp_manager.get_service("ksearch");
    if (ksearch == nullptr) {
        ksearch = tmp_manager.get_service("ksearch_gbk");
        if (ksearch == nullptr) {
            ksearch = tmp_manager.get_service("ksearch_utf8");
            if (ksearch == nullptr) {
                DB_FATAL("get_service failed");
                return -1;
            }
        }
    }

    ksearch::StoreQos *store_qos = ksearch::StoreQos::get_instance();
    ret = store_qos->init();
    if (ret < 0) {
        DB_FATAL("store qos init fail");
        return -1;
    }

    ksearch::TimeCost cost;
    ksearch::BthreadCond cond;
    for (int i = 0; i < FLAGS_qos_bthread_count; i++) {
        auto calc = [i, &cond]() {
            uint64_t sign = 123;
            if (i % 2 == 0) {
                sign = 124;
            }
            StoreQos::get_instance()->create_bthread_local(ksearch::QOS_SELECT, sign, 123);

            ksearch::QosBthreadLocal *local = StoreQos::get_instance()->get_bthread_local();
            DB_WARNING("local:%p", local);
            ksearch::TimeCost local_time;
            for (;;) {
                // 限流
                if (local) {
                    local->scan_rate_limiting();
                }

                bthread_usleep(FLAGS_qos_sleep_us);
                if (local_time.get_time() > FLAGS_peer_thread_us) {
                    break;
                }
            }


            DB_WARNING("bthread:%d, sign:%lu. time:%ld", i, sign, local_time.get_time());
            cond.decrease_signal();
        };

        cond.increase();
        ksearch::Bthread bth(&BTHREAD_ATTR_SMALL);
        bth.run(calc);
    }

    cond.wait();
    DB_WARNING("time:%ld", cost.get_time());
    store_qos->close();
    DB_WARNING("store qos close success");

    bthread_usleep(10000000);

    return 0;
}