#include <gtest/gtest.h>
#include <baidu/feed/mlarch/babylon/lite/iconv.h>

#include <ksearch/common/common.h>

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace ksearch {
    namespace babylon = baidu::feed::mlarch::babylon;

    // 该函数返回值应该恒为0
    int check_convert_charset(const std::string &origin_str) {
        std::string str_gbk_babylon;
        std::string str_gbk_ksearch;
        std::string str_utf8_babylon;
        std::string str_utf8_ksearch;

        // utf8 -> gbk
        int ret_babylon = babylon::iconv_convert<babylon::Encoding::GB18030,
            babylon::Encoding::UTF8>(str_gbk_babylon, origin_str);
        int ret_ksearch = ksearch::iconv_convert<pb::GBK, pb::UTF8>(str_gbk_ksearch, origin_str);
        if (ret_babylon != ret_ksearch || ret_babylon != 0) {
            return -1;
        }
        if (str_gbk_babylon != str_gbk_ksearch) {
            return -1;
        }

        // gbk -> utf8
        ret_babylon = babylon::iconv_convert<babylon::Encoding::UTF8,
            babylon::Encoding::GB18030>(str_utf8_babylon, str_gbk_babylon);
        ret_ksearch = ksearch::iconv_convert<pb::UTF8, pb::GBK>(str_utf8_ksearch, str_gbk_ksearch);
        if (ret_babylon != ret_ksearch || ret_babylon != 0) {
            return -1;
        }
        if (str_utf8_babylon != str_utf8_ksearch || str_utf8_babylon != origin_str) {
            return -1;
        }

        std::cout << str_gbk_babylon << std::endl;
        std::cout << str_gbk_ksearch << std::endl;
        std::cout << str_utf8_babylon << std::endl;
        std::cout << str_utf8_ksearch << std::endl;

        return 0;
    }

    TEST(test_convert_charset, convert_charset) {
        EXPECT_EQ(check_convert_charset("测试使用"), 0);
        EXPECT_EQ(check_convert_charset("%||||%，|a| |ba&&a|"), 0);
        EXPECT_EQ(check_convert_charset("06-JO [整外] 胸部-胸综合"), 0);
        EXPECT_EQ(check_convert_charset("p.c1+11.1?-营业营执照（精确）"), 0);
        EXPECT_EQ(check_convert_charset("p.c1+11.1?-营业营Ｈｅｌｌｏ　ｗｏｒｌｄ！０１２３７２１执照（精确）"), 0);
        EXPECT_EQ(check_convert_charset("天津农信达农业,多年从业经验的{关键词}{大棚管生产厂家},拥有农业科技,建筑设计团队."), 0);
    }

    TEST(test_convert_charset_abort, convert_charset) {
        std::string origin_str = "06-JO [整外] 胸部-胸综合";
        std::string str_babylon;
        std::string str_ksearch;

        int ret_babylon = babylon::iconv_convert<babylon::Encoding::UTF8,
            babylon::Encoding::GB18030>(str_babylon, origin_str);
        int ret_ksearch = ksearch::iconv_convert<pb::UTF8, pb::GBK>(str_ksearch, origin_str);

        EXPECT_EQ(ret_babylon, -1);
        EXPECT_EQ(ret_babylon, ret_ksearch);
        EXPECT_EQ(str_babylon, str_ksearch);

        std::cout << str_babylon << std::endl;
        std::cout << str_ksearch << std::endl;
    }

    TEST(test_convert_charset_ignore, convert_charset) {
        std::string origin_str = "06-JO [整外] 胸部-胸综合";
        std::string str_babylon;
        std::string str_ksearch;

        int ret_babylon = babylon::iconv_convert<babylon::Encoding::UTF8, babylon::Encoding::GB18030,
            babylon::IconvOnError::IGNORE>(str_babylon, origin_str);
        int ret_ksearch = ksearch::iconv_convert<pb::UTF8, pb::GBK, IconvOnError::IGNORE>(str_ksearch, origin_str);

        EXPECT_EQ(ret_babylon, -1);
        EXPECT_EQ(ret_babylon, ret_ksearch);
        EXPECT_EQ(str_babylon, str_ksearch);

        std::cout << str_babylon << std::endl;
        std::cout << str_ksearch << std::endl;
    }

    TEST(test_convert_charset_translit, convert_charset) {
        std::string origin_str = "06-JO [整外] 胸部-胸综合";
        std::string str_babylon;
        std::string str_ksearch;

        int ret_babylon = babylon::iconv_convert<babylon::Encoding::UTF8, babylon::Encoding::GB18030,
            babylon::IconvOnError::TRANSLIT>(str_babylon, origin_str);
        int ret_ksearch = ksearch::iconv_convert<pb::UTF8, pb::GBK, IconvOnError::TRANSLIT>(str_ksearch, origin_str);

        EXPECT_EQ(ret_babylon, -1);
        EXPECT_EQ(ret_babylon, ret_ksearch);
        EXPECT_EQ(str_babylon, str_ksearch);

        std::cout << str_babylon << std::endl;
        std::cout << str_ksearch << std::endl;
    }
}