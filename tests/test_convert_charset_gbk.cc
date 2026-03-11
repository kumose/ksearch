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

#include <gtest/gtest.h>
#include <baidu/feed/mlarch/babylon/lite/iconv.h>

#include <ksearch/common/common.h>

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace ksearch {
    namespace babylon = baidu::feed::mlarch::babylon;

    // �ú�������ֵӦ�ú�Ϊ0
    int check_convert_charset(const std::string &origin_str) {
        std::string str_gbk_babylon;
        std::string str_gbk_ksearch;
        std::string str_utf8_babylon;
        std::string str_utf8_ksearch;

        // gbk -> utf8
        int ret_babylon = babylon::iconv_convert<babylon::Encoding::UTF8,
            babylon::Encoding::GB18030>(str_utf8_babylon, origin_str);
        int ret_ksearch = ksearch::iconv_convert<pb::UTF8, pb::GBK>(str_utf8_ksearch, origin_str);
        if (ret_babylon != ret_ksearch || ret_babylon != 0) {
            return -1;
        }
        if (str_utf8_babylon != str_utf8_ksearch) {
            return -1;
        }

        // utf8 -> gbk
        ret_babylon = babylon::iconv_convert<babylon::Encoding::GB18030,
            babylon::Encoding::UTF8>(str_gbk_babylon, str_utf8_babylon);
        ret_ksearch = ksearch::iconv_convert<pb::GBK, pb::UTF8>(str_gbk_ksearch, str_utf8_ksearch);
        if (ret_babylon != ret_ksearch || ret_babylon != 0) {
            return -1;
        }
        if (str_gbk_babylon != str_gbk_ksearch || str_gbk_babylon != origin_str) {
            return -1;
        }

        std::cout << str_gbk_babylon << std::endl;
        std::cout << str_gbk_ksearch << std::endl;
        std::cout << str_utf8_babylon << std::endl;
        std::cout << str_utf8_ksearch << std::endl;

        return 0;
    }

    TEST(test_convert_charset, convert_charset) {
        EXPECT_EQ(check_convert_charset("����ʹ��"), 0);
        EXPECT_EQ(check_convert_charset("%||||%��|a| |ba&&a|"), 0);
        EXPECT_EQ(check_convert_charset("06-JO [����] �ز�-���ۺ�"), 0);
        EXPECT_EQ(check_convert_charset("p.c1+11.1?-ӪҵӪִ�գ���ȷ��"), 0);
        EXPECT_EQ(check_convert_charset("p.c1+11.1?-ӪҵӪ�ȣ���������䣡��������������ִ�գ���ȷ��"), 0);
        EXPECT_EQ(check_convert_charset("���ũ�Ŵ�ũҵ,�����ҵ�����{�ؼ���}{�������������},ӵ��ũҵ�Ƽ�,��������Ŷ�."), 0);
    }

    TEST(test_convert_charset_abort, convert_charset) {
        std::string origin_str = "06-JO [����] �ز�-���ۺ�";
        std::string str_babylon;
        std::string str_ksearch;

        int ret_babylon = babylon::iconv_convert<babylon::Encoding::GB18030,
            babylon::Encoding::UTF8>(str_babylon, origin_str);
        int ret_ksearch = ksearch::iconv_convert<pb::GBK, pb::UTF8>(str_ksearch, origin_str);

        EXPECT_EQ(ret_babylon, -1);
        EXPECT_EQ(ret_babylon, ret_ksearch);
        EXPECT_EQ(str_babylon, str_ksearch);

        std::cout << str_babylon << std::endl;
        std::cout << str_ksearch << std::endl;
    }

    TEST(test_convert_charset_ignore, convert_charset) {
        std::string origin_str = "06-JO [����] �ز�-���ۺ�";
        std::string str_babylon;
        std::string str_ksearch;

        int ret_babylon = babylon::iconv_convert<babylon::Encoding::GB18030, babylon::Encoding::UTF8,
            babylon::IconvOnError::IGNORE>(str_babylon, origin_str);
        int ret_ksearch = ksearch::iconv_convert<pb::GBK, pb::UTF8, IconvOnError::IGNORE>(str_ksearch, origin_str);

        EXPECT_EQ(ret_babylon, -1);
        EXPECT_EQ(ret_babylon, ret_ksearch);
        EXPECT_EQ(str_babylon, str_ksearch);

        std::cout << str_babylon << std::endl;
        std::cout << str_ksearch << std::endl;
    }

    TEST(test_convert_charset_translit, convert_charset) {
        std::string origin_str = "06-JO [����] �ز�-���ۺ�";
        std::string str_babylon;
        std::string str_ksearch;

        int ret_babylon = babylon::iconv_convert<babylon::Encoding::GB18030, babylon::Encoding::UTF8,
            babylon::IconvOnError::TRANSLIT>(str_babylon, origin_str);
        int ret_ksearch = ksearch::iconv_convert<pb::GBK, pb::UTF8, IconvOnError::TRANSLIT>(str_ksearch, origin_str);

        EXPECT_EQ(ret_babylon, -1);
        EXPECT_EQ(ret_babylon, ret_ksearch);
        EXPECT_EQ(str_babylon, str_ksearch);

        std::cout << str_babylon << std::endl;
        std::cout << str_ksearch << std::endl;
    }
}