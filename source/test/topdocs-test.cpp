#include "all_includes.h"
#include <gtest/gtest.h>

TEST(TopDocs, append_correctly) {
    TopDocs td({{2,  1},
                {10, 1},
                {11, 1}});
    TopDocs td2({{0,  1},
                 {1,  1},
                 {2,  1},
                 {9,  1},
                 {10, 1},
                 {11, 1}});
    td.append_multi(td2);

    auto correct = TopDocs({{0,  1},
                            {1,  1},
                            {2,  2},
                            {9,  1},
                            {10, 2},
                            {11, 2}});

    ASSERT_TRUE(std::equal(td.begin(), td.end(), td2.begin(), td2.end()));
}