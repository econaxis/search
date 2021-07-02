#include <gtest/gtest.h>
#include <span>
#include "all_includes.h"

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    std::span argvs(argv, argc);

    for(auto c=  argvs.begin(); c != argvs.end(); c++) {
        if(strcmp(*c, "--loop-multiply") == 0) {
            LOOP_ITERS_MULTIPLY = std::stof(*(c+1));
            fmt::print("Using multiply: {:.2f}\n", LOOP_ITERS_MULTIPLY);
        }
    }

    fmt::print("Args: {}\n", fmt::join(argvs, " "));
    return RUN_ALL_TESTS();
}