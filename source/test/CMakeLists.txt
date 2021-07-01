include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)


add_executable(default-tests positions_searcher_test.cpp indexing_test.cpp doc-matcher-test.cpp searching-strings.cpp)

target_link_libraries(default-tests PRIVATE gtest_main search-share general-indexer ${CMAKE_BINARY_DIR}/libnamesdb.so)
target_include_directories(default-tests PRIVATE .)
include(GoogleTest)
gtest_discover_tests(default-tests)

include(CTest)