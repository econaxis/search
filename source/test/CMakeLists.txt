include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)


add_executable(default-tests positions_searcher_test.cpp indexing_test.cpp doc-matcher-test.cpp searching-strings.cpp topdocs-test.cpp multiple-indices-test.cpp test-main.cpp)

target_include_directories(default-tests PRIVATE . ../)
#message("binary dir: " ${CMAKE_BINARY_DIR})
#target_link_directories(default-tests PUBLIC ${CMAKE_BINARY_DIR})
include(GoogleTest)
gtest_discover_tests(default-tests)

include(CTest)

