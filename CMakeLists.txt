cmake_minimum_required(VERSION 3.16)

project("search")
enable_testing()

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 20)

link_directories(${CMAKE_BINARY_DIR})

add_compile_options(-march=native -Wall -Wno-ignored-attributes -fno-inline -Wno-sign-compare -fPIC )

add_library(general-indexer SHARED
        "source/GeneralIndexer.cpp"
        )
set(CMAKE_BUILD_RPATH ${CMAKE_BINARY_DIR})


add_library(search-share SHARED
        "source/Serializer.h"
        "source/Tokenizer.cpp"
        "source/DocumentPositionPointer.h"
        "source/WordIndexEntry.h"
        "source/Serializer.cpp"
        "source/DocIDFilePair.h"
        "source/Tokenizer.h"
        "source/SortedKeysIndex.h"
        "source/SortedKeysIndex.cpp"
        "source/compactor/Compactor.cpp"
        "source/compactor/Compactor.h" source/SortedKeysIndexStub.cpp source/SortedKeysIndexStub.h
        source/IndexStubCApi.cpp
        source/Constants.cpp source/Base26Num.cpp
        source/FPStub.cpp
        source/DocumentsTier.cpp
        source/PositionsSearcher.cpp
        source/Constants.h source/DocumentsMatcher.cpp source/TopDocs.cpp source/SyncedQueue.cpp source/logger.h source/PossiblyMatchingTerm.cpp source/PossiblyMatchingTerm.h source/TopDocsResultsJoiner.cpp source/TopDocsResultsJoiner.h)
target_include_directories(search-share PUBLIC source extern extern/mio/include)

target_include_directories(general-indexer PUBLIC source extern extern/mio/include)
target_link_libraries(general-indexer PUBLIC tbb search-share)

add_executable(search source/main.cpp source/TermsListSearcher.cpp source/DocumentsTier.cpp)
target_link_libraries(search PRIVATE search-share general-indexer)


add_executable(compactor source/compactor/main.cpp)
target_link_libraries(compactor PRIVATE search-share pthread)

add_library(c-search-abi SHARED source/c_abi.cpp)
target_link_libraries(c-search-abi PRIVATE search-share)


file(GLOB rust-sources rust/webserver/src/*)

if (CMAKE_BUILD_TYPE MATCHES RelWithDebInfo OR CMAKE_BUILD_TYPE MATCHES Release)
    set(CARGO_RELEASE_MODE "--release")
    message("MESSAGE: Using release build " ${rust-build-command})
else ()
    set(CARGO_RELEASE_MODE "")
    add_compile_definitions(DEBUG_MODE_ON)
endif()
set(rust-build-command cargo build ${CARGO_RELEASE_MODE} --out-dir ${CMAKE_BINARY_DIR} -Z unstable-options)



add_executable(stdinindexer source/StdinIndexer.cpp)
target_include_directories(stdinindexer PRIVATE source/)
target_link_libraries(stdinindexer PUBLIC general-indexer search-share)


add_subdirectory(extern/fmt)
target_link_libraries(search-share PUBLIC fmt::fmt)
target_include_directories(search-share PUBLIC extern/fmt/include)

target_link_libraries(c-search-abi PRIVATE fmt::fmt)
target_include_directories(c-search-abi PUBLIC extern/fmt/include)


target_precompile_headers(search-share PUBLIC source/logger.h)
target_precompile_headers(general-indexer PRIVATE source/logger.h)

target_link_libraries(general-indexer PRIVATE fmt::fmt)

add_subdirectory(source/test)
add_dependencies(default-tests c-search-abi)


include(CheckIncludeFileCXX)
check_include_file_cxx(execinfo.h HAS_EXEC_INFO)

if (HAS_EXEC_INFO)
    target_compile_definitions(search-share PRIVATE HAS_EXEC_INFO)
    message("has exec info")
else ()
    message("no exec info")
endif ()

#
#include(FetchContent)
#FetchContent_Declare(
#        googletest
#        URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
#)
## For Windows: Prevent overriding the parent project's compiler/linker settings
#set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
#FetchContent_MakeAvailable(googletest)
#
#
#add_executable(default-tests source/test/positions_searcher_test.cpp source/test/indexing_test.cpp source/test/doc-matcher-test.cpp source/test/searching-strings.cpp)
#
#target_include_directories(default-tests PRIVATE .)
#message("binary dir: " ${CMAKE_BINARY_DIR})
#target_link_directories(default-tests PUBLIC ${CMAKE_BINARY_DIR})
#set_target_properties(default-tests
#        PROPERTIES
#        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}"
#        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}"
#        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}"
#        )
#
#
#include(GoogleTest)
#gtest_discover_tests(default-tests)
#
#include(CTest)
#target_link_directories(default-tests PRIVATE ${CMAKE_BINARY_DIR})
target_link_libraries(default-tests PRIVATE gtest_main search-share general-indexer)


add_custom_target(
        metastore
        COMMAND cargo build -p metastore --out-dir ${CMAKE_BINARY_DIR} -Z unstable-options
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/rust/metastore
        DEPENDS rust/metastore/src

)

#if(CMAKE_BUILD_TYPE MATCHES Debug)
#    find_library(pythonlib NAMES pythonlib PATHS ${CMAKE_BINARY_DIR} REQUIRED)
#    target_link_libraries(general-indexer PRIVATE pythonlib)
#endif()