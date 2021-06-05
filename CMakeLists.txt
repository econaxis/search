cmake_minimum_required(VERSION 3.16)

project("search")
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 20)

add_compile_options(-march=native -Wall -Wno-ignored-attributes -fno-inline -Wno-sign-compare)

add_library(general-indexer SHARED
        "source/GeneralIndexer.cpp"
        )



add_library(search-share SHARED "source/DocumentPositionPointer.cpp"
        "source/DocIDFilePair.cpp"
        "source/Serializer.h"
        "source/Tokenizer.cpp"
        "source/DocumentPositionPointer.h"
        "source/WordIndexEntry.h"
        "source/Serializer.cpp"
        extern/microtar/microtar.c
        "source/DocIDFilePair.h"
        "source/Tokenizer.h"
        "source/SortedKeysIndex.h"
        "source/SortedKeysIndex.cpp"
        "source/compactor/Compactor.cpp"
        "source/compactor/Compactor.h" source/SortedKeysIndexStub.cpp source/SortedKeysIndexStub.h
        source/Constants.cpp source/Base26Num.cpp
        source/FPStub.cpp
        source/DocumentsTier.cpp
        source/PositionsSearcher.cpp
        source/Constants.h source/DocumentsMatcher.cpp source/DocumentsMatcher.h source/dict_strings.h source/ResultsPrinter.cpp source/ResultsPrinter.h source/ContiguousAllocator.cpp source/ContiguousAllocator.h source/TopDocs.cpp source/TopDocs.h source/CustomAllocatedVec.cpp source/CustomAllocatedVec.h)
target_include_directories(search-share PUBLIC source extern extern/mio/include)
target_include_directories(general-indexer PUBLIC source extern extern/mio/include )

target_link_libraries(general-indexer PRIVATE pthread ${CMAKE_BINARY_DIR}/libnamesdb.so)

add_executable(search source/main.cpp source/TermsListSearcher.cpp source/DocumentsTier.cpp)
target_link_libraries(search PRIVATE search-share general-indexer)


add_executable(compactor source/compactor/main.cpp)
target_link_libraries(compactor PRIVATE search-share pthread)

target_link_options(compactor PRIVATE -rdynamic)

add_library(c-search-abi SHARED source/c_abi.cpp)
target_link_libraries(c-search-abi PRIVATE search-share)


add_custom_target(rust-library
        DEPENDS
        "${CMAKE_BINARY_DIR}/libnamesdb.so")

add_custom_target(rust-web-server
        DEPENDS
        "${CMAKE_SOURCE_DIR}/rust/src")

file(GLOB rust-sources rust/webserver/src/*)

set(rust-build-command $ENV{HOME}/.cargo/bin/cargo build --out-dir ${CMAKE_BINARY_DIR} -Z unstable-options)

if (CMAKE_BUILD_TYPE MATCHES RelWithDebInfo OR CMAKE_BUILD_TYPE MATCHES Release)
    message("MESSAGE: Using release build")
    string(APPEND ${rust-build-command} --release)
endif()

add_custom_target(
        rust-server ALL
        COMMAND ${rust-build-command}
        DEPENDS ${CMAKE_SOURCE_DIR}/rust/webserver/src
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/rust
)


add_custom_command(
        OUTPUT
            "${CMAKE_BINARY_DIR}/libnamesdb.so"
        COMMENT
            "Builds the librust target"
        COMMAND
            ${rust-build-command}
        DEPENDS
            ${rust-sources}
        WORKING_DIRECTORY
            ${CMAKE_SOURCE_DIR}/rust
)
add_dependencies(general-indexer rust-library)
add_dependencies(rust-library c-search-abi)
add_dependencies(rust-server c-search-abi)