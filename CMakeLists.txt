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
        "source/WordIndexEntry.cpp"
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
        source/Constants.h source/DocumentsMatcher.cpp source/DocumentsMatcher.h source/dict_strings.h source/ResultsPrinter.cpp source/ResultsPrinter.h source/ContiguousAllocator.cpp source/ContiguousAllocator.h source/TopDocs.cpp source/TopDocs.h source/CustomAllocatedVec.cpp source/CustomAllocatedVec.h)
target_include_directories(search-share PUBLIC source extern/porter2_stemmer extern extern/mio/include)
target_include_directories(general-indexer PUBLIC source extern/porter2_stemmer extern extern/mio/include)
add_subdirectory(extern/porter2_stemmer)

target_link_libraries(general-indexer PRIVATE pthread ${CMAKE_BINARY_DIR}/librust.so)

add_executable(search source/main.cpp)
target_link_libraries(search PRIVATE search-share general-indexer porter2-stemmer )


add_executable(compactor source/compactor/main.cpp)
target_link_libraries(compactor PRIVATE search-share porter2-stemmer)

add_library(c-search-abi SHARED source/c_abi.cpp)
target_link_libraries(c-search-abi PRIVATE search-share)


add_custom_target(rust-library
        DEPENDS
        "${CMAKE_BINARY_DIR}/librust.so")

file(GLOB rust-sources rust/src/*)

set(rust-build-command $ENV{HOME}/.cargo/bin/cargo build --lib --out-dir ${CMAKE_BINARY_DIR} -Z unstable-options)


if (CMAKE_BUILD_TYPE EQUAL RELWITHDEBINFO OR CMAKE_BUILD_TYPE EQUAL RELEASE)
    string(APPEND ${rust-build-command} --release)
endif()
add_custom_command(
        OUTPUT
            "${CMAKE_BINARY_DIR}/librust.so"
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