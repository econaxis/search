cmake_minimum_required(VERSION 3.16)

project("search")
enable_testing()

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 20)


add_compile_options(-march=native -Wall -Wno-ignored-attributes -fno-inline -Wno-sign-compare -fPIC -fconcepts)

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
        source/Constants.h source/DocumentsMatcher.cpp source/ContiguousAllocator.cpp source/ContiguousAllocator.h source/TopDocs.cpp source/CustomAllocatedVec.cpp source/SyncedQueue.cpp source/logger.h)
target_include_directories(search-share PUBLIC source extern extern/mio/include)

target_include_directories(general-indexer PUBLIC source extern extern/mio/include)

target_link_libraries(general-indexer PRIVATE pthread ${CMAKE_BINARY_DIR}/libnamesdb.so tbb)

add_executable(search source/main.cpp source/TermsListSearcher.cpp source/DocumentsTier.cpp)
target_link_libraries(search PRIVATE search-share general-indexer)


add_executable(compactor source/compactor/main.cpp)
target_link_libraries(compactor PRIVATE search-share pthread ${CMAKE_BINARY_DIR}/libnamesdb.so)

target_link_options(compactor PRIVATE -rdynamic)

add_library(c-search-abi SHARED source/c_abi.cpp)
target_link_libraries(c-search-abi PRIVATE search-share)


file(GLOB rust-sources rust/webserver/src/*)

if (CMAKE_BUILD_TYPE MATCHES RelWithDebInfo OR CMAKE_BUILD_TYPE MATCHES Release)
    set(CARGO_RELEASE_MODE "--release")
    message("MESSAGE: Using release build " ${rust-build-command})
endif ()
set(rust-build-command $ENV{HOME}/.cargo/bin/cargo build ${CARGO_RELEASE_MODE} --out-dir ${CMAKE_BINARY_DIR} -Z unstable-options)

message("Rust build command: " ${rust-build-command})


add_custom_target(
        rust-server ALL
        COMMAND ${rust-build-command}
        DEPENDS ${CMAKE_SOURCE_DIR}/rust/webserver/src
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/rust
)


add_dependencies(compactor rust-server)
add_dependencies(general-indexer rust-server)
add_dependencies(rust-server c-search-abi)


add_executable(stdinindexer source/StdinIndexer.cpp)
target_include_directories(stdinindexer PRIVATE source/)
target_link_libraries(stdinindexer PRIVATE general-indexer search-share ${CMAKE_BINARY_DIR}/libnamesdb.so)


add_subdirectory(extern/fmt)
target_link_libraries(search-share PUBLIC fmt::fmt)
target_include_directories(search-share PUBLIC extern/fmt/include)

target_link_libraries(c-search-abi PRIVATE fmt::fmt)
target_include_directories(c-search-abi PUBLIC extern/fmt/include)



target_precompile_headers(search-share PUBLIC source/logger.h)
target_precompile_headers(general-indexer PRIVATE source/logger.h)

target_link_libraries(general-indexer PRIVATE fmt::fmt)

add_subdirectory(source/test)
