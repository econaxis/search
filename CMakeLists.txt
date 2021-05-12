cmake_minimum_required(VERSION 3.16)

project("search")
find_package(TBB REQUIRED tbb)

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 20)

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
        source/Constants.cpp
        source/Constants.h source/DocumentsMatcher.cpp source/DocumentsMatcher.h source/dict_strings.h source/ResultsPrinter.cpp source/ResultsPrinter.h source/ContiguousAllocator.cpp source/ContiguousAllocator.h source/TopDocs.cpp source/TopDocs.h source/CustomAllocatedVec.cpp source/CustomAllocatedVec.h)
target_include_directories(search-share PUBLIC source extern/porter2_stemmer extern)
target_include_directories(general-indexer PUBLIC source extern/porter2_stemmer extern)
add_subdirectory(extern/porter2_stemmer)

target_link_libraries(search-share tbb)
target_link_libraries(general-indexer PRIVATE pthread)

add_executable(search source/main.cpp)
target_link_libraries(search PRIVATE search-share general-indexer porter2-stemmer)


add_executable(compactor source/compactor/main.cpp)
target_link_libraries(compactor PRIVATE search-share porter2-stemmer)

add_library(c-search-abi SHARED source/c_abi.cpp)
target_link_libraries(c-search-abi PRIVATE search-share)