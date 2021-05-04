cmake_minimum_required(VERSION 3.16)

project("search")
find_package(TBB REQUIRED tbb)

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 20)

add_compile_options(-fconcepts)

add_library(search-share SHARED "source/DocumentPositionPointer.cpp"
        "source/DocIDFilePair.cpp"
        "source/Serializer.h"
        "source/Tokenizer.cpp"
        "source/DocumentPositionPointer.h"
        "source/WordIndexEntry.cpp"
        "source/WordIndexEntry.h"
        "source/Serializer.cpp"
        "source/GeneralIndexer.cpp"
        "source/DocIDFilePair.h"
        "source/Tokenizer.h"
        "source/SortedKeysIndex.h"
        "source/SortedKeysIndex.cpp"
        "source/compactor/Compactor.cpp"
        "source/compactor/Compactor.h" source/SortedKeysIndexStub.cpp source/SortedKeysIndexStub.h
        source/Constants.cpp
        source/Constants.h source/DocumentsMatcher.cpp source/DocumentsMatcher.h source/dict_strings.h)
target_include_directories(search-share PUBLIC source)
target_include_directories(search-share PUBLIC extern/porter2_stemmer extern)
target_link_libraries(search-share PRIVATE tbb)
add_subdirectory(extern/porter2_stemmer)

target_link_libraries(search-share PRIVATE porter2-stemmer)


add_executable(search source/main.cpp)
target_link_libraries(search PRIVATE tbb search-share)


add_executable(compactor source/compactor/main.cpp)
target_link_libraries(compactor PRIVATE tbb search-share)

