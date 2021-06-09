cmake_minimum_required(VERSION 3.16)

project("kvdb")
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 20)

add_library(lmdb SHARED lmdb/mdb.c lmdb/lmdb.h lmdb/midl.c lmdb/midl.h)
target_link_libraries(lmdb PRIVATE pthread)

add_executable(mdb_dump lmdb/mdb_dump.c)
target_link_libraries(mdb_dump PRIVATE lmdb)


add_executable(kvdb source/main.cpp)
target_link_libraries(kvdb PRIVATE lmdb)
target_include_directories(kvdb PRIVATE . source)