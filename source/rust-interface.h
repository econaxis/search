//
// Created by henry on 2021-05-13.
//

#ifndef GAME_RUST_INTERFACE_H
#define GAME_RUST_INTERFACE_H

using NamesDatabase = void;
using RustVec = void;

extern "C" NamesDatabase* new_name_database(const char* name);

extern "C" bool search_name_database(const NamesDatabase* ndb, const char *key);
extern "C" void drop_name_database(NamesDatabase* ndb);
extern "C" void fill_rust_vec (RustVec*, void* data, std::size_t size);

#endif //GAME_RUST_INTERFACE_H
