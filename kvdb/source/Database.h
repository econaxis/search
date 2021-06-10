//
// Created by henry on 2021-06-09.
//

#ifndef GAME_DATABASE_H
#define GAME_DATABASE_H

#include <string>
#include <lmdb/lmdb.h>
#include <stdexcept>
#include <vector>


class Database {
    MDB_env *env;
    MDB_dbi dbi;

    enum class Tag : char {
        STR = 0x1,
        UINT64 = 0X2,
        ARRAY = 0x3
    };

    struct TagData {
        Tag tag;
        MDB_val data;
    };


    void assert(bool a) {
        if (!a) {
            throw std::runtime_error("Assertion failed\n");
        }
    }

public:

    Database(std::string &filename);

    template<typename T>
    T get_key(std::string &key) {
        throw std::runtime_error("Unspecialized");
    }


    void put_key(std::string &key, uint64_t value);

    void put_key(std::string &key, std::string &value);
};


MDB_val to_mdb_val(std::string &s) {
    return MDB_val{
            .mv_size = s.size(),
            .mv_data = s.data()
    };
}


template<>
Database::TagData Database::get_key(std::string &key) {
    MDB_txn *txn;
    mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);

    MDB_val mdbkey = to_mdb_val(key), mdbdata;

    mdb_get(txn, dbi, &mdbkey, &mdbdata);

    char *tag_ptr = static_cast<char *>(mdbdata.mv_data);
    char *data_ptr = static_cast<char *>(mdbdata.mv_data) + 1;


    Tag tag = static_cast<Tag>(*tag_ptr);

    if (tag == Tag::UINT64) {
        assert(mdbdata.mv_size == sizeof(uint64_t) + 1);
    } else if (tag == Tag::STR) {
        auto str_length = *reinterpret_cast<uint64_t *>(data_ptr);
        assert(mdbdata.mv_size == 1 + sizeof(str_length) + str_length);
    } else if (tag == Tag::ARRAY) {

    }

    mdbdata.mv_data = (void *) data_ptr;
    return {tag, mdbdata};
}

template<>
uint64_t Database::get_key(std::string &key) {
    auto[tag, mdbdata] = get_key<TagData>(key);
    auto data = (char *) mdbdata.mv_data;

    assert(tag == Tag::UINT64);
    return *reinterpret_cast<uint64_t *>(data);
}

template<>
std::string Database::get_key(std::string &key) {
    auto[tag, mdbdata] = get_key<TagData>(key);
    auto data = (char *) mdbdata.mv_data;

    assert(tag == Tag::STR);

    auto str_length = *reinterpret_cast<uint64_t *>(data);
    data += sizeof(str_length);

    std::string a(data, data + str_length);
    return a;
}

template<>
std::vector<std::string> Database::get_key(std::string &key) {
    auto[tag, mdbdata] = get_key<TagData>(key);
    auto data = (char *) mdbdata.mv_data;

    assert(tag == Tag::ARRAY);

    auto veclength = *reinterpret_cast<uint64_t *>(data);
    data += sizeof(veclength);

    std::vector<std::string> ret;
    while(veclength--) {
        auto str_length = *reinterpret_cast<uint64_t *>(data);
        data += sizeof(str_length);

        ret.emplace_back(data, data + str_length);
        data += str_length;
    }
    return ret;
}

#endif //GAME_DATABASE_H
