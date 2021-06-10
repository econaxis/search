//
// Created by henry on 2021-06-09.
//

#include "Database.h"
#include <lmdb/lmdb.h>
#include <cassert>


#define E(expr) CHECK((rc = (expr)) == MDB_SUCCESS, #expr)
#define CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
    "%s:%d: %s: %s\n", __FILE__, __LINE__, msg, mdb_strerror(rc)), abort()))

int rc;


Database::Database(std::string &filename) {
    E(mdb_env_create(&env));
    E(mdb_env_set_maxreaders(env, 1));
    E(mdb_env_set_mapsize(env, 10485760));
    E(mdb_env_open(env, "testdb", MDB_FIXEDMAP /*|MDB_NOSYNC*/, 0664));

    MDB_txn *temptxn;
    E(mdb_txn_begin(env, nullptr, 0, &temptxn));
    E(mdb_dbi_open(temptxn, nullptr, 0, &dbi));

    mdb_txn_abort(temptxn);
}

