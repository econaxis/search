/* mtest.c - memory-mapped database tester/toy */
/*
 * Copyright 2011-2021 Howard Chu, Symas Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

/*
 * API Implementation:
 *      [tag] [data] - tag is a byte representing what type of data it is
 *          possible tags:
 *              u64, u32, string, array, key_reference (same as u64)
 *
 *      Each object (document) has an associated 64 bit ID.
 *
 *     Supports range queries
 *
 *     Document-object model that allows for nesting objects
 *          u64ID/fieldName  - fieldValue
 *          u64ID/fieldName1 - fieldValue1
 *          u64ID/fieldName2 - fieldValue2
 *
 *     Index data model: {
 *          "
 *     }
 */
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "lmdb/lmdb.h"

#define E(expr) CHECK((rc = (expr)) == MDB_SUCCESS, #expr)
#define RES(err, expr) ((rc = expr) == (err) || (CHECK(!rc, #expr), 0))
#define CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
    "%s:%d: %s: %s\n", __FILE__, __LINE__, msg, mdb_strerror(rc)), abort()))

int test() {
    int i = 0, j = 0, rc;
    MDB_env *env;
    MDB_dbi dbi;
    MDB_val key, data;
    MDB_txn *txn;
    MDB_stat mst;
    MDB_cursor *cursor, *cur2;
    MDB_cursor_op op;
    int count;
    int *values;
    char sval[32] = "";

    srand(time(NULL));

    count = (rand() % 384) + 64;
    values = (int *) malloc(count * sizeof(int));

    for (i = 0; i < count; i++) {
        values[i] = rand() % 1024;
    }

    E(mdb_env_create(&env));
    E(mdb_env_set_maxreaders(env, 1));
    E(mdb_env_set_mapsize(env, 10485760));
    E(mdb_env_open(env, "testdb", MDB_FIXEDMAP /*|MDB_NOSYNC*/, 0664));

    E(mdb_txn_begin(env, NULL, 0, &txn));
    E(mdb_dbi_open(txn, NULL, 0, &dbi));

    key.mv_size = sizeof(int);
    key.mv_data = sval;

    printf("Adding %d values\n", count);
    for (i = 0; i < count; i++) {
        sprintf(sval, "%03x %d foo bar", values[i], values[i]);
        /* Set <data> in each iteration, since MDB_NOOVERWRITE may modify it */
        data.mv_size = sizeof(sval);
        data.mv_data = sval;
        if (RES(MDB_KEYEXIST, mdb_put(txn, dbi, &key, &data, MDB_NOOVERWRITE))) {
            j++;
            data.mv_size = sizeof(sval);
            data.mv_data = sval;
        }
    }
    if (j) printf("%d duplicates skipped\n", j);
    E(mdb_txn_commit(txn));
    E(mdb_env_stat(env, &mst));

    E(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
    E(mdb_cursor_open(txn, dbi, &cursor));
    while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
        printf("key: %p %.*s, data: %p %.*s\n",
               key.mv_data, (int) key.mv_size, (char *) key.mv_data,
               data.mv_data, (int) data.mv_size, (char *) data.mv_data);
    }
    CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);

    j = 0;
    key.mv_data = sval;
    for (i = count - 1; i > -1; i -= (rand() % 5)) {
        j++;
        txn = NULL;
        E(mdb_txn_begin(env, NULL, 0, &txn));
        sprintf(sval, "%03x ", values[i]);
        if (RES(MDB_NOTFOUND, mdb_del(txn, dbi, &key, NULL))) {
            j--;
            mdb_txn_abort(txn);
        } else {
            E(mdb_txn_commit(txn));
        }
    }
    free(values);
    printf("Deleted %d values\n", j);

    E(mdb_env_stat(env, &mst));
    E(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
    E(mdb_cursor_open(txn, dbi, &cursor));
    printf("Cursor next\n");
    while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
        printf("key: %.*s, data: %.*s\n",
               (int) key.mv_size, (char *) key.mv_data,
               (int) data.mv_size, (char *) data.mv_data);
    }
    CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
    printf("Cursor last\n");
    E(mdb_cursor_get(cursor, &key, &data, MDB_LAST));
    printf("key: %.*s, data: %.*s\n",
           (int) key.mv_size, (char *) key.mv_data,
           (int) data.mv_size, (char *) data.mv_data);
    printf("Cursor prev\n");
    while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_PREV)) == 0) {
        printf("key: %.*s, data: %.*s\n",
               (int) key.mv_size, (char *) key.mv_data,
               (int) data.mv_size, (char *) data.mv_data);
    }
    CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
    printf("Cursor last/prev\n");
    E(mdb_cursor_get(cursor, &key, &data, MDB_LAST));
    printf("key: %.*s, data: %.*s\n",
           (int) key.mv_size, (char *) key.mv_data,
           (int) data.mv_size, (char *) data.mv_data);
    E(mdb_cursor_get(cursor, &key, &data, MDB_PREV));
    printf("key: %.*s, data: %.*s\n",
           (int) key.mv_size, (char *) key.mv_data,
           (int) data.mv_size, (char *) data.mv_data);

    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);

    printf("Deleting with cursor\n");
    E(mdb_txn_begin(env, NULL, 0, &txn));
    E(mdb_cursor_open(txn, dbi, &cur2));
    for (i = 0; i < 50; i++) {
        if (RES(MDB_NOTFOUND, mdb_cursor_get(cur2, &key, &data, MDB_NEXT)))
            break;
        printf("key: %p %.*s, data: %p %.*s\n",
               key.mv_data, (int) key.mv_size, (char *) key.mv_data,
               data.mv_data, (int) data.mv_size, (char *) data.mv_data);
        E(mdb_del(txn, dbi, &key, NULL));
    }

    printf("Restarting cursor in txn\n");
    for (op = MDB_FIRST, i = 0; i <= 32; op = MDB_NEXT, i++) {
        if (RES(MDB_NOTFOUND, mdb_cursor_get(cur2, &key, &data, op)))
            break;
        printf("key: %p %.*s, data: %p %.*s\n",
               key.mv_data, (int) key.mv_size, (char *) key.mv_data,
               data.mv_data, (int) data.mv_size, (char *) data.mv_data);
    }
    mdb_cursor_close(cur2);
    E(mdb_txn_commit(txn));

    printf("Restarting cursor outside txn\n");
    E(mdb_txn_begin(env, NULL, 0, &txn));
    E(mdb_cursor_open(txn, dbi, &cursor));
    for (op = MDB_FIRST, i = 0; i <= 32; op = MDB_NEXT, i++) {
        if (RES(MDB_NOTFOUND, mdb_cursor_get(cursor, &key, &data, op)))
            break;
        printf("key: %p %.*s, data: %p %.*s\n",
               key.mv_data, (int) key.mv_size, (char *) key.mv_data,
               data.mv_data, (int) data.mv_size, (char *) data.mv_data);
    }
    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);

    mdb_dbi_close(env, dbi);
    mdb_env_close(env);

    return 0;
}

constexpr int mgsa = 3242382;

#include <string>
#include <iostream>
int main() {
    std::cout<<"Starting\n";
    int rc;

    MDB_env *env;
    MDB_dbi dbi;
    MDB_val key, data;
    MDB_txn *txn;

    E(mdb_env_create(&env));
    E(mdb_env_set_maxreaders(env, 1));
    E(mdb_env_set_mapsize(env, 10485760));
    E(mdb_env_open(env, "testdb", MDB_FIXEDMAP /*|MDB_NOSYNC*/, 0664));

    E(mdb_txn_begin(env, NULL, 0, &txn));
    E(mdb_dbi_open(txn, NULL, 0, &dbi));

    std::string test("fdsavcxndjahfeoqhrqpiuqeuregf");
    for (int i = 0; i < 10000; i++) {
        key.mv_size = sizeof(uint32_t);
        uint64_t n = mgsa + i * (i + 10) * 32342347242 % 4000;
        key.mv_size = sizeof(n);
        key.mv_data = (void *) &n;
        data.mv_size = test.size();
        data.mv_data = test.data();
        mdb_put(txn, dbi, &key, &data, MDB_NOOVERWRITE);
    }
    E(mdb_txn_commit(txn));
}