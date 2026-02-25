#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>

#ifdef _WIN32
#include <direct.h>
#define mkdir(path, mode) _mkdir(path)
#else
#include <sys/stat.h>
#include <sys/types.h>
#endif

#include "lsm.h"
#include "lsm_memtable.h"
#include "lsm_wal.h"
#include "lsm_sstable.h"
#include "lsm_flush.h"
#include "lsm_compaction.h"

struct lsm_db {
    char *path;

    lsm_memtable_t memtable;
    lsm_wal_t wal;
    lsm_flush_ctx_t flush_ctx;
    lsm_compaction_ctx_t compact_ctx;

    pthread_mutex_t lock;
};

lsm_db_t *lsm_open(const char *path) {
    lsm_db_t *db = malloc(sizeof(lsm_db_t));
    if (!db) return NULL;
    memset(db, 0, sizeof(*db));

    db->path = malloc(strlen(path) + 1);
    if (!db->path) {
        free(db);
        return NULL;
    }
    strcpy(db->path, path);

    if (mkdir(path, 0755) != 0) {
        if (errno != EEXIST) {
            perror("mkdir");
            goto err_mkdir;
        }
    }

    if (lsm_memtable_init(&db->memtable) != 0)
        goto err_memtable;

    char wal_path[512];
    snprintf(wal_path, sizeof(wal_path), "%s/wal.log", path);

    if (lsm_wal_open(&db->wal, wal_path) != 0)
        goto err_wal;

    // TODO: recover from WAL if exist
    // lsm_wal_recover()

    if (lsm_flush_ctx_init(&db->flush_ctx, path) != 0)
        goto err_flush;

    if (lsm_compaction_ctx_init(&db->compact_ctx, path) != 0)
        goto err_compaction;

    pthread_mutex_init(&db->lock, NULL);

    return db;

err_compaction:
    lsm_flush_ctx_free(&db->flush_ctx);
err_flush:
    lsm_wal_close(&db->wal);
err_wal:
    lsm_memtable_free(&db->memtable);
err_memtable:
err_mkdir:
    free(db->path);
    free(db);
    return NULL;
}

void lsm_close(lsm_db_t *db) {
    if (!db) return;

    pthread_mutex_lock(&db->lock);

    // flush remaning data in memtable
    if (db->memtable.size > 0) {
        char wal_path[512];
        snprintf(wal_path, sizeof(wal_path), "%s/wal.log", db->path);
        lsm_flush(&db->flush_ctx, &db->memtable, wal_path);
    }

    // perform pending compactions
    int lv;
    while ((lv = lsm_should_compact(&db->compact_ctx)) >= 0) {
        lsm_compact(&db->compact_ctx, lv);
    }

    lsm_compaction_ctx_free(&db->compact_ctx);
    lsm_flush_ctx_free(&db->flush_ctx);
    lsm_wal_close(&db->wal);
    lsm_memtable_free(&db->memtable);

    pthread_mutex_unlock(&db->lock);
    pthread_mutex_destroy(&db->lock);

    free(db->path);
    free(db);
}

int lsm_put(lsm_db_t *db, lsm_slice_t key, lsm_slice_t value) {
    pthread_mutex_lock(&db->lock);

    if (lsm_wal_append(&db->wal, key, value, 0) != 0)
        goto err;

    if (lsm_memtable_put(&db->memtable, key, value, 0) != 0)
        goto err;

    if (db->memtable.size >= LSM_FLUSH_THRESHOLD) {
        char wal_path[512];
        snprintf(wal_path, sizeof(wal_path), "%s/wal.log", db->path);

        if (lsm_flush(&db->flush_ctx, &db->memtable, wal_path) != 0)
            goto err;

        char *last_l0 = db->flush_ctx.l0_files[db->flush_ctx.l0_count - 1];
        if (lsm_compaction_add_l0(&db->compact_ctx, last_l0) != 0)
            goto err;

        lsm_memtable_free(&db->memtable);
        if (lsm_memtable_init(&db->memtable) != 0)
            goto err;

        lsm_wal_close(&db->wal);
        if (lsm_wal_open(&db->wal, wal_path) != 0)
            goto err;

        int lv;
        while ((lv = lsm_should_compact(&db->compact_ctx)) >= 0) {
            if (lsm_compact(&db->compact_ctx, lv) != 0)
                goto err;
        }
    }
    
    pthread_mutex_unlock(&db->lock);
    return 0;

err:
    pthread_mutex_unlock(&db->lock);
    return -1;
}

// TODO: lock-free
int lsm_get(lsm_db_t *db, lsm_slice_t key, lsm_slice_t *value_out) {
    pthread_mutex_lock(&db->lock);

    uint8_t deleted;
    int ret = lsm_memtable_get(&db->memtable, key, value_out, &deleted);
    if (ret == 0) {
        pthread_mutex_unlock(&db->lock);
        return deleted ? -1 : 0;
    }

    for (int lv = 0; lv < LSM_MAX_LEVELS; lv++) {
        int count = db->compact_ctx.level_counts[lv];
        for (int i = count - 1; i >= 0; i--) {
            lsm_sstable_t sst;
            if (lsm_sstable_open(&sst, db->compact_ctx.level_files[lv][i]) != 0) {
                pthread_mutex_unlock(&db->lock);
                return -1;
            }

            ret = lsm_sstable_get(&sst, key, value_out, &deleted);
            lsm_sstable_close(&sst);
            if (ret == 0) {
                pthread_mutex_unlock(&db->lock);
                return deleted ? -1 : 0;
            }
        }
    }

    pthread_mutex_unlock(&db->lock);
    return -1;
}

int lsm_delete(lsm_db_t *db, lsm_slice_t key) {
    lsm_slice_t empty = {.data = NULL, .len = 0};

    pthread_mutex_lock(&db->lock);

    if (lsm_wal_append(&db->wal, key, empty, 1) != 0) {
        pthread_mutex_unlock(&db->lock);
        return -1;
    }

    if (lsm_memtable_put(&db->memtable, key, empty, 1) != 0) {
        pthread_mutex_unlock(&db->lock);
        return -1;
    }

    pthread_mutex_unlock(&db->lock);
    return 0;
}
