#pragma once
#include <stddef.h>
#include <stdint.h>

typedef struct {
    void *data;
    size_t len;
} lsm_slice_t;

typedef struct lsm_db lsm_db_t;

lsm_db_t *lsm_open(const char *path);
void lsm_close(lsm_db_t *db);

int lsm_put(lsm_db_t *db, lsm_slice_t key, lsm_slice_t value);
int lsm_get(lsm_db_t *db, lsm_slice_t key, lsm_slice_t *value_out);
int lsm_delete(lsm_db_t *db, lsm_slice_t key);