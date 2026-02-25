#pragma once
#include <stddef.h>
#include <stdint.h>

typedef struct {
    void *data;
    size_t len;
} lsm_slice_t;

typedef struct lsm_db lsm_db_t;

lsm_db_t *lsm_open(const char *path);
void      lsm_close(lsm_db_t *db);

/* Returns 0 on success, -1 on failure. */
int lsm_put(lsm_db_t *db, lsm_slice_t key, lsm_slice_t value);

/* Returns 0 on success; value_out->data is heap-allocated, caller must free.
 * Returns -1 if not found or on failure. */
int lsm_get(lsm_db_t *db, lsm_slice_t key, lsm_slice_t *value_out);

/* Returns 0 on success, -1 on failure. */
int lsm_delete(lsm_db_t *db, lsm_slice_t key);