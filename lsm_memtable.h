#pragma once
#include "lsm.h"

/*
 * MemTable — skip list backed in-memory write buffer.
 * Keys are sorted; duplicate keys are updated in-place.
 * deleted=1 entries are tombstones that shadow older SSTable versions.
 */

typedef struct lsm_skipnode {
    lsm_slice_t key;
    lsm_slice_t value;
    uint8_t     deleted;
    struct lsm_skipnode *forward[0];
} lsm_skipnode_t;

typedef struct {
    lsm_skipnode_t *head;
    size_t          max_level;
    size_t          size;       /* number of entries stored */
    uint32_t        rand_seed;
} lsm_memtable_t;

/* Initialize an empty MemTable. Returns 0 on success, -1 on failure. */
int lsm_memtable_init(lsm_memtable_t *mt);

/* Free all memory owned by the MemTable. */
void lsm_memtable_free(lsm_memtable_t *mt);

/* Insert or update a key. Set deleted=1 for a tombstone, 0 for a normal PUT.
 * Copies key and value internally. Returns 0 on success, -1 on failure. */
int lsm_memtable_put(lsm_memtable_t *mt, lsm_slice_t key, lsm_slice_t value, uint8_t deleted);

/* Look up a key. Returns 0 if found (including tombstones), -1 if not found.
 * On success, value_out->data is a heap-allocated copy the caller must free
 * (NULL if it is a tombstone); deleted_out is set to 1 for tombstones. */
int lsm_memtable_get(lsm_memtable_t *mt, lsm_slice_t key, lsm_slice_t *value_out, uint8_t *deleted_out);