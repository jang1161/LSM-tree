#pragma once
#include "lsm.h"

typedef struct lsm_skipnode {
    lsm_slice_t key;
    lsm_slice_t value;
    uint8_t     deleted;
    struct lsm_skipnode *forward[0];
} lsm_skipnode_t;

typedef struct {
    lsm_skipnode_t *head;
    size_t          max_level;
    size_t          size;
    uint32_t        rand_seed;
} lsm_memtable_t;

int lsm_memtable_init(lsm_memtable_t *mt);
void lsm_memtable_free(lsm_memtable_t *mt);

int lsm_memtable_put(lsm_memtable_t *mt, lsm_slice_t key, lsm_slice_t vlaue, uint8_t deleted);
int lsm_memtable_get(lsm_memtable_t *mt, lsm_slice_t key, lsm_slice_t *value_out, uint8_t *deleted_out);