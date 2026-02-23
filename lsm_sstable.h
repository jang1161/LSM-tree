#pragma once
#include <stdint.h>
#include <stdio.h>
#include "lsm.h"
#include "lsm_memtable.h"

/*
 * SSTable on-disk layout:
 *
 *   [Data Section]
 *     Entry: key_len(4B) | key | val_len(4B) | val | deleted(1B)
 *     ...
 *
 *   [Index Section]
 *     IndexEntry: key_len(4B) | key | offset(8B)
 *     ...
 *
 *   [Footer — 24 bytes, always at end of file]
 *     index_offset : uint64_t
 *     entry_count  : uint64_t
 *     magic        : uint32_t  = LSM_SSTABLE_MAGIC
 *     _pad         : uint32_t
 */

#define LSM_SSTABLE_MAGIC 0x4C534D54u  /* 'LSMT' */

typedef struct {
    FILE        *fp;
    char        *path;
    uint64_t     entry_count;
    /* in-memory index loaded on open */
    uint64_t    *offsets;
    lsm_slice_t *keys;
} lsm_sstable_t;

typedef struct {
    FILE     *fp;
    uint64_t  remaining;
} lsm_sstable_iter_t;

/* Write a MemTable to a new SSTable file. */
int  lsm_sstable_write(const char *path, lsm_memtable_t *mt);

/* Open an existing SSTable for point lookups (loads index into memory). */
int  lsm_sstable_open(lsm_sstable_t *sst, const char *path);
void lsm_sstable_close(lsm_sstable_t *sst);

/* Point lookup. Returns 0 on found (including tombstone), -1 on not found/error.
 * Caller must free out->data when deleted_out==0. */
int  lsm_sstable_get(lsm_sstable_t *sst, lsm_slice_t key,
                     lsm_slice_t *out, uint8_t *deleted_out);

/* Sequential iterator (used by compaction and flush). */
int  lsm_sstable_iter_open(lsm_sstable_iter_t *it, const char *path);
/* Returns 0 on success, 1 at EOF, -1 on error.
 * Caller must free key.data and val.data after each successful call. */
int  lsm_sstable_iter_next(lsm_sstable_iter_t *it,
                            lsm_slice_t *key, lsm_slice_t *val,
                            uint8_t *deleted_out);
void lsm_sstable_iter_close(lsm_sstable_iter_t *it);
