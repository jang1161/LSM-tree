#pragma once
#include <stdint.h>
#include <stdio.h>
#include "lsm.h"
#include "lsm_memtable.h"

/*
 * WAL (Write-Ahead Log) — append-only sequential log, ZNS-friendly.
 *
 * Record format:
 *   type    : uint8_t   (WAL_PUT=1, WAL_DELETE=2)
 *   key_len : uint32_t
 *   key     : bytes
 *   val_len : uint32_t
 *   val     : bytes
 *   crc32   : uint32_t  (covers type + key_len + key + val_len + val)
 */

#define WAL_PUT    1
#define WAL_DELETE 2

typedef struct {
    FILE    *fp;
    char    *path;
} lsm_wal_t;

/* Open (or create) a WAL file. Appends to existing file if present. */
int  lsm_wal_open(lsm_wal_t *wal, const char *path);
void lsm_wal_close(lsm_wal_t *wal);

/* Append a PUT or DELETE record. Flushes to disk immediately. */
int  lsm_wal_append(lsm_wal_t *wal, lsm_slice_t key, lsm_slice_t val, uint8_t deleted);

/* Replay WAL into a MemTable (used on crash recovery).
 * Records with bad CRC are silently skipped (partial write at tail). */
int  lsm_wal_recover(const char *path, lsm_memtable_t *mt);
