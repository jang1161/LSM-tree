#pragma once
#include <stddef.h>
#include "lsm_memtable.h"
#include "lsm_wal.h"

/*
 * Flush: MemTable -> L0 SSTable
 *
 * Compaction strategy: Tiering
 *   - Each level accumulates SSTables; merge to next level when full.
 *   - L0 capacity : LSM_L0_MAX_FILES (4)
 *   - Ln capacity : LSM_L0_MAX_FILES * 4^n
 *     e.g. L1=16, L2=64, L3=256, ...
 *
 * SSTable file naming:
 *   <dir>/L<level>_<seq>.sst   (seq is a monotonically increasing sequence number)
 */

#define LSM_FLUSH_THRESHOLD (64 * 1024 * 1024)  /* 64 MB — flush when MemTable exceeds this */
#define LSM_L0_MAX_FILES    4                    /* L0 capacity; Ln = L0 * 4^n */

typedef struct {
    char    *dir;       /* directory where SSTable files are stored */
    uint64_t next_seq;  /* monotonically increasing sequence number for new files */

    /* L0 SSTable file list (oldest -> newest) */
    char   **l0_files;
    int      l0_count;
} lsm_flush_ctx_t;

int  lsm_flush_ctx_init(lsm_flush_ctx_t *ctx, const char *dir);
void lsm_flush_ctx_free(lsm_flush_ctx_t *ctx);

/* Flush a MemTable to a new L0 SSTable file.
 * If wal_path is non-NULL, the WAL file is removed after a successful flush.
 * On success, appends the new file path to l0_files and returns 0. */
int lsm_flush(lsm_flush_ctx_t *ctx, lsm_memtable_t *mt, const char *wal_path);
