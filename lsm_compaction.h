#pragma once
#include <stddef.h>
#include <stdint.h>
#include "lsm_sstable.h"

/*
 * Compaction: Merge SSTables between levels
 *
 * Strategy: Tiering (Write-optimized for ZNS SSD)
 *   - Each level accumulates multiple SSTables
 *   - L0: max 4 files
 *   - Ln: max capacity = LSM_L0_MAX_FILES * 4^n
 *     e.g. L1=16, L2=64, L3=256, ...
 *   - When a level is full, merge ALL files to next level
 *
 * Compaction flow:
 *   1. L0 reaches 4 files → merge all 4 L0 files → new L1 file(s)
 *   2. L1 reaches 16 files → merge all 16 L1 files → new L2 file(s)
 *   3. Repeat for higher levels
 *
 * Key characteristics (vs Leveling):
 *   - Files within same level may have overlapping key ranges
 *   - Lower write amplification (merge entire level at once, less frequently)
 *   - Better for write-heavy workloads and ZNS SSD
 *
 * ZNS optimization:
 *   - Same-level SSTables allocated in same zone
 *   - Entire zone invalidated/rewritten at once during merge
 *   - Minimizes zone fragmentation and write amplification
 */

#define LSM_L0_MAX_FILES    4
#define LSM_MAX_LEVELS      7  /* L0..L6 */

typedef struct {
    char    *dir;       /* SSTable directory */
    uint64_t next_seq;  /* monotonically increasing sequence number */

    /* Per-level SSTable file lists */
    char   **level_files[LSM_MAX_LEVELS];
    int      level_counts[LSM_MAX_LEVELS];
} lsm_compaction_ctx_t;

/* Initialize compaction context.
 * Scans directory for existing SSTable files and organizes them by level. */
int  lsm_compaction_ctx_init(lsm_compaction_ctx_t *ctx, const char *dir);

/* Free compaction context resources. */
void lsm_compaction_ctx_free(lsm_compaction_ctx_t *ctx);

/* Check if compaction is needed at any level.
 * Returns the level number that needs compaction, or -1 if none. */
int  lsm_should_compact(lsm_compaction_ctx_t *ctx);

/* Compact a specific level to the next level.
 * level: source level (0-based, e.g., 0 for L0 → L1, 1 for L1 → L2)
 * Returns 0 on success, -1 on error. */
int  lsm_compact(lsm_compaction_ctx_t *ctx, int level);

/* Add a new L0 SSTable file to tracking (called after flush).
 * path: full path to the new L0 file */
int  lsm_compaction_add_l0(lsm_compaction_ctx_t *ctx, const char *path);

/* Get capacity for a given level.
 * level: 0-based level number
 * Returns max number of files for that level. */
int  lsm_level_capacity(int level);
