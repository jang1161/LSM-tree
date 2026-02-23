#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include "lsm_compaction.h"

/*--------------------------- helpers ---------------------------*/

int lsm_level_capacity(int level) {
    if (level == 0)
        return LSM_L0_MAX_FILES;

    int capacity = LSM_L0_MAX_FILES;
    for (int i = 0; i < level; i++)
        capacity *= 4;

    return capacity;
}

// for qsort
static int cmp_paths(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

static int parse_filename(const char *name, int *lv_out, uint64_t *seq_out) {
    if (name[0] != 'L') return -1;

    char *end1, *end2;
    long lv = strtol(name + 1, &end1, 10);
    if (*end1 != '_') return -1;

    unsigned long long seq = strtoull(end1 + 1, &end2, 10);
    if (strcmp(end2, ".sst") != 0) return -1;

    *lv_out = (int)lv;
    *seq_out = (uint64_t)seq;
    return 0;
}

/*--------------------------- context ---------------------------*/

int lsm_compaction_ctx_init(lsm_compaction_ctx_t *ctx, const char *dir) {
    memset(ctx, 0, sizeof(*ctx));

    ctx->dir = malloc(strlen(dir) + 1);
    if (!ctx->dir) return -1;
    strcpy(ctx->dir, dir);

    DIR *d = opendir(dir);
    if (!d) {
        free(ctx->dir);
        return -1;
    }

    struct dirent *entry;
    uint64_t max_seq = 0;

    while ((entry = readdir(d)) != NULL) {
        int level;
        uint64_t seq;

        if (parse_filename(entry->d_name, &level, &seq) != 0)
            continue;

        if (level < 0 || level >= LSM_MAX_LEVELS)
            continue;

        if (seq >= max_seq)
            max_seq = seq + 1;

        char **new_list = realloc(ctx->level_files[level], (ctx->level_counts[level] + 1) * sizeof(char *));
        if (!new_list) {
            closedir(d);
            lsm_compaction_ctx_free(ctx);
            return -1;
        }
        ctx->level_files[level] = new_list;

        char path[512];
        snprintf(path, sizeof(path), "%s/%s", dir, entry->d_name);

        ctx->level_files[level][ctx->level_counts[level]] = malloc(strlen(path) + 1);
        if (!ctx->level_files[level][ctx->level_counts[level]]) {
            closedir(d);
            lsm_compaction_ctx_free(ctx);
            return -1;
        }
        strcpy(ctx->level_files[level][ctx->level_counts[level]], path);
        ctx->level_counts[level]++;
    }

    closedir(d);
    ctx->next_seq = max_seq;

    // sort
    for (int i = 0; i < LSM_MAX_LEVELS; i++)
        if (ctx->level_counts[i] > 0)
            qsort(ctx->level_files[i], ctx->level_counts[i], sizeof(char *), cmp_paths);

    return 0;
}

void lsm_compaction_ctx_free(lsm_compaction_ctx_t *ctx) {
    if (!ctx) return;

    free(ctx->dir);

    for (int i = 0; i < LSM_MAX_LEVELS; i++) {
        for (int j = 0; j < ctx->level_counts[i]; j++)
            free(ctx->level_files[i][j]);
        free(ctx->level_files[i]);
    }

    memset(ctx, 0, sizeof(*ctx));
}

int lsm_compaction_add_l0(lsm_compaction_ctx_t *ctx, const char *path) {
    char **new_list = realloc(ctx->level_files[0], (ctx->level_counts[0] + 1) * sizeof(char *));
    if (!new_list) return -1;
    ctx->level_files[0] = new_list;

    ctx->level_files[0][ctx->level_counts[0]] = malloc(strlen(path) + 1);
    if (!ctx->level_files[0][ctx->level_counts[0]]) return -1;
    strcpy(ctx->level_files[0][ctx->level_counts[0]], path);
    ctx->level_counts[0]++;

    return 0;
}

/*--------------------------- compaction ---------------------------*/

int lsm_should_compact(lsm_compaction_ctx_t *ctx) {
    for (int lv = 0; lv < LSM_MAX_LEVELS; lv++) {
        if (ctx->level_counts[lv] >= lsm_level_capacity(lv))
            return lv;
    }
    return -1;
}

// merge iterator for multiple SSTs
typedef struct {
    lsm_sstable_iter_t sst_it;
    lsm_slice_t key;
    lsm_slice_t val;
    uint8_t deleted;
    int valid; // 0 = EOF, 1 = has data
    int file_idx;
} merge_iter_t;

static int merge_iter_init(merge_iter_t *mi, const char *path, int file_idx) {
    mi->file_idx = file_idx;
    mi->valid = 0;

    if (lsm_sstable_iter_open(&mi->sst_it, path) != 0)
        return -1;

    int ret = lsm_sstable_iter_next(&mi->sst_it, &mi->key, &mi->val, &mi->deleted);
    if (ret == 0) {
        mi->valid = 1;
        return 0;
    } else if (ret == 1) { // empty SST
        lsm_sstable_iter_close(&mi->sst_it);
        return 0;
    }

    lsm_sstable_iter_close(&mi->sst_it);
    return -1;
}

static int merge_iter_next(merge_iter_t *mi) {
    if (!mi->valid) return 1; // EOF

    free(mi->key.data);
    free(mi->val.data);

    int ret = lsm_sstable_iter_next(&mi->sst_it, &mi->key, &mi->val, &mi->deleted);
    if (ret == 0) { // success
        return 0;
    } else if (ret == 1) { // EOF
        mi->valid = 0;
        lsm_sstable_iter_close(&mi->sst_it);;
        return 1;
    }

    mi->valid = 0;
    lsm_sstable_iter_close(&mi->sst_it);
    return -1; // error
}

static void merge_iter_close(merge_iter_t *mi) {
    if (mi->valid) {
        free(mi->key.data);
        free(mi->val.data);
        lsm_sstable_iter_close(&mi->sst_it);
        mi->valid = 0;
    }
}

static int slice_cmp(lsm_slice_t a, lsm_slice_t b) {
    size_t min = a.len < b.len ? a.len : b.len;
    int r = memcmp(a.data, b.data, min);
    if (r != 0) return r;
    if (a.len < b.len) return -1;
    if (a.len > b.len) return 1;
    return 0;
}

int lsm_compact(lsm_compaction_ctx_t *ctx, int lv) {
    if (lv < 0 || lv >= LSM_MAX_LEVELS - 1)
        return -1;

    int src_cnt = ctx->level_counts[lv];
    if (src_cnt == 0) return 0;

    // open all source SSTs
    merge_iter_t *iters = malloc(src_cnt * sizeof(merge_iter_t));
    if (!iters) return -1;

    for (int i = 0; i < src_cnt; i++) {
        if (merge_iter_init(&iters[i], ctx->level_files[lv][i], i) != 0) {
            for (int j = 0; j < i; j++)
                merge_iter_close(&iters[j]);
            free(iters);
            return -1;
        }
    }

    // create output SST path
    char out_path[512];
    snprintf(out_path, sizeof(out_path), "%s/L%d_%010llu.sst",
        ctx->dir, lv + 1, (unsigned long long)ctx->next_seq);
    ctx->next_seq++;

    // temp memtable for merge
    lsm_memtable_t mt;
    lsm_memtable_init(&mt);

    // find min key repeatedly
    int active_cnt = src_cnt;

    while (active_cnt > 0) {
        int min_idx = -1;
        for (int i = 0; i < src_cnt; i++) {
            if (!iters[i].valid) continue;
            if (min_idx == -1) {
                min_idx = i;
                continue;
            }

            int cmp = slice_cmp(iters[i].key, iters[min_idx].key);
            if (cmp < 0) {
                min_idx = i;
            } else if (cmp == 0) { // higher-priority on latest file
                if (iters[i].file_idx > iters[min_idx].file_idx)
                    min_idx = i;
            }
        }

        if (min_idx == -1) break;

        // add to memtable
        lsm_slice_t key = iters[min_idx].key;
        lsm_slice_t val = iters[min_idx].val;
        uint8_t del = iters[min_idx].deleted;

        lsm_memtable_put(&mt, key, val, del);

        // advance iterators
        for (int i = 0; i < src_cnt; i++) {
            if (!iters[i].valid) continue;
            if (slice_cmp(iters[i].key, key) == 0) {
                int ret = merge_iter_next(&iters[i]);
                if (ret == 1) active_cnt--;
                else if (ret < 0) { // error
                    for (int j = 0; j < src_cnt; j++)
                        merge_iter_close(&iters[j]);
                    free(iters);
                    lsm_memtable_free(&mt);
                    return -1;
                }
            }
        }
    }

    // close all iters
    for (int i = 0; i < src_cnt; i++)
        merge_iter_close(&iters[i]);
    free(iters);

    // write memtable to new SST
    if (lsm_sstable_write(out_path, &mt) != 0) {
        lsm_memtable_free(&mt);
        return -1;
    }
    lsm_memtable_free(&mt);

    // delete old SSTs
    for (int i = 0; i < src_cnt; i++) {
        remove(ctx->level_files[lv][i]);
        free(ctx->level_files[lv][i]);
    }
    free(ctx->level_files[lv]);
    ctx->level_files[lv] = NULL;
    ctx->level_counts[lv] = 0;

    // add new SST to next lv
    char **new_list = realloc(ctx->level_files[lv + 1], (ctx->level_counts[lv + 1] + 1) * sizeof(char *));
    if (!new_list) return -1;
    ctx->level_files[lv + 1] = new_list;

    ctx->level_files[lv + 1][ctx->level_counts[lv + 1]] = malloc(strlen(out_path) + 1);
    if (!ctx->level_files[lv + 1][ctx->level_counts[lv + 1]])
        return -1;
    strcpy(ctx->level_files[lv + 1][ctx->level_counts[lv + 1]], out_path);
    ctx->level_counts[lv + 1]++;

    return 0;
}
