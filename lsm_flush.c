#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "lsm_flush.h"
#include "lsm_sstable.h"

int lsm_flush_ctx_init(lsm_flush_ctx_t *ctx, const char *dir) {
    memset(ctx, 0, sizeof(*ctx));
    ctx->dir = malloc(strlen(dir) + 1);
    if (!ctx->dir) return -1;
    strcpy(ctx->dir, dir);
    return 0;
}

void lsm_flush_ctx_free(lsm_flush_ctx_t *ctx) {
    if (!ctx) return;
    free(ctx->dir);
    for (int i = 0; i < ctx->l0_count; i++)
        free(ctx->l0_files[i]);
    free(ctx->l0_files);
    memset(ctx, 0, sizeof(*ctx));
}

int lsm_flush(lsm_flush_ctx_t *ctx, lsm_memtable_t *mt, const char *wal_path) {
    // file path: <dir>/L0_<seq>.sst
    char path[512];
    snprintf(path, sizeof(path), "%s/L0_%010llu.sst", ctx->dir, (unsigned long long)ctx->next_seq);

    if (lsm_sstable_write(path, mt) != 0)
        return -1;

    ctx->next_seq++;

    // append to l0_files
    char **new_list = realloc(ctx->l0_files, (ctx->l0_count + 1) * sizeof(char *));
    if (!new_list) return -1;
    ctx->l0_files = new_list;

    ctx->l0_files[ctx->l0_count] = malloc(strlen(path) + 1);
    if (!ctx->l0_files[ctx->l0_count]) return -1;
    strcpy(ctx->l0_files[ctx->l0_count], path);
    ctx->l0_count++;

    // remove WAL
    if (wal_path)
        remove(wal_path);

    return 0;
}