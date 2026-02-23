#include <stdlib.h>
#include <string.h>
#include "lsm_sstable.h"

/*--------------------------- Helpers ---------------------------*/
static int write_u32(FILE *fp, uint32_t w) {
    return fwrite(&w, 4, 1, fp) == 1 ? 0 : -1;
}

static int write_u64(FILE *fp, uint64_t w) {
    return fwrite(&w, 8, 1, fp) == 1 ? 0 : -1;
}

static int write_slice(FILE *fp, lsm_slice_t s) {
    if (write_u32(fp, s.len) != 0) return -1;
    if (s.len > 0 && fwrite(s.data, 1, s.len, fp) != s.len) return -1;
    return 0;
}

static int read_u32(FILE *fp, uint32_t *r) {
    return fread(r, 4, 1, fp) == 1 ? 0 : -1;
}

static int read_u64(FILE *fp, uint64_t *r) {
    return fread(r, 8, 1, fp) == 1 ? 0 : -1;
}

static int read_slice(FILE *fp, lsm_slice_t *s) {
    uint32_t len;
    if (read_u32(fp, &len) != 0) return -1;
    s->len = len;
    if (len == 0) {
        s->data = NULL;
        return 0;
    }

    s->data = malloc(len);
    if (!s->data) return -1;

    if (fread(s->data, 1, len, fp) != len) {
        free(s->data);
        s->data = NULL;
        return -1;
    }

    return 0;
}

static int slice_cmp(lsm_slice_t a, lsm_slice_t b) {
    size_t min = a.len < b.len ? a.len : b.len;
    int r = memcmp(a.data, b.data, min);
    
    if (r != 0) return r;
    if (a.len < b.len) return -1;
    if (a.len > b.len) return 1;
    return 0;
}

/*--------------------------- Write ---------------------------*/
int lsm_sstable_write(const char *path, lsm_memtable_t *mt) {
    FILE *fp = fopen(path, "wb");
    if (!fp) return -1;

    uint64_t entry_count = mt->size;

    if (entry_count == 0) {
        uint64_t idx_offset = (uint64_t)ftell(fp);
        write_u64(fp, idx_offset);
        write_u64(fp, 0);
        write_u32(fp, LSM_SSTABLE_MAGIC);
        write_u32(fp, 0);
        fclose(fp);
        return 0;
    }

    uint64_t *offsets = malloc(entry_count * sizeof(uint64_t));
    lsm_slice_t *keys = malloc(entry_count * sizeof(lsm_slice_t));
    if (!offsets || !keys) {
        free(offsets);
        free(keys);
        fclose(fp);
        return -1;
    }

    // data section
    uint64_t idx = 0;
    lsm_skipnode_t *node = mt->head->forward[0];
    
    while (node) {
        offsets[idx] = (uint64_t)ftell(fp);

        keys[idx].data = node->key.data;
        keys[idx].len = node->key.len;

        if (write_slice(fp, node->key) != 0) goto err;
        if (write_slice(fp, node->value) != 0) goto err;
        uint8_t del = node->deleted;
        if (fwrite(&del, 1, 1, fp) != 1) goto err;

        idx++;
        node = node->forward[0];
    }

    // index section
    uint64_t index_offset = (uint64_t)ftell(fp);
    for (uint64_t i = 0; i < entry_count; i++) {
        if (write_slice(fp, keys[i]) != 0) goto err;
        if (write_u64(fp, offsets[i]) !=0) goto err;
    }

    // footer
    if (write_u64(fp, index_offset) != 0) goto err;
    if (write_u64(fp, entry_count) != 0) goto err;
    if (write_u32(fp, LSM_SSTABLE_MAGIC) != 0) goto err;
    if (write_u32(fp, 0) != 0) goto err;

    free(offsets);
    free(keys);
    fclose(fp);
    
    return 0;

err:
    free(offsets);
    free(keys);
    fclose(fp);
    
    return -1;
}

/*--------------------------- Open ---------------------------*/
int lsm_sstable_open(lsm_sstable_t *sst, const char *path) {
    memset(sst, 0, sizeof(*sst));

    sst->fp = fopen(path, "rb");
    if (!sst->fp) return -1;
    
    sst->path = malloc(strlen(path) + 1); // +1 왜??
    if (!sst->path) goto err;
    strcpy(sst->path, path);

    // read footer
    if (fseek(sst->fp, -24, SEEK_END) != 0) goto err;
    uint64_t index_offset, entry_count;
    uint32_t magic, pad;

    if (read_u64(sst->fp, &index_offset) != 0) goto err;
    if (read_u64(sst->fp, &entry_count) != 0) goto err;
    if (read_u32(sst->fp, &magic) != 0) goto err;
    if (read_u32(sst->fp, &pad) != 0) goto err;
    if(magic != LSM_SSTABLE_MAGIC) goto err;

    sst->entry_count = entry_count;

    
    // load index section into memory
    if (fseek(sst->fp, (long)index_offset, SEEK_SET) != 0) goto err;

    sst->offsets = malloc(entry_count * sizeof(uint64_t));
    sst->keys = malloc(entry_count * sizeof(lsm_slice_t));
    if (!sst->offsets || !sst->keys) goto err;

    for (uint64_t i = 0; i < entry_count; i++) {
        if (read_slice(sst->fp, &sst->keys[i]) != 0) goto err;
        if (read_u64(sst->fp, &sst->offsets[i]) != 0) goto err;
    }

    return 0;

err:
    lsm_sstable_close(sst);
    return -1;
}

/*--------------------------- Close ---------------------------*/
void lsm_sstable_close(lsm_sstable_t *sst) {
    if (!sst) return;
    if (sst->fp) {
        fclose(sst->fp);
        sst->fp = NULL;
    }
    if (sst->path) {
        free(sst->path);
        sst->path = NULL;
    }
    if (sst->offsets) {
        free(sst->offsets);
        sst->offsets = NULL;
    }
    if (sst->keys) {
        for (uint64_t i = 0; i < sst->entry_count; i++)
            free(sst->keys[i].data);
        free(sst->keys);
        sst->keys = NULL;
    }
    sst->entry_count = 0;
}

/*--------------------------- Point lookup ---------------------------*/
int  lsm_sstable_get(lsm_sstable_t *sst, lsm_slice_t key, lsm_slice_t *out, uint8_t *deleted_out) {
    if (!sst || sst->entry_count == 0)
        return -1;

    // binary search
    int64_t lo = 0, hi = (int64_t)sst->entry_count - 1;
    int64_t found_idx = -1;

    while (lo <= hi) {
        int64_t mid = (lo + hi) / 2;
        int cmp = slice_cmp(key, sst->keys[mid]);
        if (cmp == 0) {
            found_idx = mid;
            break;
        }
        else if (cmp < 0) hi = mid - 1;
        else lo = mid + 1;
    }

    if (found_idx < 0)
        return -1;

    // read data
    if (fseek(sst->fp, (long)sst->offsets[found_idx], SEEK_SET) != 0) 
        return -1;

    lsm_slice_t k, v;
    uint8_t del;

    if (read_slice(sst->fp, &k) != 0)
        return -1;
    if (read_slice(sst->fp, &v) != 0) {
        free(k.data);
        return -1;
    }
    if (fread(&del, 1, 1, sst->fp) != 1) {
        free(k.data);
        free(v.data);
        return -1;
    }
    free(k.data);

    if (deleted_out)
        *deleted_out = del;

    if (out) {
        if (del) {
            free(v.data);
            out->data = NULL;
            out->len = 0;
        }
        else *out = v;
    } else free(v.data);

    return 0;
}

/*--------------------------- Iterator ---------------------------*/
int  lsm_sstable_iter_open(lsm_sstable_iter_t *it, const char *path) {
    memset(it, 0, sizeof(*it));

    // read entry_count
    FILE *fp = fopen(path, "rb");
    if (!fp) return -1;
    if (fseek(fp, -24, SEEK_END) != 0) {
        fclose(fp);
        return -1;
    }

    uint64_t index_offset, entry_count;
    uint32_t magic, pad;

    if (read_u64(fp, &index_offset) != 0) goto err;
    if (read_u64(fp, &entry_count) != 0) goto err;
    if (read_u32(fp, &magic) != 0) goto err;
    if (read_u32(fp, &pad) != 0) goto err;
    if (magic != LSM_SSTABLE_MAGIC) goto err;

    if (fseek(fp, 0, SEEK_SET) != 0) goto err;
    it->fp = fp;
    it->remaining = entry_count;
    
    return 0;

err:
    fclose(fp);
    return -1;
}

int  lsm_sstable_iter_next(lsm_sstable_iter_t *it, lsm_slice_t *key, 
    lsm_slice_t *val, uint8_t *deleted_out) {
    if (!it->fp || it->remaining == 0)
        return 1; // EOF
    
    if (read_slice(it->fp, key) != 0) 
        return -1;
    if (read_slice(it->fp, val) != 0) {
        free(key->data);
        return -1;
    }
    uint8_t del;
    if (fread(&del, 1, 1, it->fp) != 1) {
        free(key->data);
        free(val->data);
        return -1;
    }
    if (deleted_out)
        *deleted_out = del;

    it->remaining--;
    return 0;
}

void lsm_sstable_iter_close(lsm_sstable_iter_t *it) {
    if (it && it->fp) {
        fclose(it->fp);
        it->fp = NULL;
    }
}