#include <stdlib.h>
#include <string.h>
#include "lsm_wal.h"

/*--------------------------- CRC32 (IEEE 802.3) ---------------------------*/

static uint32_t crc32_update(uint32_t crc, const void *buf, size_t len) {
    // generate lookup table on first call
    static uint32_t table[256];
    static int table_ready = 0;
    if (!table_ready) {
        for (int i = 0; i < 256; i++) {
            uint32_t v = (uint32_t)i;
            for (int j = 0; j < 8; j++)
                v = (v >> 1) ^ (0xEDB88320u & -(v & 1));
            table[i] = v;
        }
        table_ready = 1;
    }

    const uint8_t *p = buf;
    crc ^= 0xFFFFFFFFu;
    while (len--) crc = (crc >> 8) ^ table[(uint8_t)(crc ^ *p++)];
    return crc ^ 0xFFFFFFFFu;
}

/*--------------------------- helpers ---------------------------*/

static int w_u8(FILE *fp, uint8_t v)   { return fwrite(&v, 1, 1, fp) == 1 ? 0 : -1; }
static int w_u32(FILE *fp, uint32_t v) { return fwrite(&v, 4, 1, fp) == 1 ? 0 : -1; }
static int r_u8(FILE *fp, uint8_t *v)   { return fread(v, 1, 1, fp) == 1 ? 0 : -1; }
static int r_u32(FILE *fp, uint32_t *v) { return fread(v, 4, 1, fp) == 1 ? 0 : -1; }

/*--------------------------- open / close ---------------------------*/

int lsm_wal_open(lsm_wal_t *wal, const char *path) {
    memset(wal, 0, sizeof(*wal));

    wal->fp = fopen(path, "ab");
    if (!wal->fp) return -1;

    wal->path = malloc(strlen(path) + 1);
    if (!wal->path) {
        fclose(wal->fp);
        wal->fp = NULL;
        return -1;
    }

    strcpy(wal->path, path);
    return 0;
}

void lsm_wal_close(lsm_wal_t *wal) {
    if (!wal) return;
    if (wal->fp) {
        fclose(wal->fp);
        wal->fp = NULL;
    }
    if (wal->path) {
        free(wal->path);
        wal->path = NULL;
    }
}

/*--------------------------- append ---------------------------*/

int  lsm_wal_append(lsm_wal_t *wal, lsm_slice_t key, lsm_slice_t val, uint8_t deleted) {
    if(!wal || !wal->fp) return -1;

    uint8_t type = deleted ? WAL_DELETE : WAL_PUT;
    uint32_t key_len = (uint32_t)key.len;
    uint32_t val_len = (uint32_t)val.len;

    // conpute crc
    uint32_t crc = 0;
    crc = crc32_update(crc, &type, 1);
    crc = crc32_update(crc, &key_len, 4);
    if (key_len) crc = crc32_update(crc, key.data, key_len);
    crc = crc32_update(crc, &val_len, 4);
    if (val_len) crc = crc32_update(crc, val.data, val_len);

    if (w_u8(wal->fp, type) != 0) return -1;
    if (w_u32(wal->fp, key_len) != 0) return -1;
    if (key_len > 0 && fwrite(key.data, 1, key_len, wal->fp) != key_len) return -1;
    if (w_u32(wal->fp, val_len) != 0) return -1;
    if (val_len > 0 && fwrite(val.data, 1, val_len, wal->fp) != val_len) return -1;
    if (w_u32(wal->fp, crc) != 0) return -1;

    fflush(wal->fp);
    return 0;
}

/*--------------------------- recover ---------------------------*/

int  lsm_wal_recover(const char *path, lsm_memtable_t *mt) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return 0;

    int recovered = 0;

    for(;;) {
        uint8_t type;
        uint32_t key_len, val_len, stored_crc;

        if (r_u8(fp, &type) != 0) break;
        if (type != WAL_PUT && type != WAL_DELETE) break;

        // key
        if (r_u32(fp, &key_len) != 0) break;

        void *kbuf = key_len ? malloc(key_len) : NULL;
        if (key_len && !kbuf) break;
        if (key_len && fread(kbuf, 1, key_len, fp) != key_len) {
            free(kbuf);
            break;
        }

        // value
        if (r_u32(fp, &val_len) != 0) {
            free(kbuf);
            break;
        }

        void *vbuf = val_len ? malloc(val_len) : NULL;
        if (val_len && !vbuf) {
            free(kbuf);
            break;
        }
        if (val_len && fread(vbuf, 1, val_len, fp) != val_len) {
            free(kbuf);
            free(vbuf);
            break;
        }

        // crc
        if (r_u32(fp, &stored_crc) != 0) {
            free(kbuf);
            free(vbuf);
            break;
        }

        uint32_t crc = 0;
        crc = crc32_update(crc, &type, 1);
        crc = crc32_update(crc, &key_len, 4);
        if (key_len) crc = crc32_update(crc, kbuf, key_len);
        crc = crc32_update(crc, &val_len, 4);
        if (val_len) crc = crc32_update(crc, vbuf, val_len);

        if (crc != stored_crc) {
            free(kbuf);
            free(vbuf);
            break;
        }

        // recover
        lsm_slice_t k = {.data = kbuf, .len = key_len};
        lsm_slice_t v = {.data = vbuf, .len = val_len};

        lsm_memtable_put(mt, k, v, type == WAL_DELETE);

        free(kbuf);
        free(vbuf);

        recovered++;
    }

    fclose(fp);
    return recovered;
}