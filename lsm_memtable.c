#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "lsm_memtable.h"

// key comparison
static int lsm_slice_cmp(lsm_slice_t a, lsm_slice_t b) {
    size_t min = a.len < b.len ? a.len : b.len;
    int r = memcmp(a.data, b.data, min);
    
    if (r != 0) return r;
    if (a.len < b.len) return -1;
    if (a.len > b.len) return 1;
    return 0;
}

static lsm_slice_t lsm_slice_copy(lsm_slice_t src) {
    lsm_slice_t dst;
    dst.data = malloc(src.len);
    if(!dst.data) {
        dst.len = 0;
        return dst; // OOM
    }

    memcpy(dst.data, src.data, src.len);
    dst.len = src.len;
    return dst;
}

int lsm_memtable_init(lsm_memtable_t *mt) {
    mt->max_level = 16;
    mt->size      = 0;
    mt->rand_seed = (uint32_t)time(NULL);
    
    mt->head = calloc(1, sizeof(lsm_skipnode_t) + mt->max_level * sizeof(lsm_skipnode_t*));
    if (!mt->head) {
        mt->max_level = 0;
        return -1;
    }
    return 0;
}

void lsm_memtable_free(lsm_memtable_t *mt) {
    if (!mt || !mt->head) return;
    
    lsm_skipnode_t *curr = mt->head->forward[0];
    while (curr) {
        lsm_skipnode_t *next = curr->forward[0];
        free(curr->key.data);
        free(curr->value.data);
        free(curr);
        curr = next;
    }
    free(mt->head);
    memset(mt, 0, sizeof(*mt));
}

/* P(level >= i) = p^i, p=1/4 */
static size_t random_level(lsm_memtable_t *mt) {
    size_t level = 1;
    uint32_t r = mt->rand_seed;
    mt->rand_seed = r * 1103515245 + 12345;
    r &= 0x7fff;
    
    while (r < 0x2000 && level < mt->max_level) {
        level++;
        r = mt->rand_seed;
        mt->rand_seed = r * 1103515245 + 12345;
        r &= 0x7fff;
    }
    return level;
}

static lsm_skipnode_t *lsm_skip_find(lsm_memtable_t *mt, lsm_slice_t key, int *found) {
    lsm_skipnode_t *curr = mt->head;

    for (int lv = (int)mt->max_level - 1; lv >= 0; lv--) {
        while (curr->forward[lv] && lsm_slice_cmp(key, curr->forward[lv]->key) > 0)
            curr = curr->forward[lv];
    }

    curr = curr->forward[0];
    if (curr && lsm_slice_cmp(key, curr->key) == 0) {
        *found = 1;
        return curr;
    }

    *found = 0;
    return NULL;
}

int lsm_memtable_put(lsm_memtable_t *mt, lsm_slice_t key, lsm_slice_t vlaue, uint8_t deleted) {
    int found;
    lsm_skipnode_t *target = lsm_skip_find(mt, key, &found);

    lsm_slice_t copy_key = lsm_slice_copy(key);
    lsm_slice_t copy_val = lsm_slice_copy(vlaue);
    if (!copy_key.data || !copy_val.data) {
        free(copy_key.data);
        free(copy_val.data);
        return -1;
    }

    if (found) {
        free(target->key.data);
        free(target->value.data);
        target->key = copy_key;
        target->value = copy_val;
        target->deleted = deleted;
        return 0;
    }

    size_t new_lv = random_level(mt);

    size_t node_size = sizeof(lsm_skipnode_t) + new_lv * sizeof(lsm_skipnode_t*);
    lsm_skipnode_t *new_node = calloc(1, node_size);
    if (!new_node) {
        free(copy_key.data);
        free(copy_val.data);
        return -1;
    }

    new_node->key = copy_key;
    new_node->value = copy_val;
    new_node->deleted = deleted;

    lsm_skipnode_t *update[16] = {0};
    lsm_skipnode_t *curr = mt->head;

    for (int lv = (int)mt->max_level - 1; lv >= 0; lv--) {
        while (curr->forward[lv] && lsm_slice_cmp(key, curr->forward[lv]->key) > 0)
            curr = curr->forward[lv];
        update[lv] = curr;
    }

    for (size_t lv = 0; lv < new_lv; lv++) {
        new_node->forward[lv] = update[lv]->forward[lv];
        update[lv]->forward[lv] = new_node;
    }

    mt->size++;
    return 0;
}

int lsm_memtable_get(lsm_memtable_t *mt, lsm_slice_t key, lsm_slice_t *value_out, uint8_t *deleted_out) {
    int found;
    lsm_skipnode_t *node = lsm_skip_find(mt, key, &found);

    if (!found)
        return -1;

    if (deleted_out)
        *deleted_out = node->deleted;

    if (value_out) {
        if (node->deleted) {
            value_out->data = NULL;
            value_out->len  = 0;
        } else {
            *value_out = lsm_slice_copy(node->value);
            if (!value_out->data)
                return -1;
        }
    }

    return 0;
}