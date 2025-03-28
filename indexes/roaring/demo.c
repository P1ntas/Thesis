#include <stdio.h>
#include <stdlib.h>
#include "roaring.c"
int main() {
    roaring_bitmap_t *r1 = roaring_bitmap_create();
    for (uint32_t i = 100; i < 1000; i++) roaring_bitmap_add(r1, i);
    printf("cardinality = %d\n", (int) roaring_bitmap_get_cardinality(r1));
    roaring_bitmap_free(r1);

    bitset_t *b = bitset_create();
    for (int k = 0; k < 1000; ++k) {
            bitset_set(b, 3 * k);
    }
    printf("%zu \n", bitset_count(b));
    bitset_free(b);
    return EXIT_SUCCESS;
}