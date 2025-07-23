#include "bptree.h"

int main() {
    // 创建一个4阶的B+树 (每个节点最多3个键)
    int order = 4;
    BPTree* tree = create_bptree(order);

    printf("B+Tree of order %d created.\n", order);
    printf("Inserting values from 1 to 10 to demonstrate splits...\n\n");

    // 插入数据，这将触发叶子分裂和内部节点分裂
    insert(tree, 10, (void*)100L);
    insert(tree, 20, (void*)200L);
    insert(tree, 30, (void*)300L);
    printf("After inserting 10, 20, 30 (Leaf is full):\n");
    print_tree(tree);

    insert(tree, 40, (void*)400L);
    printf("After inserting 40 (Triggers first leaf split, creates new root):\n");
    print_tree(tree);

    insert(tree, 5, (void*)50L);
    insert(tree, 15, (void*)150L);
    insert(tree, 25, (void*)250L);
    printf("After inserting 5, 15, 25 (Fills up leaves):\n");
    print_tree(tree);

    insert(tree, 35, (void*)350L);
    printf("After inserting 35 (Triggers another leaf split):\n");
    print_tree(tree);
    
    insert(tree, 50, (void*)500L);
    printf("After inserting 50 (Triggers an *internal node* split, tree grows taller):\n");
    print_tree(tree);


    // 查找
    printf("\n--- Searching ---\n");
    int key_to_find = 25;
    long found_value = (long)search(tree, key_to_find);
    if (found_value) {
        printf("Found key %d with value %ld\n", key_to_find, found_value);
    } else {
        printf("Key %d not found.\n", key_to_find);
    }

    key_to_find = 99;
    found_value = (long)search(tree, key_to_find);
    if (found_value) {
        printf("Found key %d with value %ld\n", key_to_find, found_value);
    } else {
        printf("Key %d not found.\n", key_to_find);
    }

    // 销毁树，释放内存
    destroy_tree(tree);
    printf("\nTree destroyed.\n");

    return 0;
}

