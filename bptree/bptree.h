#ifndef BPTREE_H
#define BPTREE_H

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

// B+ 树节点
typedef struct Node {
    bool is_leaf;
    int num_keys;
    int* keys;
    void** pointers; // 在叶子节点中是数据指针，在内部节点中是子节点指针
    struct Node* parent;
    struct Node* next; // 用于连接叶子节点的链表
} Node;

// B+ 树结构
typedef struct BPTree {
    Node* root;
    int order;
} BPTree;

// --- 函数声明 ---

// 创建与销毁
BPTree* create_bptree(int order);
void destroy_tree(BPTree* tree);

// 插入
void insert(BPTree* tree, int key, void* value);

// 查找
void* search(BPTree* tree, int key);

// 打印 (用于调试和展示)
void print_tree(BPTree* tree);
void print_leaves(BPTree* tree);

#endif // BPTREE_H

