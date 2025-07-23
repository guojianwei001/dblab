#include "bptree.h"

// --- 内部辅助函数声明 ---
Node* create_node(int order, bool is_leaf);
Node* find_leaf(Node* root, int key);
void insert_into_leaf(Node* leaf, int key, void* value);
void insert_into_parent(BPTree* tree, Node* left, int key, Node* right);
void split_leaf_and_insert(BPTree* tree, Node* leaf, int key, void* value);
void insert_into_internal(Node* parent, int left_index, int key, Node* right);
void split_internal_and_insert(BPTree* tree, Node* old_node, int left_index, int key, Node* right);
void insert_into_new_root(BPTree* tree, Node* left, int key, Node* right);

// --- 创建与销毁 ---

BPTree* create_bptree(int order) {
    BPTree* tree = (BPTree*)malloc(sizeof(BPTree));
    tree->order = order;
    tree->root = create_node(order, true);
    return tree;
}

Node* create_node(int order, bool is_leaf) {
    Node* node = (Node*)malloc(sizeof(Node));
    node->is_leaf = is_leaf;
    node->num_keys = 0;
    node->keys = (int*)malloc((order - 1) * sizeof(int));
    node->pointers = (void**)malloc(order * sizeof(void*));
    node->parent = NULL;
    node->next = NULL;
    return node;
}

void destroy_node(Node* node) {
    if (node == NULL) return;
    if (!node->is_leaf) {
        for (int i = 0; i < node->num_keys + 1; i++) {
            destroy_node((Node*)node->pointers[i]);
        }
    }
    free(node->keys);
    free(node->pointers);
    free(node);
}

void destroy_tree(BPTree* tree) {
    destroy_node(tree->root);
    free(tree);
}

// --- 查找操作 ---

void* search(BPTree* tree, int key) {
    Node* leaf = find_leaf(tree->root, key);
    for (int i = 0; i < leaf->num_keys; i++) {
        if (leaf->keys[i] == key) {
            return leaf->pointers[i];
        }
    }
    return NULL;
}

Node* find_leaf(Node* root, int key) {
    Node* current = root;
    while (!current->is_leaf) {
        int i = 0;
        // 找到第一个大于等于key的键，然后选择其左边的指针
        while (i < current->num_keys && key >= current->keys[i]) {
            i++;
        }
        current = (Node*)current->pointers[i];
    }
    return current;
}


// --- 插入操作 ---

void insert(BPTree* tree, int key, void* value) {
    // 检查键是否已存在 (B+树通常不允许重复键)
    if (search(tree, key) != NULL) {
        // 在真实实现中，可以选择更新值或返回错误
        return;
    }
    
    Node* leaf = find_leaf(tree->root, key);

    if (leaf->num_keys < tree->order - 1) {
        insert_into_leaf(leaf, key, value);
    } else {
        split_leaf_and_insert(tree, leaf, key, value);
    }
}

void insert_into_leaf(Node* leaf, int key, void* value) {
    int i = 0;
    while (i < leaf->num_keys && leaf->keys[i] < key) {
        i++;
    }
    for (int j = leaf->num_keys; j > i; j--) {
        leaf->keys[j] = leaf->keys[j - 1];
        leaf->pointers[j] = leaf->pointers[j - 1];
    }
    leaf->keys[i] = key;
    leaf->pointers[i] = value;
    leaf->num_keys++;
}

void split_leaf_and_insert(BPTree* tree, Node* leaf, int key, void* value) {
    Node* new_leaf = create_node(tree->order, true);
    int* temp_keys = (int*)malloc(tree->order * sizeof(int));
    void** temp_pointers = (void**)malloc(tree->order * sizeof(void*));

    int insertion_point = 0;
    while (insertion_point < tree->order - 1 && leaf->keys[insertion_point] < key) {
        insertion_point++;
    }

    for (int i = 0, j = 0; i < leaf->num_keys; i++, j++) {
        if (j == insertion_point) j++;
        temp_keys[j] = leaf->keys[i];
        temp_pointers[j] = leaf->pointers[i];
    }
    temp_keys[insertion_point] = key;
    temp_pointers[insertion_point] = value;

    int split = (tree->order + 1) / 2;
    leaf->num_keys = split;
    for (int i = 0; i < split; i++) {
        leaf->keys[i] = temp_keys[i];
        leaf->pointers[i] = temp_pointers[i];
    }

    new_leaf->num_keys = tree->order - split;
    for (int i = 0, j = split; i < new_leaf->num_keys; i++, j++) {
        new_leaf->keys[i] = temp_keys[j];
        new_leaf->pointers[i] = temp_pointers[j];
    }

    free(temp_keys);
    free(temp_pointers);

    new_leaf->next = leaf->next;
    leaf->next = new_leaf;

    new_leaf->parent = leaf->parent;
    insert_into_parent(tree, leaf, new_leaf->keys[0], new_leaf);
}

void insert_into_parent(BPTree* tree, Node* left, int key, Node* right) {
    Node* parent = left->parent;
    if (parent == NULL) {
        insert_into_new_root(tree, left, key, right);
        return;
    }

    int left_index = 0;
    while (left_index <= parent->num_keys && parent->pointers[left_index] != left) {
        left_index++;
    }

    if (parent->num_keys < tree->order - 1) {
        insert_into_internal(parent, left_index, key, right);
    } else {
        // 父节点已满，触发内部节点分裂
        split_internal_and_insert(tree, parent, left_index, key, right);
    }
}

void insert_into_internal(Node* node, int left_index, int key, Node* right) {
    for (int i = node->num_keys; i > left_index; i--) {
        node->keys[i] = node->keys[i - 1];
        node->pointers[i + 1] = node->pointers[i];
    }
    node->keys[left_index] = key;
    node->pointers[left_index + 1] = right;
    node->num_keys++;
}

// 【已补全】分裂内部节点
void split_internal_and_insert(BPTree* tree, Node* old_node, int left_index, int key, Node* right) {
    // 1. 创建临时空间，容纳所有旧键/指针和新键/指针
    int temp_keys[tree->order];
    Node* temp_pointers[tree->order + 1];

    for (int i = 0, j = 0; i < old_node->num_keys + 1; i++, j++) {
        if (j == left_index + 1) j++;
        temp_pointers[j] = (Node*)old_node->pointers[i];
    }
    for (int i = 0, j = 0; i < old_node->num_keys; i++, j++) {
        if (j == left_index) j++;
        temp_keys[j] = old_node->keys[i];
    }
    temp_pointers[left_index + 1] = right;
    temp_keys[left_index] = key;

    // 2. 分裂并提升中间键
    int split = (tree->order) / 2;
    int key_to_promote = temp_keys[split];
    
    // 3. 创建新内部节点，并分配键和指针
    Node* new_node = create_node(tree->order, false);
    old_node->num_keys = split;
    for (int i = 0; i < split; i++) {
        old_node->keys[i] = temp_keys[i];
        old_node->pointers[i] = temp_pointers[i];
    }
    old_node->pointers[split] = temp_pointers[split];

    new_node->num_keys = tree->order - 1 - split;
    for (int i = 0, j = split + 1; i < new_node->num_keys; i++, j++) {
        new_node->keys[i] = temp_keys[j];
        new_node->pointers[i] = temp_pointers[j];
    }
    new_node->pointers[new_node->num_keys] = temp_pointers[tree->order];

    // 4. 更新所有受影响子节点的父指针
    new_node->parent = old_node->parent;
    for (int i = 0; i < new_node->num_keys + 1; i++) {
        Node* child = (Node*)new_node->pointers[i];
        child->parent = new_node;
    }

    // 5. 递归地将提升的键插入到父节点
    insert_into_parent(tree, old_node, key_to_promote, new_node);
}

void insert_into_new_root(BPTree* tree, Node* left, int key, Node* right) {
    Node* new_root = create_node(tree->order, false);
    new_root->keys[0] = key;
    new_root->pointers[0] = left;
    new_root->pointers[1] = right;
    new_root->num_keys++;
    left->parent = new_root;
    right->parent = new_root;
    tree->root = new_root;
}


// --- 打印函数 ---

void print_leaves(BPTree* tree) {
    if (tree->root == NULL) return;
    Node* c = find_leaf(tree->root, 0);
    if (c == NULL) return;
    printf("Leaves: [");
    while (c != NULL) {
        for (int i = 0; i < c->num_keys; i++) {
            printf(" %d ", c->keys[i]);
        }
        if (c->next != NULL) printf(" |");
        c = c->next;
    }
    printf(" ]\n");
}

void print_tree_recursive(Node* node, int level) {
    if (node == NULL) return;
    for (int i = 0; i < level; i++) printf("  ");
    
    if (node->is_leaf) {
        printf("Leaf: Keys(");
    } else {
        printf("Internal: Keys(");
    }
    
    for (int i = 0; i < node->num_keys; i++) {
        printf("%d", node->keys[i]);
        if (i < node->num_keys - 1) printf(",");
    }
    printf(")\n");

    if (!node->is_leaf) {
        for (int i = 0; i < node->num_keys + 1; i++) {
            print_tree_recursive((Node*)node->pointers[i], level + 1);
        }
    }
}

void print_tree(BPTree* tree) {
    printf("---- B+Tree Structure ----\n");
    if (tree->root == NULL) {
        printf("Tree is empty.\n");
    } else {
        print_tree_recursive(tree->root, 0);
    }
    print_leaves(tree);
    printf("--------------------------\n\n");
}

