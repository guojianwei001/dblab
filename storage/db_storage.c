#include "db_storage.h"

// --- 内部辅助函数 (LRU Replacer) ---
void lru_replacer_pin(BufferPoolManager* bpm, int frame_id);
void lru_replacer_unpin(BufferPoolManager* bpm, int frame_id);
bool lru_replacer_evict(BufferPoolManager* bpm, int* frame_id);


// --- 磁盘管理器实现 ---

DiskManager* create_disk_manager(const char* db_file) {
    DiskManager* dm = (DiskManager*)malloc(sizeof(DiskManager));
    dm->file_name = strdup(db_file);
    dm->file_descriptor = open(db_file, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (dm->file_descriptor == -1) {
        perror("无法打开或创建数据库文件");
        free(dm->file_name);
        free(dm);
        return NULL;
    }
    return dm;
}

void destroy_disk_manager(DiskManager* disk_manager) {
    if (disk_manager) {
        close(disk_manager->file_descriptor);
        free(disk_manager->file_name);
        free(disk_manager);
    }
}

void read_page_from_disk(DiskManager* disk_manager, page_id_t page_id, char* page_data) {
    off_t offset = page_id * PAGE_SIZE;
    if (lseek(disk_manager->file_descriptor, offset, SEEK_SET) == -1) {
        perror("读页面时定位文件失败");
        return;
    }
    ssize_t bytes_read = read(disk_manager->file_descriptor, page_data, PAGE_SIZE);
    if (bytes_read < 0) {
        perror("读取页面数据失败");
    }
    // 如果读取的字节数少于一个页面，说明是文件末尾，用0填充剩余部分
    if (bytes_read < PAGE_SIZE) {
        memset(page_data + bytes_read, 0, PAGE_SIZE - bytes_read);
    }
}

void write_page_to_disk(DiskManager* disk_manager, page_id_t page_id, const char* page_data) {
    off_t offset = page_id * PAGE_SIZE;
    if (lseek(disk_manager->file_descriptor, offset, SEEK_SET) == -1) {
        perror("写页面时定位文件失败");
        return;
    }
    ssize_t bytes_written = write(disk_manager->file_descriptor, page_data, PAGE_SIZE);
    if (bytes_written != PAGE_SIZE) {
        perror("写入页面数据失败");
    }
}

page_id_t allocate_page_on_disk(DiskManager* disk_manager) {
    // 简单地将新页面追加到文件末尾
    off_t file_size = lseek(disk_manager->file_descriptor, 0, SEEK_END);
    return file_size / PAGE_SIZE;
}


// --- 缓冲池管理器实现 ---

BufferPoolManager* create_buffer_pool_manager(DiskManager* disk_manager) {
    BufferPoolManager* bpm = (BufferPoolManager*)malloc(sizeof(BufferPoolManager));
    bpm->disk_manager = disk_manager;
    
    bpm->pages = (Page*)malloc(BUFFER_POOL_SIZE * sizeof(Page));
    bpm->page_table = (int*)malloc(TABLE_MAX_PAGES * sizeof(int)); // 假设最多管理TABLE_MAX_PAGES个页
    bpm->free_list = (int*)malloc(BUFFER_POOL_SIZE * sizeof(int));
    bpm->lru_replacer = (int*)malloc(BUFFER_POOL_SIZE * sizeof(int));
    bpm->lru_in_replacer = (bool*)calloc(BUFFER_POOL_SIZE, sizeof(bool));

    for (int i = 0; i < TABLE_MAX_PAGES; i++) {
        bpm->page_table[i] = INVALID_PAGE_ID;
    }
    for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
        bpm->pages[i].page_id = INVALID_PAGE_ID;
        bpm->pages[i].pin_count = 0;
        bpm->pages[i].is_dirty = false;
        bpm->free_list[i] = i; // 所有帧最初都是空闲的
    }
    bpm->free_list_size = BUFFER_POOL_SIZE;
    bpm->lru_head = 0;
    bpm->lru_tail = 0;

    return bpm;
}

void destroy_buffer_pool_manager(BufferPoolManager* bpm) {
    if (bpm) {
        flush_all_pages(bpm);
        free(bpm->pages);
        free(bpm->page_table);
        free(bpm->free_list);
        free(bpm->lru_replacer);
        free(bpm->lru_in_replacer);
        free(bpm);
    }
}

Page* fetch_page(BufferPoolManager* bpm, page_id_t page_id) {
    // 1. 在页表中查找页面 (缓存命中)
    if (bpm->page_table[page_id] != INVALID_PAGE_ID) {
        int frame_id = bpm->page_table[page_id];
        printf("缓冲池: 缓存命中 page %d (在 frame %d).\n", page_id, frame_id);
        bpm->pages[frame_id].pin_count++;
        lru_replacer_pin(bpm, frame_id); // 从LRU淘汰队列中移除
        return &bpm->pages[frame_id];
    }

    // 2. 缓存未命中，需要从磁盘加载
    printf("缓冲池: 缓存未命中 page %d. 尝试加载...\n", page_id);
    int frame_id = -1;
    // 首先尝试从空闲帧列表中获取
    if (bpm->free_list_size > 0) {
        frame_id = bpm->free_list[--bpm->free_list_size];
        printf("缓冲池: 使用空闲 frame %d.\n", frame_id);
    } else {
        // 如果没有空闲帧，使用LRU算法淘汰一个
        if (!lru_replacer_evict(bpm, &frame_id)) {
            printf("缓冲池: 错误! 所有页面都被钉住，无法淘汰.\n");
            return NULL; // 所有页都被钉住，无法获取新页
        }
        printf("缓冲池: 淘汰 frame %d 中的 page %d.\n", frame_id, bpm->pages[frame_id].page_id);
        
        // 如果被淘汰的页是脏页，写回磁盘
        if (bpm->pages[frame_id].is_dirty) {
            printf("缓冲池: 被淘汰的 page %d 是脏页，正在写回磁盘...\n", bpm->pages[frame_id].page_id);
            write_page_to_disk(bpm->disk_manager, bpm->pages[frame_id].page_id, bpm->pages[frame_id].data);
        }
        // 从页表中移除旧页的映射
        bpm->page_table[bpm->pages[frame_id].page_id] = INVALID_PAGE_ID;
    }

    // 3. 加载新页面到获取到的帧中
    read_page_from_disk(bpm->disk_manager, page_id, bpm->pages[frame_id].data);
    bpm->pages[frame_id].page_id = page_id;
    bpm->pages[frame_id].pin_count = 1;
    bpm->pages[frame_id].is_dirty = false;
    
    // 更新页表
    bpm->page_table[page_id] = frame_id;
    
    // 从LRU淘汰队列中移除（因为它刚被访问）
    lru_replacer_pin(bpm, frame_id);

    return &bpm->pages[frame_id];
}

bool unpin_page(BufferPoolManager* bpm, page_id_t page_id, bool is_dirty) {
    if (bpm->page_table[page_id] == INVALID_PAGE_ID) {
        return false; // 页面不在缓冲池中
    }
    int frame_id = bpm->page_table[page_id];
    if (bpm->pages[frame_id].pin_count <= 0) {
        return false; // pin_count 已经为0
    }

    bpm->pages[frame_id].pin_count--;
    if (is_dirty) {
        bpm->pages[frame_id].is_dirty = true;
    }

    // 如果 pin_count 降为0，则该页可以被淘汰，将其加入LRU队列
    if (bpm->pages[frame_id].pin_count == 0) {
        lru_replacer_unpin(bpm, frame_id);
    }
    return true;
}

Page* new_page(BufferPoolManager* bpm, page_id_t* new_page_id) {
    *new_page_id = allocate_page_on_disk(bpm->disk_manager);
    // fetch_page 会处理缓存未命中、淘汰和加载的逻辑
    return fetch_page(bpm, *new_page_id);
}

bool flush_page(BufferPoolManager* bpm, page_id_t page_id) {
    if (bpm->page_table[page_id] == INVALID_PAGE_ID) {
        return false;
    }
    int frame_id = bpm->page_table[page_id];
    write_page_to_disk(bpm->disk_manager, page_id, bpm->pages[frame_id].data);
    bpm->pages[frame_id].is_dirty = false;
    printf("缓冲池: 已将 page %d (在 frame %d) 刷新到磁盘.\n", page_id, frame_id);
    return true;
}

void flush_all_pages(BufferPoolManager* bpm) {
    printf("缓冲池: 正在刷新所有脏页到磁盘...\n");
    for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
        if (bpm->pages[i].page_id != INVALID_PAGE_ID && bpm->pages[i].is_dirty) {
            flush_page(bpm, bpm->pages[i].page_id);
        }
    }
}


// --- LRU Replacer 实现 ---

// 当一个页面被访问时，它不能被淘汰，从LRU队列中移除
void lru_replacer_pin(BufferPoolManager* bpm, int frame_id) {
    bpm->lru_in_replacer[frame_id] = false;
}

// 当一个页面pin_count降为0时，它可以被淘汰，加入LRU队列末尾
void lru_replacer_unpin(BufferPoolManager* bpm, int frame_id) {
    if (!bpm->lru_in_replacer[frame_id]) {
        bpm->lru_replacer[bpm->lru_tail] = frame_id;
        bpm->lru_tail = (bpm->lru_tail + 1) % BUFFER_POOL_SIZE;
        bpm->lru_in_replacer[frame_id] = true;
    }
}

// 从LRU队列头部取出一个可淘汰的页面
bool lru_replacer_evict(BufferPoolManager* bpm, int* frame_id) {
    int current_head = bpm->lru_head;
    while (current_head != bpm->lru_tail) {
        int candidate_frame = bpm->lru_replacer[current_head];
        // 再次确认该帧是否在等待被淘汰
        if (bpm->lru_in_replacer[candidate_frame]) {
            *frame_id = candidate_frame;
            bpm->lru_head = (current_head + 1) % BUFFER_POOL_SIZE;
            bpm->lru_in_replacer[candidate_frame] = false;
            return true;
        }
        current_head = (current_head + 1) % BUFFER_POOL_SIZE;
    }
    return false; // 没有可淘汰的页面
}

