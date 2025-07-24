#ifndef DB_STORAGE_H
#define DB_STORAGE_H

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

// --- 常量定义 ---

#define PAGE_SIZE 4096              // 每个页面的大小 (4KB)
#define BUFFER_POOL_SIZE 10         // 缓冲池中可以容纳的页面数量
#define INVALID_PAGE_ID -1          // 无效页面ID的标记

// 【已修正】添加了缺失的常量定义
// 这个常量定义了页表(page_table)可以管理的最大页面数量，
// 决定了数据库文件的理论最大尺寸 (TABLE_MAX_PAGES * PAGE_SIZE)
#define TABLE_MAX_PAGES 100

// --- 数据结构定义 ---

// 页面ID类型 (通常是整数)
typedef int32_t page_id_t;

// 页面对象结构体
// 这是在缓冲池中管理的单位
typedef struct Page {
    char data[PAGE_SIZE];       // 存储页面数据的数组
    page_id_t page_id;          // 该页面在磁盘文件中的ID
    int pin_count;              // 被“钉住”的次数，只要 > 0 就不能被淘汰
    bool is_dirty;              // 页面内容是否被修改过
} Page;

// 磁盘管理器结构体
typedef struct DiskManager {
    int file_descriptor;        // 数据库文件的文件描述符
    char* file_name;            // 数据库文件名
} DiskManager;

// 缓冲池管理器结构体
typedef struct BufferPoolManager {
    Page* pages;                // 指向缓冲池页面数组的指针 (大小为 BUFFER_POOL_SIZE)
    DiskManager* disk_manager;  // 指向磁盘管理器的指针
    
    int* page_table;            // 页表: 映射 page_id -> frame_id (在缓冲池中的索引)
    int* free_list;             // 空闲帧列表
    int free_list_size;

    // 用于LRU页面替换算法的数据
    int* lru_replacer;          // 一个简单的队列，存储可被淘汰的 frame_id
    int lru_head;
    int lru_tail;
    bool* lru_in_replacer;      // 标记一个frame是否在lru_replacer中

} BufferPoolManager;


// --- 函数声明 ---

// 磁盘管理器函数
DiskManager* create_disk_manager(const char* db_file);
void destroy_disk_manager(DiskManager* disk_manager);
void read_page_from_disk(DiskManager* disk_manager, page_id_t page_id, char* page_data);
void write_page_to_disk(DiskManager* disk_manager, page_id_t page_id, const char* page_data);
page_id_t allocate_page_on_disk(DiskManager* disk_manager);

// 缓冲池管理器函数
BufferPoolManager* create_buffer_pool_manager(DiskManager* disk_manager);
void destroy_buffer_pool_manager(BufferPoolManager* bpm);
Page* fetch_page(BufferPoolManager* bpm, page_id_t page_id);
bool unpin_page(BufferPoolManager* bpm, page_id_t page_id, bool is_dirty);
Page* new_page(BufferPoolManager* bpm, page_id_t* new_page_id);
bool flush_page(BufferPoolManager* bpm, page_id_t page_id);
void flush_all_pages(BufferPoolManager* bpm);

#endif // DB_STORAGE_H

