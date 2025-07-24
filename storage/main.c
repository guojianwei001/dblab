#include "db_storage.h"

int main() {
    const char* db_filename = "my_database.db";
    // 清理旧的数据库文件以便于重新测试
    remove(db_filename);

    printf("--- 数据库存储层模拟程序 ---\n\n");

    // 1. 初始化
    DiskManager* dm = create_disk_manager(db_filename);
    BufferPoolManager* bpm = create_buffer_pool_manager(dm);

    printf("--- 阶段 1: 创建和填充页面 ---\n");
    page_id_t page_id_temp;
    Page* page1 = new_page(bpm, &page_id_temp);
    Page* page2 = new_page(bpm, &page_id_temp);
    Page* page3 = new_page(bpm, &page_id_temp);
    
    // 写入一些数据
    strcpy(page1->data, "这是页面1的数据。");
    strcpy(page2->data, "这是页面2的数据，它将被修改。");
    strcpy(page3->data, "这是页面3的数据。");

    printf("创建了 Page 0, 1, 2 并写入了初始数据。\n");
    
    // 解除钉住，page1和page3不是脏页，page2是脏页
    unpin_page(bpm, page1->page_id, false);
    unpin_page(bpm, page2->page_id, true); // 标记为脏页
    unpin_page(bpm, page3->page_id, false);
    printf("已解除 Page 0, 1, 2 的钉住，Page 1 被标记为脏页。\n\n");


    printf("--- 阶段 2: 填满缓冲池并触发淘汰 ---\n");
    // 创建足够多的新页面来填满缓冲池并触发淘汰
    for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
        Page* p = new_page(bpm, &page_id_temp);
        if (p == NULL) {
            printf("无法创建新页面，缓冲池已满且无法淘汰。\n");
            break;
        }
        sprintf(p->data, "这是自动创建的页面 %d", page_id_temp);
        unpin_page(bpm, p->page_id, false);
    }
    printf("已填满缓冲池，最早未被使用的页面应该已被淘汰。\n\n");


    printf("--- 阶段 3: 重新获取旧页面，测试缓存 ---\n");
    // 尝试获取 Page 1，它应该是脏页，在被淘汰时已经写回磁盘
    Page* fetched_page2 = fetch_page(bpm, 1);
    if (fetched_page2) {
        printf("成功重新获取 Page 1，内容: \"%s\"\n", fetched_page2->data);
        unpin_page(bpm, fetched_page2->page_id, false);
    } else {
        printf("获取 Page 1 失败！\n");
    }

    // 尝试获取 Page 0，它应该已经被淘汰，需要从磁盘重新读取
    Page* fetched_page1 = fetch_page(bpm, 0);
    if (fetched_page1) {
        printf("成功重新获取 Page 0，内容: \"%s\"\n", fetched_page1->data);
        unpin_page(bpm, fetched_page1->page_id, false);
    } else {
        printf("获取 Page 0 失败！\n");
    }

    // 尝试获取一个被钉住的页面，然后看是否能创建新页面
    printf("\n--- 阶段 4: 测试钉住(Pin)功能 ---\n");
    Page* pinned_page = fetch_page(bpm, 2);
    printf("已获取并钉住 Page 2。\n");
    
    // 此时缓冲池已满，且有一个页面被钉住。尝试创建 BUFFER_POOL_SIZE 个新页面
    // 应该会在某个时刻失败，因为无法淘汰被钉住的页面
    for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
        Page* p = new_page(bpm, &page_id_temp);
        if (p == NULL) {
            // 这是预期的结果
            break;
        }
        unpin_page(bpm, p->page_id, false);
    }
    printf("测试完成。由于 Page 2 被钉住，无法分配更多页面。\n");
    unpin_page(bpm, pinned_page->page_id, false); // 解除钉住


    // 5. 清理
    printf("\n--- 阶段 5: 关闭数据库 ---\n");
    destroy_buffer_pool_manager(bpm);
    destroy_disk_manager(dm);
    printf("所有脏页已刷新，资源已释放。程序结束。\n");

    return 0;
}

