#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

#define NUM_ACCOUNTS 5
#define INITIAL_BALANCE 1000
#define MAX_LOCKS_PER_TX 10 // 一个事务最多能持有10个锁

// 事务隔离级别枚举
typedef enum {
    READ_COMMITTED,
    REPEATABLE_READ
} IsolationLevel;

// 账户结构体 (数据库中的一行)
typedef struct {
    int id;
    int balance;
    pthread_rwlock_t lock; // 使用读写锁更精确地模拟
} Account;

// "数据库"
Account bank[NUM_ACCOUNTS];

// 事务上下文结构体，用于跟踪单个事务所持有的锁
typedef struct {
    IsolationLevel level;
    int held_locks[MAX_LOCKS_PER_TX];
    int lock_count;
} TxContext;

// 事务开始：初始化上下文
void tx_begin(TxContext* ctx, IsolationLevel level) {
    ctx->level = level;
    ctx->lock_count = 0;
    for (int i = 0; i < MAX_LOCKS_PER_TX; ++i) {
        ctx->held_locks[i] = -1;
    }
}

// 事务提交：释放该事务所持有的所有锁 (这是2PL的解锁阶段)
void tx_commit(TxContext* ctx) {
    for (int i = 0; i < ctx->lock_count; ++i) {
        int account_id = ctx->held_locks[i];
        if (account_id != -1) {
            pthread_rwlock_unlock(&bank[account_id].lock);
        }
    }
    ctx->lock_count = 0; // 重置
}

// 内部函数：获取一个读锁
void acquire_read_lock(TxContext* ctx, int id) {
    pthread_rwlock_rdlock(&bank[id].lock);
    // 如果是 REPEATABLE_READ，则记录该锁，并持有到事务结束
    if (ctx->level == REPEATABLE_READ) {
        // 避免重复记录
        for (int i = 0; i < ctx->lock_count; ++i) {
            if (ctx->held_locks[i] == id) return;
        }
        ctx->held_locks[ctx->lock_count++] = id;
    }
}

// 内部函数：获取一个写锁
void acquire_write_lock(TxContext* ctx, int id) {
    pthread_rwlock_wrlock(&bank[id].lock);
    // 写锁总是要持有到事务结束，以遵守Strict 2PL
    for (int i = 0; i < ctx->lock_count; ++i) {
        if (ctx->held_locks[i] == id) return;
    }
    ctx->held_locks[ctx->lock_count++] = id;
}


// "事务"操作1: 读取余额
int get_balance(TxContext* ctx, int id) {
    acquire_read_lock(ctx, id);
    
    int balance = bank[id].balance;
    // 模拟耗时
    usleep(1000); 

    // **隔离级别行为差异的关键点**
    // 如果是 READ_COMMITTED，读完立刻释放锁
    if (ctx->level == READ_COMMITTED) {
        pthread_rwlock_unlock(&bank[id].lock);
    }
    
    return balance;
}

// "事务"操作2: 转账
void transfer(TxContext* ctx, int from, int to, int amount) {
    // 死锁预防：按ID顺序加写锁
    int lock1 = (from < to) ? from : to;
    int lock2 = (from > to) ? from : to;

    acquire_write_lock(ctx, lock1);
    acquire_write_lock(ctx, lock2);
    
    // 临界区
    if (bank[from].balance >= amount) {
        bank[from].balance -= amount;
        bank[to].balance += amount;
    }
    // 锁在 tx_commit 中释放，这里不释放
}

// =================== 线程工作流 ===================

// 读取者线程：测试可重复读
void* reader_workflow(void* arg) {
    IsolationLevel level = *(IsolationLevel*)arg;
    TxContext ctx;
    
    tx_begin(&ctx, level);
    
    int account_id = 0;
    printf("[Reader Thread %lu, Level: %s] Reading balance of account %d for the 1st time...\n", 
           pthread_self(), (level == READ_COMMITTED ? "READ_COMMITTED" : "REPEATABLE_READ"), account_id);
    
    int balance1 = get_balance(&ctx, account_id);
    printf("[Reader Thread %lu] First read balance: %d\n", pthread_self(), balance1);
    
    // 等待一小段时间，让写者线程有机会修改数据
    usleep(50000); 
    
    printf("[Reader Thread %lu] Reading balance of account %d for the 2nd time...\n", pthread_self(), account_id);
    int balance2 = get_balance(&ctx, account_id);
    printf("[Reader Thread %lu] Second read balance: %d\n", pthread_self(), balance2);
    
    if (balance1 != balance2) {
        printf("\n\t!!! NON-REPEATABLE READ DETECTED on thread %lu !!!\n\n", pthread_self());
    } else {
        printf("\n\t>>> Repeatable Read successful on thread %lu >>>\n\n", pthread_self());
    }
    
    tx_commit(&ctx);
    return NULL;
}

// 写者线程：不断进行转账
void* writer_workflow(void* arg) {
    // 写者总是以最高隔离性执行，确保数据修改的正确性
    TxContext ctx;
    tx_begin(&ctx, REPEATABLE_READ); 
    
    printf("[Writer Thread %lu] Transferring 100 from account 0 to 1.\n", pthread_self());
    transfer(&ctx, 0, 1, 100);
    
    tx_commit(&ctx);
    return NULL;
}

void run_simulation(IsolationLevel reader_level) {
    printf("====================================================\n");
    printf("     STARTING SIMULATION FOR: %s\n", (reader_level == READ_COMMITTED ? "READ COMMITTED" : "REPEATABLE READ"));
    printf("====================================================\n");

    // 初始化
    for (int i = 0; i < NUM_ACCOUNTS; ++i) {
        bank[i].id = i;
        bank[i].balance = INITIAL_BALANCE;
        pthread_rwlock_init(&bank[i].lock, NULL);
    }
    
    pthread_t reader_thread, writer_thread;

    // 创建一个读者线程和一个写者线程
    pthread_create(&reader_thread, NULL, reader_workflow, &reader_level);
    
    // 稍微延迟，确保读者先开始并获取第一个读锁
    usleep(10000); 
    
    pthread_create(&writer_thread, NULL, writer_workflow, NULL);
    
    pthread_join(reader_thread, NULL);
    pthread_join(writer_thread, NULL);

    for (int i = 0; i < NUM_ACCOUNTS; ++i) {
        pthread_rwlock_destroy(&bank[i].lock);
    }
    printf("\n\n");
}


int main() {
    // 场景一：读者使用 READ_COMMITTED 级别
    run_simulation(READ_COMMITTED);
    
    // 场景二：读者使用 REPEATABLE_READ 级别
    run_simulation(REPEATABLE_READ);

    return 0;
}
