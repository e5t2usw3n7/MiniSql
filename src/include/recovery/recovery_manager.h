#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#define UNUSED(x) (void)(x)

#include <map>
#include <unordered_map>
#include <vector>
#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
    lsn_t checkpoint_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase persist_data_{};

    inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

    inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
public:
    // 初始化恢复管理器
    void Init(CheckPoint &last_checkpoint) {
        persist_lsn_ = last_checkpoint.checkpoint_lsn_;
        active_txns_ = last_checkpoint.active_txns_;
        data_.clear(); 
    }
    void RedoPhase() {
        // 查找第一个大于等于 persist_lsn_ 的日志
        auto it = log_recs_.lower_bound(persist_lsn_); 
        // 如果日志记录的起始位置不是检查点后面的一条，则跳过检查点前的记录
        if (it != log_recs_.end() && it != log_recs_.begin()) {
            ++it; // 从检查点后的第一条日志开始
        }
        // 遍历所有日志记录，执行恢复操作
        while ( it != log_recs_.end() ) {
            auto log_rec = it->second;//

            active_txns_[log_rec->txn_id_] = log_rec->lsn_;
            switch ( log_rec->type_ ) {
                case LogRecType::kInsert:
                    data_[log_rec->ins_key_] = log_rec->ins_val_;
                    data_[log_rec->key1_] = log_rec->val1_;
                    break;
                case LogRecType::kDelete:
                    data_.erase(log_rec->del_key_);
                    data_.erase(log_rec->key1_);
                    break;
                case LogRecType::kUpdate:
                    data_.erase(log_rec->old_key_);
                    data_[log_rec->new_key_] = log_rec->new_val_;
                    data_.erase(log_rec->key1_);
                    data_[log_rec->key2_] = log_rec->val2_;
                    break;
                case LogRecType::kBegin:
                    break;
                case LogRecType::kCommit:
                    active_txns_.erase(log_rec->txn_id_);
                    break;
                case LogRecType::kAbort: {
                    back(log_rec->txn_id_);
                    active_txns_.erase(log_rec->txn_id_); // 移除事务
                    break;
                }
                default:
                    break;
            }
            ++it ;//下一个
        }
    }
// back 函数：用于回滚指定事务的所有操作
// 该函数会根据事务的 LSN（日志序列号）从当前事务的最后一条日志开始，逐步回溯至日志链表的起始位置，
// 执行回滚操作。回滚操作根据日志记录的类型执行不同的操作：
// - 对于插入操作（kInsert），删除插入的数据。
// - 对于删除操作（kDelete），恢复删除的数据。
// - 对于更新操作（kUpdate），恢复原数据（删除新的数据并恢复旧的数据）。
// - 对于其他类型的操作（kBegin, kCommit），不需要回滚。
// 
// 设计此函数的原因：
// 1. 事务日志通常是链式结构，每个日志记录都有一个指向上一条日志的指针（prev_lsn）。回滚操作需要按照
//    这个顺序逐条回退，直到回滚到事务开始前的状态。
// 2. `back` 函数将事务的回滚操作统一处理，避免在不同地方重复实现相同的回滚逻辑，提高代码的复用性。
// 3. 该函数可以灵活适应不同类型的日志记录，保证了事务的完整性和一致性。
    void back(txn_id_t txn_id) {
        lsn_t current_lsn = active_txns_[txn_id];
        while(current_lsn != INVALID_LSN) {
            auto log_rec = log_recs_[current_lsn];
            switch (log_rec->type_) {
                case LogRecType::kInsert:
                    data_.erase(log_rec->ins_key_);
                    data_.erase(log_rec->key1_);
                    break;
                case LogRecType::kDelete:
                    data_[log_rec->del_key_] = log_rec->del_val_;
                    data_[log_rec->key1_] = log_rec->val1_;
                    break;
                case LogRecType::kUpdate:
                    data_.erase(log_rec->new_key_);  // 删除更新后的记录
                    data_[log_rec->old_key_] = log_rec->old_val_;  // 恢复原值
                    data_.erase(log_rec->key2_);
                    data_[log_rec->key1_] = log_rec->val1_;
                    break;
                case LogRecType::kBegin:
                    break;// 对于事务开始类型不进行操作
                case LogRecType::kCommit:
                    break;// 对于事务提交类型不进行操作
                default:
                    break;
            }
            current_lsn = log_rec->prev_lsn_;
        }
    }

    /**
    * TODO: Student Implement
    */
    void UndoPhase() {
        // 做完redo之后，回滚所有事务，因为此时活跃事务还是未提交状态
        for(auto &txn : active_txns_) {
            back(txn.first);
        }
        active_txns_.clear();
    }
    // 向日志中追加记录
    void AppendLogRec(LogRecPtr log_rec) {
        log_recs_.emplace(log_rec->lsn_, log_rec);
    }

    // 获取数据库
    inline KvDatabase &GetDatabase() { return data_; }

private:
    std::map<lsn_t, LogRecPtr> log_recs_;  // 日志记录
    lsn_t persist_lsn_{INVALID_LSN};  // 持久化日志序列号
    ATT active_txns_;  // 活跃事务
    KvDatabase data_;  // 数据库中的数据
};

#endif  // MINISQL_RECOVERY_MANAGER_H
