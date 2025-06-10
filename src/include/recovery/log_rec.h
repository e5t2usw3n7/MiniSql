#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#define UNUSED(x) (void)(x)

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
    kInvalid,
    kInsert,
    kDelete,
    kUpdate,
    kBegin,
    kCommit,
    kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * TODO: Student Implement
 */
struct LogRec {
    // 默认构造函数
    LogRec() 
        : txn_id_(0), 
          type_(LogRecType::kInvalid),
          lsn_(INVALID_LSN), 
          prev_lsn_(INVALID_LSN),
          ins_key_(""), 
          ins_val_(0), 
          old_key_(""), 
          old_val_(0), 
          new_key_(""), 
          new_val_(0), 
          del_key_(""), 
          del_val_(0) {}

    LogRecType type_;   // 日志类型
    txn_id_t txn_id_;   // 事务ID
    lsn_t lsn_;         // 日志序列号
    lsn_t prev_lsn_;    // 前一个日志序列号
    KeyType ins_key_;   // 插入键
    ValType ins_val_;   // 插入值
    KeyType old_key_;   // 更新前的键
    ValType old_val_;   // 更新前的值
    KeyType new_key_;   // 更新后的键
    ValType new_val_;   // 更新后的值
    KeyType del_key_;   // 删除的键
    ValType del_val_;   // 删除的值

    KeyType key1_;  // insert/delete的key，update的old_key
    ValType val1_ = 0;
    KeyType key2_;  // update的new_key
    ValType val2_ = 0;

    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;  // 事务ID对应的LSN
    static lsn_t next_lsn_;  // 下一个LSN

    static void Reset() {
        next_lsn_ = 0;  // 重置下一个LSN为0
    }
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {

    auto log_rec = std::make_shared<LogRec>();//

    log_rec->txn_id_ = txn_id;//

    log_rec->type_ = LogRecType::kInsert;//

    log_rec->key1_ = std::move(ins_key);

    log_rec->ins_key_ = std::move(ins_key);

    log_rec->val1_ = ins_val;//

    log_rec->lsn_ = LogRec::next_lsn_++;//

    auto it = LogRec::prev_lsn_map_.find(txn_id);//

    log_rec->prev_lsn_ = (it != LogRec::prev_lsn_map_.end()) ? it->second : INVALID_LSN;//

    LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_;

    return log_rec;//
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val ) {
    auto log_rec = std::make_shared<LogRec>();//
    log_rec->txn_id_ = txn_id;//
    log_rec->type_ = LogRecType::kDelete;//

    log_rec->key1_ = std::move(del_key);//

    log_rec->val1_ = del_val;

    log_rec->del_key_ = std::move(del_key);
    log_rec->del_val_ = std::move(del_val);

    //
    log_rec->lsn_ = LogRec::next_lsn_++;
    //
    auto it = LogRec::prev_lsn_map_.find(txn_id);
    log_rec->prev_lsn_ = (it != LogRec::prev_lsn_map_.end()) ? it->second : INVALID_LSN;
    LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_;
    return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
    // 创建一个日志记录对象
    auto log_rec = std::make_shared<LogRec>();//

    // 设置事务ID和日志类型
    log_rec->txn_id_ = txn_id;//
    log_rec->type_ = LogRecType::kUpdate;//

    // 使用std::move来避免不必要的复制
    log_rec->key1_ = std::move(old_key);
    log_rec->key2_ = std::move(new_key);
    log_rec->old_val_ = std::move(old_val);
    log_rec->new_val_ = std::move(new_val);
    log_rec->val1_ = old_val;
    log_rec->val2_ = new_val;

    // 生成日志序列号
    log_rec->lsn_ = LogRec::next_lsn_++;//

    auto it = LogRec::prev_lsn_map_.find(txn_id);//

    if (it != LogRec::prev_lsn_map_.end()) {
        log_rec->prev_lsn_ = it->second;  // 获取之前的LSN
    } else {
        log_rec->prev_lsn_ = INVALID_LSN;  // 如果找不到，设置为无效LSN
    }
    // 更新当前事务的LSN
    LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_;//

    return log_rec;//
}
/**
 * TODO: Student Implement
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
        // 创建开始日志
        auto log_rec = std::make_shared<LogRec>();
        log_rec->txn_id_ = txn_id;
        log_rec->type_ = LogRecType::kBegin;
        log_rec->lsn_ = LogRec::next_lsn_++;    // 生成日志序列号
        log_rec->prev_lsn_ = INVALID_LSN;
        LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_;
        return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
    // 创建提交日志
    auto log_rec = std::make_shared<LogRec>();
    
    // 设置事务ID
    log_rec->txn_id_ = txn_id;
    // 设置日志类型为提交
    log_rec->type_ = LogRecType::kCommit;
    
    // 生成日志序列号
    log_rec->lsn_ = LogRec::next_lsn_++;

    // 检查事务ID是否存在于prev_lsn_map_中
    auto prev_lsn_it = LogRec::prev_lsn_map_.find(txn_id);
    
    // 如果存在，则将prev_lsn_设置为前一个日志的LSN，否则设置为INVALID_LSN
    log_rec->prev_lsn_ = (prev_lsn_it != LogRec::prev_lsn_map_.end()) ? prev_lsn_it->second : INVALID_LSN;
    
    // 更新事务的prev_lsn_映射
    LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_;

    return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
    // 创建回滚日志
    auto log_rec = std::make_shared<LogRec>();

    // 设置事务ID和日志类型
    log_rec->txn_id_ = txn_id;
    log_rec->type_ = LogRecType::kAbort;
    
    // 生成日志序列号
    log_rec->lsn_ = LogRec::next_lsn_++;

    // 尝试从日志记录映射中查找前一个日志的LSN
    lsn_t prev_lsn = INVALID_LSN;
    auto it = LogRec::prev_lsn_map_.find(txn_id);
    if (it != LogRec::prev_lsn_map_.end()) {
        prev_lsn = it->second;
    }
    log_rec->prev_lsn_ = prev_lsn;  // 将找到的LSN或者INVALID_LSN赋给prev_lsn_

    // 更新当前事务的prev_lsn_map_
    LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_;

    return log_rec;
}


#endif  // MINISQL_LOG_REC_H
