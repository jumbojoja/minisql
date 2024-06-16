#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <memory>
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
    kCompensate,    /*自己加的，为了模拟真实情景*/
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * TODO: Student Implement
 */
struct LogRec {
    LogRec() = default;

    LogRecType type_{LogRecType::kInvalid};
    lsn_t lsn_{INVALID_LSN};
    lsn_t prev_lsn_{INVALID_LSN};

    /**
     * Implemented by liang 2024.06.15
     * 从test来看，所有日志的序列号是连续的，但是为了找到同一个事务的日志，需要使用prev_lsn进行查找。要利用prev_lsn_map
     * 所以这个prev_lsn事实上是同一事务的前一条日志序列号。例子如下：
     *    T1 insert lsn:9 prev_lsn:6
     *    T2 update lsn:10 preV_lsn:8
     *    T1 commit lsn:11 prev_lsn:9
     */
    txn_id_t txn_id_;
    KeyType old_key;
    ValType old_val;
    KeyType new_key;
    ValType new_val;
    KeyType com_key;
    ValType com_val;


    /* used for testing only */
    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
    static lsn_t next_lsn_;
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
    /*插入一条日志，即将插入的值放到new里面*/
    LogRecPtr LRP (new LogRec());
    if(LogRec::prev_lsn_map_.find(txn_id)==LogRec::prev_lsn_map_.end()) {
     /*说明没找到，这个事务的第一条日志不是begin*/
      ASSERT(false,"An transaction log error: start with non_begin log!");
    }
    LRP->prev_lsn_ = LogRec::prev_lsn_map_.find(txn_id)->second;  /*查找上一个序列号，将其赋值给prev_lsn，同时更新*/
    LRP->lsn_=LogRec::next_lsn_++;
    LogRec::prev_lsn_map_.find(txn_id)->second = LRP->lsn_;

    LRP->type_ = LogRecType::kInsert;
    LRP->txn_id_ = txn_id;
    LRP->old_key.clear();
    LRP->old_val = -1;
    LRP->com_key.clear();
    LRP->com_val = -1;
    LRP->new_key = ins_key;
    LRP->new_val = ins_val;

    return LRP;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
  LogRecPtr LRP (new LogRec());
  if(LogRec::prev_lsn_map_.find(txn_id)==LogRec::prev_lsn_map_.end()) {
   /*说明没找到，这个事务的第一条日志不是begin*/
   ASSERT(false,"An transaction log error: start with non_begin log!");
  }
  LRP->prev_lsn_ = LogRec::prev_lsn_map_.find(txn_id)->second;  /*查找上一个序列号，将其赋值给prev_lsn，同时更新*/
  LRP->lsn_=LogRec::next_lsn_++;
  LogRec::prev_lsn_map_.find(txn_id)->second = LRP->lsn_;

  LRP->type_ = LogRecType::kDelete;
  LRP->txn_id_ = txn_id;
  LRP->old_key = del_key;
  LRP->old_val = del_val;
  LRP->new_key.clear();
  LRP->new_val = -1;
  LRP->com_key.clear();
  LRP->com_val = -1;

  return LRP;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
  LogRecPtr LRP (new LogRec());
  if(LogRec::prev_lsn_map_.find(txn_id)==LogRec::prev_lsn_map_.end()) {
   /*说明没找到，这个事务的第一条日志不是begin*/
   ASSERT(false,"An transaction log error: start with non_begin log!");
  }
  LRP->prev_lsn_ = LogRec::prev_lsn_map_.find(txn_id)->second;  /*查找上一个序列号，将其赋值给prev_lsn，同时更新*/
  LRP->lsn_ = LogRec::next_lsn_++;
  LogRec::prev_lsn_map_.find(txn_id)->second = LRP->lsn_;

  LRP->type_ = LogRecType::kUpdate;
  LRP->txn_id_ = txn_id;
  LRP->old_key = old_key;
  LRP->old_val = old_val;
  LRP->new_key = new_key;
  LRP->new_val = new_val;
  LRP->com_key.clear();
  LRP->com_val = -1;

  return LRP;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
  LogRecPtr LRP (new LogRec());

  LRP->prev_lsn_ = INVALID_LSN;
  LRP->lsn_= LogRec::next_lsn_++;

  LRP->type_ = LogRecType::kBegin;
  LRP->txn_id_ = txn_id;
  LRP->old_key.clear();
  LRP->old_val = -1;
  LRP->new_key.clear();
  LRP->new_val = -1;
  LRP->com_key.clear();
  LRP->com_val = -1;

  LogRec::prev_lsn_map_.insert(std::pair(LRP->txn_id_,LRP->lsn_));

  return LRP;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
  LogRecPtr LRP (new LogRec());

  LRP->prev_lsn_ = LogRec::prev_lsn_map_.find(txn_id)->second;  /*查找上一个序列号，将其赋值给prev_lsn，同时更新*/
  LRP->lsn_ = LogRec::next_lsn_++;
  LogRec::prev_lsn_map_.find(txn_id)->second = LRP->lsn_;

  LRP->type_ = LogRecType::kCommit;
  LRP->txn_id_ = txn_id;
  LRP->old_key.clear();
  LRP->old_val = -1;
  LRP->new_key.clear();
  LRP->new_val = -1;
  LRP->com_key.clear();
  LRP->com_val = -1;

  LogRec::prev_lsn_map_.erase(LRP->txn_id_);

  return LRP;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
  LogRecPtr LRP (new LogRec());

  LRP->prev_lsn_ = LogRec::prev_lsn_map_.find(txn_id)->second;  /*查找上一个序列号，将其赋值给prev_lsn，同时更新*/
  LRP->lsn_ = LogRec::next_lsn_++;
  LogRec::prev_lsn_map_.find(txn_id)->second = LRP->lsn_;

  LRP->type_ = LogRecType::kAbort;
  LRP->txn_id_ = txn_id;
  LRP->old_key .clear();
  LRP->old_val = -1;
  LRP->new_key.clear();
  LRP->new_val = -1;
  LRP->com_key.clear();
  LRP->com_val = -1;

  LogRec::prev_lsn_map_.erase(LRP->txn_id_);

  return LRP;
}

/**
 * Create by liang
 * 为了更接近真实的基于日志的复原，这里加入补偿日志的功能，忠于教材
 * 这个补偿日志只针对update
 * 现在有 insert undo操作是delete
 *       delete undo操作是insert
 *       update undo操作是compensate
 *       compensate没有undo操作
 */
static LogRecPtr CreateCompensationLog(txn_id_t txn_id, KeyType com_key, ValType com_val) {
 LogRecPtr LRP (new LogRec());

 LRP->prev_lsn_ = LogRec::prev_lsn_map_.find(txn_id)->second;  /*查找上一个序列号，将其赋值给prev_lsn，同时更新*/
 LRP->lsn_ = LogRec::next_lsn_++;
 LogRec::prev_lsn_map_.find(txn_id)->second = LRP->lsn_;

 LRP->type_ = LogRecType::kAbort;
 LRP->txn_id_ = txn_id;
 LRP->old_key .clear();
 LRP->old_val = -1;
 LRP->new_key.clear();
 LRP->new_val = -1;
 LRP->com_key = com_key;
 LRP->com_val = com_val;

 return LRP;
}

#endif  // MINISQL_LOG_REC_H
