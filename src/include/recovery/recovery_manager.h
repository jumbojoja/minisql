#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

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
  /**
   * TODO: Student Implement
   */
  void Init(CheckPoint &last_checkpoint) {
    this->active_txns_ = last_checkpoint.active_txns_;
    this->data_ = last_checkpoint.persist_data_;
    this->persist_lsn_ = last_checkpoint.checkpoint_lsn_;
  }

  /**
   * TODO: Student Implement
   */
  void RedoPhase() {
    lsn_t current_lsn = this->persist_lsn_ + 1; /*从检查点的下一条日志开始*/
    lsn_t crash_point = this->log_recs_.size() - 1; /*记录当前日志数目，这就是系统crash时候的最后一条日志的序列号*/

    while (current_lsn <= crash_point) {
      LogRecType log_type = this->log_recs_.find(current_lsn)->second->type_;
      txn_id_t txn_id = this->log_recs_.find(current_lsn)->second->txn_id_;
      /*检查当前序列号的日志类型，分类型处理*/
      switch (log_type) {
        case LogRecType::kInvalid: {
          ASSERT(false, "The log type is invalid!");
        }
        case LogRecType::kBegin: {
          if (this->active_txns_.find(txn_id) == this->active_txns_.end()) {
            /*说明当前活跃的事务中还没有这个事务*/
            this->active_txns_.insert(std::pair(txn_id, current_lsn));
          }
          break;
        }
        case LogRecType::kCommit: {
          if (this->active_txns_.find(txn_id) != this->active_txns_.end()) {
            /*说明当前活跃的事务中找得到这个事务*/
            this->active_txns_.erase(txn_id);
          }
          break;
        }
        case LogRecType::kAbort: {
          if (this->active_txns_.find(txn_id) != this->active_txns_.end()) {
            /*这里需要打补丁，原因是minisql的基于日志恢复本身没有补偿日志，所以遇到abort就傻了,如果有补偿日志是不会出现这种情况的*/
            /*这边的补丁全部都不插入日志*/
            lsn_t abort_lsn = this->log_recs_.find(current_lsn)->second->prev_lsn_;
            while (1) {
              if (abort_lsn == INVALID_LSN) {
                break;
              }
              LogRecType log_type = this->log_recs_.find(abort_lsn)->second->type_;
              txn_id_t txn_id = this->log_recs_.find(abort_lsn)->second->txn_id_;

              switch (log_type) {
                case LogRecType::kInvalid: {
                  ASSERT(false, "The log type is invalid!");
                }
                case LogRecType::kInsert: {
                  /*如果是插入，使用delete来undo*/
                  if (this->active_txns_.find(txn_id) != this->active_txns_.end()) {
                    /*只有事务在undolist里面的日志才需要重做*/
                    KeyType del_key = this->log_recs_.find(abort_lsn)->second->new_key;
                    this->data_.erase(del_key);
                  }
                  break;
                }
                case LogRecType::kDelete: {
                  /*如果是删除，使用插入来undo*/
                  if (this->active_txns_.find(txn_id) != this->active_txns_.end()) {
                    /*只有事务在undolist里面的日志才需要重做*/
                    KeyType ins_key = this->log_recs_.find(abort_lsn)->second->old_key;
                    ValType ins_val = this->log_recs_.find(abort_lsn)->second->old_val;

                    this->data_.insert(std::pair(ins_key, ins_val));
                  }
                  break;
                }
                case LogRecType::kUpdate: {
                  /*如果是更新，使用补偿来undo*/
                  if (this->active_txns_.find(txn_id) != this->active_txns_.end()) {
                    /*只有事务在undolist里面的日志才需要重做*/
                    KeyType com_key = this->log_recs_.find(abort_lsn)->second->old_key;
                    ValType com_val = this->log_recs_.find(abort_lsn)->second->old_val;
                    KeyType update_key = this->log_recs_.find(abort_lsn)->second->new_key;
                    this->data_.erase(update_key);
                    this->data_.insert(std::pair(com_key, com_val));
                  }
                  break;
                }
                default: {
                  break;
                }
              }
              abort_lsn = this->log_recs_.find(abort_lsn)->second->prev_lsn_;
            }
            this->active_txns_.erase(txn_id);
          }
          break;
        }
        case LogRecType::kInsert: {
          /*如果这条是插入，使用Insert重做它*/
          if (this->active_txns_.find(txn_id) != this->active_txns_.end()) {
            KeyType ins_key = this->log_recs_.find(current_lsn)->second->new_key;
            ValType ins_val = this->log_recs_.find(current_lsn)->second->new_val;

            this->data_.insert(std::pair(ins_key, ins_val));
          }
          break;
        }
        case LogRecType::kDelete: {
          /*如果这条是delete，使用erase重做它*/
          if (this->active_txns_.find(txn_id) != this->active_txns_.end()) {
            KeyType del_key = this->log_recs_.find(current_lsn)->second->old_key;

            this->data_.erase(del_key);
          }
          break;
        }
        case LogRecType::kUpdate: {
          /*如果这条是update，使用修改重做它*/
          if (this->active_txns_.find(txn_id) != this->active_txns_.end()) {
            KeyType old_key = this->log_recs_.find(current_lsn)->second->old_key;
            KeyType new_key = this->log_recs_.find(current_lsn)->second->new_key;
            ValType new_val = this->log_recs_.find(current_lsn)->second->new_val;

            this->data_.erase(old_key);
            this->data_.insert(std::pair(new_key, new_val));
          }
          break;
        }
        default: {
          break;
        }
      }
      current_lsn++;
    }
  }

  /**
   * TODO: Student Implement
   */
  void UndoPhase() {
    lsn_t crash_point = this->log_recs_.size() - 1;
    lsn_t current_lsn = crash_point;

    while (!active_txns_.empty()) {
      LogRecType log_type = this->log_recs_.find(current_lsn)->second->type_;
      txn_id_t txn_id = this->log_recs_.find(current_lsn)->second->txn_id_;

      switch (log_type) {
        case LogRecType::kInvalid: {
          ASSERT(false, "The log type is invalid!");
        }
        case LogRecType::kInsert: {
          /*如果是插入，使用delete来undo*/
          if (this->active_txns_.find(txn_id) != this->active_txns_.end()) {
            /*只有事务在undolist里面的日志才需要重做*/
            KeyType del_key = this->log_recs_.find(current_lsn)->second->new_key;
            ValType del_val = this->log_recs_.find(current_lsn)->second->new_val;
            LogRecPtr LRP = CreateDeleteLog(txn_id, del_key, del_val);
            this->log_recs_.insert(std::pair(LRP->lsn_, LRP));

            this->data_.erase(del_key);
          }
          break;
        }
        case LogRecType::kDelete: {
          /*如果是删除，使用插入来undo*/
          if (this->active_txns_.find(txn_id) != this->active_txns_.end()) {
            /*只有事务在undolist里面的日志才需要重做*/
            KeyType ins_key = this->log_recs_.find(current_lsn)->second->old_key;
            ValType ins_val = this->log_recs_.find(current_lsn)->second->old_val;
            LogRecPtr LRP = CreateInsertLog(txn_id, ins_key, ins_val);
            this->log_recs_.insert(std::pair(LRP->lsn_, LRP));

            this->data_.insert(std::pair(ins_key, ins_val));
          }
          break;
        }
        case LogRecType::kUpdate: {
          /*如果是更新，使用补偿来undo*/
          if (this->active_txns_.find(txn_id) != this->active_txns_.end()) {
            /*只有事务在undolist里面的日志才需要重做*/
            KeyType com_key = this->log_recs_.find(current_lsn)->second->old_key;
            ValType com_val = this->log_recs_.find(current_lsn)->second->old_val;
            LogRecPtr LRP = CreateCompensationLog(txn_id, com_key, com_val);
            this->log_recs_.insert(std::pair(LRP->lsn_, LRP));

            KeyType update_key = this->log_recs_.find(current_lsn)->second->new_key;
            this->data_.erase(update_key);
            this->data_.insert(std::pair(com_key, com_val));
          }
          break;
        }
        case LogRecType::kBegin: {
          if (this->active_txns_.find(txn_id) != this->active_txns_.end()) {
            LogRecPtr LRP = CreateAbortLog(txn_id);
            this->log_recs_.insert(std::pair(LRP->lsn_, LRP));
            this->active_txns_.erase(txn_id);
          }
        }
        default: {
          break;
        }
      }
      current_lsn--;
    }
  }

  // used for test only
  void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

  // used for test only
  inline KvDatabase &GetDatabase() { return data_; }

 private:
  std::map<lsn_t, LogRecPtr> log_recs_{};
  lsn_t persist_lsn_{INVALID_LSN};
  ATT active_txns_{};  /*所谓当前活跃事务就是教材中的undo list*/
  KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H

