#ifndef MINISQL_TXN_H
#define MINISQL_TXN_H

#include <thread>
#include <unordered_set>

#include "common/macros.h"
#include "common/rowid.h"

/**
 * Transaction isolation level.
 */
enum class IsolationLevel { kReadUncommitted, kReadCommitted, kRepeatedRead };

/**
 * Reason to a transaction abortion
 */
enum class AbortReason {
  kLockOnShrinking,
  kUnlockOnShrinking,
  kUpgradeConflict,
  kDeadlock,
  kLockSharedOnReadUncommitted
};

/**
 * Transaction states for 2PL:
 *
 *     _________________________
 *    |                         v
 * GROWING -> SHRINKING -> COMMITTED   ABORTED
 *    |__________|________________________^
 *
 * Transaction states for Non-2PL:
 *     __________
 *    |          v
 * GROWING  -> COMMITTED     ABORTED
 *    |_________________________^
 *
 **/
enum class TxnState { kGrowing, kShrinking, kCommitted, kAborted };

class TxnAbortException : public std::exception {
 public:
  explicit TxnAbortException(txn_id_t txn_id, AbortReason abort_reason)
      : txn_id_(txn_id), abort_reason_(abort_reason) {}

 public:
  txn_id_t txn_id_{INVALID_TXN_ID};
  AbortReason abort_reason_{AbortReason::kLockOnShrinking};
};

class Txn {
 public:
  explicit Txn(txn_id_t txn_id = INVALID_TXN_ID, IsolationLevel iso_level = IsolationLevel::kRepeatedRead)
      : txn_id_(txn_id), iso_level_(iso_level), thread_id_(std::this_thread::get_id()) {}

  ~Txn() = default;

  DISALLOW_COPY(Txn);

  inline txn_id_t GetTxnId() const { return txn_id_; }

  inline IsolationLevel GetIsolationLevel() const { return iso_level_; }

  inline std::thread::id GetThreadId() const { return thread_id_; }

  inline TxnState GetState() { return state_; }

  inline void SetState(TxnState state) { state_ = state; }

  inline std::unordered_set<RowId> &GetSharedLockSet() { return shared_lock_set_; }

  inline std::unordered_set<RowId> &GetExclusiveLockSet() { return exclusive_lock_set_; }

 private:
  txn_id_t txn_id_{INVALID_TXN_ID};
  IsolationLevel iso_level_{IsolationLevel::kRepeatedRead};
  TxnState state_{TxnState::kGrowing};
  std::thread::id thread_id_;
  std::unordered_set<RowId> shared_lock_set_;
  std::unordered_set<RowId> exclusive_lock_set_;
};

#endif  // MINISQL_TXN_H
