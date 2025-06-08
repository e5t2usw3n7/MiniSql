#include "concurrency/txn_manager.h"

#include "concurrency/lock_manager.h"

TxnManager::TxnManager(LockManager *lock_mgr) : lock_mgr_(lock_mgr) { lock_mgr_->SetTxnMgr(this); }

Txn *TxnManager::Begin(Txn *txn, IsolationLevel isolationLevel) {
  if (nullptr == txn) {
    txn = new Txn(next_txn_id_++, isolationLevel);
  }
  std::unique_lock<std::shared_mutex> lock(rw_latch_);
  txn_map_[txn->GetTxnId()] = txn;
  return txn;
}

void TxnManager::Commit(Txn *txn) {
  // change state
  txn->SetState(TxnState::kCommitted);
  // release all locks
  ReleaseLocks(txn);
}

void TxnManager::Abort(Txn *txn) {
  // change state
  txn->SetState(TxnState::kAborted);
  // release all locks
  ReleaseLocks(txn);
}

Txn *TxnManager::GetTransaction(txn_id_t txn_id) {
  std::shared_lock<std::shared_mutex> lock(rw_latch_);
  auto iter = txn_map_.find(txn_id);
  if (iter != txn_map_.end()) {
    return iter->second;
  }
  return nullptr;
}

void TxnManager::ReleaseLocks(Txn *txn) {
  std::unordered_set<RowId> lock_set;
  for (auto o : txn->GetExclusiveLockSet()) {
    lock_set.emplace(o);
  }
  for (auto o : txn->GetSharedLockSet()) {
    lock_set.emplace(o);
  }
  for (auto rid : lock_set) {
    lock_mgr_->Unlock(txn, rid);
  }
}