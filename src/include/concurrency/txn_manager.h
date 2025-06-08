#ifndef MINISQL_TXN_MANAGER_H
#define MINISQL_TXN_MANAGER_H

#include <atomic>
#include <shared_mutex>
#include <unordered_map>

#include "common/config.h"
#include "concurrency/txn.h"

class LockManager;

class TxnManager {
 public:
  explicit TxnManager(LockManager *lock_mgr);

  ~TxnManager() = default;

  /**
   * Begins a new transaction.
   * @param txn an optional transaction object to be initialized, otherwise a new transaction is created.
   * @param isolation_level an optional isolation level of the transaction.
   * @return an initialized transaction
   */
  Txn *Begin(Txn *txn = nullptr, IsolationLevel isolationLevel = IsolationLevel::kRepeatedRead);

  /**
   * Commits a transaction.
   * @param txn the transaction to commit
   */
  void Commit(Txn *txn);

  /**
   * Aborts a transaction
   * @param txn the transaction to abort
   */
  void Abort(Txn *txn);

  /**
   * Locates and returns the transaction with the given transaction ID.
   * @param txn_id the id of the transaction to be found, it must exist!
   * @return the transaction with the given transaction id
   */
  Txn *GetTransaction(txn_id_t txn_id);

 private:
  void ReleaseLocks(Txn *txn);

 private:
  LockManager *lock_mgr_{nullptr};
  std::atomic<txn_id_t> next_txn_id_{0};
  /** The transaction map is a global list of all the running transactions in the system. */
  std::unordered_map<txn_id_t, Txn *> txn_map_{};
  std::shared_mutex rw_latch_{};
};

#endif  // MINISQL_TXN_MANAGER_H
