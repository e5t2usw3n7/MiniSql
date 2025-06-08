#include "concurrency/lock_manager.h"

#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"
#include "gtest/gtest.h"

void CheckGrowing(Txn &txn) { ASSERT_EQ(TxnState::kGrowing, txn.GetState()); }

void CheckShrinking(Txn &txn) { ASSERT_EQ(TxnState::kShrinking, txn.GetState()); }

void CheckAborted(Txn &txn) { ASSERT_EQ(TxnState::kAborted, txn.GetState()); }

void CheckCommitted(Txn &txn) { ASSERT_EQ(TxnState::kCommitted, txn.GetState()); }

void CheckTxnLockSize(Txn &txn, size_t shared_expected, size_t exclusive_expected) {
  ASSERT_EQ(shared_expected, txn.GetSharedLockSet().size());
  ASSERT_EQ(exclusive_expected, txn.GetExclusiveLockSet().size());
}

class LockManagerTest : public testing::Test {
 protected:
  void SetUp() override {
    lock_mgr_ = new LockManager();
    txn_mgr_ = new TxnManager(lock_mgr_);
  }

  void TearDown() override {
    delete txn_mgr_;
    delete lock_mgr_;
  }

 protected:
  LockManager *lock_mgr_{nullptr};
  TxnManager *txn_mgr_{nullptr};
};

TEST_F(LockManagerTest, SLockInReadUncommittedTest) {
  Txn t{0, IsolationLevel::kReadUncommitted};
  RowId r1(0, 0);
  try {
    lock_mgr_->LockShared(&t, r1);
  } catch (TxnAbortException &e) {
    ASSERT_EQ(AbortReason::kLockSharedOnReadUncommitted, e.abort_reason_);
  }
  CheckAborted(t);
  CheckTxnLockSize(t, 0, 0);
}

TEST_F(LockManagerTest, TwoPhaseLockingTest) {
  RowId r0(0, 0);
  RowId r1(0, 1);
  Txn *txn = txn_mgr_->Begin();  // REPEATED_READ

  bool res = false;
  res = lock_mgr_->LockShared(txn, r0);
  ASSERT_TRUE(res);
  CheckGrowing(*txn);
  CheckTxnLockSize(*txn, 1, 0);

  res = lock_mgr_->LockExclusive(txn, r1);
  ASSERT_TRUE(res);
  CheckGrowing(*txn);
  CheckTxnLockSize(*txn, 1, 1);

  res = lock_mgr_->Unlock(txn, r0);
  ASSERT_TRUE(res);
  CheckShrinking(*txn);
  CheckTxnLockSize(*txn, 0, 1);

  try {
    lock_mgr_->LockShared(txn, r0);
  } catch (TxnAbortException &e) {
    ASSERT_EQ(AbortReason::kLockOnShrinking, e.abort_reason_);
  }
  CheckAborted(*txn);
  CheckTxnLockSize(*txn, 0, 1);

  txn_mgr_->Abort(txn);
  CheckAborted(*txn);
  CheckTxnLockSize(*txn, 0, 0);

  delete txn;
}

TEST_F(LockManagerTest, UpgradeLockInShrinkingPhase) {
  RowId r(0, 0);
  Txn *t = txn_mgr_->Begin();

  bool res = false;
  res = lock_mgr_->LockShared(t, r);
  ASSERT_TRUE(res);
  CheckGrowing(*t);
  CheckTxnLockSize(*t, 1, 0);

  res = lock_mgr_->Unlock(t, r);
  ASSERT_TRUE(res);
  CheckTxnLockSize(*t, 0, 0);
  CheckShrinking(*t);

  try {
    lock_mgr_->LockUpgrade(t, r);
  } catch (TxnAbortException &e) {
    ASSERT_EQ(AbortReason::kLockOnShrinking, e.abort_reason_);
  }
  CheckAborted(*t);
  txn_mgr_->Abort(t);

  delete t;
}

TEST_F(LockManagerTest, UpgradeConflictTest) {
  RowId r(0, 0);
  Txn *t0 = txn_mgr_->Begin();
  Txn *t1 = txn_mgr_->Begin();

  bool res = false;
  res = lock_mgr_->LockShared(t0, r);
  ASSERT_TRUE(res);
  CheckGrowing(*t0);
  CheckTxnLockSize(*t0, 1, 0);

  res = lock_mgr_->LockShared(t1, r);
  ASSERT_TRUE(res);
  CheckGrowing(*t1);
  CheckTxnLockSize(*t0, 1, 0);

  auto w0 = [&]() {
    // t0 try to lock upgrade but blocked, waits for t1 to release lock
    bool flag = lock_mgr_->LockUpgrade(t0, r);
    // continue after t1 abort
    ASSERT_TRUE(flag);
    CheckGrowing(*t0);
    CheckTxnLockSize(*t0, 0, 1);
  };

  auto w1 = [&]() {
    try {
      // t1 try to lock upgrade but t0 already apply upgrade
      lock_mgr_->LockUpgrade(t1, r);
    } catch (TxnAbortException &e) {
      ASSERT_EQ(AbortReason::kUpgradeConflict, e.abort_reason_);
    }
    CheckAborted(*t1);
    txn_mgr_->Abort(t1);
    CheckTxnLockSize(*t1, 0, 0);
  };

  std::thread i0(w0);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  std::thread i1(w1);

  i1.join();
  i0.join();

  delete t0;
  delete t1;
}

TEST_F(LockManagerTest, UpgradeTest) {
  RowId r(0, 0);
  Txn *t = txn_mgr_->Begin();

  bool res = false;
  res = lock_mgr_->LockShared(t, r);
  ASSERT_TRUE(res);
  CheckGrowing(*t);
  CheckTxnLockSize(*t, 1, 0);

  res = lock_mgr_->LockUpgrade(t, r);
  ASSERT_TRUE(res);
  CheckGrowing(*t);
  CheckTxnLockSize(*t, 0, 1);

  res = lock_mgr_->Unlock(t, r);
  ASSERT_TRUE(res);
  CheckTxnLockSize(*t, 0, 0);
  CheckShrinking(*t);

  txn_mgr_->Commit(t);
  CheckCommitted(*t);

  delete t;
}

TEST_F(LockManagerTest, UpgradeAfterAbortTest) {
  RowId r(0, 0);
  Txn *t0 = txn_mgr_->Begin();
  Txn *t1 = txn_mgr_->Begin();

  bool res = false;
  res = lock_mgr_->LockShared(t0, r);
  ASSERT_TRUE(res);
  CheckGrowing(*t0);
  CheckTxnLockSize(*t0, 1, 0);

  res = lock_mgr_->LockShared(t1, r);
  ASSERT_TRUE(res);
  CheckGrowing(*t1);
  CheckTxnLockSize(*t1, 1, 0);

  auto w0 = [&]() {
    try {
      lock_mgr_->LockUpgrade(t0, r);
    } catch (TxnAbortException &e) {
      ASSERT_EQ(AbortReason::kDeadlock, e.abort_reason_);
    }
    CheckAborted(*t0);
    CheckTxnLockSize(*t0, 0, 0);
  };

  auto w1 = [&]() { txn_mgr_->Abort(t0); };

  std::thread i0(w0);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  std::thread i1(w1);

  i1.join();
  i0.join();

  delete t0;
  delete t1;
}

TEST_F(LockManagerTest, BasicCycleTest1) {
  lock_mgr_->AddEdge(0, 1);
  lock_mgr_->AddEdge(1, 0);
  ASSERT_EQ(2, lock_mgr_->GetEdgeList().size());

  txn_id_t newest_tid_in_cycle = INVALID_TXN_ID;
  bool res = false;
  res = lock_mgr_->HasCycle(newest_tid_in_cycle);
  ASSERT_TRUE(res);
  ASSERT_EQ(1, newest_tid_in_cycle);

  lock_mgr_->RemoveEdge(1, 0);
  res = lock_mgr_->HasCycle(newest_tid_in_cycle);
  ASSERT_FALSE(res);
}

TEST_F(LockManagerTest, BasicCycleTest2) {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges_init = {{0, 1}, {1, 2}, {2, 5}, {5, 1},
                                                           {2, 4}, {1, 3}, {3, 6}, {6, 0}};
  for (const auto &p : edges_init) {
    lock_mgr_->AddEdge(p.first, p.second);
  }
  ASSERT_EQ(edges_init.size(), lock_mgr_->GetEdgeList().size());

  bool res = false;
  txn_id_t newest_tid_in_cycle = INVALID_TXN_ID;

  res = lock_mgr_->HasCycle(newest_tid_in_cycle);
  ASSERT_TRUE(res);
  ASSERT_EQ(5, newest_tid_in_cycle);

  res = false;
  newest_tid_in_cycle = INVALID_TXN_ID;
  // break one cycle
  lock_mgr_->RemoveEdge(5, 1);
  ASSERT_EQ(edges_init.size() - 1, lock_mgr_->GetEdgeList().size());
  res = lock_mgr_->HasCycle(newest_tid_in_cycle);
  ASSERT_TRUE(res);
  ASSERT_EQ(6, newest_tid_in_cycle);

  newest_tid_in_cycle = INVALID_TXN_ID;
  // break another one cycle
  lock_mgr_->RemoveEdge(6, 0);
  ASSERT_EQ(edges_init.size() - 2, lock_mgr_->GetEdgeList().size());
  res = lock_mgr_->HasCycle(newest_tid_in_cycle);
  ASSERT_FALSE(res);
}

TEST_F(LockManagerTest, DeadlockDetectionTest1) {
  RowId r0(0, 0);
  RowId r1(0, 1);
  Txn *t0 = txn_mgr_->Begin();
  Txn *t1 = txn_mgr_->Begin();
  auto cycle_detection_interval = std::chrono::milliseconds(500);
  lock_mgr_->EnableCycleDetection(cycle_detection_interval);

  auto w0 = [&]() {
    // execution flow 1
    bool res = lock_mgr_->LockExclusive(t0, r0);
    ASSERT_TRUE(res);
    ASSERT_EQ(TxnState::kGrowing, t0->GetState());

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // execution flow 2
    res = lock_mgr_->LockExclusive(t0, r1);  // this will be blocked
    ASSERT_TRUE(res);

    // execution flow 4
    txn_mgr_->Commit(t0);
    ASSERT_EQ(TxnState::kCommitted, t0->GetState());
  };

  auto w1 = [&]() {
    // execution flow 1
    bool res = lock_mgr_->LockExclusive(t1, r1);
    ASSERT_TRUE(res);
    ASSERT_EQ(TxnState::kGrowing, t1->GetState());

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // execution flow 2
    try {
      lock_mgr_->LockExclusive(t1, r0);  // this will be blocked
    } catch (TxnAbortException &e) {
      ASSERT_EQ(AbortReason::kDeadlock, e.abort_reason_);
    }

    // execution flow 3
    ASSERT_EQ(TxnState::kAborted, t1->GetState());
    txn_mgr_->Abort(t1);  // locks will be released after deadlock detection
  };

  // start deadlock detect worker
  std::thread detect_worker(std::bind(&LockManager::RunCycleDetection, lock_mgr_));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  std::thread i0(w0);
  std::thread i1(w1);

  std::this_thread::sleep_for(cycle_detection_interval * 2);

  i1.join();
  i0.join();

  lock_mgr_->DisableCycleDetection();
  detect_worker.join();

  delete t0;
  delete t1;
}

TEST_F(LockManagerTest, DeadlockDetectionTest2) {
  const uint32_t n = 4;
  std::vector<RowId> row(n);
  for (uint32_t i = 0; i < row.size(); ++i) {
    row[i] = {0, i};
  }
  std::vector<Txn *> txn(n);
  for (auto &t : txn) {
    t = txn_mgr_->Begin();
  }

  for (uint32_t i = 0; i < n; ++i) {
    bool res = lock_mgr_->LockShared(txn[i], row[i]);
    ASSERT_TRUE(res);
    ASSERT_EQ(TxnState::kGrowing, txn[i]->GetState());
  }

  {
    bool res = lock_mgr_->LockShared(txn[2], row[1]);
    ASSERT_TRUE(res);
    ASSERT_EQ(TxnState::kGrowing, txn[2]->GetState());
  }

  std::thread i0([&]() {
    bool res = lock_mgr_->LockExclusive(txn[0], row[1]);  // this will be blocked for a while
    ASSERT_TRUE(res);

    txn_mgr_->Commit(txn[0]);
    ASSERT_EQ(TxnState::kCommitted, txn[0]->GetState());
  });

  std::thread i1([&]() {
    bool res = lock_mgr_->LockExclusive(txn[1], row[3]);  // this will be blocked for a while
    ASSERT_TRUE(res);

    txn_mgr_->Commit(txn[1]);
    ASSERT_EQ(TxnState::kCommitted, txn[1]->GetState());
  });

  std::thread i2([&]() {
    try {
      lock_mgr_->LockExclusive(txn[2], row[0]);  // this will be blocked
    } catch (TxnAbortException &e) {
      ASSERT_EQ(AbortReason::kDeadlock, e.abort_reason_);
    }

    ASSERT_EQ(TxnState::kAborted, txn[2]->GetState());
    txn_mgr_->Abort(txn[2]);
  });

  std::thread i3([&]() {
    try {
      lock_mgr_->LockExclusive(txn[3], row[0]);  // this will be blocked
    } catch (TxnAbortException &e) {
      ASSERT_EQ(AbortReason::kDeadlock, e.abort_reason_);
    }

    ASSERT_EQ(TxnState::kAborted, txn[3]->GetState());
    txn_mgr_->Abort(txn[3]);
  });

  // waits for all lock request applied
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // start deadlock detect worker
  auto cycle_detection_interval = std::chrono::milliseconds(500);
  lock_mgr_->EnableCycleDetection(cycle_detection_interval);
  std::thread detect_worker(std::bind(&LockManager::RunCycleDetection, lock_mgr_));
  std::this_thread::sleep_for(cycle_detection_interval * 2);

  i2.join();
  i3.join();
  i1.join();
  i0.join();

  lock_mgr_->DisableCycleDetection();
  detect_worker.join();

  for (auto &t : txn) {
    delete t;
  }
}