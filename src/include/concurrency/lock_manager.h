#ifndef MINISQL_LOCK_MANAGER_H
#define MINISQL_LOCK_MANAGER_H

#include <assert.h>
#include <atomic>
#include <condition_variable>
#include <list>
#include <mutex>
#include <set>
#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/config.h"
#include "common/rowid.h"

class Txn;

class TxnManager;

/**
 * LockManager handles transactions asking for locks on records
 */
class LockManager {
public:
    enum class LockMode {
        kNone, kShared, kExclusive
    };

    /**
     * This class represents a lock request made by a transaction (txn_id)
     */
    class LockRequest {
    public:
        LockRequest(txn_id_t txn_id, LockMode lock_mode)
                : txn_id_(txn_id), lock_mode_(lock_mode), granted_(LockMode::kNone) {}

        txn_id_t txn_id_{0};
        // The type of lock requested (e.g., shared or exclusive)
        LockMode lock_mode_{LockMode::kShared};
        // The type of lock that has been granted to the transaction
        LockMode granted_{LockMode::kNone};
    };

    /**
     * This class manages a queue of lock requests and provides methods to manipulate it. It uses a list (req_list_) to
     * store the requests and an unordered map (req_list_iter_map_) to keep track of the iterators to each request in the
     * list. It also includes a condition variable (cv_) for synchronization purposes, along with some flags to manage
     * concurrent access.
     */
    class LockRequestQueue {
    public:
        using ReqListType = std::list<LockRequest>;

        void EmplaceLockRequest(txn_id_t txn_id, LockMode lock_mode) {
            req_list_.emplace_front(txn_id, lock_mode);
            bool res = req_list_iter_map_.emplace(txn_id, req_list_.begin()).second;
            assert(res);
        }

        bool EraseLockRequest(txn_id_t txn_id) {
            auto iter = req_list_iter_map_.find(txn_id);
            if (iter == req_list_iter_map_.end()) {
                return false;
            }
            req_list_.erase(iter->second);   // erase lock request from req_list
            req_list_iter_map_.erase(iter);  // erase iter from iter_map
            return true;
        }

        ReqListType::iterator GetLockRequestIter(txn_id_t txn_id) {
            auto iter = req_list_iter_map_.find(txn_id);
            assert(iter != req_list_iter_map_.end());
            return iter->second;
        }

    public:
        ReqListType req_list_{};
        std::unordered_map<txn_id_t, ReqListType::iterator> req_list_iter_map_{};

        // for notify blocked txn on this rid.
        std::condition_variable cv_{};

        // A boolean flag indicating whether there's an exclusive write lock currently held.
        bool is_writing_{false};
        // A boolean flag indicating whether a lock upgrade is in progress.
        bool is_upgrading_{false};

        // An integer count of the number of transactions holding shared locks.
        int32_t sharing_cnt_{0};
    };

public:
    LockManager() = default;

    ~LockManager() = default;

    void SetTxnMgr(TxnManager *txn_mgr);

    /**
     * Acquire a lock on RID in shared mode. See [LOCK_NOTE] in header file.
     * @param txn the transaction requesting the shared lock
     * @param rid the RID to be locked in shared mode
     * @return true if the lock is granted, false otherwise
     */

    bool LockShared(Txn *txn, const RowId &rid);

    /**
     * Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
     * @param txn the transaction requesting the exclusive lock
     * @param rid the RID to be locked in exclusive mode
     * @return true if the lock is granted, false otherwise
     */

    bool LockExclusive(Txn *txn, const RowId &rid);

    /**
     * Upgrade a lock from a shared lock to an exclusive lock.
     * @param txn the transaction requesting the lock upgrade
     * @param rid the RID that should already be locked in shared mode by the requesting transaction
     * @return true if the upgrade is successful, false otherwise
     */

    bool LockUpgrade(Txn *txn, const RowId &rid);

    /**
     * Release the lock held by the transaction.
     * @param txn the transaction releasing the lock, it should actually hold the lock
     * @param rid the RID that is locked by the transaction
     * @return true if the unlock is successful, false otherwise
     */
    bool Unlock(Txn *txn, const RowId &rid);

    /*
     * Add edge t1->t2 in the dependency graph
     * */
    void AddEdge(txn_id_t t1, txn_id_t t2);

    /** Removes an edge from t1 -> t2. */
    void RemoveEdge(txn_id_t t1, txn_id_t t2);

    /**
     *  Looks for a cycle by using the Depth First Search (DFS) algorithm.
     *  If it finds a cycle, HasCycle should store the transaction id of the youngest
     *  transaction in the cycle in txn_id and return true. Your function should
     *  return the first cycle it finds. If your graph has no cycles, HasCycle should return false.
     */
    bool HasCycle(txn_id_t &newest_tid_in_cycle);

    void DeleteNode(txn_id_t txn_id);

    /** Runs cycle detection in the background. */
    void RunCycleDetection();

    /*
     * return the set of all edges in the graph, used for testing only!
     * */
    std::vector<std::pair<txn_id_t, txn_id_t>> GetEdgeList();

    inline void EnableCycleDetection(std::chrono::milliseconds &interval) {
        enable_cycle_detection_ = true;
        cycle_detection_interval_ = interval;
    }

    inline void DisableCycleDetection() { enable_cycle_detection_ = false; }

private:
    void LockPrepare(Txn *txn, const RowId &rid);

    void CheckAbort(Txn *txn, LockRequestQueue &req_queue);

private:
    /** Lock table for lock requests. */
    std::unordered_map<RowId, LockRequestQueue> lock_table_{};
    std::mutex latch_{};

    /** Waits-for graph representation. */
    std::unordered_map<txn_id_t, std::set<txn_id_t>> waits_for_{};
    std::unordered_set<txn_id_t> visited_set_{};
    std::stack<txn_id_t> visited_path_{};
    txn_id_t revisited_node_{INVALID_TXN_ID};
    std::atomic<bool> enable_cycle_detection_{false};
    std::chrono::milliseconds cycle_detection_interval_{100};
    TxnManager *txn_mgr_{nullptr};
};

#endif  // MINISQL_LOCK_MANAGER_H
