package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LockManager {
	
	private Map<PageId, Lock> locks;
	private Map<TransactionId, Set<PageId>> transactions;
	private final int DEADLOCK_TIMEOUT = 1000;
	
	public LockManager() {
		locks = new HashMap<PageId, Lock>();
		transactions = new HashMap<TransactionId, Set<PageId>>();
	}
	
	/**
	 * Acquires a lock for the current transaction and page. If a lock on the page
	 * is already held by another transaction, this method will block until the 
	 * lock on that page has been released.
	 * @param tid
	 * @param pid
	 * @throws TransactionAbortedException 
	 */
	public void getLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
		System.out.println("---------------------------------");
		System.out.println("Started to acquire the lock");
		System.out.println("It has permissions: " + perm);
		if (locks.containsKey(pid)) {
			System.out.println("\tThere was already a lock on this page");
			Lock l = locks.get(pid);
			if (l instanceof SharedLock) {
				System.out.println("\t\tThe lock is a shared lock");
				SharedLock sl = (SharedLock) l;
				if (perm == Permissions.READ_ONLY) {
					if (!l.heldByTransaction(tid)) {
						sl.incrementCount(tid);
					}
				} else {
					block(sl, tid);
					ExclusiveLock el = new ExclusiveLock(pid, tid);
					locks.put(pid, el);
				}
			} else if (l instanceof ExclusiveLock){
				System.out.println("\t\tThe lock is an exclusive lock");
				if (!l.heldByTransaction(tid)) {
					ExclusiveLock el = (ExclusiveLock) l;
					block(el);
				}
			}
		} else {
			System.out.println("\tThere was no prior lock on this page");
			// Add this page to the current set for this transactions
			if (transactions.containsKey(tid)) {
				transactions.get(tid).add(pid);
			} else {
				Set<PageId> ps = new HashSet<PageId>();
				ps.add(pid);
				transactions.put(tid, ps);
			}
			// Create a new lock on the page
			if (perm == Permissions.READ_ONLY) {
				System.out.println("\t\tI just created a shared lock");
				SharedLock sl = new SharedLock(pid);
				sl.incrementCount(tid);
				locks.put(pid, sl);
			} else {
				System.out.println("\t\tI just created an exclusive lock");
				ExclusiveLock el = new ExclusiveLock(pid, tid);
				locks.put(pid, el);
			}
		}
		System.out.println("\t\t\tIt has permissions: " + perm);
		System.out.println("\t\t\t\tIt has " + pid);
		System.out.println("---------------------------------");
	}
	
	public void releaseLock(TransactionId tid, PageId pid) {
		if (!locks.containsKey(pid)) {
			throw new IllegalArgumentException("This page with pid: " + pid.toString() +
					"does not currently have a lock on it");
		}
		Lock l = locks.get(pid);	
		if (!l.heldByTransaction(tid)) {
			throw new IllegalArgumentException("The lock on page " + pid.toString() + 
					" is not currently held by transaction " + tid.toString());
		}
		if (l instanceof SharedLock) {
			SharedLock sl = (SharedLock) l;
			sl.decrementCount(tid);
			if (sl.count() == 0) {
				locks.remove(pid);
			}
		} else {
			ExclusiveLock el = (ExclusiveLock) l;
			el.releaseLock();
			locks.remove(pid);
		}
	}
	
	public Set<PageId> releaseAllLocks(TransactionId tid) {
		Set<PageId> pids = transactions.get(tid);
		for (PageId pid: pids) {
			this.releaseLock(tid, pid);
		}
		return pids;
	}
	
	public boolean holdsLock(TransactionId tid, PageId pid) {
		if (locks.containsKey(pid)) {
			Lock l = locks.get(pid);
			return l.heldByTransaction(tid);
		}
		return false;
	}
	
	private void block(SharedLock sl, TransactionId tid) throws TransactionAbortedException {
		long t0 = System.currentTimeMillis();
		while (sl.count() > 0 && !(sl.count() == 1 && sl.heldByTransaction(tid))) {
			waitNMillis(10);
			if (System.currentTimeMillis() - t0 > DEADLOCK_TIMEOUT) {
				throw new TransactionAbortedException();
			}
		}
	}
	
	private void block(ExclusiveLock el) throws TransactionAbortedException {
		long t0 = System.currentTimeMillis();
		while (!el.isReleased()) {
			waitNMillis(10);
			if (System.currentTimeMillis() - t0 > DEADLOCK_TIMEOUT) {
				throw new TransactionAbortedException();
			}
		}
	}
	
	private void waitNMillis(int n) {
		long t0 = System.currentTimeMillis();
		long t1;
		do {
			t1 = System.currentTimeMillis();
		} while ((t1 - t0) > n);
	}
}
