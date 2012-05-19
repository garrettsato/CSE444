package simpledb;

import java.util.HashSet;
import java.util.Set;

public class SharedLock implements Lock {

	Set<TransactionId> transactions;
	PageId pid;
	
	public SharedLock(PageId pid) {
		transactions = new HashSet<TransactionId>();
		this.pid = pid;
	}
	
	/**
	 * Adds the specified transaction from the lock
	 * @param tid the transaction to be added to this lock
	 */
	public void incrementCount(TransactionId tid) {
		transactions.add(tid);
	}
	
	/**
	 * Releases the specified transaction's hold on this lock
	 * @param tid the transaction to release this lock
	 * @throws IllegalArgumentException if the specified transaction 
	 * does not have a hold on this lock
	 */
	public void decrementCount(TransactionId tid) {
		if (!transactions.contains(tid)) {
			throw new IllegalArgumentException("The transaction id specified does not currently" +
					"have a hold on this lock");
		}
		transactions.remove(tid);
	}
	
	/**
	 * @return the number of transactions that currently hold this lock
	 */
	public int count() {
		return transactions.size();
	}
	
	/**
	 * 
	 * @param tid the specified transaction to compare
	 * @return true if the transaction has a hold on this lock, false otherwise
	 */
	@Override
	public boolean heldByTransaction(TransactionId tid) {
		return transactions.contains(tid);
	}
	
	/**
	 * @return true if the object contains the same page id, false otherwise
	 */
	@Override
	public boolean equals(Object other) {
		if (other instanceof SharedLock) {
			SharedLock o = (SharedLock) other;
			return this.pid.equals(o.pid);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return pid.hashCode();
	}
}
