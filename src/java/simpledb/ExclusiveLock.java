package simpledb;

public class ExclusiveLock implements Lock {
	
	private PageId pid;
	boolean isReleased;
	TransactionId tid;
	
	public ExclusiveLock(PageId pid, TransactionId tid) {
		this.pid = pid;
		this.tid = tid;
		this.isReleased = false;
	}
	
	public void releaseLock() {
		this.isReleased = true;
	}
	
	public boolean isReleased() {
		return this.isReleased;
	}
	
	/**
	 * @return true if the object contains the same page id, false otherwise
	 */
	@Override
	public boolean equals(Object other) {
		if (other instanceof ExclusiveLock) {
			ExclusiveLock o = (ExclusiveLock) other;
			return this.pid.equals(o.pid);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return pid.hashCode();
	}

	@Override
	public boolean heldByTransaction(TransactionId tid) {
		return this.tid.equals(tid);
	}
}
