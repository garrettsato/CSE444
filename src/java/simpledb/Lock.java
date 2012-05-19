package simpledb;

public interface Lock {

	public boolean heldByTransaction(TransactionId tid);
}
