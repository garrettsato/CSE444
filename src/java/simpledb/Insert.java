package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private  DbIterator child;
    private int tableid;
    private DbFile table;
    private boolean called;
   
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.tableid = tableid;
        this.table = Database.getCatalog().getDbFile(tableid);
        if (!table.getTupleDesc().equals(child.getTupleDesc())) {
        	throw new DbException("Child has a different tuple description from the table");
        }
    }

    public TupleDesc getTupleDesc() {
        Type[] typeArray = {Type.INT_TYPE};
        return new TupleDesc(typeArray);
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
    }

    public void close() {
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (called) {
    		return null;
    	}
    	int count = 0;
        try {
			while (child.hasNext()) {
				Tuple t = child.next();
				Database.getBufferPool().insertTuple(tid, tableid, t);
				count++;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        called = true;
        Type[] typeArray = {Type.INT_TYPE};
        TupleDesc td = new TupleDesc(typeArray);
        Tuple result = new Tuple(td);	
        result.setField(0, new IntField(count));
        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] children = {child};
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }
}
