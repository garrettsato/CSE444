package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
	
	
    private static final long serialVersionUID = 1L;
    
    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    HashMap<Field, Tuple> aggregateMap;
    TupleDesc tupleDesc;
    private static final StringField NO_GROUPING_FIELD = new StringField("-1", 10);



    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	if (what != Aggregator.Op.COUNT) {
        	throw new IllegalArgumentException("Expected Op of type COUNT, instead received: " + what);
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    	aggregateMap = new HashMap<Field, Tuple>();
        if (gbfield == Aggregator.NO_GROUPING) {
        	Type[] type = {Type.INT_TYPE};
        	tupleDesc = new TupleDesc(type);
        } else {
        	Type[] type = {gbfieldtype, Type.INT_TYPE};
        	tupleDesc = new TupleDesc(type);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (gbfield == Aggregator.NO_GROUPING) {
        	if (aggregateMap.containsKey(NO_GROUPING_FIELD)) {
        		Tuple t2 = aggregateMap.get(NO_GROUPING_FIELD);
        		int value = ((IntField) t2.getField(0)).getValue();
        		t2.setField(0, new IntField(value + 1));
        		aggregateMap.put(NO_GROUPING_FIELD, t2);
        	} else {
        		Type[] type = {Type.INT_TYPE};
        		Tuple t2 = new Tuple(new TupleDesc(type));
        		t2.setField(0, new IntField(1));
        		aggregateMap.put(NO_GROUPING_FIELD, t2);
        	}
        } else {
        	Field gbfieldValue = tup.getField(gbfield);
        	if (aggregateMap.containsKey(gbfieldValue)) {
        		Tuple t2 = aggregateMap.get(gbfieldValue);
        		int value = ((IntField) t2.getField(afield)).getValue();
        		t2.setField(1, new IntField(value + 1));
        	} else {
        		Tuple t2 = new Tuple(tupleDesc);
        		t2.setField(0, gbfieldValue);
        		t2.setField(1, new IntField(1));
        		aggregateMap.put(gbfieldValue, t2);
        	}
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        return new StringDbIterator();
    }
    
    private class StringDbIterator implements DbIterator {
    	
    	/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private Iterator<Tuple> itr;
		private boolean open;
		
		public StringDbIterator() {
			itr = null;
			open = false;
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			this.itr = aggregateMap.values().iterator();
			open = true;
		}
	
		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			return open && itr.hasNext();
		}
	
		@Override
		public Tuple next() throws DbException, TransactionAbortedException,
				NoSuchElementException {
			return itr.next();
		}
	
		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			this.itr = aggregateMap.values().iterator();
		}
	
		@Override
		public TupleDesc getTupleDesc() {
			return tupleDesc;
		}
	
		@Override
		public void close() {
			itr = null;
			open = false;
		}
    }
}
