package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    HashMap<Field, Tuple> aggregateMap;
    HashMap<Field, Integer> countMap;
    TupleDesc tupleDesc;
    
    private static final IntField NO_GROUPING_FIELD = new IntField(-1);

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    	aggregateMap = new HashMap<Field, Tuple>();
        if (what == Aggregator.Op.AVG) {
        	countMap = new HashMap<Field, Integer>();
        }
        if (gbfield == Aggregator.NO_GROUPING) {
        	Type[] type = {Type.INT_TYPE};
        	tupleDesc = new TupleDesc(type);
        } else {
        	Type[] type = {gbfieldtype, Type.INT_TYPE};
        	tupleDesc = new TupleDesc(type);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
		IntField field1 = (IntField) tup.getField(afield);
        if (gbfield == Aggregator.NO_GROUPING) {
        	if (aggregateMap.containsKey(NO_GROUPING_FIELD)) {
        		Tuple t2 = aggregateMap.get(NO_GROUPING_FIELD);
        		IntField field2 = (IntField) t2.getField(0);
        		IntField result = computeAggregate(field1, field2, NO_GROUPING_FIELD);
        		t2.setField(0, result);
        	} else {
        		int value = initializedUngrouped(NO_GROUPING_FIELD);
    			IntField field2 = new IntField(value);
        		IntField result = computeAggregate(field1, field2, NO_GROUPING_FIELD);
        		Tuple t2 = new Tuple(tupleDesc);
        		t2.setField(0, result);
        		aggregateMap.put(NO_GROUPING_FIELD, t2);
        	}
        } else {
        	Field gbfieldValue = tup.getField(gbfield);
        	if (aggregateMap.containsKey(gbfieldValue)) {
        		Tuple t2 = aggregateMap.get(gbfieldValue);
        		IntField field2 = (IntField) t2.getField(1);
        		IntField result = computeAggregate(field1, field2, gbfieldValue);
        		t2.setField(1, result);
        	} else {
        		int value = initializedUngrouped(gbfieldValue);
    			IntField field2 = new IntField(value);
        		IntField result = computeAggregate(field1, field2, gbfieldValue);
        		Tuple t2 = new Tuple(tupleDesc);
        		t2.setField(0, gbfieldValue);
        		t2.setField(1, result);
        		aggregateMap.put(gbfieldValue, t2);
        	}
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        return new IntDbIterator();
    }

    private IntField computeAggregate(IntField field1, IntField field2, Field gbfieldValue) {
		int value1 = field1.getValue();
		int value2 = field2.getValue();
    	if (what == Aggregator.Op.MIN) {
    		int min = Math.min(value1, value2);
    		return new IntField(min);
    	} else if (what == Aggregator.Op.MAX) {
    		int max = Math.max(value1, value2);
    		return new IntField(max);
    	} else if (what == Aggregator.Op.COUNT) {
    		int count = value2 + 1;
    		return new IntField(count);
    	} else if (what == Aggregator.Op.AVG) {
    		int count = countMap.get(gbfieldValue);
    		count++;
    		int sum = value1 + value2;
    		countMap.put((IntField) gbfieldValue, count);
    		return new IntField(sum);
    	} else {
    		int sum = field1.getValue() + field2.getValue();
    		return new IntField(sum);
    	}
    }
    
    private int initializedUngrouped(Field gbfieldValue) {
        if (what == Aggregator.Op.AVG) {
        	countMap.put(gbfieldValue, 0);
        }
		int value;
		if (what == Aggregator.Op.MAX) {
			value = Integer.MIN_VALUE;
		} else if (what == Aggregator.Op.MIN) {
			value = Integer.MAX_VALUE;
		} else {
			value = 0;
		}
		return value;
    }
   
    private class IntDbIterator implements DbIterator {
    	
    	/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private Iterator<Tuple> itr;
		private boolean open;
		
		public IntDbIterator() {
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
			if (what == Aggregator.Op.AVG) {
				Tuple t = itr.next();
				Field groupByField;
				int sumIndex;
				if (gbfield == NO_GROUPING) {
					groupByField = NO_GROUPING_FIELD;
					sumIndex = 0;
				} else {
					groupByField = t.getField(gbfield);
					sumIndex = 1;
				}
				IntField sum = (IntField) t.getField(sumIndex);
				int sumValue = sum.getValue();
				int countValue = countMap.get(groupByField);
				IntField avg = new IntField(sumValue / countValue);
				Tuple result = Tuple.getTuple(t);
				result.setField(sumIndex, avg);
				return result;
			}
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
