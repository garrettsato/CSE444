package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	
	private TDItem[] fields;
	private int size;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        private Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
        
        public Type getFieldType() {
        	return fieldType;
        }
        
        public String getFieldName() {
        	return fieldName;
        }
    }
 
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return Arrays.asList(fields).iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	size = 0;
    	this.fields = new TDItem[typeAr.length];
    	for (int i = 0; i < typeAr.length; i++) {
    		fields[i] = new TDItem(typeAr[i], fieldAr[i]);
    		size += typeAr[i].getLen();
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	size = 0;
    	this.fields = new TDItem[typeAr.length];
    	for (int i = 0; i < typeAr.length; i++) {
    		fields[i] = new TDItem(typeAr[i], null);
    		size += typeAr[i].getLen();
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
    	return fields.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i >= fields.length || fields.length < 0) {
        	throw new NoSuchElementException("This tuple does not contain a field at index " + i);
        } else {
        	return fields[i].fieldName;
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i >= fields.length || fields.length < 0) {
        	throw new NoSuchElementException("This tuple does not contain a field at index " + i);
        } else {
        	return fields[i].fieldType;
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	for (int i = 0; i < fields.length; i++) {
        	if (fields[i].fieldName != null && fields[i].fieldName.equals(name)) {
        		return i;
        	}
        }
    	throw new NoSuchElementException("There is no field with name: " + name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
       return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int mergedLength = td1.numFields() + td2.numFields();
        Type[] mergedTypeAr = new Type[mergedLength];
        String[] mergedFieldAr = new String[mergedLength];
        boolean hasFieldName = false;
        TDItem[] td1Fields = td1.fields;
        TDItem[] td2Fields = td2.fields;
        for (int i = 0; i < td1Fields.length; i++) {
        	mergedTypeAr[i] = td1Fields[i].fieldType;
        	if (td1Fields[i].fieldName != null) {
        		mergedFieldAr[i] = td1Fields[i].fieldName;
        		hasFieldName = true;
        	}
        }
        int initialLength = td1Fields.length;
        for (int i = 0; i < td2Fields.length; i++) {
        	mergedTypeAr[i + initialLength] = td2Fields[i].fieldType;
        	if (td2Fields[i].fieldName != null) {
        		mergedFieldAr[i + initialLength] = td2Fields[i].fieldName;
        		hasFieldName = true;
        	}
        }
        if (hasFieldName) {
        	return new TupleDesc(mergedTypeAr, mergedFieldAr);
        } else {
        	return new TupleDesc(mergedTypeAr);
        }
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    // try to add better functionality 
    //
    //
    //
    //
    public boolean equals(Object o) {
        if (o instanceof TupleDesc) {
        	TupleDesc other = (TupleDesc) o;
        	if (this.getSize() == other.getSize() && this.numFields() == other.numFields()) {
        		for (int i = 0; i < this.numFields(); i++) { 
        			Type thisType = this.fields[i].getFieldType();
        			Type otherType = other.fields[i].getFieldType();
        			if (!thisType.equals(otherType)) {
        				return false;
        			}
        			return true;
        		}
        	} 
        }
		return false;
    }
    /*
     * 
     * 
     * 
     * 
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return fields.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	String s = fields[0].fieldType + "(" + fields[0].fieldName + ")";
    	for (int i = 1; i < fields.length; i++) {
    		s += ", " + fields[i].fieldType + "(" + fields[i].fieldName + ")";
    	}
    	return s;
    }
}
