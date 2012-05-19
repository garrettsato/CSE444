package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	
	double[] hist;
	double width;
	double min;
	double numTups;
	double max;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.hist = new double[buckets];
    	this.min = min;
    	this.numTups = 0;
    	this.max = max;
    	this.width = (this.max - this.min + 1) / buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	int bucket = (int) (Math.max((v - min), 0) / width);	
    	this.hist[bucket]++;
    	this.numTups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	int bucket = 0;
    	if (v <= max && v >= min) {
    		bucket = (int) ((v - min) / width);
    	}
    	double total_select = 0;
    	switch (op) {
    		case EQUALS: {
    	    	if (v > max || v < min) {
    	    		return 0;
    	    	}
    			return computeEquals(v, bucket);
    		}
    		case GREATER_THAN_OR_EQ: {
    	    	if (v > max) {
    	    		return 0;
    	    	} else if (v <= min) {
    	    		return 1;
    	    	}
    			return computeGreaterThanOrEqualTo(v, bucket);
    		}
    		case GREATER_THAN: {
    			if (v > max) {
    				return 0;
    			} else if (v < min) {
    				return 1;
    			}
    			return computeGreaterThan(v, bucket);
    		}
    		case LESS_THAN_OR_EQ: {
    			if (v < min) {
    				return 0;
    			} else if (v > max) {
    				return 1;
    			}
    			return 1 - computeGreaterThan(v, bucket);
    		}
    		case LESS_THAN: {
    			if (v <= min) {
    				return 0;
    			} else if (v > max) {
    				return 1;
    			}
    			return 1 - computeGreaterThanOrEqualTo(v, bucket);
    		}
    		case NOT_EQUALS: {
    	    	if (v > max || v < min) {
    	    		return 1;
    	    	}
    			return 1 - computeEquals(v, bucket);
    		}
    		case LIKE: 
    			total_select += (hist[bucket] / width) / numTups;
    	}
		return total_select;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
    	String s = "";
    	for (int i = 0; i < hist.length; i++) {
    		s += "bucket: " + i + "contains " + hist[i] + "\n";
    	}
    	s += "numTuples: " + numTups + "\n";
    	s += "buckets: " + hist.length;
    	return s;
    }
    
    private double computeGreaterThanOrEqualTo(int v, int bucket) {
    	double total_select = computeEquals(v, bucket);
    	return total_select + computeGreaterThan(v, bucket);
    }
    
    private double computeGreaterThan(int v, int bucket) {
    	double computedWidth = Math.max(1, width);
		double b_f = hist[bucket] / numTups;
		double b_part = ((width * bucket) - (v - min)) / computedWidth;
		double b_select = b_f * b_part;
		double total_select = b_select;
		for (int i = bucket + 1; i < hist.length; i++) {
			total_select += (hist[i] / numTups);
		}
		return total_select;
	}
    
    private double computeEquals(int v, int bucket) {
    	double computedWidth = Math.max(1, width);
    	return ((double) hist[bucket] / computedWidth) / numTups;
    }
}

