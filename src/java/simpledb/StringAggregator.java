package simpledb;

import java.util.*;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField, aField;
    private Type gbFieldType;
    private Op what;

    private HashMap<Field, Integer> results;
    private int countNoGp = 0;

    private TupleDesc td=null;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) throw new IllegalArgumentException();
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.what = what;
        this.results = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbField == Aggregator.NO_GROUPING) {
            countNoGp++;
        } else {
            Integer count = results.get(tup.getField(gbField));
            if (count == null) {
                results.put(tup.getField(gbField), 1);
            } else {
                results.put(tup.getField(gbField), count + 1);
            }
        }
    }

    private TupleDesc getTupleDesc() {
        if (td != null) return td;
        if (gbField == Aggregator.NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            td =  new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE});
        }
        return td;
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        class StringAggregatorIterator implements OpIterator {
            boolean noGpHasNext = false;
            Iterator<HashMap.Entry<Field, Integer>> iterator = null;
            public boolean hasNext() {
                if (gbField == Aggregator.NO_GROUPING) return noGpHasNext;
                return iterator.hasNext();
            }
            public Tuple next() {
                Tuple tp = new Tuple(getTupleDesc());
                if (gbField == Aggregator.NO_GROUPING) {
                    if (!noGpHasNext) {
                        throw new NoSuchElementException();
                    }
                    tp.setField(0, new IntField(countNoGp));
                    return tp;
                }
                HashMap.Entry<Field, Integer> entry = iterator.next();
                tp.setField(0, entry.getKey());
                tp.setField(1, new IntField(entry.getValue()));
                return tp;
            }
            public void open() {
                noGpHasNext = true;
                iterator = results.entrySet().iterator();
            }
            public void close() {
                noGpHasNext = false;
                iterator = null;
            }
            public void rewind() {
                close(); open();
            }
            public TupleDesc getTupleDesc() {
                return StringAggregator.this.getTupleDesc();
            }
        }
        return new StringAggregatorIterator();
    }

}
