package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final IntField defaultGroup = new IntField(0);

    private int gbField, aField;
    private Type gbFieldType;
    private Op what;

    private HashMap<Field, Integer> results;
    private HashMap<Field, Integer> counts;

    private TupleDesc td = null;
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
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.what = what;
        this.results = new HashMap<Field, Integer>();
        this.counts = new HashMap<Field, Integer>();
    }

    private void merge(Field key, int val) {
        Integer count = counts.get(key);
        Integer result = results.get(key);
        if (count == null) {
            counts.put(key, 0);
            count = 0;
        }
        if (result == null) {
            results.put(key, 0);
            if (what == Op.MAX) result = Integer.MIN_VALUE;
            else if (what == Op.MIN) result = Integer.MAX_VALUE;
            else result = 0;
        }
        switch (what) {
        case AVG:
            counts.put(key, counts.get(key) + 1);
            results.put(key, results.get(key) + val);
            break;
        case COUNT:
            counts.put(key, counts.get(key) + 1); break;
        case MAX:
            results.put(key, Integer.max(val, result)); break;
        case MIN:
            results.put(key, Integer.min(val, result)); break;
        case SUM:
            results.put(key, val + result); break;
        default:
            assert(false);
        }
    }
    private int finalVal(Field key) {
        switch (what) {
        case AVG:
            return results.get(key) / counts.get(key);
        case COUNT:
            return counts.get(key);
        case MAX:
        case MIN:
        case SUM:
            return results.get(key);
        default:
            assert(false);
            return 0;
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
        // some code goes here
        if (gbField == Aggregator.NO_GROUPING) {
            merge(defaultGroup, ((IntField)tup.getField(aField)).getValue());
        } else {
            merge(tup.getField(gbField),((IntField)tup.getField(aField)).getValue());
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
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        class IntegerAggregatorIterator implements OpIterator {
            // boolean noGpHasNext = false;
            Iterator<Field> iterator = null;
            public boolean hasNext() {
                return iterator.hasNext();
            }
            public Tuple next() {
                Tuple tp = new Tuple(getTupleDesc());
                Field key = iterator.next();
                int result = finalVal(key);
                if (gbField == Aggregator.NO_GROUPING) {
                    tp.setField(0, new IntField(result));
                    return tp;
                } else {
                    tp.setField(0, key);
                    tp.setField(1, new IntField(result));
                }
                return tp;
            }
            public void open() {
                iterator = results.keySet().iterator();
            }
            public void close() {
                iterator = null;
            }
            public void rewind() {
                close(); open();
            }
            public TupleDesc getTupleDesc() {
                return IntegerAggregator.this.getTupleDesc();
            }
        }
        return new IntegerAggregatorIterator();
    }

}
