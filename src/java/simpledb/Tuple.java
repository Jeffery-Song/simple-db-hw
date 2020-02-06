package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * reference, do not copy the td to here.
     */
    TupleDesc tupleDesc;
    RecordId rid; // lab1 todo
    Field fieldList[];

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        // only store desc referrence here?
        tupleDesc = td;
        fieldList = new Field[td.numFields()];
        for (int i = 0; i < td.numFields(); i++) {
            // by type?
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        // lab1 todo
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        // lab1 todo
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        assert(i < fieldList.length && i >= 0);
        fieldList[i] = f.clone();
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        assert(i < fieldList.length && i >= 0);
        return fieldList[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        String s = "";
        for (Field f : fieldList) {
            s += f.toString() + "\t";
        }
        return s;
    }


    private class TupleIterator implements Iterator<Field> {
        private int idx;
        private Tuple tp;
        public TupleIterator(Tuple tp) {
            this.tp = tp;
            idx = 0;
        }
        public boolean hasNext() {
            return idx < tp.fieldList.length;
        }
        public Field next() {
            idx+=1;
            return tp.fieldList[idx-1];
        }
    };
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return new TupleIterator(this);
        // some code goes here
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        tupleDesc = td;
    }
}
