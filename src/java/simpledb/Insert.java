package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId tid;
    OpIterator child;
    int tableId;

    TupleDesc td = null;
    boolean once = false; // true for has next

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        if (td != null) return td;
        td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Inserted Tuples"});
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        once = true;
    }

    public void close() {
        // some code goes here
        super.close();
        once = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        super.close();
        super.open();
        once = true;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
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
        // some code goes here
        if (!once) return null;
        once = false;
        int cnt = 0;
        try {
            child.open();
            while (child.hasNext()) {
                Database.getBufferPool().insertTuple(tid, tableId, child.next());
                cnt ++;
            }
            child.close();
        } catch (IOException e) {
            throw new DbException(e.getMessage());
        }
        Tuple rst = new Tuple(getTupleDesc());
        rst.setField(0, new IntField(cnt));
        return rst;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child = children[0];
    }
}
