package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId tid;
    OpIterator child;

    TupleDesc td;
    boolean once = false; // true for there is next

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        if (td != null) return td;
        td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Deleted Tuples"});
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
        super.close(); super.open();
        once = true;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!once) return null;
        once = false;
        int cnt = 0;
        try {
            child.open();
            while (child.hasNext()) {
                Database.getBufferPool().deleteTuple(tid, child.next());
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
