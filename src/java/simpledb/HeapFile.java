package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    File f;
    TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (pid == null) 
            throw new IllegalArgumentException();

        if (pid.getTableId() != getId())
            throw new IllegalArgumentException();

        if (!(pid instanceof HeapPageId))
            throw new IllegalArgumentException();

        int pgNo = ((HeapPageId)pid).getPageNumber();
        if (f.length() < (pgNo + 1) * BufferPool.getPageSize()) {
            throw new IllegalArgumentException();
        }
        HeapPage hp = null;
        try {
            RandomAccessFile ras = new RandomAccessFile(f, "r");
            ras.seek(pgNo * BufferPool.getPageSize());
            byte data[] = new byte[BufferPool.getPageSize()];
            ras.read(data);
            ras.close();
            hp = new HeapPage((HeapPageId)pid, data);
        } catch (IOException e) {
            assert(false);
        }
        return hp;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(f.length() / (long)BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    private class HeapFileIterator implements DbFileIterator {
        HeapPage currentPage = null;
        int currentPgNo = 0, countPg;
        Iterator<Tuple> tpIterator = null;
        public void open() throws DbException, TransactionAbortedException {
            countPg = numPages();
            if (countPg == 0) return;
            currentPage = (HeapPage)(Database.getBufferPool().getPage(null, new HeapPageId(getId(), 0), null));
            tpIterator = currentPage.iterator();
        }
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (currentPage == null) return false;
            if (tpIterator.hasNext()) return true;
            currentPgNo++;
            for (; currentPgNo < countPg; currentPgNo++) {
                currentPage = (HeapPage)(Database.getBufferPool().getPage(null, new HeapPageId(getId(), currentPgNo), null));

                tpIterator = currentPage.iterator();
                if (tpIterator.hasNext()) return true;
            }
            return false;
        }
        public Tuple next() throws DbException, TransactionAbortedException {
            if (tpIterator == null) throw new NoSuchElementException();
            return tpIterator.next();
        }
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }
        public void close() {
            currentPage = null;
            tpIterator = null;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator();
    }

}

