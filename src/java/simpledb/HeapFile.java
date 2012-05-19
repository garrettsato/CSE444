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
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private File file;
	private TupleDesc tupleDesc;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
    	return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	try {
			RandomAccessFile ras = new RandomAccessFile(file, "r");
			byte[] b = new byte[BufferPool.PAGE_SIZE];
			long byteOffSet = BufferPool.PAGE_SIZE * pid.pageNumber();
			ras.seek(byteOffSet);
			ras.read(b);
			HeapPageId hpid = new HeapPageId(pid.getTableId(), pid.pageNumber());
			ras.close();
			return (Page) new HeapPage(hpid, b);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
		RandomAccessFile ras = new RandomAccessFile(file, "rw");
    	PageId pid = page.getId();
        byte[] b = page.getPageData();
		long byteOffSet = BufferPool.PAGE_SIZE * pid.pageNumber();
		ras.seek(byteOffSet);
		ras.write(b);
		ras.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> a = new ArrayList<Page>();
        for (int i = 0; i < numPages(); i++) {
        	HeapPage currentPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
        	if (currentPage.getNumEmptySlots() > 0) {
        		currentPage.insertTuple(t);
        		a.add(currentPage);
        		return a;
        	}
        }
    	FileOutputStream fos = new FileOutputStream(file, true);
		byte[] b = new byte[BufferPool.PAGE_SIZE];
		fos.write(b);
		HeapPageId hpid = new HeapPageId(getId(), numPages() - 1);
    	HeapPage currentPage = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_WRITE);
    	currentPage.insertTuple(t);
		return a;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        for (int i = 0; i < numPages(); i++) {
        	HeapPage currentPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_ONLY);
        	if (currentPage.containsTuple(t)) {
    			currentPage.deleteTuple(t);
    			return currentPage;
        	}
        }
        throw new DbException("The file does not contain this tuple: " + t);
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }
}

