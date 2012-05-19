package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	int currentPageNo;
	HeapPage currentPage;
	Iterator<Tuple> currentPageIterator;
	TransactionId tid;
	private HeapFile file;
	boolean open;
	
	public HeapFileIterator(TransactionId tid, HeapFile file) {
		this.tid = tid;
		this.open = false;
		this.file = file;
	}
	
	@Override
	public void open() throws DbException, TransactionAbortedException {
		currentPageNo = 0;
		open = true;
		this.setCurrentPage();
	}

	@Override
	public boolean hasNext() throws DbException {
		try {
			return hasNextHelper();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException,
			NoSuchElementException {
		if (open) { 
			if  (currentPageIterator.hasNext()) {
				return currentPageIterator.next();
			} 
		}
		throw new NoSuchElementException("There are no more tuples in the file");
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		currentPageNo = 0;
		setCurrentPage();
	}

	@Override
	public void close() {
		currentPage = null;
		currentPageIterator = null;
		open = false;
	}
	
	private void setCurrentPage() throws TransactionAbortedException, DbException {
		currentPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(file.getId(), currentPageNo), Permissions.READ_WRITE);
		currentPageIterator = currentPage.iterator();
	}
	
	private boolean hasNextHelper() throws TransactionAbortedException, DbException {
		if (open) {
			if (currentPageIterator.hasNext()) {
				return true;
			} else if (currentPageNo < file.numPages() - 1) {
				currentPageNo++;
				setCurrentPage();
				return hasNextHelper();
			}
		} 
		return false;
	}
}
