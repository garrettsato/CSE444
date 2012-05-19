package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */ 
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private Map<PageId, Page> pages;
    private ArrayList<PageId> orderOfPages;
    private int numPages;
    private LockManager lockMan;
    

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
    	this.pages = new HashMap<PageId, Page>(numPages);
    	orderOfPages = new ArrayList<PageId>(numPages);
    	this.numPages = numPages;
    	lockMan = new LockManager();
    }	

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction..,l
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        if (orderOfPages.size() == numPages) { 
        	try {
				evictPage();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        lockMan.getLock(tid, pid, perm);
    	if (pages.containsKey(pid)) {
    		orderOfPages.remove(pid);
    		orderOfPages.add(pid);
    		return pages.get(pid);
    	} else {
	        int tableid = pid.getTableId();
	        DbFile dbfile = Database.getCatalog().getDbFile(tableid);
	        Page p = dbfile.readPage(pid);
			orderOfPages.add(pid);
	    	return pages.put(pid, p);
    	}
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        lockMan.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return lockMan.holdsLock(tid, pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Set<PageId> pids = lockMan.releaseAllLocks(tid);
		if (commit) {
			flushPages(pids);
		} else {
			revertPages(pids);
		}  		
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile table = Database.getCatalog().getDbFile(tableId);
        ArrayList<Page> ps = table.insertTuple(tid, t);
        for (Page p: ps) {
        	p.markDirty(true, tid);
        	PageId pageId = p.getId();
        	if (this.pages.containsKey(pageId)) {
        		this.pages.put(pageId, p);
        	}
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
    	int tableid = t.getRecordId().getPageId().getTableId();
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(tableid);
        Page p = table.deleteTuple(tid, t);
    	p.markDirty(true, tid);
    	PageId pageId = p.getId();
    	if (this.pages.containsKey(pageId)) {
    		this.pages.put(pageId, p);
    	}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        /**
         * 
         * 
         * 
         * 
         * 
         * 
         * 
         */
    	for (PageId pid: orderOfPages) {
    		flushPage(pid);
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        /**
         * 
         * 
         * 
         * 
         * 
         * 
         * 
         */
    	int tableid = pid.getTableId();
    	DbFile tableFile = Database.getCatalog().getDbFile(tableid);
    	Page p = pages.remove(pid);
    	p.markDirty(false, null);
    	tableFile.writePage(p);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(Set<PageId> pids) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	for (PageId pid: pids) {
    		flushPage(pid);
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * @throws IOException 
     */
    private synchronized  void evictPage() throws DbException, IOException {
        /**
         * 
         * 
         * 
         * 
         * 
         * 
         * 
         */
    	for (int i = 0; i < orderOfPages.size(); i++) {
    		PageId pid = orderOfPages.get(i);
    		Page p = pages.get(pid);
    		if (p.isDirty() == null) {
    			orderOfPages.remove(i);
    	    	flushPage(pid);
    	    	return;
    		}
    	}
    }
    
    private synchronized void revertPage(PageId pid) {
    	int tableid = pid.getTableId();
    	DbFile tableFile = Database.getCatalog().getDbFile(tableid);
    	Page p = pages.get(pid);
    	p.markDirty(false, null);
    	pages.put(pid, tableFile.readPage(pid));
    }
    
    private synchronized void revertPages(Set<PageId> pids) {
    	for (PageId pid: pids) {
    		revertPage(pid);
    	}
    }
}
