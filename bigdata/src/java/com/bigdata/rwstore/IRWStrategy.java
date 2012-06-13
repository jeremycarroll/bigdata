package com.bigdata.rwstore;

import java.nio.ByteBuffer;

import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ICommitter;

/**
 * Defines a marker interface to be used to indicate strategies
 * that share RW semantics such as RWStrategy and MemStrategy
 * 
 * <p>Methods have been added as required to support the clients
 * (mostly AbstractJournal).
 * 
 * @author Martyn Cutcher
 *
 */
public interface IRWStrategy {
	
	/**
	 * @return IRawTx to enable activate/deactvate
	 */
	IRawTx newTx();

	void registerContext(IAllocationContext context);

	void detachContext(IAllocationContext context);

	void abortContext(IAllocationContext context);

	void registerExternalCache(
			ConcurrentWeakValueCache<Long, ICommitter> historicalIndexCache,
			int byteCount);

	long write(ByteBuffer data, long oldAddr, IAllocationContext context);

	long write(ByteBuffer data, IAllocationContext context);

	void delete(long addr, IAllocationContext context);

	/**
	 * Called by DeleteBlockCommitter
	 * 
	 * @return the address of the deferred release data
	 */
	long saveDeferrals();

	/**
	 * Called from AbstractJournal commitNow
	 */
	int checkDeferredFrees(AbstractJournal abstractJournal);

	/**
	 * Checked by AbstractJournal for index validation
	 */
	long getLastReleaseTime();
		
}
