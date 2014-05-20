package com.bigdata.rdf.sail;

import java.util.Random;

/**
 * TestCase to test single writer/mutiple transaction committed readers with
 * SAIL interface.
 * 
 * @author Martyn Cutcher
 * 
 */
public class TestMROWTransactionsNoHistory extends TestMROWTransactions {

	/**
     * 
     */
	public TestMROWTransactionsNoHistory() {
	}

	/**
	 * @param arg0
	 */
	public TestMROWTransactionsNoHistory(String arg0) {
		super(arg0);
	}

	@Override
    protected void setUp() throws Exception {
        super.setUp();
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }
    
//	// similar to test_multiple_transactions but uses direct AbsractTripleStore
//	// manipulations rather than RepositoryConnections
//	public void test_multiple_csem_transaction_nohistory() throws Exception {
//		
////		domultiple_csem_transaction(0);
//		
//		domultiple_csem_transaction2(0/* retentionMillis */,
//				2/* nreaderThreads */, 1000/* nwriters */, 20 * 1000/* nreaders */);
//
//	}
//
//	public void test_multiple_csem_transaction_nohistory_oneReaderThread() throws Exception {
//
//		domultiple_csem_transaction2(0/* retentionMillis */,
//				1/* nreaderThreads */, 1000/* nwriters */, 20 * 1000/* nreaders */);
//
//	}
	
	public void test_multiple_csem_transaction_nohistory_stress() throws Exception {

		final Random r = new Random();
		
		for (int i = 0; i < 10; i++) {

			final int nreaderThreads = r.nextInt(19) + 1;
			
			log.warn("Trial: " + i + ", nreaderThreads=" + nreaderThreads);

			domultiple_csem_transaction2(0/* retentionMillis */,
					nreaderThreads, 20/* nwriters */, 400/* nreaders */);

		}
		
	}
	
//	public void notest_stress_multiple_csem_transaction_nohistory() throws Exception {
//
//		final int retentionMillis = 0;
//		
//		for (int i = 0; i< 50; i++) {
//			
//			domultiple_csem_transaction2(retentionMillis, 2/* nreaderThreads */,
//					1000/* nwriters */, 20 * 1000/* nreaders */);
//
//		}
//		
//	}
//	
//	public void test_multiple_csem_transaction_onethread_nohistory() throws Exception {
//
//		domultiple_csem_transaction_onethread(0);
//		
//	}
//	
//// Open a read committed transaction
//    //do reads
//    //do write without closing read
//    //commit write
//    //close read
//    //repeat
//	public void notest_multiple_csem_transaction_onethread_nohistory_debug() throws Exception {
//		PseudoRandom r = new PseudoRandom(2000);
//
//		for (int run = 0; run < 200; run++) {
//			final int uris = 1 + r.nextInt(599);
//			final int preds = 1 + r.nextInt(49);
//			try {
//				System.err.println("Testing with " + uris + " uris, " + preds +  " preds");
//				domultiple_csem_transaction_onethread(0, uris, preds);
//			} catch (Exception e) {
//				System.err.println("problem with " + uris + " uris, " + preds +  " preds");
//				throw e;
//			}
//		}
//	}
}
