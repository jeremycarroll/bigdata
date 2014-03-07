package com.bigdata.journal.jini.ha;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.ha.HAGlue;

import net.jini.config.Configuration;


public class TestHA1JournalServer extends AbstractHA3JournalServerTestCase {

    /**
     * {@inheritDoc}
     * <p>
     * Note: This overrides some {@link Configuration} values for the
     * {@link HAJournalServer} in order to establish conditions suitable for
     * testing the {@link ISnapshotPolicy} and {@link IRestorePolicy}.
     */
    @Override
    protected String[] getOverrides() {
        
        return new String[]{
//        		"com.bigdata.journal.HAJournal.properties=" +TestHA3JournalServer.getTestHAJournalProperties(com.bigdata.journal.HAJournal.properties),
                "com.bigdata.journal.jini.ha.HAJournalServer.restorePolicy=new com.bigdata.journal.jini.ha.DefaultRestorePolicy(0L,1,0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.NoSnapshotPolicy()",
//                "com.bigdata.journal.jini.ha.HAJournalServer.HAJournalClass=\""+HAJournalTest.class.getName()+"\"",
                "com.bigdata.journal.jini.ha.HAJournalServer.onlineDisasterRecovery=true",
                "com.bigdata.journal.jini.ha.HAJournalServer.replicationFactor=1",
        };
        
    }
    
    protected String getZKConfigFile() {
    	return "zkClient1.config"; // 1 stage pipeline
    }
    
    public TestHA1JournalServer() {
    }

    public TestHA1JournalServer(String name) {
        super(name);
    }

    public void testStartA() throws Exception {
    	doStartA();
    }
    
    protected void doStartA() throws Exception {

        try {
        	quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);
        	
        	fail("HA1 requires quorum of 1!");
        } catch (TimeoutException te) {
        	// expected
        }

        // Start 1 service.
        final HAGlue serverA = startA();
        
        // this should succeed
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);
        
        assertEquals(token, awaitFullyMetQuorum());
        
        final HAGlue leader = quorum.getClient().getLeader(token);
        
        assertEquals(serverA, leader);
    }
    
    public void testSimpleTransaction() throws Exception {
    	doStartA();
    	
    	simpleTransaction();
    }
    
    /**
     * Simple test to see how service take over
     */
    public void testStartA_Fail_then_B() throws Exception {
        // Start 1 service.
        final HAGlue serverA = startA();
        
        awaitPipeline(20, TimeUnit.SECONDS, new HAGlue[] {serverA});
        
        // this should succeed
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);
        
        assertEquals(token, awaitFullyMetQuorum());
        
        // A is leader
        assertEquals(serverA, quorum.getClient().getLeader(token));

        // now shutdown A
        shutdownA();
        
        // await break
        awaitPipeline(20, TimeUnit.SECONDS, new HAGlue[] {});
        
        final HAGlue serverB = startB();

        awaitPipeline(20, TimeUnit.SECONDS, new HAGlue[] {serverB});
        
        final long ntoken =  awaitFullyMetQuorum();
        assertFalse(token == ntoken);
        
        // B is leader
        assertEquals(serverB, quorum.getClient().getLeader(ntoken));
     }
    
    public void testStartAB() throws Exception {
        // Start 1 service.
        final HAGlue serverA = startA();
        
        awaitPipeline(20, TimeUnit.SECONDS, new HAGlue[] {serverA});
        
        // this should succeed
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);
        
        assertEquals(token, awaitFullyMetQuorum());
        
        assertEquals(serverA, quorum.getClient().getLeader(token));

        final HAGlue serverB = startB();
        
        awaitPipeline(20, TimeUnit.SECONDS, new HAGlue[] {serverA, serverB});

        // Adding a new service should not break the quorum
        
        assertEquals(token, awaitFullyMetQuorum());
        
        // A remains leader
        assertEquals(serverA, quorum.getClient().getLeader(token));

        // now shutdown A
        shutdownA();
        
        // await break and B takes over   
        awaitPipeline(20, TimeUnit.SECONDS, new HAGlue[] {serverB});
        
        final long ntoken = awaitFullyMetQuorum();
        assertFalse(token == ntoken);
        
        // B has taken over
        assertEquals(serverA, quorum.getClient().getLeader(ntoken));
     }
    
    /**
     * Although quorum meets on one service, other services can join and be
     * kept in sync
     * 
     * @throws Exception
     */
    public void testStartAB_Transaction() throws Exception {
        // Start 1 service.
        startA();
        
        awaitPipeline(20, TimeUnit.SECONDS, new HAGlue[] {serverA});
        
        // this should succeed
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);
        
        assertEquals(token, awaitFullyMetQuorum());
        
        startB();
        
        awaitPipeline(20, TimeUnit.SECONDS, new HAGlue[] {serverA, serverB});
        
        final HAGlue leader = quorum.getClient().getLeader(token);
        assertEquals(serverA, leader);
               
        simpleTransaction();

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB });
     }
    
}
