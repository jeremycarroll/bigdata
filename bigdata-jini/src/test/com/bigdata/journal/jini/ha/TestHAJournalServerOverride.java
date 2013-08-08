/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Oct 31, 2012
 */
package com.bigdata.journal.jini.ha;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;

import com.bigdata.ha.HACommitGlue;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.msg.IHA2PhaseCommitMessage;
import com.bigdata.ha.msg.IHA2PhasePrepareMessage;
import com.bigdata.ha.msg.IHANotifyReleaseTimeRequest;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.jini.ha.HAJournalTest.HAGlueTest;
import com.bigdata.journal.jini.ha.HAJournalTest.SpuriousTestException;
import com.bigdata.rdf.sail.webapp.client.HttpException;
import com.bigdata.util.ClocksNotSynchronizedException;
import com.bigdata.util.InnerCause;

/**
 * Unit test of the ability to override the {@link HAJournal} implementation
 * class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHAJournalServerOverride extends AbstractHA3JournalServerTestCase {

    public TestHAJournalServerOverride() {
    }

    public TestHAJournalServerOverride(final String name) {
        super(name);
    }

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
                "com.bigdata.journal.jini.ha.HAJournalServer.restorePolicy=new com.bigdata.journal.jini.ha.DefaultRestorePolicy(0L,1,0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.NoSnapshotPolicy()",
                "com.bigdata.journal.jini.ha.HAJournalServer.HAJournalClass=\""+HAJournalTest.class.getName()+"\""
        };
        
    }
    
    /**
     * One service starts, quorum does not meet (replication factor is 3). This
     * also serves to verify the <code>HAJournal-A.config</code> file.
     */
    public void testStartA() throws Exception {
        
        final HAGlue serverA = startA();
        
        try {

            quorum.awaitQuorum(awaitQuorumTimeout, TimeUnit.MILLISECONDS);
            
            fail("Not expecting quorum meet");

        } catch (TimeoutException ex) {
        
            // ignore.
            
        }

        // Service is not met in any role around a quorum.
        try {
            serverA.awaitHAReady(awaitQuorumTimeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            // Ignore expected exception.
        }

        // Verify can access the REST API "status" page.
        doNSSStatusRequest(serverA);

        // Verify self-reports as NotReady.
        awaitHAStatus(serverA, HAStatusEnum.NotReady);
        
        // Verify that service self-reports as NotReady via the REST API.
        assertEquals(HAStatusEnum.NotReady, getNSSHAStatus(serverA));

        // Verify can not read on service.
        assertReadRejected(serverA);

        // Verify can not write on service.
        assertWriteRejected(serverA);

        assertTrue(getHAJournalFileA().exists());
        assertTrue(getHALogDirA().exists());
        assertTrue(getSnapshotDirA().exists());
        
        // Verify that we can invoke extension methods on the service.
        ((HAGlueTest)serverA).helloWorld();

        // Get the serviceId.
        final UUID serviceId = serverA.getServiceId();

        // Setup to fail the next invocation.
        ((HAGlueTest) serverA).failNext("getServiceId", new Class[] {},
                0/* nwait */, 1/* nfail */);

        // Verify that the next invocation fails.
        try {
            serverA.getServiceId();
            fail("Expecting: " + SpuriousTestException.class);
        } catch (RuntimeException t) {
            assertTrue(InnerCause.isInnerCause(t, SpuriousTestException.class));
        }

        // Verify 2nd try succeeds.
        assertEquals(serviceId, serverA.getServiceId());

        // Setup to fail the next invocation.
        ((HAGlueTest) serverA).failNext("getServiceId", new Class[] {},
                0/* nwait */, 1/* nfail */);

        // Clear registered method failure.
        ((HAGlueTest) serverA).clearFail("getServiceId", new Class[] {});

        // Verify RMI succeeds.
        assertEquals(serviceId, serverA.getServiceId());

//        serverA.enterErrorState().get();
//        
//        Thread.sleep(10000/*ms*/);
        
    }

    /**
     * A user level transaction abort must not cause a service leave or quorum
     * break. It should simply discard the buffered write set for that
     * transactions.
     * 
     * TODO Currently, there is a single unisolated connection commit protocol.
     * When we add concurrent unisolated writers, the user level transaction
     * abort will just discard the buffered writes for a specific
     * {@link AbstractTask}.
     * 
     * @throws Exception
     */
    public void testStartABC_userLevelAbortDoesNotCauseQuorumBreak()
            throws Exception {

        fail("write test");

    }

    /**
     * This test forces clock skew on one of the followers causing it to
     * encounter an error in its GatherTask. This models the problem that was
     * causing a deadlock in an HA3 cluster with BSBM UPDATE running on the
     * leader (EXPLORE was running on the follower, but analysis of the root
     * cause shows that this was not required to trip the deadlock). The
     * deadlock was caused by clock skew resulting in an exception and either
     * {@link IHANotifyReleaseTimeRequest} message that was <code>null</code>
     * and thus could not be processed or a failure to send that message back to
     * the leader.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/677" > HA
     *      deadlock under UPDATE + QUERY </a>
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/673" > DGC
     *      in release time consensus protocol causes native thread leak in
     *      HAJournalServer at each commit </a>
     */
    public void testStartABC_releaseTimeConsensusProtocol_clockSkew()
            throws Exception {

        // Enforce the join order.
        final ABC startup = new ABC(true /*sequential*/);

        final long token = awaitFullyMetQuorum();

        // Should be one commit point.
        awaitCommitCounter(1L, startup.serverA, startup.serverB,
                startup.serverC);

        /*
         * Setup B with a significant clock skew to force an error during the
         * GatherTask.
         */
        ((HAGlueTest) startup.serverB).setNextTimestamp(10L);
        
        try {

            // Simple transaction.
            simpleTransaction();

        } catch (Throwable t) {
            /*
             * TODO This test is currently failing because the consensus
             * releaseTime protocol will fail if one of the joined services
             * reports an error. The protocol should be robust to an error and
             * move forward if a consensus can be formed. If a consensus can not
             * be formed (due to some curable error), then any queries running
             * on that service should break (force a service leave). Else, if
             * the remaining services were to advance the release time since
             * otherwise the service could not get through another releaseTime
             * consensus protocol exchange successfully if it is reading on a
             * commit point that has been released by the other services.
             */
            if (!t.getMessage().contains(
                    ClocksNotSynchronizedException.class.getName())) {
                /*
                 * Wrong inner cause.
                 * 
                 * Note: The stack trace of the local exception does not include
                 * the remote stack trace. The cause is formatted into the HTTP
                 * response body.
                 */
                fail("Expecting " + ClocksNotSynchronizedException.class, t);
            }

        } finally {

            // Restore the clock.
            ((HAGlueTest) startup.serverB).setNextTimestamp(-1L);

        }

        /*
         * The quorum token was not advanced. The 2-phase commit was rejected by
         * all services and a 2-phase abort was performed. All services are at
         * the same commit point.
         */

        // Should be one commit point.
        awaitCommitCounter(1L, startup.serverA, startup.serverB,
                startup.serverC);

        // The quorum token is unchanged.
        assertEquals(token, quorum.token());

        // The join order is unchanged.
        awaitJoined(new HAGlue[] { startup.serverA, startup.serverB,
                startup.serverC });

        /*
         * New transactions are still accepted.
         */
        ((HAGlueTest)startup.serverA).log("2nd transaction");
        ((HAGlueTest)startup.serverB).log("2nd transaction");
        ((HAGlueTest)startup.serverC).log("2nd transaction");
        simpleTransaction();

        // Should be one commit point.
        awaitCommitCounter(2L, startup.serverA, startup.serverB,
                startup.serverC);

        // The quorum token is unchanged.
        assertEquals(token, quorum.token());

    }

    /**
     * Three services are started in [A,B,C] order. B is setup for
     * {@link HACommitGlue#prepare2Phase(IHA2PhasePrepareMessage)} to throw an
     * exeption. A simple transaction is performed. We verify that the
     * transaction completes successfully, that the quorum token is unchanged,
     * and that [A,C] both participated in the commit. We also verify that B is
     * moved to the end of the pipeline (by doing a serviceLeave and then
     * re-entering the pipeline) and that it resyncs with the met quorum and
     * finally re-joins with the met quorum. The quorum should not break across
     * this test.
     * 
     * TODO Test timeout scenarios (RMI is non-responsive) where we cancel the
     * PREPARE once the timeout is elapsed.
     */
    public void testStartABC_prepare2Phase_B_votes_NO()
            throws Exception {

        // Enforce the join order.
        final ABC startup = new ABC(true /*sequential*/);

        final long token = awaitFullyMetQuorum();

        // Should be one commit point.
        awaitCommitCounter(1L, startup.serverA, startup.serverB,
                startup.serverC);

        // Setup B to vote "NO" on the next PREPARE request.
        ((HAGlueTest) startup.serverB).voteNo();
        ((HAGlueTest) startup.serverA).log("B will vote NO.");
        ((HAGlueTest) startup.serverB).log("B will vote NO.");
        ((HAGlueTest) startup.serverC).log("B will vote NO.");
        
        // Simple transaction.
        simpleTransaction();
        
        ((HAGlueTest) startup.serverA).log("Transaction done");
        ((HAGlueTest) startup.serverB).log("Transaction done");
        ((HAGlueTest) startup.serverC).log("Transaction done");

        // Verify quorum is unchanged.
        assertEquals(token, quorum.token());
        
        // Should be two commit points on {A,C].
        awaitCommitCounter(2L, startup.serverA, startup.serverC);
        
        ((HAGlueTest) startup.serverA).log("Commit Counter #2");
        ((HAGlueTest) startup.serverB).log("Commit Counter #2");
        ((HAGlueTest) startup.serverC).log("Commit Counter #2");
        /*
         * B should go into an ERROR state and then into SeekConsensus and from
         * there to RESYNC and finally back to RunMet. We can not reliably
         * observe the intervening states. So what we really need to do is watch
         * for B to move to the end of the pipeline and catch up to the same
         * commit point.
         */

        /*
         * The pipeline should be reordered. B will do a service leave, then
         * enter seek consensus, and then re-enter the pipeline.
         */
        awaitPipeline(new HAGlue[] { startup.serverA, startup.serverC,
                startup.serverB });

        final long token2 = awaitFullyMetQuorum();
        
        assertEquals(token, token2);

        /*
         * There should be two commit points on {A,C,B} (note that this assert
         * does not pay attention to the pipeline order).
         */
        awaitCommitCounter(2L, startup.serverA, startup.serverC,
                startup.serverB);

        // B should be a follower again.
        awaitHAStatus(startup.serverB, HAStatusEnum.Follower);

        // quorum token is unchanged.
        assertEquals(token, quorum.token());

    }
    
    /**
     * Three services are started in [A,B,C] order. B is setup for
     * {@link HACommitGlue#prepare2Phase(IHA2PhasePrepareMessage)} to vote "NO".
     * A simple transaction is performed. We verify that the transaction
     * completes successfully, that the quorum token is unchanged, and that
     * [A,C] both participated in the commit. We also verify that B is moved to
     * the end of the pipeline (by doing a serviceLeave and then re-entering the
     * pipeline) and that it resyncs with the met quorum and finally re-joins
     * with the met quorum. The quorum should not break across this test.
     */
    public void testStartABC_prepare2Phase_B_throws_exception()
            throws Exception {

        // Enforce the join order.
        final ABC startup = new ABC(true /*sequential*/);

        final long token = awaitFullyMetQuorum();

        // Should be one commit point.
        awaitCommitCounter(1L, startup.serverA, startup.serverB,
                startup.serverC);
        
        // Setup B to fail the next PREPARE request.
        ((HAGlueTest) startup.serverB)
                .failNext("prepare2Phase",
                        new Class[] { IHA2PhasePrepareMessage.class },
                        0/* nwait */, 1/* nfail */);

        // Simple transaction.
        simpleTransaction();
        
        // Verify quorum is unchanged.
        assertEquals(token, quorum.token());
        
        // Should be two commit points on {A,C].
        awaitCommitCounter(2L, startup.serverA, startup.serverC);
        
        /*
         * Note: Unlike the test above, if there is a problem making the RMI
         * call, then B will not go through its doRejectedCommit() handler and
         * will not enter the ERROR state directly. We need to have B notice
         * that it is no longer at the same commit point, e.g., by observing a
         * LIVE write cache message with an unexpected value for the
         * commitCounter (especially, GT its current expected value). That is
         * the indication that B needs to enter an error state. Until then it
         * does not know that there was an attempt to PREPARE since it did not
         * get the prepare2Phase() message.
         * 
         * - Modified HAJournalServer to enter the error state if we observe a
         * live write cache block for a commitCounter != the expected
         * commitCounter.
         * 
         * - Modified commit2Phase() to accept the #of services that are
         * participating in the commit. If it is not a full quorum, then we can
         * not purge the HA logs in commit2Phase() regardless of what the quorum
         * state looks like.
         * 
         * - Modified this test to do another transaction. B can not notice the
         * problem until there is another write cache flushed through the
         * pipeline.
         * 
         * - Modified this test to await B to move to the end of the pipeline,
         * resync, and rejoin.
         */
        
        // Should be two commit points on {A,C}.
        awaitCommitCounter(2L, startup.serverA, startup.serverC);

        // Should be ONE commit points on {B}.
        awaitCommitCounter(1L, startup.serverB);

        // A commit is necessary for B to notice that it did not prepare.
        simpleTransaction();
        
        /*
         * The pipeline should be reordered. B will do a service leave, then
         * enter seek consensus, and then re-enter the pipeline.
         */
        awaitPipeline(new HAGlue[] { startup.serverA, startup.serverC,
                startup.serverB });

        /*
         * There should be three commit points on {A,C,B} (note that this assert
         * does not pay attention to the pipeline order).
         */
        awaitCommitCounter(3L, startup.serverA, startup.serverC,
                startup.serverB);

        // B should be a follower again.
        awaitHAStatus(startup.serverB, HAStatusEnum.Follower);

        // quorum token is unchanged.
        assertEquals(token, quorum.token());

    }

    /**
     * Three services are started in [A,B,C] order. B is setup for
     * {@link HACommitGlue#prepare2Phase(IHA2PhasePrepareMessage)} to throw an
     * exeption. A simple transaction is performed. We verify that the
     * transaction completes successfully, that the quorum token is unchanged,
     * and that [A,C] both participated in the commit. We also verify that B is
     * moved to the end of the pipeline (by doing a serviceLeave and then
     * re-entering the pipeline) and that it resyncs with the met quorum and
     * finally re-joins with the met quorum. The quorum should not break across
     * this test.
     * 
     * FIXME Variant where the commit2Phase fails. Note: The COMMIT message is
     * design to do as little work as possible. In practice, this requires an
     * RMI to the followers, each follower must not encounter an error when it
     * validates the COMMIT message, and each follower must put down its new
     * root block (from the prepare message) and then sync the disk. Finally,
     * the RMI response must be returned.
     * <p>
     * Under what conditions can a COMMIT message fail where we can still
     * recover? Single node failure? Leader failure? (QuorumCommitImpl currently
     * fails the commit if there is a single failure, even though the quourm
     * might have a consensus around the new commit point.)
     * 
     * TODO Consider leader failure scenarios in this test suite, not just
     * scenarios where B fails. Probably we should also cover failures of C (the
     * 2nd follower). We should also cover scenariors where the quorum is barely
     * met and a single failure causes a rejected commit (local decision) or
     * 2-phase abort (joined services in joint agreement).
     */
    public void testStartABC_commit2Phase_B_fails()
            throws Exception {

        // Enforce the join order.
        final ABC startup = new ABC(true /*sequential*/);

        final long token = awaitFullyMetQuorum();

        // Should be one commit point.
        awaitCommitCounter(1L, startup.serverA, startup.serverB,
                startup.serverC);

        // Setup B to fail the "COMMIT" message.
        ((HAGlueTest) startup.serverB)
                .failNext("commit2Phase",
                        new Class[] { IHA2PhaseCommitMessage.class },
                        0/* nwait */, 1/* nfail */);

        // Simple transaction.
        simpleTransaction();
        
//        try {
//            // Simple transaction.
//            simpleTransaction();
//            fail("Expecting failed transaction");
//        } catch(HttpException ex) {
//        if (!ex.getMessage().contains(
//                SpuriousTestException.class.getName())) {
//            /*
//             * Wrong inner cause.
//             * 
//             * Note: The stack trace of the local exception does not include
//             * the remote stack trace. The cause is formatted into the HTTP
//             * response body.
//             */
//            fail("Expecting " + ClocksNotSynchronizedException.class, t);
//        }
//        }
        
        // Verify quorum is unchanged.
        assertEquals(token, quorum.token());
        
        // Should be two commit points on {A,C].
        awaitCommitCounter(2L, startup.serverA, startup.serverC);
        
        /*
         * B should go into an ERROR state and then into SeekConsensus and from
         * there to RESYNC and finally back to RunMet. We can not reliably
         * observe the intervening states. So what we really need to do is watch
         * for B to move to the end of the pipeline and catch up to the same
         * commit point.
         */

        /*
         * The pipeline should be reordered. B will do a service leave, then
         * enter seek consensus, and then re-enter the pipeline.
         */
        awaitPipeline(new HAGlue[] { startup.serverA, startup.serverC,
                startup.serverB });

        /*
         * There should be two commit points on {A,C,B} (note that this assert
         * does not pay attention to the pipeline order).
         */
        awaitCommitCounter(2L, startup.serverA, startup.serverC,
                startup.serverB);

        // B should be a follower again.
        awaitHAStatus(startup.serverB, HAStatusEnum.Follower);

        // quorum token is unchanged.
        assertEquals(token, quorum.token());

    }
    
}
