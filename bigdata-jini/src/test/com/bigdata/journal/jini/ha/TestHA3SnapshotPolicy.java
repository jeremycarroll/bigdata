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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import com.bigdata.btree.BytesUtil;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.msg.HADigestRequest;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.ha.msg.HASnapshotRequest;
import com.bigdata.ha.msg.IHASnapshotResponse;
import com.bigdata.journal.DumpJournal;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.Journal;
import com.bigdata.quorum.Quorum;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.sail.webapp.client.HAStatusEnum;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;

/**
 * Test suites for an {@link HAJournalServer} quorum with a replication factor
 * of THREE (3) and a fully met {@link Quorum}.
 * 
 * TODO Verify will not take snapshot if size on disk of HALog files since the
 * last snapshot is LT some percentage.
 * 
 * FIXME Verify release of old snapshot(s) and HALog(s) when a new snapshot is
 * taken in accordence with the {@link IRestorePolicy}.
 * <p>
 * Make sure that we never release the most current snapshot or HALogs required
 * to reconstruct a commit point protected by the restore policy.
 * <p>
 * Test for release of snapshots and HALogs as they are aged out.
 * 
 * TODO Test suite for
 * {@link IHABufferStrategy#writeOnStream(OutputStream, Quorum, long)}. This is
 * used to generate snapshots.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHA3SnapshotPolicy extends AbstractHA3JournalServerTestCase {

    public TestHA3SnapshotPolicy() {
    }

    public TestHA3SnapshotPolicy(String name) {
        super(name);
    }
 
//    /**
//     * {@inheritDoc}
//     * <p>
//     * Note: This overrides some {@link Configuration} values for the
//     * {@link HAJournalServer} in order to establish conditions suitable for
//     * testing the {@link ISnapshotPolicy} and {@link IRestorePolicy}.
//     */
//    @Override
//    protected String[] getOverrides() {
//        
//        return new String[]{
//                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.DefaultSnapshotPolicy()"
//        };
//        
//    }

    /**
     * Start A. Verify that we can not take a snapshot since it is not joined
     * with the met quorum.
     */
    public void testA_snapshot_refused_since_not_met() throws Exception {

        // Start A.
        final HAGlue serverA = startA();

        // Verify the REST API is up and service is not ready.
        // TODO Might have to retry this if 404 observed.
        assertEquals(HAStatusEnum.NotReady, getNSSHAStatus(serverA));
        
        // Request a snapshot.
        final Future<IHASnapshotResponse> ft = serverA
                .takeSnapshot(new HASnapshotRequest(0/* percentLogSize */));

        if(ft == null) {
        
            // Ok. No snapshot will be taken.
            return;

        }

        ft.cancel(true/* mayInterruptIfRunning */);
        
        fail("Not expecting a future since service is not joined with a met quorum.");

    }

    /**
     * Start two services. The quorum meets. Take a snapshot. Verify that the
     * snapshot appears within a resonable period of time and that it is for
     * <code>commitCounter:=1</code> (just the KB create). Verify that the
     * digest of the snapshot agrees with the digest of the journal.
     */
    public void testAB_snapshot() throws Exception {

        // Start 2 services.
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA, serverB);

        final HAGlue leader = quorum.getClient().getLeader(token);
        assertEquals(serverA, leader); // A is the leader.
        {

            // Verify quorum is still valid.
            quorum.assertQuorum(token);

            // Verify quorum is at the expected commit point.
            assertEquals(
                    1L,
                    leader.getRootBlock(
                            new HARootBlockRequest(null/* storeUUID */))
                            .getRootBlock().getCommitCounter());

            // Snapshot directory is empty.
            assertEquals(0, getSnapshotDirA().list().length);

            final Future<IHASnapshotResponse> ft = leader
                    .takeSnapshot(new HASnapshotRequest(0/* percentLogSize */));

            // wait for the snapshot.
            try {
                ft.get(5, TimeUnit.SECONDS);
            } catch (TimeoutException ex) {
                ft.cancel(true/* mayInterruptIfRunning */);
                throw ex;
            }

            final IRootBlockView snapshotRB = ft.get().getRootBlock();

            final long commitCounter = 1L;
            
            // Verify snapshot is for the expected commit point.
            assertEquals(commitCounter, snapshotRB.getCommitCounter());

            // Snapshot directory contains the desired filename.
            assertEquals(new String[] { "00000000000000000001"
                    + SnapshotManager.SNAPSHOT_EXT }, getSnapshotDirA().list());

            // Verify digest of snapshot agrees with digest of journal.
            assertSnapshotDigestEquals(leader, commitCounter);

        }

    }

    /**
     * Start two services. The quorum meets. Take a snapshot using B (NOT the
     * leader). Verify that the snapshot appears within a resonable period of
     * time and that it is for <code>commitCounter:=1</code> (just the KB
     * create). Verify that the digest of the snapshot agrees with the digest of
     * the journal.
     */
    public void testAB_snapshotB() throws Exception {

        // Start 2 services.
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA, serverB);

        {

            // Verify quorum is still valid.
            quorum.assertQuorum(token);

            // Snapshot directory is empty.
            assertEquals(0, getSnapshotDirA().list().length);
            assertEquals(0, getSnapshotDirB().list().length);

            final Future<IHASnapshotResponse> ft = serverB
                    .takeSnapshot(new HASnapshotRequest(0/* percentLogSize */));

            // wait for the snapshot.
            try {
                ft.get(5, TimeUnit.SECONDS);
            } catch (TimeoutException ex) {
                ft.cancel(true/* mayInterruptIfRunning */);
                throw ex;
            }

            final IRootBlockView snapshotRB = ft.get().getRootBlock();

            final long commitCounter = 1L;
            
            // Verify snapshot is for the expected commit point.
            assertEquals(commitCounter, snapshotRB.getCommitCounter());

            // Snapshot directory remains empty on A.
            assertEquals(0, getSnapshotDirA().list().length);

            // Snapshot directory contains the desired filename on B.
            assertEquals(new String[] { "00000000000000000001"
                    + SnapshotManager.SNAPSHOT_EXT }, getSnapshotDirB().list());

            // Verify digest of snapshot agrees with digest of journal.
            assertSnapshotDigestEquals(serverB, commitCounter);

        }

    }

    /**
     * Start two services. The quorum meets. Take a snapshot. Verify that the
     * snapshot appears within a resonable period of time and that it is for
     * <code>commitCounter:=1</code> (just the KB create). Request a second
     * snapshot for the same commit point and verify that a <code>null</code> is
     * returned since we already have a snapshot for that commit point.
     */
    public void testAB_snapshot_await_snapshot_null() throws Exception {

        // Start 2 services.
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA, serverB);

        final HAGlue leader = quorum.getClient().getLeader(token);

        {

            // Verify quorum is still valid.
            quorum.assertQuorum(token);

            // Verify quorum is at the expected commit point.
            assertEquals(
                    1L,
                    leader.getRootBlock(
                            new HARootBlockRequest(null/* storeUUID */))
                            .getRootBlock().getCommitCounter());

            // Snapshot directory is empty.
            assertEquals(0, getSnapshotDirA().list().length);

            final Future<IHASnapshotResponse> ft = leader
                    .takeSnapshot(new HASnapshotRequest(0/* percentLogSize */));

            // wait for the snapshot.
            try {
                ft.get(5, TimeUnit.SECONDS);
            } catch (TimeoutException ex) {
                ft.cancel(true/* mayInterruptIfRunning */);
                throw ex;
            }

            final IRootBlockView snapshotRB = ft.get().getRootBlock();

            final long commitCounter = 1L;
            
            // Verify snapshot is for the expected commit point.
            assertEquals(commitCounter, snapshotRB.getCommitCounter());

            // Snapshot directory contains the desired filename.
            assertEquals(new String[] { "00000000000000000001"
                    + SnapshotManager.SNAPSHOT_EXT }, getSnapshotDirA().list());

            // Verify digest of snapshot agrees with digest of journal.
            assertSnapshotDigestEquals(leader, commitCounter);

        }

        /*
         * Verify 2nd request returns null since snapshot exists for that
         * commit point.
         */
        {

            // Verify quorum is still at the expected commit point.
            assertEquals(
                    1L,
                    leader.getRootBlock(
                            new HARootBlockRequest(null/* storeUUID */))
                            .getRootBlock().getCommitCounter());

            // request another snapshot.
            final Future<IHASnapshotResponse> ft = leader
                    .takeSnapshot(new HASnapshotRequest(0/* percentLogSize */));

            if (ft != null) {

                ft.cancel(true/* mayInteruptIfRunning */);

                fail("Expecting null since snapshot exists for current commit point.");

            }

        }

    }

    /**
     * Test ability to request a snapshot using an HTTP GET
     * <code>.../status?snapshot</code>.
     * 
     * TODO Variant where the percentLogSize parameter is also expressed (and
     * maybe make it a 2nd parameter so we could have more choices to drive the
     * policy).
     */
    public void testAB_snapshot_HTTP_GET() throws Exception {

        // Start 2 services.
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA, serverB);

        final HAGlue leader = quorum.getClient().getLeader(token);

        {

            // Verify quorum is still valid.
            quorum.assertQuorum(token);

            // Verify quorum is at the expected commit point.
            assertEquals(
                    1L,
                    leader.getRootBlock(
                            new HARootBlockRequest(null/* storeUUID */))
                            .getRootBlock().getCommitCounter());

            // Snapshot directory is empty.
            assertEquals(0, getSnapshotDirA().list().length);

            doSnapshotRequest(leader);

            /*
             * Get the Future. Should still be there, but if not then will be
             * null (it which case the snapshot is already done).
             */
            final Future<IHASnapshotResponse> ft = leader
                    .takeSnapshot(new HASnapshotRequest(1000/* percentLogSize */));

            // wait for the snapshot.
            if (ft != null) {
                try {
                    ft.get(5, TimeUnit.SECONDS);
                } catch (TimeoutException ex) {
                    ft.cancel(true/* mayInterruptIfRunning */);
                    throw ex;
                }

                final IRootBlockView snapshotRB = ft.get().getRootBlock();

                // Verify snapshot is for the expected commit point.
                assertEquals(1L, snapshotRB.getCommitCounter());

            } else {
                
                // Snapshot completed before we got the Future.
                
            }

            // Snapshot directory contains the desired filename.
            assertEquals(new String[] { "00000000000000000001"
                    + SnapshotManager.SNAPSHOT_EXT }, getSnapshotDirA().list());

            assertSnapshotDigestEquals(leader, 1L/* commitCounter */);

        }

    }

    /**
     * Verify will not take snapshot if running. Loads a bunch of data and then
     * issues (2) snapshot requests. Should both should return the same future
     * since the snapshot will take longer to create than the latency for the
     * 2nd RMI.
     */
    public void testAB_snapshot_running_2nd_snapshot_same_future()
            throws Exception {

        // Start 2 services.
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA, serverB);

        // The joined services, in their service join order.
        final UUID[] joined = quorum.getJoined();

        // The HAGlue interfaces for those joined services, in join order.
        final HAGlue[] services = new HAGlue[joined.length];

        final RemoteRepository[] repos = new RemoteRepository[joined.length];
        
        for (int i = 0; i < joined.length; i++) {

            services[i] = quorum.getClient().getService(joined[i]);

            repos[i] = getRemoteRepository(services[i]);
            
        }
        
        /*
         * LOAD data on leader.
         */
        new LargeLoadTask(token, true/* reallyLargeLoad */).call();

        /*
         * Verify that query on all nodes is allowed and now provides a
         * non-empty result.
         */
        for (RemoteRepository r : repos) {

            // Should have data.
            assertEquals(100L,
                    countResults(r.prepareTupleQuery("SELECT * {?a ?b ?c} LIMIT 100")
                            .evaluate()));

        }

        // Current commit point.
        final long lastCommitCounter2 = serverA
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getCommitCounter();

        // There are now TWO (2) commit points.
        assertEquals(2L, lastCommitCounter2);

        final HAGlue leader = quorum.getClient().getLeader(token);

        {

            // Verify quorum is still valid.
            quorum.assertQuorum(token);

            // Snapshot directory is empty.
            assertEquals(0, getSnapshotDirA().list().length);

            final Future<IHASnapshotResponse> ft = leader
                    .takeSnapshot(new HASnapshotRequest(0/* percentLogSize */));

            final Future<IHASnapshotResponse> ft2 = leader
                    .takeSnapshot(new HASnapshotRequest(0/* percentLogSize */));

            // Both Futures are non-null.
            assertNotNull(ft);
            
            assertNotNull(ft2);

            // Neither Future is done.
            assertFalse(ft.isDone());

            assertFalse(ft2.isDone());
            
            // wait for the snapshot.
            try {
                ft.get(20, TimeUnit.SECONDS);
            } catch (TimeoutException ex) {
                // Interrupt both futures.
                ft.cancel(true/* mayInterruptIfRunning */);
                ft2.cancel(true/* mayInterruptIfRunning */);
                throw ex;
            }
            
            // Verify 2nd future is also done (the should be two proxies for the
            // same future).
            assertTrue(ft2.isDone());

            // Verify no error on 2nd future.
            ft2.get();

            final IRootBlockView snapshotRB = ft.get().getRootBlock();

            final long commitCounter = 2L;
            
            // Verify snapshot is for the expected commit point.
            assertEquals(commitCounter, snapshotRB.getCommitCounter());

            // Snapshot directory contains the desired filename.
            assertEquals(
                    new String[] { SnapshotManager.getSnapshotFile(
                            getSnapshotDirA(), 2L).getName() },
                    getSnapshotDirA().list());

            // Verify digest of snapshot agrees with digest of journal.
            assertSnapshotDigestEquals(leader, commitCounter);

        }

    }
    
    /**
     * Issue HTTP request to a service to take a snapshot.
     * 
     * @param haGlue
     *            The service.
     *            
     * @throws Exception
     * 
     * TODO Add percentLogSize query parameter option.
     */
    private void doSnapshotRequest(final HAGlue haGlue) throws Exception {

        // Client for talking to the NSS.
        final HttpClient httpClient = new DefaultHttpClient(ccm);

        // The NSS service URL (NOT the SPARQL end point).
        final String serviceURL = getNanoSparqlServerURL(haGlue);

        final ConnectOptions opts = new ConnectOptions(serviceURL
                + "/status?snapshot");

        opts.method = "GET";

        try {

            final HttpResponse response;

            RemoteRepository.checkResponseCode(response = doConnect(httpClient,
                    opts));

            EntityUtils.consume(response.getEntity());

        } catch (IOException ex) {

            log.error(ex, ex);
            
            throw ex;
            
        }

    }

    /**
     * Unit test starts A+B and runs N transactions. It then takes a snapshot.
     * The existance of the snapshot is verified, as is the existence of the
     * HALog files for each transaction. Finally, it runs another M
     * transactions.
     * <p>
     * The {@link HARestore} utility is then used to reconstruct a
     * {@link Journal} from the snapshot and replay the HALog files. We then
     * verify that the new journal is at the correct commit point and compare it
     * for binary equality with the original journal on A.
     * 
     * FIXME HARestore test suite: Verify that the snapshot may be unziped and
     * halogs applied by the {@link HARestore} utility in order to obtain a
     * journal corresponding to a specific commit point.
     * <p>
     * Test can read and compare the snapshot with the journal if the journal is
     * static. They should have the same digest.
     */
    public void testAB_snapshot_multipleTx_restore_validate() throws Exception {

        final int N = 7; // #of transactions to run before the snapshot.
        final int M = 8; // #of transactions to run after the snapshot.
        
        // Start 2 services.
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        // wait until A is ready.
        assertEquals(HAStatusEnum.Leader, awaitNSSAndHAReady(serverA));

        // Verify A is the leader.
        assertEquals(serverA, quorum.getClient().getLeader(token));

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA, serverB);
        
        assertCommitCounter(1L, serverA);
        
        // Now run N transactions.
        for (int i = 0; i < N; i++) {

            simpleTransaction();
            
        }

        final long commitCounterN = N + 1;

        assertCommitCounter(commitCounterN, serverA);

        // Check HALogs equal on A, B.
        assertHALogDigestsEquals(1L/* firstCommitCounter */, commitCounterN,
                new HAGlue[] { serverA, serverB });

        /*
         * Take a snapshot.
         */
        {

            // Verify quorum is still valid.
            quorum.assertQuorum(token);

            // Snapshot directory is empty.
            assertEquals(0, getSnapshotDirA().list().length);

            final Future<IHASnapshotResponse> ft = serverA
                    .takeSnapshot(new HASnapshotRequest(0/* percentLogSize */));

            // wait for the snapshot.
            try {
                ft.get(10, TimeUnit.SECONDS);
            } catch (TimeoutException ex) {
                ft.cancel(true/* mayInterruptIfRunning */);
                throw ex;
            }

            final IRootBlockView snapshotRB = ft.get().getRootBlock();

            // Verify snapshot is for the expected commit point.
            assertEquals(commitCounterN, snapshotRB.getCommitCounter());

            // Snapshot directory contains the desired filename.
            assertEquals(
                    new String[] { SnapshotManager.getSnapshotFile(
                            getSnapshotDirA(), commitCounterN).getName() },
                    getSnapshotDirA().list());

            // Verify digest of snapshot agrees with digest of journal.
            assertSnapshotDigestEquals(serverA, commitCounterN);

        }

        // Now run M transactions.
        for (int i = 0; i < M; i++) {

            simpleTransaction();
            
        }

        final long commitCounterM = M + N + 1;

        assertCommitCounter(commitCounterM, serverA);

        /*
         * FIXME Pick up this test again here in a new version of the test, but
         * this time focus on the IRestorePolicy and when the snapshots and
         * HALog files are released. We may have to override the restorePolicy,
         * in which case we might need to put this test into a different file.
         */

        // Check HALogs equal on A, B.
        assertHALogDigestsEquals(1L/* firstCommitCounter */, commitCounterM,
                new HAGlue[] { serverA, serverB });

        /*
         * Now, get the snapshot that we took above, decompress it, and
         * then roll it forward and verify it against the current committed journal.
         */
        {
            final File snapshotFile = SnapshotManager.getSnapshotFile(
                    getSnapshotDirA(), commitCounterN);

            final String basename = snapshotFile.getName().substring(
                    0,
                    snapshotFile.getName().length()
                            - SnapshotManager.SNAPSHOT_EXT.length());

            // temporary file in the same directory as the snapshot.
            final File out = File.createTempFile(basename + "-",
                    Journal.Options.JNL, snapshotFile.getAbsoluteFile()
                            .getParentFile());

            try {

                // Decompress the snapshot.
                SnapshotManager.decompress(snapshotFile, out);

                // Verify that we can open the decompressed file as a Journal.
                {

                    final Properties p = new Properties();
                    
                    p.setProperty(Journal.Options.FILE, out.getAbsoluteFile()
                            .toString());
                    
                    Journal jnl = new Journal(p);

                    try {

                        // TODO Should really be logged output, not stdout.
                        final PrintWriter w = new PrintWriter(System.out,
                                true/* autoFlush */);

                        // Verify snapshot at the expected commit point.
                        assertEquals(commitCounterN, jnl.getRootBlockView()
                                .getCommitCounter());
                        
                        // Verify can dump journal.
                        new DumpJournal(jnl).dumpJournal(w,
                                null/* namespaces */, true/* dumpHistory */,
                                true/* dumpPages */, true/* dumpIndices */,
                                false/* showTuples */);

                        /*
                         * Now roll that journal forward using the HALog directory.
                         */
                        final HARestore rest = new HARestore(jnl,
                                getHALogDirA());

                        /*
                         * TODO Also write test where we stop at the specified
                         * commit point.
                         */
                        rest.restore(false/* listCommitPoints */,
                                Long.MAX_VALUE/* haltingCommitCounter */);

                        // Verify journal now at the expected commit point.
                        assertEquals(commitCounterM, jnl.getRootBlockView()
                                .getCommitCounter());

                        /*
                         * Compute digest of the restored journal. The digest
                         * should agree with the digest of the Journal on A
                         * since we rolled it forward to the same commit point.
                         */
                        {
 
                            // digest of A
                            final byte[] digestA = serverA.computeDigest(
                                    new HADigestRequest(null/* storeUUID */))
                                    .getDigest();

                            final MessageDigest digest = MessageDigest
                                    .getInstance("MD5");

                            // digest of restored journal.
                            ((IHABufferStrategy) (jnl.getBufferStrategy()))
                                    .computeDigest(null/* snapshot */, digest);

                            final byte[] digest2 = digest.digest();
                            
                            if (!BytesUtil.bytesEqual(digestA, digest2)) {

                                final String digestAStr = new BigInteger(1,
                                        digestA).toString(16);

                                final String digest2Str = new BigInteger(1,
                                        digest2).toString(16);

                                fail("Digests differ after restore and replay: expected="
                                        + digestAStr + ", actual=" + digest2Str);

                            }

                        }

                        /*
                         * FIXME For some reason, we need to close and reopen
                         * the journal before it can be used.  See HARestore.
                         */
                        if (true) {
                            jnl.close();

                            // reopen.
                            jnl = new Journal(p);
                        }
                        
                        // Verify can dump journal after restore.
                        new DumpJournal(jnl).dumpJournal(w,
                                null/* namespaces */, true/* dumpHistory */,
                                true/* dumpPages */, true/* dumpIndices */,
                                false/* showTuples */);

                    } finally {

                        if (jnl != null) {
                        
                            jnl.close();
                            
                        }

                    }
                    
                }
                
            } finally {
               
                if (out.delete()) {
                
                    log.warn("Could not delete: " + out);
                    
                }
                
            }

        }

    }

}
