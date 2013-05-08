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
package com.bigdata.journal.jini.ha;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.math.BigInteger;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import com.bigdata.btree.BytesUtil;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.msg.HADigestRequest;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.journal.CommitCounterUtility;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;

/**
 * Abstract base class for testing the {@link ISnapshotPolicy} and
 * {@link IRestorePolicy} that support HA Backup and restore.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class AbstractHA3BackupTestCase extends AbstractHA3JournalServerTestCase {

    public AbstractHA3BackupTestCase() {
    }

    public AbstractHA3BackupTestCase(final String name) {
        super(name);
    }

    /**
     * Issue HTTP request to a service to take a snapshot.
     * 
     * @param haGlue
     *            The service.
     *            
     * @throws Exception
     */
    protected void doSnapshotRequest(final HAGlue haGlue) throws Exception {

        doSnapshotRequest(haGlue, null/* percentLogSize */);

    }

    /**
     * Issue HTTP request to a service to take a snapshot.
     * 
     * @param haGlue
     *            The service.
     * @param percentLogSize
     *            The percent log size parameter for the request (optional).
     * 
     * @throws Exception
     */
    protected void doSnapshotRequest(final HAGlue haGlue,
            final Integer percentLogSize) throws Exception {

        // Client for talking to the NSS.
        final HttpClient httpClient = new DefaultHttpClient(ccm);

        // The NSS service URL (NOT the SPARQL end point).
        final String serviceURL = getNanoSparqlServerURL(haGlue);

        final ConnectOptions opts = new ConnectOptions(serviceURL
                + "/status?snapshot"
                + (percentLogSize != null ? "=" + percentLogSize : ""));

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
     * Restore the journal from the specified snapshot on the specified server,
     * applying all available HALog files GT the snapshot and then compare the
     * digest of the restored journal with the digest of the specified server.
     * 
     * @param serverA
     *            NOTE: This ONLY works for serverA. The access to the snapshot
     *            directory for serverA is hardcoded into this method.
     * 
     * @param commitCounterN
     *            The commit counter of the snapshot that will be used to
     *            restore the state of the journal.
     * 
     * @throws IOException
     * @throws InterruptedException 
     * @throws DigestException 
     * @throws NoSuchAlgorithmException 
     */
    protected void doRestoreA(final HAGlue serverA, final long commitCounterN)
            throws IOException, InterruptedException, DigestException,
            NoSuchAlgorithmException {

        /*
         * The current commit counter on the server. This is the commit point
         * that should be restored.
         */
        final long commitCounterM = serverA
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getCommitCounter();

        final File snapshotFile = SnapshotManager.getSnapshotFile(
                getSnapshotDirA(), commitCounterN);

        final String basename = CommitCounterUtility.getBaseName(
                snapshotFile.getName(), SnapshotManager.SNAPSHOT_EXT);
        
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

                    // Verify snapshot at the expected commit point.
                    assertEquals(commitCounterN, jnl.getRootBlockView()
                            .getCommitCounter());

                    // Verify journal can be dumped without error.
                    dumpJournal(jnl);

                    /*
                     * Now roll that journal forward using the HALog directory.
                     */
                    final HARestore rest = new HARestore(jnl, getHALogDirA());

                    /*
                     * Note: We can not test where we stop at the specified
                     * commit point in this method because the Journal state on
                     * the server could not be used to validate the restored
                     * Journal's state.
                     */
                    rest.restore(false/* listCommitPoints */, Long.MAX_VALUE/* haltingCommitCounter */);

                    // Verify journal now at the expected commit point.
                    assertEquals(commitCounterM, jnl.getRootBlockView()
                            .getCommitCounter());

                    /*
                     * Compute digest of the restored journal. The digest should
                     * agree with the digest of the Journal on A since we rolled
                     * it forward to the same commit point.
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

                            final String digestAStr = new BigInteger(1, digestA)
                                    .toString(16);

                            final String digest2Str = new BigInteger(1, digest2)
                                    .toString(16);

                            fail("Digests differ after restore and replay: expected="
                                    + digestAStr + ", actual=" + digest2Str);

                        }

                    }

                    /*
                     * FIXME For some reason, we need to close and reopen the
                     * journal before it can be used. See HARestore.
                     */
                    if (true) {
                        jnl.close();

                        // reopen.
                        jnl = new Journal(p);
                    }

                    // Verify can dump journal after restore.
                    dumpJournal(jnl);

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

    /**
     * Verify the existence of the snapshot files for the specified commit
     * points and verify that no other snapshot files exist.
     * 
     * @param snapshotDir
     *            The snapshot directory.
     * @param commitCounters
     *            The commit points.
     */
    protected void assertExpectedSnapshots(final File snapshotDir,
            final long[] commitCounters) {

        for (long commitCounter : commitCounters) {

            final File file = SnapshotManager.getSnapshotFile(snapshotDir,
                    commitCounter);
            
            if (!file.exists())
                fail("Snapshot not found: " + file);

        }
        
        assertEquals(commitCounters.length,
                recursiveCount(snapshotDir, SnapshotManager.SNAPSHOT_FILTER));

    }

}
