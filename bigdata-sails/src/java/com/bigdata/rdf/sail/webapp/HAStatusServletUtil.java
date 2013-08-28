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
package com.bigdata.rdf.sail.webapp;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.halog.IHALogReader;
import com.bigdata.ha.msg.HARemoteRebuildRequest;
import com.bigdata.ha.msg.HASnapshotRequest;
import com.bigdata.ha.msg.IHARemoteRebuildRequest;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.jini.ha.HAJournal;
import com.bigdata.journal.jini.ha.HALogIndex.IHALogRecord;
import com.bigdata.journal.jini.ha.HALogNexus;
import com.bigdata.journal.jini.ha.ISnapshotPolicy;
import com.bigdata.journal.jini.ha.SnapshotIndex.ISnapshotRecord;
import com.bigdata.journal.jini.ha.SnapshotManager;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.zk.ZKQuorumImpl;
import com.bigdata.zookeeper.DumpZookeeper;

/**
 * Class supports the {@link StatusServlet} and isolates code that has a
 * dependency on zookeeper so we do not drag in zookeeper for embedded
 * {@link NanoSparqlServer} deployments.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/612 (Bigdata scale-up
 *      depends on zookeper)
 */
public class HAStatusServletUtil {

    private static final boolean debug = true;
    
    static private final transient Logger log = Logger.getLogger(HAStatusServletUtil.class);
    
    /**
     * Disaster recover of this service from the leader (REBUILD).
     * 
     * @see HAGlue#rebuildFromLeader(IHARemoteRebuildRequest)
     * 
     * TODO Move this declaration to {@link StatusServlet} once we are done
     * reconciling between the 1.2.x maintenance branch and the READ_CACHE
     * branch.
     */
    static final String REBUILD = "rebuild";

    final private IIndexManager indexManager;

    public HAStatusServletUtil(final IIndexManager indexManager) {

        if (indexManager == null)
            throw new IllegalArgumentException();

        this.indexManager = indexManager;

    }

    /**
     * Show the interesting things about the quorum.
     * <ol>
     * <li>QuorumState</li>
     * <li>Who is the leader, who is a follower.</li>
     * <li>What is the SPARQL end point for each leader and follower.</li>
     * <li>Dump of the zookeeper state related to the quorum.</li>
     * <li>listServices (into pre element).</li>
     * </ol>
     * 
     * @throws IOException
     */
    public void doGet(final HttpServletRequest req,
            final HttpServletResponse resp, final XMLBuilder.Node current)
            throws IOException {

        if (!(indexManager instanceof HAJournal))
            return;

        final HAJournal journal = (HAJournal) indexManager;

        final ZKQuorumImpl<HAGlue, QuorumService<HAGlue>> quorum = (ZKQuorumImpl<HAGlue, QuorumService<HAGlue>>) journal
                .getQuorum();

        // The current token.
        final long quorumToken = quorum.token();

        // The last valid token.
        final long lastValidToken = quorum.lastValidToken();

        // This token is a bit different. It is senstive to the journal role in
        // the quorum (joined or not).
        final long haReadyToken = journal.getHAReady();
        
        final int njoined = quorum.getJoined().length;

        // Note: This is the *local* HAGlueService.
        final QuorumService<HAGlue> quorumService = quorum.getClient();

        final boolean digests = req.getParameter(StatusServlet.DIGESTS) != null;
        
        current.node("h1", "High Availability");

        // The quorum state.
        {

            final XMLBuilder.Node p = current.node("p");

            p.text("The quorum is " + (quorum.isQuorumMet() ? "" : "not")
                    + " met.").node("br").close();

            p.text("" + njoined + " out of " + quorum.replicationFactor()
                    + " services are joined.").node("br").close();

            p.text("quorumToken=" + quorumToken + ", lastValidToken="
                    + lastValidToken).node("br").close();

            p.text("logicalServiceZPath=" + quorumService.getLogicalServiceZPath())
                    .node("br").close();

            // Note: This is the *local* value of getHAStatus().
            // Note: The HAReady token reflects whether or not the service is
            // joined.
            p.text("HAStatus: " + quorumService.getService().getHAStatus()
                    + ", HAReadyToken=" + haReadyToken).node("br").close();

            /*
             * Report on the Service.
             */
            {
                p.text("Service: serviceId=" + quorumService.getServiceId())
                        .node("br").close();
                p.text("Service: pid=" + quorumService.getPID()).node("br")
                        .close();
                p.text("Service: path=" + quorumService.getServiceDir())
                        .node("br").close();

            }
            
            /*
             * Report on the HA backup status (snapshot and restore policy).
             * 
             * Note: The age and commit counter for the available snapshots
             * are provided in another section (below).
             */
            {

                // snapshot policy.
                {
                    final SnapshotManager mgr = journal.getSnapshotManager();
//                    final IRootBlockView lastSnapshotRB = mgr
//                            .getNewestSnapshot();
//                    final long sinceCommitCounter = lastSnapshotRB == null ? -1L
//                            : lastSnapshotRB.getCommitCounter();
//                    final long haLogBytesOnDiskSinceLastSnapshot = mgr
//                            .getHALogFileBytesSinceCommitCounter(sinceCommitCounter);
                    final ISnapshotPolicy snapshotPolicy = mgr
                            .getSnapshotPolicy();
                    final boolean takeSnapshot = mgr
                            .isReadyToSnapshot(snapshotPolicy
                                    .newSnapshotRequest());
                    p.text("Service"//
                            + ": snapshotPolicy="
                            + snapshotPolicy//
                            + ", shouldSnapshot="
                            + takeSnapshot//
//                            + ", lastSnapshotCommitCounter="
//                            + sinceCommitCounter//
//                            + ", HALogFileBytesOnDiskSinceLastSnapshot="
//                            + haLogBytesOnDiskSinceLastSnapshot//
                    ).node("br").close();
                }
                // restore policy.
                p.text("Service: restorePolicy="
                        + journal.getSnapshotManager().getRestorePolicy())
                        .node("br").close();
                
//                if(true) {
//                    /*
//                     * HABackup: disable this code block. It is for
//                     * debug purposes only.
//                     */
//                    p.text("Service: getEarliestRestorableCommitPoint()="
//                            + journal.getSnapshotManager().getRestorePolicy().getEarliestRestorableCommitPoint(journal))
//                            .node("br").close();
//                }
                    
            }

            /*
             * Report on the Journal.
             */
            {
                final File file = journal.getFile();
                if (file != null) {
                    String digestStr = null;
                    if (digests) {
                        try {
                            final MessageDigest digest = MessageDigest
                                    .getInstance("MD5");
                            journal.getBufferStrategy().computeDigest(
                                    null/* snapshot */, digest);
                            digestStr = new BigInteger(1, digest.digest())
                                    .toString(16);
                        } catch (NoSuchAlgorithmException ex) {
                            // ignore
                        } catch (DigestException ex) {
                            // ignore
                        }
                    }
                    final IRootBlockView rb = journal.getRootBlockView();
                    final long commitCounter = rb.getCommitCounter();
//                    // Move this stuff to a TXS Status section?
//                    long releaseTime = -1;
//                    try {
//                        // Note: Can throw exception if quorum is not met.
//                        releaseTime = journal.getTransactionService()
//                                .getReleaseTime();
//                    } catch (QuorumException ex) {
//                        // Ignore.
//                    }
                    final long fileSize = file == null ? 0L : file.length();
                    p.text("HAJournal: file=" + file //
                            + ", commitCounter=" + commitCounter //
                            + ", nbytes=" + fileSize//
                            + (digestStr == null ? "" : ", md5=" + digestStr)//
//                            + (releaseTime != -1L ? ", releaseTime="
//                                    + RootBlockView.toString(releaseTime)//
//                            : "")//
                    ).node("br").close();
                    // Show the current root block.
                    if(debug)
                        current.node("pre", rb.toString());
                }
            }

            /**
             * Report #of files and bytes in the HALog directory.
             * 
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/670">
             *      Accumulating HALog files cause latency for HA commit</a>
             */
            {
                final HALogNexus nexus = journal.getHALogNexus();
                {
                    /*
                     * Use efficient index to compute the #of bytes (scan with
                     * sum) and the #of files.
                     */
                    int nfiles = 0;
                    long nbytes = 0L;
                    final Iterator<IHALogRecord> itr = nexus.getHALogs();
                    while (itr.hasNext()) {
                        final IHALogRecord r = itr.next();
                        nbytes += r.sizeOnDisk();
                        nfiles++;
                    }
                    /*
                     * Add in the current HALog file (if any).
                     */
                    final File currentFile = nexus.getHALogWriter().getFile();
                    if (currentFile != null) {
                        nbytes += currentFile.length();
                        nfiles++;
                    }
                    final String compressorKey = journal.getProperties().getProperty(
                            com.bigdata.journal.Options.HALOG_COMPRESSOR,
                            com.bigdata.journal.Options.DEFAULT_HALOG_COMPRESSOR);
                    p.text("HALogDir: nfiles=" + nfiles + ", nbytes=" + nbytes
                            + ", path=" + nexus.getHALogDir()
                            + ", compressorKey=" + compressorKey).node("br")
                            .close();
                }
                if (digests) {
                    /*
                     * List each historical HALog file together with its digest.
                     * 
                     * Note: This can be VERY expensive.
                     */
                    final Iterator<IHALogRecord> itr = nexus.getHALogs();
                    while (itr.hasNext()) {
                        final IHALogRecord rec = itr.next();
                        final long nbytes = rec.sizeOnDisk();
                        final long closingCommitCounter = rec.getRootBlock()
                                .getCommitCounter();
                        String digestStr = null;
                        final File file = nexus
                                .getHALogFile(closingCommitCounter);
                        final IHALogReader r = nexus.getHALogWriter()
                                .getReader(closingCommitCounter);
                        try {
                            if (digests && !r.isEmpty()) {
                                try {
                                    final MessageDigest digest = MessageDigest
                                            .getInstance("MD5");
                                    r.computeDigest(digest);
                                    digestStr = new BigInteger(1,
                                            digest.digest()).toString(16);
                                } catch (NoSuchAlgorithmException ex) {
                                    // ignore
                                } catch (DigestException ex) {
                                    // ignore
                                }
                            }
                        } finally {
                            r.close();
                        }
                        p.text("HALogFile: closingCommitCounter="
                                + closingCommitCounter//
                                + ", file="
                                + file//
                                + ", nbytes="
                                + nbytes//
                                + (digestStr == null ? "" : ", md5="
                                        + digestStr)).node("br").close();
                    }
                }
            }

            /*
             * Report #of files and bytes in the snapshot directory.
             * 
             * Note: This uses the in-memory index rather than scanning the
             * directory in order to reduce latency associated with the file
             * system.
             */
            {
                {
//                    final File snapshotDir = journal
//                            .getSnapshotManager().getSnapshotDir();
//                    final File[] a = snapshotDir.listFiles(new FilenameFilter() {
//                        @Override
//                        public boolean accept(File dir, String name) {
//                            return name.endsWith(SnapshotManager.SNAPSHOT_EXT);
//                        }
//                    });
//                    for (File file : a) {
//                        nbytes += file.length();
//                        nfiles++;
//                    }
                    /*
                     * List the available snapshots (in order by increasing
                     * commitTime).
                     */
                    final Iterator<ISnapshotRecord> itr = journal
                            .getSnapshotManager().getSnapshots();
                    int nfiles = 0;
                    long nbytes = 0L;
                    while (itr.hasNext()) {
                        final ISnapshotRecord sr = itr.next();
                        nbytes += sr.sizeOnDisk();
                        nfiles++;
                    }
                    p.text("SnapshotDir: nfiles=" + nfiles + ", nbytes="
                            + nbytes + ", path="
                            + journal.getSnapshotManager().getSnapshotDir())
                            .node("br").close();
                }
                if (true) {

                    /*
                     * List the available snapshots (in order by increasing
                     * commitTime).
                     */
                    final Iterator<ISnapshotRecord> itr = journal
                            .getSnapshotManager().getSnapshots();

                    while(itr.hasNext()) {
                        final ISnapshotRecord r = itr.next();
                        final IRootBlockView rb = r.getRootBlock();
                        final long nbytes = r.sizeOnDisk();
//                        final File file = journal.getSnapshotManager()
//                                .getSnapshotFile(rb.getCommitCounter());
                        String digestStr = null;
                        if (digests) {
                            try {
                                final MessageDigest digest = MessageDigest
                                        .getInstance("MD5");
                                journal.getSnapshotManager().getDigest(
                                        rb.getCommitCounter(), digest);
                                digestStr = new BigInteger(1, digest.digest())
                                        .toString(16);
                            } catch (NoSuchAlgorithmException ex) {
                                // ignore
                            } catch (DigestException ex) {
                                // ignore
                            }
                        }

                        p.text("SnapshotFile: commitTime="
                                + RootBlockView.toString(rb.getLastCommitTime())
                                + ", commitCounter="
                                + rb.getCommitCounter()
                                + ", nbytes="
                                + nbytes
                                + (digestStr == null ? "" : ", md5="
                                        + digestStr)).node("br").close();

                    }
                    
                }
                
            }

            /*
             * If requested, conditionally start a snapshot.
             */
            {
                final String val = req.getParameter(StatusServlet.SNAPSHOT);

                if (val != null) {

                    /*
                     * Attempt to interpret the parameter as a percentage
                     * (expressed as an integer).
                     * 
                     * Note: The default threshold will trigger a snapshot
                     * regardless of the size of the journal and the #of HALog
                     * files. A non-default value of 100 will trigger the
                     * snapshot if the HALog files occupy as much space on the
                     * disk as the Journal. Other values may be used as
                     * appropriate.
                     */
                    int percentLogSize = 0;
                    try {
                        percentLogSize = Integer.parseInt(val);
                    } catch (NumberFormatException ex) {
                        // ignore.
                    }

                    journal.getSnapshotManager().takeSnapshot(
                            new HASnapshotRequest(percentLogSize));

                }
                
            }
            
            /*
             * Report if a snapshot is currently running.
             */
            if (journal.getSnapshotManager().getSnapshotFuture() != null) {

                p.text("Snapshot running.").node("br").close();
                
            }
            
            p.close();

            if(debug)
                current.node("pre", quorum.toString());

        }

        /**
         * If requested, conditionally REBUILD the service from the leader
         * (disaster recover).
         * 
         * FIXME This should only be triggered by a POST (it is modestly safe
         * since a REBUILD can not be triggered if the service is joined, at the
         * same commit point as the leader, or already running, but it is not so
         * safe that you should be able to use a GET to demand a REBUILD).
         */
        {
            
            final String val = req.getParameter(HAStatusServletUtil.REBUILD);

            if (val != null) {

                // Local HAGlue interface for this service (not proxy).
                final HAGlue haGlue = quorumService.getService();

                // Request RESTORE.
                if (haGlue.rebuildFromLeader(new HARemoteRebuildRequest()) != null) {

                    current.node("h2",
                            "Running Disaster Recovery for this service (REBUILD).");

                }

            }

        }
        
        /*
         * Display the NSS port, host, and leader/follower/not-joined
         * status for each service in the quorum.
         */
        current.node("h2", "Quorum Services");
        {
            final XMLBuilder.Node p = current.node("p");
            
            final UUID[] joined = quorum.getJoined();

            final UUID[] pipeline = quorum.getPipeline();

            // In pipeline order.
            for (UUID serviceId : pipeline) {

                final HAGlue remoteService;
                try {

                    remoteService = quorumService.getService(serviceId);

                } catch (RuntimeException ex) {

                    /*
                     * Ignore. Might not be an HAGlue instance.
                     */
                    
                    if (log.isInfoEnabled())
                        log.info(ex, ex);
                    
                    continue;

                }

                /*
                 * Do all RMIs to the remote service in a try/catch. This allows
                 * us to catch problems with communications to the remote
                 * service and continue to paint the page.
                 */
                final String hostname;
                final int nssPort;
                final InetSocketAddress writePipelineAddr;
                final String extendedRunState;
                try {
                    
                    hostname = remoteService.getHostname();

                    /*
                     * TODO When there are multiple ethernet interfaces, is not
                     * necessarily reporting the interface(s) that the port is
                     * exposed to.
                     */
                    nssPort = remoteService.getNSSPort();

                    // address where the downstream service will listen.
                    writePipelineAddr = remoteService.getWritePipelineAddr();

                    // The AbstractServer and HAQuorumService run states.
                    extendedRunState = remoteService.getExtendedRunState();

                } catch (IOException ex) {

                    /*
                     * Note error and continue with the next service.
                     */
 
                    p.text("Unable to reach service: " + remoteService).close();

                    log.error(ex, ex);
                    
                    continue;
                    
                }

                final boolean isLeader = serviceId.equals(quorum
                        .getLeaderId());

                final boolean isFollower = indexOf(serviceId, joined) > 0;

                final boolean isSelf = serviceId.equals(quorumService
                        .getServiceId());

                final int pipelineIndex = indexOf(serviceId, pipeline);
                
                final String nssUrl = "http://" + hostname + ":" + nssPort;
                
                // hyper link to NSS service.
                p.node("a").attr("href", nssUrl).text(nssUrl).close();

                // plus the other metadata.
                p.text(" : "//
                        + (isLeader ? "leader" : (isFollower ? "follower"
                                : " is not joined"))//
                        + ", pipelineOrder="
                        + (pipelineIndex == -1 ? "N/A" : pipelineIndex)//
                        + ", writePipelineAddr=" + writePipelineAddr//
                        + ", service=" + (isSelf ? "self" : "other")//
                        + ", extendedRunState=" + extendedRunState//
                ).node("br").close();
            }

            p.close();
            
        }

        // DumpZookeeper
        {
            
            current.node("h2", "Zookeeper");

            // final XMLBuilder.Node section = current.node("pre");
            // flush writer before writing on PrintStream.
            current.getBuilder().getWriter().flush();

            // dump onto the response.
            final PrintWriter out = new PrintWriter(
                    resp.getOutputStream(), true/* autoFlush */);

            out.print("<pre>\n");

            try {

                final DumpZookeeper dump = new DumpZookeeper(
                        quorum.getZookeeper());

                dump.dump(out, true/* showDatatrue */,
                        quorumService.getLogicalServiceZPath()/* zpath */,
                        0/* depth */);

            } catch (InterruptedException e) {

                e.printStackTrace(out);

            } catch (KeeperException e) {

                e.printStackTrace(out);

            }

            // close section.
            out.print("\n</pre>");

            // flush PrintWriter before resuming writes on Writer.
            out.flush();

        }

    }

    /**
     * Return the index of the given {@link UUID} in the array of {@link UUID}s.
     * 
     * @param x
     *            The {@link UUID}
     * @param a
     *            The array of {@link UUID}s.
     *            
     * @return The index of the {@link UUID} in the array -or- <code>-1</code>
     *         if the {@link UUID} does not appear in the array.
     */
    static private int indexOf(final UUID x, final UUID[] a) {

        if (x == null)
            throw new IllegalArgumentException();

        for (int i = 0; i < a.length; i++) {

            if (x.equals(a[i])) {

                return i;

            }

        }

        return -1;

    }

    /**
     * Special reporting request for HA status.
     * 
     * @param req
     * @param resp
     * @throws TimeoutException
     * @throws InterruptedException
     * @throws AsynchronousQuorumCloseException
     * @throws IOException
     */
    public void doHAStatus(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        if (!(indexManager instanceof HAJournal))
            return;

        final HAJournal journal = (HAJournal) indexManager;

        final Quorum<HAGlue, QuorumService<HAGlue>> quorum = journal
                .getQuorum();
        
        final QuorumService<HAGlue> quorumService = quorum.getClient();

        // Local (non-RMI) HAGlue implementation object.
        final HAGlue haGlue = quorumService.getService();
        
        // Local method all (non-RMI).
        final HAStatusEnum status = haGlue.getHAStatus();
        
        // TODO Alternatively "max-age=1" for max-age in seconds.
        resp.addHeader("Cache-Control", "no-cache");

        BigdataRDFServlet.buildResponse(resp, BigdataRDFServlet.HTTP_OK,
                BigdataRDFServlet.MIME_TEXT_PLAIN, status.name());

        return;

    }

//    /**
//     * Impose a lexical ordering on the file names. This is used for the HALog
//     * and snapshot file names. The main component of those file names is the
//     * commit counter, so this places the files into order by commit counter.
//     */
//    private static class FilenameComparator implements Comparator<File> {
//
//        @Override
//        public int compare(File o1, File o2) {
//            
//            return o1.getName().compareTo(o2.getName());
//        }
//        
//    }
    
}
