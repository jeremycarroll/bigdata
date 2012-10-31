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
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.zookeeper.KeeperException;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.halog.HALogWriter;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IIndexManager;
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
    public void showQuorum(final HttpServletRequest req,
            final HttpServletResponse resp, final XMLBuilder.Node current)
            throws IOException {

        if (!(indexManager instanceof AbstractJournal))
            return;

        final AbstractJournal journal = (AbstractJournal) indexManager;

        final ZKQuorumImpl<HAGlue, QuorumService<HAGlue>> quorum = (ZKQuorumImpl<HAGlue, QuorumService<HAGlue>>) journal
                .getQuorum();

        // The current token.
        final long quorumToken = quorum.token();

        // The last valid token.
        final long lastValidToken = quorum.lastValidToken();

        final int njoined = quorum.getJoined().length;

        final QuorumService<HAGlue> quorumService = quorum.getClient();

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

            p.text("logicalServiceId="                    + quorumService.getLogicalServiceId()).node("br")
                    .close();

            /*
             * Report on the Service.
             */
            {
                final File serviceDir = quorumService.getServiceDir();
                p.text("ServiceDir: path=" + serviceDir).node("br")
                        .close();
            }
            
            /*
             * Report on the Journal.
             */
            {
                final File file = journal.getFile();
                if (file != null) {
                    p.text("DataDir: path=" + file.getParent())
                            .node("br").close();
                }
            }

            /*
             * Report #of files and bytes in the HALog directory.
             */
            {
                final File haLogDir = quorumService.getHALogDir();
                final File[] a = haLogDir
                        .listFiles(new FilenameFilter() {
                            @Override
                            public boolean accept(File dir, String name) {
                                return name
                                        .endsWith(HALogWriter.HA_LOG_EXT);
                            }
                        });
                int nfiles = 0;
                long nbytes = 0L;
                for (File file : a) {
                    nbytes += file.length();
                    nfiles++;
                }
                p.text("HALogDir: nfiles=" + nfiles + ", nbytes="
                        + nbytes + ", path=" + haLogDir).node("br")
                        .close();
            }
            p.close();

            current.node("pre", quorum.toString());

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

            for (UUID serviceId : quorum.getMembers()) {

                final HAGlue remoteService;
                try {

                    remoteService = quorumService.getService(serviceId);

                } catch (RuntimeException ex) {

                    /*
                     * Ignore. Might not be an HAGlue instance.
                     */

                    continue;

                }

                /*
                 * Note: This is not actually reporting the interface
                 * that the port is exposed to.
                 */

                final String hostname = remoteService.getHostname();

                final int nssPort = remoteService.getNSSPort();

                final boolean isLeader = serviceId.equals(quorum
                        .getLeaderId());

                final boolean isFollower = indexOf(serviceId, joined) > 0;

                final int pipelineIndex = indexOf(serviceId, pipeline);
                
                p.text(hostname
                        + " : nssPort="
                        + nssPort
                        + " : "
                        + (isLeader ? "leader"
                                : (isFollower ? "follower"
                                        : " is not joined"))
                        + ", pipelineOrder="
                        + (pipelineIndex == -1 ? "N/A"
                                : pipelineIndex)).node("br").close();

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
                        quorumService.getLogicalServiceId()/* zpath */,
                        0/* depth */);

            } catch (InterruptedException e) {

                e.printStackTrace(out);

            } catch (KeeperException e) {

                e.printStackTrace(out);

            }

            // flush PrintWriter before resuming writes on Writer.
            out.flush();

            // close section.
            out.print("\n</pre>");

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
    
}
