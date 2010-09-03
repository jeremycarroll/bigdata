/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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

package com.bigdata.service.jini.master;

import com.bigdata.relation.accesspath.BlockingBuffer;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;

/**
 * A resource factory which searches for data files to bulk load
 * in a given directory and makes those files available via  http from
 * the provided given web server (assumed to be rooted at that directory)
 * at URLs which are created for the purpose.
 */
public class FileSystemScannerServer extends AbstractResourceScanner<URL> {

    private transient final FileServer server;
    private final File dir;
    private final FilenameFilter filter;

    @Override
    public String toString() {
        return getClass() + //
                "{acceptCount=" + super.getAcceptCount() + //
                ",fileOrDir=" + dir + //
                ",filter=" + (filter == null ? "N/A" : filter) + //
                "}";
    }
    
    /**
     * Create the resource factory for data files in the specified directory.
     * @param buffer
     *            Chunks of files to be loaded are added to this buffer.
     * @param dir the directory containing the data files to be loaded.
     * @param filter
     *            An optional filter on files that will be accepted when
     *            processing a directory.
     * @param server
     */
    public FileSystemScannerServer(final BlockingBuffer<URL[]> buffer,
            final File dir, final FilenameFilter filter,
            final FileServer server) {

        super(buffer);
        if (dir == null || server == null)
            throw new IllegalArgumentException();
        this.dir = dir;
        this.filter = filter; // MAY be null.
        this.server = server;
    }

    /**
     * Search the directory for files, creating and adding URLs to the
     * queue for data files to be bulk loaded.
     * @throws InterruptedException if an interrupt occurs while waiting
     * for space in the queue.
     */
    @Override
    protected void runScanner() throws InterruptedException {
        process2(dir);
    }

    /**
     * Stop the web server which serves up files from the given directory.
     */
    @Override
    protected void stopScanner() {
        server.stop();
    }
    
    /**
     * Scans file(s) recursively starting with the named file, and, for each
     * file that passes the filter, submits the task.
     * 
     * @param file
     *            Either a URL, a plain file or directory containing files
     *            to be processed.
     * 
     * @throws InterruptedException
     *             if the thread is interrupted while queuing tasks.
     */
    private void process2(final File file) throws InterruptedException {

        if (file.isHidden()) {
            return;
        }

        if (file.isDirectory()) {
            if (log.isInfoEnabled())
                log.info("Scanning directory: " + file);
            // filter is optional.
            final File[] files = (filter == null)
                               ? file.listFiles()
                               : file.listFiles(filter);
            for (final File f : files) {
                process2(f);
            }

        } else {
            /*
             * Processing a standard file.
             */
            String serverPath = dir.getPath();
            String filePath = file.getPath();
            assert filePath.length() >= serverPath.length();
            assert filePath.startsWith(serverPath);
            String relPath = filePath.substring(serverPath.length());
            accept(server.getURL(relPath));
        }

    }

    /**
     * Create the resource factory for identifying all of the data files in
     * a given directory, computing the URLs for those files relative to the
     * provided web server, and passing them to the bulk loading mechanism.
     * This method is intended to be called from the cluster configuration file.
     * @param dir the parent directory containing all of the datafiles to
     * be processed.
     * @param filter an optional file name filter to allow inclusion only
     * of relevant files (e.g. by suffix examination).
     * @param server
     * @return the resource factory based on the list of URLs.
     */
    public static IResourceScannerFactory<URL> newFactory(
            final File dir, final FilenameFilter filter,
            final FileServer server) {

        return new IResourceScannerFactory<URL>() {
            private static final long serialVersionUID = 4L;

            public AbstractResourceScanner<URL> newScanner(
                    final BlockingBuffer<URL[]> buffer) throws IOException {
                
                return new FileSystemScannerServer(buffer, dir, filter, server);
            }
        };
    }
}
