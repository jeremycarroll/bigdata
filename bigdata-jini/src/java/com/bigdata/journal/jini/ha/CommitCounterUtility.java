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
import java.util.Formatter;

import org.apache.log4j.Logger;

/**
 * Utility class for operations on files that are named using a commit counter.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class CommitCounterUtility {

    private static final Logger log = Logger
            .getLogger(CommitCounterUtility.class);
    
    /**
     * Return the name of the {@link File} associated with the commitCounter.
     * 
     * @param dir
     *            The directory spanning all such files.
     * @param commitCounter
     *            The commit counter for the current root block on the journal.
     * @param ext
     *            The filename extension.
     * @return The name of the corresponding snapshot file.
     */
    public static File getCommitCounterFile(final File dir,
            final long commitCounter, final String ext) {

        /*
         * Format the name of the file.
         * 
         * Note: The commit counter in the file name should be zero filled to 20
         * digits so we have the files in lexical order in the file system (for
         * convenience).
         */
        final String file;
        {

            final StringBuilder sb = new StringBuilder();

            final Formatter f = new Formatter(sb);

            f.format("%020d" + ext, commitCounter);
            f.flush();
            f.close();

            file = sb.toString();

        }

        return new File(dir, file);

    }

    /**
     * Parse out the commitCounter from the file name.
     * 
     * @param name
     *            The file name
     * @param ext
     *            The expected file extension.
     * 
     * @return The commit counter from the file name.
     * 
     * @throws IllegalArgumentException
     *             if either argument is <code>null</code>
     * @throws NumberFormatException
     *             if the file name can not be interpreted as a commit counter.
     */
    public static long parseCommitCounterFile(final String name,
            final String ext) throws NumberFormatException {

        if (name == null)
            throw new IllegalArgumentException();

        if (ext == null)
            throw new IllegalArgumentException();

        // Strip off the filename extension.
        final int len = name.length() - ext.length();

        final String fileBaseName = name.substring(0, len);

        // Closing commitCounter for snapshot file.
        final long commitCounter = Long.parseLong(fileBaseName);

        return commitCounter;
        
    }

    /**
     * Return the basename of the file (strip off the extension).
     * 
     * @param name
     *            The file name.
     * @param ext
     *            The extension.
     *            
     * @return The base name of the file without the extension.
     */
    public static String getBaseName(final String name, final String ext) {

        final String basename = name.substring(0, name.length() - ext.length());

        return basename;

    }

    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself. Only files recognized by
     * {@link #getFileFilter()} will be deleted.
     * <p>
     * Note: A dedicated version of this method exists here to thrown an
     * {@link IOException} if we can not delete a file. This is deliberate. It
     * is thrown to prevent a REBUILD from proceeding unless we can clear out
     * the old snapshot and HALog files.
     * 
     * @param errorIfDeleteFails
     *            When <code>true</code> and {@link IOException} is thrown if a
     *            file matching the filter or an empty directory matching the
     *            filter can not be removed. When <code>false</code>, that event
     *            is logged @ WARN instead.
     * @param f
     *            A file or directory.
     * @param fileFilter
     *            A filter matching the files and directories to be visited and
     *            removed. If directories are matched, then they will be removed
     *            iff they are empty. A depth first visitation is used, so the
     *            files and sub-directories will be cleared before we attempt to
     *            remove the parent directory.
     * @throws IOException
     *             if any file or non-empty directory can not be deleted (iff
     *             <i>errorIfDeleteFails</i> is <code>true</code>).
     */
    public static void recursiveDelete(final boolean errorIfDeleteFails,
            final File f, final FileFilter fileFilter) throws IOException {

        if (f.isDirectory()) {

            final File[] children = f.listFiles(fileFilter);

            for (int i = 0; i < children.length; i++) {

                recursiveDelete(errorIfDeleteFails, children[i], fileFilter);

            }

        }

        if (log.isInfoEnabled())
            log.info("Removing: " + f);

        if (f.exists() && !f.delete()) {
            if (f.isDirectory() && f.list().length != 0) {
                // Ignore non-empty directory.
                return;
            }
            final String msg = "Could not remove file: " + f;
            if (errorIfDeleteFails) {
                // Complete if we can not delete a file.
                throw new IOException(msg);
            } else {
                log.warn(msg);
            }
        }

    }

}
