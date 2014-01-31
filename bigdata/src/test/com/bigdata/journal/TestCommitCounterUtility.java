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
 * Created on May 8th, 2013
 */
package com.bigdata.journal;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import junit.framework.TestCase2;

/**
 * Test suite for the {@link CommitCounterUtility}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestCommitCounterUtility extends TestCase2 {

    public TestCommitCounterUtility() {
    }

    public TestCommitCounterUtility(String name) {
        super(name);
    }

    public void test01() {
        
        final File dir = new File("/tmp");
        
        final String ext = ".tmp";

        final File f = new File(
                "/tmp/000/000/000/000/000/000/000000000000000000001.tmp");

        assertEquals(f, CommitCounterUtility.getCommitCounterFile(dir, 1L, ext));

        assertEquals(1L,
                CommitCounterUtility.parseCommitCounterFile(f.getName(), ext));

        assertEquals("000000000000000000001",
                CommitCounterUtility.getBaseName(f.getName(), ext));

    }
    
    public void test_findGreatestCommitCounter() throws IOException {
        
        final String ext = ".tmp";

        final FileFilter fileFilter = new FileFilter() {

            @Override
            public boolean accept(final File f) {
                if (f.isDirectory()) {

                    return true;

                }
                return f.getName().endsWith(ext);
            }
            
        };

        // temp directory for this test.
        final File dir = File.createTempFile(getName(), "");
        try {

            if (!dir.delete())
                fail("Could not delete: " + dir);
            if (!dir.mkdirs())
                fail("Could not create: " + dir);

            final File f1 = CommitCounterUtility.getCommitCounterFile(dir, 1L,
                    ext);
            final File f10 = CommitCounterUtility.getCommitCounterFile(dir,
                    10L, ext);
            final File f100 = CommitCounterUtility.getCommitCounterFile(dir,
                    100L, ext);
            final File f1000 = CommitCounterUtility.getCommitCounterFile(dir,
                    1000L, ext);
            final File f10000 = CommitCounterUtility.getCommitCounterFile(dir,
                    10000L, ext);

            // No files. Returns null.
            assertEquals(null, CommitCounterUtility.findGreatestCommitCounter(
                    dir, fileFilter));
            
            // Create directory structure.
            if (!f10.getParentFile().mkdirs())
                fail("Could not create directory structure: " + f1000);

            // No files. Returns null.
            assertEquals(null, CommitCounterUtility.findGreatestCommitCounter(
                    dir, fileFilter));

            if (!f10.createNewFile())
                fail("Could not create: " + f10);

            // This is the only file. It should be returned.
            assertEquals(f10, CommitCounterUtility.findGreatestCommitCounter(
                    dir, fileFilter));

            // Create a file with a commit counter LT that file.
            if (!f1.createNewFile())
                fail("Could not create: " + f1);

            // The return value should not change.
            assertEquals(f10, CommitCounterUtility.findGreatestCommitCounter(
                    dir, fileFilter));

            // Create a file with a larger commit counter.
            if (!f100.createNewFile())
                fail("Could not create: " + f100);

            // That file should now be returned.
            assertEquals(f100, CommitCounterUtility.findGreatestCommitCounter(
                    dir, fileFilter));

            // Create a file with a larger commit counter. The commit counter
            // will cause another directory to be created.
            if (!f1000.getParentFile().mkdirs())
                fail("Could not create directory structure: " + f1000);
            if (!f1000.createNewFile())
                fail("Could not create: " + f1000);

            // That file should now be returned.
            assertEquals(f1000, CommitCounterUtility.findGreatestCommitCounter(
                    dir, fileFilter));

            // Create a new directory structure, but do not add a file. The new
            // directory structure is ordered GT the existing files.  For this 
            // case the algorithm needs to work backwards to see if it can find
            // a non-empty directory.
            if (!f10000.getParentFile().mkdirs())
                fail("Could not create directory structure: " + f10000);
            
            // The same file should be returned since the new dir is empty.
            assertEquals(f1000, CommitCounterUtility.findGreatestCommitCounter(
                    dir, fileFilter));

            // Add a file to that directory.
            if (!f10000.createNewFile())
                fail("Could not create: " + f10000);

            // That file should be returned.
            assertEquals(f10000,
                    CommitCounterUtility.findGreatestCommitCounter(dir,
                            fileFilter));

        } finally {

            CommitCounterUtility.recursiveDelete(false/* errorIfDeleteFails */,
                    dir, fileFilter);

        }

    }

}
