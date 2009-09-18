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
/*
 * Created on Sep 18, 2009
 */

package com.bigdata.samples;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LoadNamedGraphs extends SampleCode {

    /**
     * Load all data from some directory.
     * 
     * @param dir
     * 
     * @throws Exception
     */
    public void loadAll(final File file) throws Exception {

        /*
         * We are going to use the "quads" mode. Right now, the quads mode does
         * not do inference AT ALL.
         */
        final Properties properties = loadProperties("quads.properties");

        // create a backing file
        final File journal = new File("d:/bigdata.jnl");
//        final File journal = File.createTempFile("bigdata", ".jnl");
//        journal.deleteOnExit();
        properties.setProperty(BigdataSail.Options.FILE, journal
                .getAbsolutePath());

        /*
         * Override the write retention queue (default is 500).
         * 
         * This makes a BIG difference in the journal size and throughput if you
         * are bulk loading data and have enough RAM.
         */
        properties.setProperty(
                IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY, "8000");

//        properties.setProperty(IndexMetadata.Options.BTREE_BRANCHING_FACTOR,
//                "64");

        // instantiate a sail
        BigdataSail sail = new BigdataSail(properties);
        Repository repo = new BigdataSailRepository(sail);
        repo.initialize();

        RepositoryConnection cxn = repo.getConnection();
        cxn.setAutoCommit(false);
        try {
            final long stmtsBefore = cxn.size();
            final long start = System.currentTimeMillis();

            // then process the sample data files one at a time
            InputStream is = new FileInputStream(file);
            ZipInputStream zis = new ZipInputStream(new BufferedInputStream(is));
            ZipEntry ze = null;
            while ((ze = zis.getNextEntry()) != null) {
                if (ze.isDirectory()) {
                    continue;
                }
                String name = ze.getName();
                log.info(name);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                byte[] bytes = new byte[4096];
                int count;
                while ((count = zis.read(bytes, 0, 4096)) != -1) {
                    baos.write(bytes, 0, count);
                }
                baos.close();
                final Reader reader = new InputStreamReader(
                        new ByteArrayInputStream(baos.toByteArray()));
                String baseIRI = file.toURI()+"/"+name;
                
                cxn.add(reader, baseIRI, RDFFormat.forFileName(name));

                // note: due to buffering, this reports stmts flush to the db
                // not stmts added to the cxn.
                long elapsed = System.currentTimeMillis() - start;
                long stmtsAfter = cxn.size();
                long stmtsAdded = stmtsAfter - stmtsBefore;
                int throughput = (int) ((double) stmtsAdded / (double) elapsed * 1000d);
                System.err.println("loaded: " + name + " : " + stmtsAdded
                        + " stmts in " + elapsed + " millis: " + throughput
                        + " stmts/sec");
            }
            zis.close();

            // autocommit is false, we need to commit our SAIL "transaction"
            cxn.commit();

            // gather statistics
            long elapsed = System.currentTimeMillis() - start;
            long stmtsAfter = cxn.size();
            long stmtsAdded = stmtsAfter - stmtsBefore;
            int throughput = (int) ((double) stmtsAdded / (double) elapsed * 1000d);
            log.info("statements after: " + stmtsAfter);
            log.info("loaded: " + stmtsAdded + " in " + elapsed + " millis: "
                    + throughput + " stmts/sec");

        } catch (Exception ex) {
            cxn.rollback();
            throw ex;
        } finally {
            // close the repository connection
            cxn.close();
        }

    }

    /**
     * Loads a bunch of data from a zip file.
     * 
     * @param args
     *            The file name.
     * 
     * @throws Exception
     */
    public static void main(String[] args) {

        if (args.length == 0) {

            System.out.println("usage: file.zip");

            System.exit(1);

        }

        try {
       
            new LoadNamedGraphs().loadAll(new File(args[0]));
            
        } catch (Exception ex) {
            
            ex.printStackTrace(System.err);
            
        }

    }

}
