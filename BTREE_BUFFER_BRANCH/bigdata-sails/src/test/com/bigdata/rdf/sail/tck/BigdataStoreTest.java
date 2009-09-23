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
 * Created on Jun 19, 2008
 */
package com.bigdata.rdf.sail.tck;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.openrdf.sail.RDFStoreTest;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.Options;

public class BigdataStoreTest extends RDFStoreTest {

    static File createTempFile() {
        try {
            return File.createTempFile("bigdata-tck", ".jnl");
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Overridden to destroy the backend database and its files on the disk.
     */
    @Override
    protected void tearDown()
        throws Exception
    {
        
        final IIndexManager backend = sail == null ? null
                : ((BigdataSail) sail).getDatabase().getIndexManager();

        super.tearDown();

        if (backend != null)
            backend.destroy();

    }
    
    public BigdataStoreTest(String name) {
        super(name);
    }

    /**
     * @todo The problem here is that the {@link BigdataSail} uses a semaphore
     *       to grant the unisolated write connection. If a thread requests two
     *       sail connections then it will deadlock. This could be fixed either
     *       by supporting full transactions in the sail or by allowing more
     *       than one connection but having them interleave their incremental
     *       writes.
     */
    @Override
    public void testDualConnections(){
        fail("Not supported yet.");
    }

    @Override
    protected Sail createSail() throws SailException {
        File journal = createTempFile();

        Properties props = new Properties();
        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());
        props.setProperty(Options.STATEMENT_IDENTIFIERS, "false");
        props.setProperty(Options.QUADS, "true");
        props.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());

        Sail sail = new BigdataSail(props);
        sail.initialize();
        SailConnection conn = sail.getConnection();
        try {
            conn.clear();
            conn.clearNamespaces();
            conn.commit();
        } finally {
            conn.close();
        }
        return sail;
    }

}
