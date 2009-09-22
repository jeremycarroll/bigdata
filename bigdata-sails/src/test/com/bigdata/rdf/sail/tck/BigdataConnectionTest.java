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

import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnectionTest;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSail.Options;

public class BigdataConnectionTest extends RepositoryConnectionTest {

	public BigdataConnectionTest(String name) {
		super(name);
	}

	@Override
	protected Repository createRepository()
		throws IOException
	{
        File journal = BigdataStoreTest.createTempFile();
        
        Properties props = new Properties();
        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());
        props.setProperty(Options.STATEMENT_IDENTIFIERS, "false");
        props.setProperty(Options.QUADS, "true");
        props.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        BigdataSail sail = new BigdataSail(props);
        return new BigdataSailRepository(sail);
	}
		
    /**
     * Overridden to destroy the backend database and its files on the disk.
     */
    @Override
    protected void tearDown()
        throws Exception
    {

        final IIndexManager backend = testRepository == null ? null
                : ((BigdataSailRepository) testRepository).getDatabase()
                        .getIndexManager();

        super.tearDown();

        if (backend != null)
            backend.destroy();

    }
    
}
