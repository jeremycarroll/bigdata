/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2008.
 *
 * Licensed under the Open Software License version 3.0.
 */
package com.bigdata.rdf.sail.tck;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnectionTest;
import org.openrdf.repository.sail.SailRepository;

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
	
	
}
