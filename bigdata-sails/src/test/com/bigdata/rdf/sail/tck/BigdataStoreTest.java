/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2008.
 *
 * Licensed under the Open Software License version 3.0.
 */
package com.bigdata.rdf.sail.tck;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import info.aduna.io.FileUtil;

import org.openrdf.sail.RDFStoreTest;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.Options;

/**
 * An extension of RDFStoreTest for testing the class {@link MulgaraStore}.
 */
public class BigdataStoreTest extends RDFStoreTest {

	static File createTempFile() {
		try {
			return File.createTempFile("bigdata-tck", ".jnl");
		}
		catch (IOException e) {
			throw new AssertionError(e);
		}
	}

	/*--------------*
	 * Constructors *
	 *--------------*/

	public BigdataStoreTest(String name) {
		super(name);
	}

	/*---------*
	 * Methods *
	 *---------*/

	@Override
	protected Sail createSail()
		throws SailException
	{
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
		}
		finally {
			conn.close();
		}
		return sail;
	}
}
