package com.bigdata.rdf.sail.remote;

/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;

import com.bigdata.rdf.sail.webapp.client.DefaultClientConnectionManagerFactory;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;

/**
 * An implementation of Sesame's RepositoryConnection interface that wraps a
 * bigdata RemoteRepository. This provides SAIL API based client access to a
 * bigdata remote NanoSparqlServer. This implementation operates only in
 * auto-commit mode (each mutation operation results in a commit on the server).
 * It also throws UnsupportedOperationExceptions all over the place due to
 * incompatibilities with our own remoting interface. If there is something
 * important that you need implemented for your application don't be afraid to
 * reach out and contact us.
 * 
 * TODO Implement buffering of adds and removes so that we can turn off
 * 		auto-commit. 
 * TODO Fix all the Query objects (TupleQuery, GraphQuery,
 * 		BooleanQuery) to support the various possible operations on them, such as
 * 		setting a binding. 
 * TODO Support baseURIs
 */
public class BigdataSailRemoteRepository implements Repository {

    private final ClientConnectionManager ccm;

    private final ExecutorService executor;
    
    private final RemoteRepository nss;

    /**
     * Ctor that simply specifies an endpoint and lets this class manage the
     * ClientConnectionManager for the HTTP client and the manage the
     * ExecutorService. More convenient.
     */
	public BigdataSailRemoteRepository(final String sparqlEndpointURL) {
		
        this.executor = Executors.newCachedThreadPool();

        this.ccm = DefaultClientConnectionManagerFactory.getInstance()
                .newInstance();

        final DefaultHttpClient httpClient = new DefaultHttpClient(ccm);

        /*
         * Enable a standard http redirect policy. This allows references to
         * http://localhost:8080 to be redirected to
         * http://localhost:8080/bigdata.
         */
        httpClient.setRedirectStrategy(new DefaultRedirectStrategy());

        this.nss = new RemoteRepository(
                sparqlEndpointURL, httpClient, executor);
		
	}

	/**
	 * Ctor that allows the caller to manage the ClientConnectionManager for 
	 * the HTTP client and the manage the ExecutorService. More flexible.  
	 */
	public BigdataSailRemoteRepository(final RemoteRepository nss) {
		
		this.ccm = null;
		
		this.executor = null;
		
		this.nss = nss;
		
	}
	
	public RemoteRepository getRemoteRepository() {
		
		return nss;
		
	}
	
	@Override
	public void shutDown() throws RepositoryException {

		if (ccm != null)
			ccm.shutdown();
		
		if (executor != null)
			executor.shutdownNow();
		
	}

	@Override
	public BigdataSailRemoteRepositoryConnection getConnection() 
	        throws RepositoryException {
		
		return new BigdataSailRemoteRepositoryConnection(this);
		
	}

	@Override
	public void initialize() throws RepositoryException {
		// noop
	}

	@Override
	public boolean isInitialized() {
		return true;
	}

	@Override
	public boolean isWritable() throws RepositoryException {
		return true;
	}

	@Override
	public void setDataDir(File arg0) {
		throw new UnsupportedOperationException();
	}

	@Override
	public File getDataDir() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ValueFactory getValueFactory() {
		throw new UnsupportedOperationException();
	}

}
