/*
 * Copyright SYSTAP, LLC 2006-2007.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Nov 7, 2007
 */

package com.bigdata.rdf.sail;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

import junit.framework.TestCase;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.SailUtil;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Options;
import com.bigdata.rdf.inf.InferenceEngine;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.store.StatementIterator;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractInferenceEngineTestCase extends TestCase {

    /**
     * 
     */
    public AbstractInferenceEngineTestCase() {
        super();
    }

    /**
     * @param arg0
     */
    public AbstractInferenceEngineTestCase(String arg0) {
        super(arg0);
    }

    public Properties getProperties() {
        
        Properties properties = new Properties();
        
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

        properties.setProperty(Options.CREATE_TEMP_FILE, "true");

        properties.setProperty(Options.DELETE_ON_EXIT, "true");

        properties
                .setProperty(
                        com.bigdata.rdf.store.LocalTripleStore.Options.ISOLATABLE_INDICES,
                        "false");

        properties.setProperty(
                com.bigdata.rdf.sail.BigdataSail.Options.STORE_CLASS,
                LocalTripleStore.class.getName());

        /*
         * Note: overrides properties to make sure that the OWL axioms are
         * not defined since they are not going to be in the graph produced
         * by Sesame.
         */
        properties.setProperty(com.bigdata.rdf.inf.InferenceEngine.Options.RDFS_ONLY, "true");

        return properties;
        
    }
    
    public AbstractTripleStore getStore() {
    
        return new LocalTripleStore(getProperties());
        
    }
    
    /*
     * compares two RDF models for equality.
     */

    /**
     * Wraps up the {@link AbstractTripleStore} as an {@link RdfRepository} to
     * facilitate using {@link #modelsEqual(RdfRepository, RdfRepository)} for
     * ground truth testing.
     * 
     * @see SailUtil#modelsEqual(org.openrdf.sesame.sail.RdfSource, org.openrdf.sesame.sail.RdfSource)
     * which might be used instead.  However, it does not provide any details on
     * how the models differ.
     */
    public boolean modelsEqual(RdfRepository expected, InferenceEngine inf)
            throws SailException {

        BigdataSailConnection repo = new BigdataSailConnection(inf.database);
        
        Properties properties = new Properties(getProperties());
        
        properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE,
                "" + true);
        
        repo.initialize( properties );
        
        return modelsEqual(expected,repo);
        
    }
    
    /**
     * Compares two RDF graphs for equality (same statements) - does NOT handle
     * bnodes, which much be treated as variables for RDF semantics.
     * 
     * @param expected
     * 
     * @param actual
     * 
     * @return true if all statements in the expected graph are in the actual
     *         graph and if the actual graph does not contain any statements
     *         that are not also in the expected graph.
     * 
     * @todo Sesame probably bundles this logic in
     *       {@link SailUtil#modelsEqual(org.openrdf.sesame.sail.RdfSource, org.openrdf.sesame.sail.RdfSource)}
     *       in a manner that handles bnodes.
     */
    public static boolean modelsEqual(RdfRepository expected,
            BigdataSailConnection actual) {

        Collection<Statement> expectedRepo = getStatements(expected);

        int actualSize = 0; 
        boolean sameStatements1 = true;
        {

            StatementIterator it = actual.getStatements(null, null, null);

            try {

                for (; it.hasNext();) {

                    Statement stmt = it.next();

                    if (!expected.hasStatement(stmt.getSubject(), stmt
                            .getPredicate(), stmt.getObject())) {

                        sameStatements1 = false;

                        log("Not expecting: " + stmt);

                    }

                    actualSize++; // count #of statements actually visited.
                    
                }

            } finally {

                it.close();

            }
            
            log("all the statements in actual in expected? " + sameStatements1);

        }

        int expectedSize = 0;
        boolean sameStatements2 = true;
        {

            for (Iterator<Statement> it = expectedRepo.iterator(); it.hasNext();) {

                Statement stmt = it.next();

                if (!actual.hasStatement(stmt.getSubject(),
                        stmt.getPredicate(), stmt.getObject())) {

                    sameStatements2 = false;

                    log("    Expecting: " + stmt);

                }
                
                expectedSize++; // counts statements actually visited.

            }

            log("all the statements in expected in actual? " + sameStatements2);

        }

        final boolean sameSize = expectedSize == actualSize;
        
        log("size of 'expected' repository: " + expectedSize);

        log("size of 'actual'   repository: " + actualSize);

        return sameSize && sameStatements1 && sameStatements2;

    }

    private static Collection<Statement> getStatements(RdfRepository repo) {

        /*
         * Note: do NOT use a hash set here since it will hide conceal the
         * presence of duplicate statements in either graph.
         */

        Collection<Statement> c = new LinkedList<Statement>();

        StatementIterator statIter = repo.getStatements(null, null, null);

        while (statIter.hasNext()) {

            Statement stmt = statIter.next();

            c.add(stmt);

        }

        statIter.close();

        return c;

    }

    private static void log(String s) {

        System.err.println(s);

    }

    /**
     * Read the resource into an {@link RdfSchemaRepository}. This
     * automatically computes the closure of the told triples.
     * <p>
     * Note: We treat the closure as computed by the {@link RdfSchemaRepository}
     * as if it were ground truth.
     * 
     * @return The {@link RdfSchemaRepository} with the loaded resource.
     * 
     * @throws SailInitializationException
     * @throws IOException
     * @throws UpdateException
     */
    protected RdfRepository getGroundTruth(String resource, String baseURL,
            RDFFormat format) throws SailException, IOException {

        return getGroundTruth(new String[] { resource },
                new String[] { baseURL }, new RDFFormat[] { format });
        
    }

    /**
     * Read the resource(s) into an {@link RdfSchemaRepository}. This
     * automatically computes the closure of the told triples.
     * <p>
     * Note: We treat the closure as computed by the {@link RdfSchemaRepository}
     * as if it were ground truth.
     * 
     * @return The {@link RdfSchemaRepository} with the loaded resource.
     * 
     * @throws SailInitializationException
     * @throws IOException
     * @throws UpdateException
     */
    protected RdfRepository getGroundTruth(String[] resource, String[] baseURL,
            RDFFormat[] format) throws SailException, IOException {

        assert resource.length == baseURL.length;
        
        RdfRepository repo = new org.openrdf.sesame.sailimpl.memory.RdfSchemaRepository();

        repo.initialize(new HashMap());
        
        for(int i=0; i<resource.length; i++) {
            
            upload(repo, resource[i], baseURL[i], format[i]);
            
        }

        return repo;
        
    }

    /**
     * Uploads an file into an {@link RdfRepository}.
     *
     * @see RdfAdmin
     */
    protected void upload(RdfRepository repo, String resource, String baseURL,
            RDFFormat format)
            throws IOException {

        InputStream rdfStream = getClass().getResourceAsStream(resource);

        if (rdfStream == null) {

            // If we do not find as a Resource then try the file system.
            rdfStream = new BufferedInputStream(new FileInputStream(resource));

        }

        try {

            RdfAdmin admin = new RdfAdmin(repo);

            final boolean validate = true;

            admin.addRdfModel(rdfStream, baseURL, new StdOutAdminListener(),
                    format, validate);

        } finally {

            rdfStream.close();

        }

    }

}
