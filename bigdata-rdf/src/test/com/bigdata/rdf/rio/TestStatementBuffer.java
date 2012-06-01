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
 * Created on Jan 29, 2007
 */

package com.bigdata.rdf.rio;

import java.util.Properties;

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.bnode.SidIV;
import com.bigdata.rdf.load.IStatementBufferFactory;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.vocab.decls.DCTermsVocabularyDecl;

/**
 * Test suite for {@link StatementBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Parameterize with {@link IStatementBufferFactory} and use to
 *          test other implementations as well? If not, then port tests so that
 *          they are also run against other implementations (especially the
 *          tests for reification done right).
 */
public class TestStatementBuffer extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestStatementBuffer() {
    }

    /**
     * @param name
     */
    public TestStatementBuffer(String name) {
        super(name);
    }

    public void test_ctor() {
        
        final int capacity = 27;
        
        final AbstractTripleStore store = getStore();
        
        try {

			final StatementBuffer<Statement> buffer = new StatementBuffer<Statement>(
					store, capacity);

            assertEquals(store, buffer.getDatabase());
            assertTrue(buffer.distinct);
            assertEquals(capacity, buffer.capacity);
            assertEquals(capacity * store.getSPOKeyArity(), buffer.values.length);
            assertEquals(capacity, buffer.stmts.length);
            assertEquals(0, buffer.numURIs);
            assertEquals(0, buffer.numLiterals);
            assertEquals(0, buffer.numBNodes);
            assertEquals(0, buffer.numStmts);

        } finally {

            store.__tearDownUnitTest();

        }

    }

    /**
     * Test verifies detection of duplicate terms and their automatic
     * replacement with a canonicalizing term.
     */
    public void test_handleStatement_distinctTerm() {

        final int capacity = 5;

        final AbstractTripleStore store = getStore();

        try {

			final StatementBuffer<Statement> buffer = new StatementBuffer<Statement>(
					store, capacity);

            assertTrue(buffer.distinct);

            /*
             * add a statement.
             */

            final URI s1 = new URIImpl("http://www.foo.org");
            final URI p1 = RDF.TYPE;
            final URI o1 = RDFS.RESOURCE;
            final URI c1 = null; // no context.

            buffer.handleStatement(s1, p1, o1, c1, StatementEnum.Explicit);

            assertEquals(3, buffer.numURIs);
            assertEquals(0, buffer.numLiterals);
            assertEquals(0, buffer.numBNodes);
            assertEquals(1, buffer.numStmts);

            /*
             * add another statement.
             */

            final URI s2 = new URIImpl("http://www.foo.org"); // duplicate term!
            final URI p2 = RDFS.LABEL;
            final Literal o2 = new LiteralImpl("test lit.");
            final URI c2 = null;

            buffer.handleStatement(s2, p2, o2, c2, StatementEnum.Explicit);

            assertEquals(4, buffer.numURIs); // only 4 since one is duplicate.
            assertEquals(1, buffer.numLiterals);
            assertEquals(0, buffer.numBNodes);
            assertEquals(2, buffer.numStmts);

            /*
             * add a duplicate statement.
             */

            final URI s3 = new URIImpl("http://www.foo.org"); // duplicate term
            final URI p3 = RDFS.LABEL;                        // duplicate term
            final Literal o3 = new LiteralImpl("test lit.");  // duplicate term
            final URI c3 = null;

            buffer.handleStatement(s3, p3, o3, c3, StatementEnum.Explicit);

            assertEquals(4, buffer.numURIs);
            assertEquals(1, buffer.numLiterals);
            assertEquals(0, buffer.numBNodes);
            assertEquals(3, buffer.numStmts);

            /*
             * add a duplicate statement using the _same_ term objects.
             */

            buffer.handleStatement(s3, p3, o3, c3, StatementEnum.Explicit);

            assertEquals(4, buffer.numURIs);
            assertEquals(1, buffer.numLiterals);
            assertEquals(0, buffer.numBNodes);
            assertEquals(4, buffer.numStmts);
            
            buffer.flush();

        } finally {

            store.__tearDownUnitTest();

        }

    }

	/**
	 * Test verifies interpretation of triples by the {@link StatementBuffer} by
	 * validating how the triples written onto the statement buffer are loaded
	 * into the {@link AbstractTripleStore}.
	 */
    public void test_statementBuffer() {

        final int capacity = 5;

		final Properties properties = new Properties(getProperties());

		// turn off entailments.
		properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
				NoAxioms.class.getName());

        final AbstractTripleStore store = getStore(properties);

        try {
        	
        		// store is empty.
        		assertEquals(0,store.getStatementCount());

        		final BigdataValueFactory vf = store.getValueFactory();
        	
			final StatementBuffer<Statement> buffer = new StatementBuffer<Statement>(
					store, capacity);

            /*
             * add a statement.
             */

            final URI s1 = new URIImpl("http://www.foo.org");
            final URI p1 = RDF.TYPE;
            final URI o1 = RDFS.RESOURCE;
            final URI c1 = null; // no context.

            buffer.add(vf.createStatement(s1, p1, o1, c1, StatementEnum.Explicit));

            /*
             * add another statement.
             */

            final URI s2 = new URIImpl("http://www.foo.org"); // duplicate term!
            final URI p2 = RDFS.LABEL;
            final Literal o2 = new LiteralImpl("test lit.");
            final URI c2 = null;

            buffer.add(vf.createStatement(s2, p2, o2, c2, StatementEnum.Explicit));

            /*
             * add a duplicate statement.
             */

            final URI s3 = new URIImpl("http://www.foo.org"); // duplicate term
            final URI p3 = RDFS.LABEL;                        // duplicate term
            final Literal o3 = new LiteralImpl("test lit.");  // duplicate term
            final URI c3 = null;

            buffer.handleStatement(s3, p3, o3, c3, StatementEnum.Explicit);

            // store is still empty (statements are buffered).
            assertEquals(0,store.getStatementCount());

            // flush the buffer.
			buffer.flush();

			// the statements are now in the store.
			assertEquals(2, store.getStatementCount());

			assertTrue(store.hasStatement(s1, p1, o1));
			assertTrue(store.hasStatement(s2, p2, o2));
			assertFalse(store.hasStatement(s1, p2, o1));

        } finally {

            store.__tearDownUnitTest();

        }

    }

	/**
	 * A unit test in which the translation of reified statements into inline
	 * statements disabled. This test uses the same data as the test below.
	 */
	public void test_reificationDoneRight_disabled() {

        final int capacity = 20;

		final Properties properties = new Properties(getProperties());

		// turn off entailments.
		properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
				NoAxioms.class.getName());

        final AbstractTripleStore store = getStore(properties);

        try {
       
			// * @prefix : <http://example.com/> .
			// * @prefix news: <http://example.com/news/> .
			// * @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
			// * @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
			// * @prefix dc: <http://purl.org/dc/terms/> .
			// * @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
			// *
			// * :SAP :bought :sybase .
			// * _:s1 rdf:subject :SAP .
			// * _:s1 rdf:predicate :bought .
			// * _:s1 rdf:object :sybase .
			// * _:s1 rdf:type rdf:Statement .
			// * _:s1 dc:source news:us-sybase .
			// * _:s1 dc:created    "2011-04-05T12:00:00Z"^^xsd:dateTime .

			final BigdataValueFactory vf = store.getValueFactory();

			final BigdataURI SAP = vf.createURI("http://example.com/SAP");
			final BigdataURI bought = vf.createURI("http://example.com/bought");
			final BigdataURI sybase = vf.createURI("http://example.com/sybase");
			final BigdataURI dcSource = vf.asValue(DCTermsVocabularyDecl.source);
			final BigdataURI dcCreated = vf.asValue(DCTermsVocabularyDecl.created);
			final BigdataURI newsSybase = vf.createURI("http://example.com/news/us-sybase");
			final BigdataLiteral createdDate = vf.createLiteral("2011-04-05T12:00:00Z",XSD.DATETIME);
			final BigdataBNode s1 = vf.createBNode("s1");

			// store is empty.
			assertEquals(0, store.getStatementCount());

			final StatementBuffer<Statement> buffer = new StatementBuffer<Statement>(
					store, capacity);

			// ground statement.
			buffer.add(vf.createStatement(SAP, bought, sybase,
					null/* context */, StatementEnum.Explicit));
			
			// model of that statement (RDF reification).
			buffer.add(vf.createStatement(s1, RDF.SUBJECT, SAP,
					null/* context */, StatementEnum.Explicit));

			buffer.add(vf.createStatement(s1, RDF.PREDICATE, bought,
					null/* context */, StatementEnum.Explicit));

			buffer.add(vf.createStatement(s1, RDF.OBJECT, sybase,
					null/* context */, StatementEnum.Explicit));

			buffer.add(vf.createStatement(s1, RDF.TYPE, RDF.STATEMENT,
					null/* context */, StatementEnum.Explicit));

			// metadata statements.
			
			buffer.add(vf.createStatement(s1, dcSource, newsSybase,
					null/* context */, StatementEnum.Explicit));

			buffer.add(vf.createStatement(s1, dcCreated, createdDate,
					null/* context */, StatementEnum.Explicit));

            // flush the buffer.
			buffer.flush();

			// the statements are now in the store.
			assertEquals(7, store.getStatementCount());

			assertTrue(store.hasStatement(SAP, bought, sybase));
			assertTrue(store.hasStatement(s1, RDF.SUBJECT, SAP));
			assertTrue(store.hasStatement(s1, RDF.PREDICATE, bought));
			assertTrue(store.hasStatement(s1, RDF.OBJECT, sybase));
			assertTrue(store.hasStatement(s1, RDF.TYPE, RDF.STATEMENT));
			assertTrue(store.hasStatement(s1, dcSource, newsSybase));
			assertTrue(store.hasStatement(s1, dcCreated, createdDate));

        } finally {

            store.__tearDownUnitTest();

        }

	}

	/**
	 * Unit test verifies that triples which look like part of a reified model
	 * of a statement are collected and then reported using SIDs.
	 * <p>
	 * For example, given the following source data:
	 * 
	 * <pre>
	 * @prefix :          <http://example.com/> .
	 * @prefix news:      <http://example.com/news/> .
	 * @prefix rdf:       <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . 
	 * @prefix rdfs:      <http://www.w3.org/2000/01/rdf-schema#> . 
	 * @prefix dc:        <http://purl.org/dc/terms/> . 
	 * @prefix xsd:       <http://www.w3.org/2001/XMLSchema#> .
	 * 
	 * :SAP :bought :sybase .
	 * _:s1 rdf:subject   :SAP .
	 * _:s1 rdf:predicate :bought .
	 * _:s1 rdf:object    :sybase .
	 * _:s1 rdf:type      rdf:Statement .
	 * _:s1 dc:source     news:us-sybase .
	 * _:s1 dc:created    "2011-04-05T12:00:00Z"^^xsd:dateTime .
	 * </pre>
	 * 
	 * Only the following three triples would actually be reported:
	 * 
	 * <pre>
	 * :SAP :bought :sybase .
	 * <<:SAP :bought :sybase>> dc:source news:us-sybase .
	 * <<:SAP :bought :sybase>> dc:created "2011-04-05T12:00:00Z"^^xsd:dateTime .
	 * </pre>
	 * 
	 * TODO Implement this test. This parser mode should be conditioned on
	 * {@link QueryHints#DEFAULT_REIFICATION_DONE_RIGHT} for now. It should be
	 * configurable so the parser can be reused without having this SIDS style
	 * interpretation (at least for benchmarking).
	 * <p>
	 * We need to be careful that reified (versus inlined) statements about
	 * statements do not make it accidentally into the DB. Intercepting things
	 * at the {@link StatementBuffer} goes a long way toward that goal. We will
	 * also have to review the scale-out data loader and the sail truth
	 * maintenance assertion and retraction buffers in this regard. Retraction
	 * could be a bit messy if people only pull out, e.g., rdf:subject, while
	 * leaving in the rest of the triples. However, maybe it is not so bad since
	 * you have to give all of the triples in the statement model in order to
	 * cause anything about that statement to be removed (and also give the
	 * ground triple itself).
	 * <p>
	 * Note that the following has exactly the same interpretation - it will
	 * generate the same three triples, including the ground triple and the two
	 * statements about that ground triple.
	 * 
	 * <pre>
	 * <<:SAP :bought :sybase>> dc:source news:us-sybase ;
	 *                          dc:created "2011-04-05T12:00:00Z"^^xsd:dateTime .
	 * </pre>
	 */
	public void test_reificationDoneRight_enabled() {

        final int capacity = 20;

		final Properties properties = new Properties(getProperties());

		// turn off entailments.
		properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
				NoAxioms.class.getName());

        final AbstractTripleStore store = getStore(properties);

        try {
       
			// * @prefix : <http://example.com/> .
			// * @prefix news: <http://example.com/news/> .
			// * @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
			// * @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
			// * @prefix dc: <http://purl.org/dc/terms/> .
			// * @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
			// *
			// * :SAP :bought :sybase .
			// * _:s1 rdf:subject :SAP .
			// * _:s1 rdf:predicate :bought .
			// * _:s1 rdf:object :sybase .
			// * _:s1 rdf:type rdf:Statement .
			// * _:s1 dc:source news:us-sybase .
			// * _:s1 dc:created    "2011-04-05T12:00:00Z"^^xsd:dateTime .

			final BigdataValueFactory vf = store.getValueFactory();

			final BigdataURI SAP = vf.createURI("http://example.com/SAP");
			final BigdataURI bought = vf.createURI("http://example.com/bought");
			final BigdataURI sybase = vf.createURI("http://example.com/sybase");
			final BigdataURI dcSource = vf.asValue(DCTermsVocabularyDecl.source);
			final BigdataURI dcCreated = vf.asValue(DCTermsVocabularyDecl.created);
			final BigdataURI newsSybase = vf.createURI("http://example.com/news/us-sybase");
			final BigdataLiteral createdDate = vf.createLiteral("2011-04-05T12:00:00Z",XSD.DATETIME);
			final BigdataBNode s1 = vf.createBNode("s1");

			// store is empty.
			assertEquals(0, store.getStatementCount());

			final StatementBuffer<Statement> buffer = new StatementBuffer<Statement>(
					store, capacity);

			// ground statement.
			buffer.add(vf.createStatement(SAP, bought, sybase,
					null/* context */, StatementEnum.Explicit));
			
			// model of that statement (RDF reification).
			buffer.add(vf.createStatement(s1, RDF.SUBJECT, SAP,
					null/* context */, StatementEnum.Explicit));

			buffer.add(vf.createStatement(s1, RDF.PREDICATE, bought,
					null/* context */, StatementEnum.Explicit));

			buffer.add(vf.createStatement(s1, RDF.OBJECT, sybase,
					null/* context */, StatementEnum.Explicit));

			buffer.add(vf.createStatement(s1, RDF.TYPE, RDF.STATEMENT,
					null/* context */, StatementEnum.Explicit));

			// metadata statements.
			
			final BigdataStatement mds1 = vf.createStatement(s1, dcSource,
					newsSybase, null/* context */, StatementEnum.Explicit);

			final BigdataStatement mds2 = vf.createStatement(s1, dcCreated,
					createdDate, null/* context */, StatementEnum.Explicit);

			buffer.add(mds1);

			buffer.add(mds2);

            // flush the buffer.
			buffer.flush();

			/*
			 * FIXME This is failing because the StatementBuffer has not been
			 * modified to recognize reified statements for which a ground
			 * statement exists and then write them onto the database using
			 * SidIVs instead of statement models.
			 */
			// the statements are now in the store.
			assertEquals(3, store.getStatementCount());
			
			assertTrue(store.hasStatement(SAP, bought, sybase));
			assertFalse(store.hasStatement(s1, RDF.SUBJECT, SAP));
			assertFalse(store.hasStatement(s1, RDF.PREDICATE, bought));
			assertFalse(store.hasStatement(s1, RDF.OBJECT, sybase));
			assertFalse(store.hasStatement(s1, RDF.TYPE, RDF.STATEMENT));
			assertTrue(store.hasStatement(s1, dcSource, newsSybase));
			assertTrue(store.hasStatement(s1, dcCreated, createdDate));

			/*
			 * FIXME BigdataStatementImpl currently relies on c() to be the
			 * SidIV. This needs to be changed. The SidIV should now be formed
			 * dynamically from the concatenation of the subject, predicate, and
			 * object roles IVs. The context role [c] needs to remain available
			 * for use with named graphs. The SidIV can be formed dynamically
			 * since it depends solely on the (s,p,o) components.
			 */
			
			mds1.setStatementIdentifier(true);
			mds2.setStatementIdentifier(true);
			
			assertTrue(mds1.hasStatementIdentifier());
			assertTrue(mds2.hasStatementIdentifier());

			final SidIV<?> sidIV1 = (SidIV<?>) mds1.getStatementIdentifier();
			final SidIV<?> sidIV2 = (SidIV<?>) mds2.getStatementIdentifier();
			
			assertEquals(sidIV1.getInlineValue().s(), mds1.s());
			assertEquals(sidIV1.getInlineValue().p(), mds1.p());
			assertEquals(sidIV1.getInlineValue().o(), mds1.o());
			assertNull(sidIV1.getInlineValue().c());

			assertEquals(sidIV2.getInlineValue().s(), mds2.s());
			assertEquals(sidIV2.getInlineValue().p(), mds2.p());
			assertEquals(sidIV2.getInlineValue().o(), mds2.o());
			assertNull(sidIV2.getInlineValue().c());

        } finally {

            store.__tearDownUnitTest();

        }

    }
    
}
