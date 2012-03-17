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
/*
 * Created on Mar 16, 2012
 */

package com.bigdata.rdf.sparql.ast.eval.update;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.rio.RDFFormat;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.rdf.update.ChunkedResolutionTask;
import com.bigdata.bop.rdf.update.CommitOp;
import com.bigdata.bop.rdf.update.InsertStatementsOp;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.RDFParserOptions;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.InsertData;
import com.bigdata.rdf.sparql.ast.UpdateRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;
import com.bigdata.rdf.store.DataLoader.CommitEnum;

/**
 * Boot strapped test suite for core UPDATE functionality.
 * 
 * TODO Integrate into {@link AST2BOpUtility} (plan generation).
 * 
 * TODO Validate outcomes. For the data driven tests, we could validate a
 * post-condition with a query. Or a set of post-conditions with a set of
 * queries if that is really necessary. We also need to verify things such as
 * whether or not the data are committed and support for triples, sids, quads,
 * s/o, and full transactions (on a journal). It would also be good to verify
 * support against an SPO only graph on a temporary store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestUpdateBootstrap extends AbstractASTEvaluationTestCase {

    private static final Logger log = Logger
            .getLogger(TestUpdateBootstrap.class);
    
    /**
     * 
     */
    public TestUpdateBootstrap() {
    }

    /**
     * @param name
     */
    public TestUpdateBootstrap(String name) {
        super(name);
    }

    /**
     * Unit test for inserting ground triples or quads.
     * <pre>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * INSERT DATA
     * { 
     *   <http://example/book1> dc:title "A new book" ;
     *                          dc:creator "A.N.Other" .
     * }
     * </pre>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_insert_data() throws Exception {

        final UpdateRoot updateRoot = new UpdateRoot();
        {

            final InsertData op = new InsertData();

            updateRoot.addChild(op);

            final BigdataURI book1 = valueFactory.createURI("http://example/book1");
            final BigdataURI dcCreator = valueFactory.createURI("http://purl.org/dc/elements/1.1/creator");
            final BigdataURI dcTitle = valueFactory.createURI("http://purl.org/dc/elements/1.1/title");
            final BigdataLiteral label1 = valueFactory.createLiteral("A new book");
            final BigdataLiteral label2 = valueFactory.createLiteral("A.N.Other");

            final ISPO[] data = new ISPO[] { //
                    valueFactory.createStatement(
                    book1, dcTitle, label1, null, StatementEnum.Explicit),//
                    valueFactory.createStatement(
                    book1, dcCreator, label2, null, StatementEnum.Explicit),//
            };
            op.setData(data);
            
        }

        /*
         * Turn that AST Update operation into a pipeline bop to add the terms
         * and write the statements. This will be done by AST2BOpUtility, but I
         * can mock the translation target up first.
         * 
         * TODO When translating, the notion that we might translate either
         * incrementally or all operations in the sequence at once is related to
         * the notion of interactive evaluation which would help to enable the
         * RTO.
         */
 
        final ASTContainer astContainer = new ASTContainer(updateRoot);
        
        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        final long txId = ITx.UNISOLATED;
        int bopId = 1;
        final int resolutionId = bopId++;
        final int insertStatementsId = bopId++;
        final int commitId = bopId++;
        PipelineOp left = null; // TODO left-or-null
        
        /*
         * Resolve/add terms against the lexicon.
         * 
         * TODO Must do SIDs support. Probably pass the database mode in as an
         * annotation.
         */
        left = new ChunkedResolutionTask(BOp.NOARGS, NV.asMap(//
                new NV(ChunkedResolutionTask.Annotations.BOP_ID, resolutionId),//
                new NV(ChunkedResolutionTask.Annotations.TIMESTAMP, txId),//
                new NV(ChunkedResolutionTask.Annotations.RELATION_NAME,
                        new String[] { context.getLexiconNamespace() })//
                ));

        /*
         * Insert statements.
         * 
         * Note: namespace is the triple store, not the spo relation. This is
         * because insert is currently on the triple store for historical SIDs
         * support.
         * 
         * Note: This already does TM for SIDs mode.
         * 
         * TODO This must to TM for the subject-centric text index.
         * 
         * TODO This must be able to do TM for triples+inference.
         */
        left = new InsertStatementsOp(new BOp[] { left }, NV.asMap(new NV(
                ChunkedResolutionTask.Annotations.BOP_ID, insertStatementsId),//
                new NV(ChunkedResolutionTask.Annotations.TIMESTAMP, txId),//
                new NV(ChunkedResolutionTask.Annotations.RELATION_NAME,
                        new String[] { context.getNamespace() })//
                ));
        
        /*
         * Commit.
         * 
         * TODO Not required on cluster.
         * 
         * TODO Not required unless the end of the UpdateRoot or we desired a
         * checkpoint on the sequences of operations.
         */
        left = new CommitOp(new BOp[] { left }, NV.asMap(//
                new NV(ChunkedResolutionTask.Annotations.BOP_ID, commitId),//
                new NV(ChunkedResolutionTask.Annotations.TIMESTAMP, txId)//
                ));

        /**
         * The statements to be asserted.
         * 
         * <pre>
         *     :Mike rdf:type foaf:Person .
         *     :Bryan rdf:type foaf:Person .
         *     :Mike rdfs:label "Mike" .
         *     :Bryan rdfs:label "Bryan" .
         *     :DC rdfs:label "DC" .
         * </pre>
         */
        final List<IBindingSet> bsets = new LinkedList<IBindingSet>();
        {

            final Var<?> s = Var.var("s"), p = Var.var("p"), o = Var
                    .var("o"), c = Var.var("c");

            {
                final ListBindingSet bset = new ListBindingSet();
                bset.set(s, new Constant(makeIV(new URIImpl(
                        "http://www.bigdata.com/Mike"))));
                bset.set(p, new Constant(makeIV(RDF.TYPE)));
                bset.set(o, new Constant(makeIV(new URIImpl(
                        "http://xmlns.com/foaf/0.1/Person"))));
                bset.set(c, new Constant(makeIV(new URIImpl(
                        "http://www.bigdata.com/g1"))));
                bsets.add(bset);
            }
            {
                final ListBindingSet bset = new ListBindingSet();
                bset.set(s, new Constant(makeIV(new URIImpl(
                        "http://www.bigdata.com/Bryan"))));
                bset.set(p, new Constant(makeIV(RDF.TYPE)));
                bset.set(o, new Constant(makeIV(new URIImpl(
                        "http://xmlns.com/foaf/0.1/Person"))));
                bset.set(c, new Constant(makeIV(new URIImpl(
                        "http://www.bigdata.com/g1"))));
                bsets.add(bset);
            }
            {
                final ListBindingSet bset = new ListBindingSet();
                bset.set(s, new Constant(makeIV(new URIImpl(
                        "http://www.bigdata.com/Mike"))));
                bset.set(p, new Constant(makeIV(RDFS.LABEL)));
                bset.set(o, new Constant(makeIV(new LiteralImpl("Mike"))));
                bset.set(c, new Constant(makeIV(new URIImpl(
                        "http://www.bigdata.com/g1"))));
                bsets.add(bset);
            }
            {
                final ListBindingSet bset = new ListBindingSet();
                bset.set(s, new Constant(makeIV(new URIImpl(
                        "http://www.bigdata.com/Bryan"))));
                bset.set(p, new Constant(makeIV(RDFS.LABEL)));
                bset.set(o, new Constant(makeIV(new LiteralImpl("Bryan"))));
                bset.set(c, new Constant(makeIV(new URIImpl(
                        "http://www.bigdata.com/g1"))));
                bsets.add(bset);
            }
            {
                final ListBindingSet bset = new ListBindingSet();
                bset.set(s, new Constant(makeIV(new URIImpl(
                        "http://www.bigdata.com/DC"))));
                bset.set(p, new Constant(makeIV(RDFS.LABEL)));
                bset.set(o, new Constant(makeIV(new LiteralImpl("DC"))));
                bset.set(c, new Constant(makeIV(new URIImpl(
                        "http://www.bigdata.com/g1"))));
                bsets.add(bset);
            }

        }
        
        // Run the update.
        final IRunningQuery future = context.queryEngine.eval(left,
                bsets.toArray(new IBindingSet[bsets.size()]));
        
        // Look for errors.
        future.get();

        if (log.isInfoEnabled())
            log.info(store.dumpStore());
        
        {
            final BigdataValueFactory f = store.getValueFactory();
            final BigdataURI mike = f.createURI("http://www.bigdata.com/Mike");
            final BigdataURI bryan = f.createURI("http://www.bigdata.com/Bryan");
            final BigdataURI dc = f.createURI("http://www.bigdata.com/DC");
            final BigdataURI g1 = f.createURI("http://www.bigdata.com/g1");
            final BigdataURI rdfType = f.asValue(RDF.TYPE);
            final BigdataURI rdfsLabel = f.asValue(RDFS.LABEL);
            final BigdataURI foafPerson = f.createURI("http://xmlns.com/foaf/0.1/Person");
            final BigdataLiteral mikeL = f.createLiteral("Mike");
            final BigdataLiteral bryanL = f.createLiteral("Bryan");
            final BigdataLiteral DCL = f.createLiteral("DC");

            final BigdataValue[] values = new BigdataValue[] { mike, bryan, dc,
                    g1, rdfType, rdfsLabel, foafPerson, mikeL, bryanL, DCL };

            // Batch resolve (read-only).
            store.getLexiconRelation()
                    .addTerms(values, values.length, true/* readOnly */);
            
            // Verify IV assignment.
            for (BigdataValue v : values) {

                final IV<?, ?> iv = v.getIV();

                // a real IV.
                assertFalse(iv.isNullIV());
                
            }

            /*
             * Verify statements in the store.
             */
            assertTrue(store.hasStatement(mike, rdfType, foafPerson, g1));
            assertTrue(store.hasStatement(bryan, rdfType, foafPerson, g1));
            assertTrue(store.hasStatement(mike, rdfsLabel, mikeL, g1));
            assertTrue(store.hasStatement(bryan, rdfsLabel, bryanL, g1));
            assertTrue(store.hasStatement(dc, rdfsLabel, DCL, g1));

            /*
             * Check the mutation counters for the lexicon.
             */
            {

                final BOpStats stats = future.getStats().get(resolutionId);

                /*
                 * Note: This assumes that rdf:type and rdfs:label are not
                 * defined by the vocabulary.  We have to subtract out each
                 * Value which was declared by the vocabulary or is otherwise
                 * represented as a fully inline IV.
                 */
                
                long expectedCount = values.length;

                for (int i = 0; i < values.length; i++) {

                    if (values[i].getIV().isInline())
                        expectedCount--;

                }

                assertEquals("mutationCount", expectedCount,
                        stats.mutationCount.get());
            
            }
            
            /*
             * Check the mutation counters for the statements relation.
             */
            {

                final BOpStats stats = future.getStats()
                        .get(insertStatementsId);

                final long expectedCount = 5;

                assertEquals("mutationCount", expectedCount,
                        stats.mutationCount.get());

            }

        }
        
    }
    
//    /**
//     * Unit test for removing ground triples or quads.
//     */
//    public void test_delete_data() throws Exception {
//        fail("write test");
//    }

    /**
     * Unit test for parsing and then loading ground triples or quads.
     * 
     * TODO The parser integration is actually going to be pretty tricky. We
     * need to handle the {@link RDFFormat} identification, override and/or
     * refactor the {@link PresortRioLoader} and/or the {@link StatementBuffer}
     * to provide incremental writes plus blank node and SIDs resolution (which
     * requires that the bnodes map has document scope and hence requires the
     * use of shared state for the ParseOp, unless the op runs with no inputs
     * and just parses the document identified by the annotation). We need to
     * handle load from the classpath, URI, and the file system, but we might
     * not expose all of those sources to SPARQL UPDATE.
     * 
     * TODO There are also a host of options on the {@link DataLoader} which
     * should be exposed to LOAD, probably through a syntax extension of SPARQL.
     * That includes whether or not to do truth maintenance during a load (vs
     * bulk load), figuring out whether to commit after each document or after a
     * series of documents, figuring out whether to do database at once closure
     * after the load operations (maybe a special SPARQL UPDATE request?), the
     * buffer capacity, whether or not to verify data when parsing, whether or
     * not to stop at the first error or continue, whether or not to trim large
     * values (nxparser), told bnodes semantics, datatype handling, etc. Also,
     * whether or not to setup the assertion and retraction buffers for truth
     * maintenance, etc.
     * 
     * TODO We should probably have an indirected LOAD2 for a cluster
     * (distributes load requests for files identified by some source).
     * 
     * @see PresortRioLoader
     * @see StatementBuffer
     * @see DataLoader
     * @see DataLoader.Options
     * @see RDFParserOptions
     * @see ClosureEnum
     * @see CommitEnum
     */
    public void test_load_data() throws Exception {
        fail("write test");
    }

}
