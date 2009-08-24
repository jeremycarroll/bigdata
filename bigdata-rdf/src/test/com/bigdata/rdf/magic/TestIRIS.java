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
 * Created on Apr 28, 2009
 */

package com.bigdata.rdf.magic;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Iterator;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.deri.iris.api.factory.IBasicFactory;
import org.deri.iris.api.factory.IBuiltinsFactory;
import org.deri.iris.api.factory.ITermFactory;
import org.deri.iris.basics.BasicFactory;
import org.deri.iris.builtins.BuiltinsFactory;
import org.deri.iris.terms.TermFactory;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.axioms.OwlAxioms;
import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rules.AbstractInferenceEngineTestCase;
import com.bigdata.rdf.rules.BaseClosure;
import com.bigdata.rdf.rules.DoNotAddFilter;
import com.bigdata.rdf.rules.FastClosure;
import com.bigdata.rdf.rules.FullClosure;
import com.bigdata.rdf.rules.MappedProgram;
import com.bigdata.rdf.rules.RuleContextEnum;
import com.bigdata.rdf.rules.RuleOwlSameAs2;
import com.bigdata.rdf.rules.RuleOwlSameAs3;
import com.bigdata.rdf.rules.RuleRdfs11;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.Program;
import com.bigdata.relation.rule.QueryOptions;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.Var;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlanFactory2;
import com.bigdata.relation.rule.eval.IEvaluationPlanFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Test suite for IRIS-based truth maintenance on delete.
 * <p>
 * I would use a one up counter to assign the variable names during the
 * conversion and append it to the variable names in our rules.
 * 
 * The constraints are really just additional conditions on the rule. E.g.,
 * {@link RuleRdfs11} looks like this in prolog:
 * 
 * <pre>
 * triple(U,uri(rdfs:subClassOf),X) :-
 *  triple(U,uri(rdfs:subClassOf),V),
 *  triple(V,uri(rdfs:subClassOf),X),
 *  U != V,
 *  V != X.
 * </pre>
 * 
 * The RDF values will be expressed a atoms with the follow arity: uri/1,
 * literal/3, and bnode/1.
 * 
 * <pre>
 * uri(stringValue)
 * literal(stringValue,languageCode,datatTypeUri)
 * bnode(id)
 * </pre>
 * 
 * All for the values for those atoms will be string values.
 * 
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestIRIS extends AbstractInferenceEngineTestCase {

    final IBasicFactory BASIC = BasicFactory.getInstance();
    
    final ITermFactory TERM = TermFactory.getInstance();
    
    final IBuiltinsFactory BUILTINS = BuiltinsFactory.getInstance();
    
    final org.deri.iris.api.basics.IPredicate EQUAL = BASIC.createPredicate( "EQUAL", 2 );
    
    final org.deri.iris.api.basics.IPredicate NOT_EQUAL = BASIC.createPredicate( "NOT_EQUAL", 2 );
    
    final org.deri.iris.api.basics.IPredicate TRIPLE = BASIC.createPredicate("triple", 3);
    

    
    /**
     * 
     */
    public TestIRIS() {
        super();
    }

    /**
     * @param name
     */
    public TestIRIS(String name) {
        super(name);
    }

    /**
     * We are trying to eliminate the use of justification chains inside the
     * database.  These justification chains are used to determine whether or
     * not a statement is grounded by other facts in the database.  To determine 
     * this without justifications, we could use a magic sets optimization
     * against the normal inference rules using the statement to test as the
     * query.
     * 
     * @fixme variables for bigdata rules are assigned locally to the rule. will
     * this create problems for the iris program, where all the rules are mushed
     * together?
     * 
     * @fixme what do we do about the IConstraints on bigdata rules?  do they
     * get promoted to full-fledged IRIS rules in the IRIS program?
     */
    private void XXXtestPointTest() {
        
        Properties properties = getProperties();
        
        // override the default axiom model.
        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        // override the default closure.
        properties.setProperty(Options.CLOSURE_CLASS, SimpleClosure.class.getName());
        
        AbstractTripleStore store = getStore(properties);
        
        final Properties tmp = store.getProperties();
        
        tmp.setProperty(AbstractTripleStore.Options.LEXICON, "false");

        final TempMagicStore magicStore = new TempMagicStore(
                store.getIndexManager().getTempStore(), tmp, store);
        
        final String[] relations = {
            store.getSPORelation().getNamespace(),
            magicStore.getSPORelation().getNamespace()
        };
        
        try {

            final BigdataValueFactory f = store.getValueFactory();
            final BigdataURI U = f.createURI("http://www.bigdata.com/U");
            final BigdataURI V = f.createURI("http://www.bigdata.com/V");
            final BigdataURI W = f.createURI("http://www.bigdata.com/W");
            final BigdataURI X = f.createURI("http://www.bigdata.com/X");
            final BigdataURI Y = f.createURI("http://www.bigdata.com/Y");
            final BigdataURI Z = f.createURI("http://www.bigdata.com/Z");
            final BigdataURI sco = f.asValue(RDFS.SUBCLASSOF);
            
            // set the stage
            // U sco V and V sco X implies U sco X
            // let's pretend all three statements were originally in the
            // database, and { U sco X } is retracted, leaving only the other
            // two.
            // we should be able to create an adorned program based on our
            // normal inference rules using the "retracted" statement as the
            // query.
            
            store.addStatement(U, sco, V);
            store.addStatement(V, sco, W);
            store.addStatement(X, sco, Y);
            store.addStatement(Y, sco, Z);
            
            log.info("U: " + U.getTermId());
            log.info("V: " + V.getTermId());
            log.info("W: " + W.getTermId());
            log.info("X: " + X.getTermId());
            log.info("Y: " + Y.getTermId());
            log.info("Z: " + Z.getTermId());
            log.info("sco: " + sco.getTermId());
            
            // use the full closure program for this test
            BaseClosure closure = new FullClosure(store); //store.getClosureInstance();
            
            Program program = closure.getProgram(
                    store.getSPORelation().getNamespace(),
                    null
                    );
            
            IRule query = new Rule(
                    "the query",
                    null, // head
                    new SPOPredicate[] {
                        new SPOPredicate(
                            relations,
                            new Constant<Long>(U.getTermId()),
                            new Constant<Long>(sco.getTermId()),
                            new Constant<Long>(W.getTermId()))
                    },
                    QueryOptions.NONE,
                    null // constraints
                    );
            
            SPO spo = new SPO(U.getTermId(), sco.getTermId(), W.getTermId());
            
            testMagicSets(store, magicStore, program, query, spo);
                    
        } catch( Exception ex ) {
            
            ex.printStackTrace();
            
            throw new RuntimeException(ex);
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }
    
    /**
     * 
     */
    private void XXXtestJoin() {
        
        Properties properties = getProperties();
        
        // override the default axiom model.
        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        // override the default closure.
        properties.setProperty(Options.CLOSURE_CLASS, SimpleClosure.class.getName());
        
        // no closure on the data loader
        properties.setProperty(DataLoader.Options.CLOSURE, ClosureEnum.None.toString());
        
        AbstractTripleStore store = getStore(properties);
        
        final Properties tmp = store.getProperties();
        
        tmp.setProperty(AbstractTripleStore.Options.LEXICON, "false");

        final TempMagicStore magicStore = new TempMagicStore(
                store.getIndexManager().getTempStore(), tmp, store);
        
        final String[] relations = {
            store.getSPORelation().getNamespace(),
            magicStore.getSPORelation().getNamespace()
        };
        
        try {

            DataLoader loader = store.getDataLoader();

            // first add the LUBM ontology
            {
                InputStream is = TestIRIS.class.getResourceAsStream("univ-bench.owl");
                Reader reader = new BufferedReader(new InputStreamReader(is));
                loader.loadData(reader, LUBM.NS, RDFFormat.RDFXML);
            }
            
            // then process the LUBM sample data files one at a time
            InputStream is = getClass().getResourceAsStream("U1.zip");
            ZipInputStream zis = 
                new ZipInputStream(new BufferedInputStream(is));
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
                Reader reader = new InputStreamReader(
                    new ByteArrayInputStream(baos.toByteArray())
                    );
                loader.loadData(reader, LUBM.NS, RDFFormat.RDFXML);
            }
            zis.close();
            
            final long type = store.getTermId(RDF.TYPE);
            final long professor = store.getTermId(LUBM.PROFESSOR);
            final long worksFor = store.getTermId(LUBM.WORKS_FOR);
            final long d0u0 = store.getTermId(new URIImpl("http://www.Department0.University0.edu"));
            final long name = store.getTermId(LUBM.NAME);
            final long email = store.getTermId(LUBM.EMAIL_ADDRESS);
            final long phone = store.getTermId(LUBM.TELEPHONE);
            
            // use the full closure program for this test
            BaseClosure closure = new FullClosure(store); //store.getClosureInstance();
            
            Program program = closure.getProgram(
                    store.getSPORelation().getNamespace(),
                    null
                    );
            
            IRule query = new Rule(
                    "the query",
                    null, // head
                    new SPOPredicate[] {
                        new SPOPredicate(
                            relations,
                            Var.var("x"),
                            new Constant<Long>(type),
                            new Constant<Long>(professor)),
                        new SPOPredicate(
                            relations,
                            Var.var("x"),
                            new Constant<Long>(worksFor),
                            new Constant<Long>(d0u0)),
                        new SPOPredicate(
                            relations,
                            Var.var("x"),
                            new Constant<Long>(name),
                            Var.var("y1")),
                        new SPOPredicate(
                            relations,
                            Var.var("x"),
                            new Constant<Long>(email),
                            Var.var("y2")),
                        new SPOPredicate(
                            relations,
                            Var.var("x"),
                            new Constant<Long>(phone),
                            Var.var("y3")),
                    },
                    QueryOptions.NONE,
                    null // constraints
                    );
            
            testMagicSets(store, magicStore, program, query);
                    
        } catch( Exception ex ) {
            
            ex.printStackTrace();
            
            throw new RuntimeException(ex);
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }
    
    /**
     * 
     */
    public void testOwlSameAs() {
        
        Properties properties = getProperties();
        
        // use the owl axioms
        properties.setProperty(Options.AXIOMS_CLASS, OwlAxioms.class.getName());
        
        // use fast closure.
        properties.setProperty(Options.CLOSURE_CLASS, FastClosure.class.getName());
        
        // do not forward chain owl:sameAs properties, we'll calculate that later
        properties.setProperty(Options.FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES, "false");
        
        // no closure on the data loader
        properties.setProperty(DataLoader.Options.CLOSURE, ClosureEnum.None.toString());
        
        AbstractTripleStore store = getStore(properties);
        
        final Properties tmp = store.getProperties();
        
        tmp.setProperty(AbstractTripleStore.Options.LEXICON, "false");

        final TempMagicStore magicStore = new TempMagicStore(
                store.getIndexManager().getTempStore(), tmp, store);
        
        final String[] relations = {
            store.getSPORelation().getNamespace(),
            magicStore.getSPORelation().getNamespace()
        };
        
        try {

            DataLoader loader = store.getDataLoader();

            final URI A = new URIImpl("http://www.bigdata.com/A");
            final URI B = new URIImpl("http://www.bigdata.com/B");
//            final URI C = new URIImpl("http://www.bigdata.com/C");
//            final URI D = new URIImpl("http://www.bigdata.com/D");
//            final URI E = new URIImpl("http://www.bigdata.com/E");

//            final URI V = new URIImpl("http://www.bigdata.com/V");
            final URI W = new URIImpl("http://www.bigdata.com/W");
            final URI X = new URIImpl("http://www.bigdata.com/X");
            final URI Y = new URIImpl("http://www.bigdata.com/Y");
            final URI Z = new URIImpl("http://www.bigdata.com/Z");

            {
//                TMStatementBuffer buffer = new TMStatementBuffer
//                ( inf, 100/* capacity */, BufferEnum.AssertionBuffer
//                  );
                StatementBuffer buffer = new StatementBuffer
                    ( store, 100/* capacity */
                      );

                buffer.add(X, A, Z);
                buffer.add(Y, B, W);
                buffer.add(X, OWL.SAMEAS, Y);
                buffer.add(Z, OWL.SAMEAS, W);
                
                // write statements on the database.
                buffer.flush();
                
                // database at once closure.
                store.getInferenceEngine().computeClosure(null/*focusStore*/);

                // write on the store.
//                buffer.flush();
            }
            
            final long a = store.getTermId(A);
            final long b = store.getTermId(B);
//            final long c = noClosure.getTermId(C);
//            final long d = noClosure.getTermId(D);
//            final long e = noClosure.getTermId(E);
//            final long v = noClosure.getTermId(V);
            final long w = store.getTermId(W);
            final long x = store.getTermId(X);
            final long y = store.getTermId(Y);
            final long z = store.getTermId(Z);
            final long same = store.getTermId(OWL.SAMEAS);
            final long type = store.getTermId(RDF.TYPE);
            final long property = store.getTermId(RDF.PROPERTY);
            final long subpropof = store.getTermId(RDFS.SUBPROPERTYOF);
            
/*            
            // use the full closure program for this test
            BaseClosure closure = new FullClosure(store); //store.getClosureInstance();
            
            Program program = closure.getProgram(
                    store.getSPORelation().getNamespace(),
                    null
                    );
*/            
            final MappedProgram program = new MappedProgram("owlPropertiesClosure",
                    null, false/* parallel */, true/* closure */);
            final String ns = store.getSPORelation().getNamespace();
            final Vocabulary vocab = store.getVocabulary();
            program.addStep(new RuleOwlSameAs2(ns, vocab));
            program.addStep(new RuleOwlSameAs3(ns, vocab));

            IRule query = new Rule(
                    "the query",
                    null, // head
                    new SPOPredicate[] {
                        new SPOPredicate(
                            relations,
                            new Constant<Long>(y),
                            Var.var("p"),
                            Var.var("o"))
                    },
                    QueryOptions.NONE,
                    null // constraints
                    );
            
            testMagicSets(store, magicStore, program, query);
            
            log.info("a: " + a);
            log.info("b: " + b);
            log.info("w: " + w);
            log.info("x: " + x);
            log.info("y: " + y);
            log.info("z: " + z);
            log.info("same: " + same);
            log.info("type: " + type);
            log.info("property: " + property);
            log.info("subPropertyOf: " + subpropof);
                    
        } catch( Exception ex ) {
            
            ex.printStackTrace();
            
            throw new RuntimeException(ex);
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }
    
    private void testMagicSets(AbstractTripleStore store, 
            TempMagicStore magicStore, Program program, IRule query) 
            throws Exception {
        
        testMagicSets(store, magicStore, program, query, null);
        
    }
        
    private void testMagicSets(AbstractTripleStore store, 
            TempMagicStore magicStore, Program program, IRule query, SPO spo) 
            throws Exception {
        /*
        if (log.isInfoEnabled())
            log.info("database contents:\n"
                    + store.dumpStore(store,
                            true, true, true, true));
        */
        log.info("bigdata program before magic sets:");
        
        Iterator<IStep> steps = program.steps();
        while (steps.hasNext()) {
            IRule rule = (IRule) steps.next();
            log.info(rule);
        }

        // now we take the optimized set of rules and convert it back to a
        // bigdata program
        
        log.info("using magic sets to optimize program...");
        
        Program magicProgram = IRISUtils.magicSets(store, magicStore, 
                program, query);
        
        log.info("bigdata program after magic sets:");
        
        int stepCount = 0;
        steps = magicProgram.steps();
        while (steps.hasNext()) {
            com.bigdata.relation.rule.IRule rule =
                    (com.bigdata.relation.rule.IRule) steps.next();
            log.info(rule);
            stepCount++;
        }
        log.info("magic program has " + stepCount + " steps");

        // then we somehow run the magic program and see if the fact in
        // question exists in the resulting closure, if it does, then the
        // statement is supported by other facts in the database
        
        log.info("executing magic program...");
        
        ClosureStats stats = computeClosure(store, magicStore, magicProgram);
        
        log.info("done.");
        log.info("closure stats:\n"+stats);
        
        log.info("database contents\n"+
                store.dumpStore(store, true, true, false).toString());
        log.info("focus store contents\n"+
                magicStore.dumpStore(store, true, true, true, false, true, 
                        SPOKeyOrder.SPO).toString());
         
        if (spo == null) {  // query
            
            log.info("running query...");
            
            IChunkedOrderedIterator<ISolution> solutions = 
                runQuery(store, query);
            
            int i = 0;
            while (solutions.hasNext()) {
                ISolution solution = solutions.next();
                IBindingSet b = solution.getBindingSet();
                log.info("solution #" + (i++) + b);
            }
            
        } else {  // point test
            
            log.info("doing point test...");
            
            final boolean a = store.hasStatement(spo.s, spo.p, spo.o);
            final boolean b = magicStore.hasStatement(spo.s, spo.p, spo.o);
            
            if (a && b) {
                log.info("found statement in both store and magic store.");
            } else if (a) {
                log.info("found statement in store but not in magic store.");
            } else if (b) {
                log.info("found statement in magic store but not in store.");
            } else {
                log.info("statement not found.");
            }
            
        }
        
    }

    public ClosureStats computeClosure(
            AbstractTripleStore database, AbstractTripleStore focusStore, 
            Program program) {

        final boolean justify = false;
        
        final DoNotAddFilter doNotAddFilter = new DoNotAddFilter(
                database.getVocabulary(), database.getAxioms(), 
                true /*forwardChainRdfTypeRdfsResource*/);
        
        try {

            final long begin = System.currentTimeMillis();

            /*
             * FIXME remove IJoinNexus.RULE once we we can generate the
             * justifications from just the bindings and no longer need the rule
             * to generate the justifications (esp. for scale-out).
             */
            final int solutionFlags = IJoinNexus.ELEMENT//
                    | (justify ? IJoinNexus.RULE | IJoinNexus.BINDINGS : 0)//
//                  | IJoinNexus.RULE  // iff debugging.
                  ;
          
            final RuleContextEnum ruleContext = focusStore == null
                ? RuleContextEnum.DatabaseAtOnceClosure
                : RuleContextEnum.TruthMaintenance
                ;
            
            final IJoinNexusFactory joinNexusFactory = database
                    .newJoinNexusFactory(ruleContext, ActionEnum.Insert,
                            solutionFlags, doNotAddFilter, justify,
                            false/* backchain */,
                            DefaultEvaluationPlanFactory2.INSTANCE);

            final IJoinNexus joinNexus = joinNexusFactory.newInstance(database
                    .getIndexManager());

            final long mutationCount = joinNexus.runMutation(program);

            final long elapsed = System.currentTimeMillis() - begin;

            return new ClosureStats(mutationCount, elapsed);

        } catch (Exception ex) {

            throw new RuntimeException(ex);
            
        }
        
    }
    
    /**
     * Run a rule based on a {@link TupleExpr} as a query.
     * 
     * @param rule
     *            The rule to execute.
     * 
     * @return The Sesame 2 iteration that visits the {@link BindingSet}s that
     *         are the results for the query.
     * 
     * @throws QueryEvaluationException
     */
    public IChunkedOrderedIterator<ISolution> runQuery(
            final AbstractTripleStore database,
            final IRule rule)
            throws QueryEvaluationException {

        log.info(rule);
        
        // run the query as a native rule.
        final IChunkedOrderedIterator<ISolution> itr1;
        try {

            final IEvaluationPlanFactory planFactory = 
                DefaultEvaluationPlanFactory2.INSTANCE;

            final IJoinNexusFactory joinNexusFactory = database
                    .newJoinNexusFactory(RuleContextEnum.HighLevelQuery,
                            ActionEnum.Query, IJoinNexus.BINDINGS,
                            null, // filter
                            false, // justify 
                            false, // backchain
                            planFactory//
                            );

            final IJoinNexus joinNexus = joinNexusFactory.newInstance(database
                    .getIndexManager());
    
            itr1 = joinNexus.runQuery(rule);

        } catch (Exception ex) {
            
            throw new QueryEvaluationException(ex);
            
        }
        
        return itr1;
        
    }
    
}
