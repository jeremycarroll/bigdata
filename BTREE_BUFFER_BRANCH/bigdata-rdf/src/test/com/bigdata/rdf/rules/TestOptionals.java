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
 * Created on March 11, 2008
 */

package com.bigdata.rdf.rules;

import java.util.HashMap;
import java.util.Map;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import com.bigdata.rdf.inf.BackchainOwlSameAsPropertiesIterator;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.Var;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlanFactory2;
import com.bigdata.relation.rule.eval.IEvaluationPlan;
import com.bigdata.relation.rule.eval.IEvaluationPlanFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestOptionals extends AbstractInferenceEngineTestCase {

    /**
     * 
     */
    public TestOptionals() {
        super();
    }

    /**
     * @param name
     */
    public TestOptionals(String name) {
        super(name);
    }

    /**
     * Test the various access paths for backchaining the property collection
     * normally done through owl:sameAs {2,3}.
     */
    public void test_optionals() 
    {
     
        // store with no owl:sameAs closure
        AbstractTripleStore db = getStore();
        
        try {

            Map<Value, Long> termIds = new HashMap<Value, Long>();
            
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

            final Literal foo = new LiteralImpl("foo");
            final Literal bar = new LiteralImpl("bar");

            {
                StatementBuffer buffer = new StatementBuffer
                    ( db, 100/* capacity */
                      );

                buffer.add(A, RDF.TYPE, X);
                buffer.add(A, RDFS.LABEL, foo);
                buffer.add(A, RDFS.COMMENT, bar);
                buffer.add(B, RDF.TYPE, X);
                // buffer.add(B, RDFS.LABEL, foo);
                buffer.add(B, RDFS.COMMENT, bar);
                
                // write statements on the database.
                buffer.flush();
                
                // database at once closure.
                db.getInferenceEngine().computeClosure(null/*focusStore*/);

                // write on the store.
//                buffer.flush();
            }
            
            final long a = db.getTermId(A); termIds.put(A, a);
            final long b = db.getTermId(B); termIds.put(B, b);
//            final long c = noClosure.getTermId(C);
//            final long d = noClosure.getTermId(D);
//            final long e = noClosure.getTermId(E);
//            final long v = noClosure.getTermId(V);
            // final long w = db.getTermId(W); termIds.put(W, w);
            final long x = db.getTermId(X); termIds.put(X, x);
            // final long y = db.getTermId(Y); termIds.put(Y, y);
            // final long z = db.getTermId(Z); termIds.put(Z, z);
            // final long SAMEAS = db.getTermId(OWL.SAMEAS); termIds.put(OWL.SAMEAS, SAMEAS);
            termIds.put(foo, db.getTermId(foo));
            termIds.put(bar, db.getTermId(bar));
            final long TYPE = db.getTermId(RDF.TYPE); termIds.put(RDF.TYPE, TYPE);
            final long LABEL = db.getTermId(RDFS.LABEL); termIds.put(RDFS.LABEL, LABEL);
            final long COMMENT = db.getTermId(RDFS.COMMENT); termIds.put(RDFS.COMMENT, COMMENT);
            final long RESOURCE = db.getTermId(RDFS.RESOURCE); termIds.put(RDFS.RESOURCE, RESOURCE);
            
            if (log.isInfoEnabled())
                log.info("\n" +db.dumpStore(true, true, false));
  
            for (Map.Entry<Value, Long> e : termIds.entrySet()) {
                System.err.println(e.getKey() + " = " + e.getValue());
            }
            
            { // works great
                
                final String SPO = db.getSPORelation().getNamespace();
                final IVariableOrConstant<Long> s = Var.var("s");
                final IVariableOrConstant<Long> type = new Constant<Long>(TYPE);
                final IVariableOrConstant<Long> t = new Constant<Long>(x);
                final IVariableOrConstant<Long> label = new Constant<Long>(LABEL);
                final IVariableOrConstant<Long> comment = new Constant<Long>(COMMENT);
                final IVariableOrConstant<Long> l = Var.var("l");
                final IVariableOrConstant<Long> c = Var.var("c");
                final IRule rule =
                        new Rule("test_optional", null, // head
                                new IPredicate[] {
                                        new SPOPredicate(SPO, s, type, t),
                                        new SPOPredicate(SPO, s, comment, c),
                                        new SPOPredicate(SPO, s, label, l, true) },
                                // constraints on the rule.
                                null
                                );
                
                try {
                
                    int numSolutions = 0;
                    
                    IChunkedOrderedIterator<ISolution> solutions = runQuery(db, rule);
                    
                    while (solutions.hasNext()) {
                        
                        ISolution solution = solutions.next();
                        
                        System.err.println(solution);
                        
                        numSolutions++;
                        
                    }
                    
                    assertTrue("wrong # of solutions", numSolutions == 2);
                    
                } catch(Exception ex) {
                    
                    ex.printStackTrace();
                    
                }
            }

            { // does not work, only difference is the order the heads are
              // presented in the rule definition.  Note that the plan still gets
              // the execution order correct.  (See output from line 429, should
              // be [2,0,1], and it is.
            
                final String SPO = db.getSPORelation().getNamespace();
                final IVariableOrConstant<Long> s = Var.var("s");
                final IVariableOrConstant<Long> type = new Constant<Long>(TYPE);
                final IVariableOrConstant<Long> t = new Constant<Long>(x);
                final IVariableOrConstant<Long> label = new Constant<Long>(LABEL);
                final IVariableOrConstant<Long> comment = new Constant<Long>(COMMENT);
                final IVariableOrConstant<Long> l = Var.var("l");
                final IVariableOrConstant<Long> c = Var.var("c");
                final IRule rule =
                        new Rule("test_optional", null, // head
                                new IPredicate[] {
                                        new SPOPredicate(SPO, s, type, t),
                                        new SPOPredicate(SPO, s, label, l, true),
                                        new SPOPredicate(SPO, s, comment, c) },
                                // constraints on the rule.
                                null
                                );
                
                try {
                
                    int numSolutions = 0;
                    
                    IChunkedOrderedIterator<ISolution> solutions = runQuery(db, rule);
                    
                    while (solutions.hasNext()) {
                        
                        ISolution solution = solutions.next();
                        
                        System.err.println(solution);
                        
                        numSolutions++;
                        
                    }
                    
                    assertTrue("wrong # of solutions", numSolutions == 2);
                    
                } catch(Exception ex) {
                    
                    ex.printStackTrace();
                    
                }
            }

            { // two optionals does not work either
            
                final String SPO = db.getSPORelation().getNamespace();
                final IVariableOrConstant<Long> s = Var.var("s");
                final IVariableOrConstant<Long> type = new Constant<Long>(TYPE);
                final IVariableOrConstant<Long> t = new Constant<Long>(x);
                final IVariableOrConstant<Long> label = new Constant<Long>(LABEL);
                final IVariableOrConstant<Long> comment = new Constant<Long>(COMMENT);
                final IVariableOrConstant<Long> l = Var.var("l");
                final IVariableOrConstant<Long> c = Var.var("c");
                final IRule rule =
                        new Rule("test_optional", null, // head
                                new IPredicate[] {
                                        new SPOPredicate(SPO, s, type, t),
                                        new SPOPredicate(SPO, s, label, l, true),
                                        new SPOPredicate(SPO, s, comment, c, true) },
                                // constraints on the rule.
                                null
                                );
                
                try {
                
                    int numSolutions = 0;
                    
                    IChunkedOrderedIterator<ISolution> solutions = runQuery(db, rule);
                    
                    while (solutions.hasNext()) {
                        
                        ISolution solution = solutions.next();
                        
                        System.err.println(solution);
                        
                        numSolutions++;
                        
                    }
                    
                    assertTrue("wrong # of solutions", numSolutions == 2);
                    
                } catch(Exception ex) {
                    
                    ex.printStackTrace();
                    
                }
            }

        } finally {
            
            db.__tearDownUnitTest();
            
        }
        
    }

    private void __test_owlsameas() 
    {
     
        // store with no owl:sameAs closure
        AbstractTripleStore db = getStore();
        
        try {

            Map<Value, Long> termIds = new HashMap<Value, Long>();
            
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
                StatementBuffer buffer = new StatementBuffer
                    ( db, 100/* capacity */
                      );

                buffer.add(X, A, Z);
                buffer.add(Y, B, W);
                buffer.add(X, OWL.SAMEAS, Y);
                buffer.add(Z, OWL.SAMEAS, W);
                
                // write statements on the database.
                buffer.flush();
                
                // database at once closure.
                db.getInferenceEngine().computeClosure(null/*focusStore*/);

                // write on the store.
//                buffer.flush();
            }
            
            final long a = db.getTermId(A); termIds.put(A, a);
            final long b = db.getTermId(B); termIds.put(B, b);
//            final long c = noClosure.getTermId(C);
//            final long d = noClosure.getTermId(D);
//            final long e = noClosure.getTermId(E);
//            final long v = noClosure.getTermId(V);
            final long w = db.getTermId(W); termIds.put(W, w);
            final long x = db.getTermId(X); termIds.put(X, x);
            final long y = db.getTermId(Y); termIds.put(Y, y);
            final long z = db.getTermId(Z); termIds.put(Z, z);
            final long sameAs = db.getTermId(OWL.SAMEAS); termIds.put(OWL.SAMEAS, sameAs);
            final long type = db.getTermId(RDF.TYPE); termIds.put(RDF.TYPE, type);
            final long property = db.getTermId(RDF.PROPERTY); termIds.put(RDF.PROPERTY, property);
            final long subpropof = db.getTermId(RDFS.SUBPROPERTYOF); termIds.put(RDFS.SUBPROPERTYOF, subpropof);
            
            SPO[] dbGroundTruth = new SPO[]{
                    new SPO(x,a,z,
                            StatementEnum.Explicit),
                    new SPO(y,b,w,
                            StatementEnum.Explicit),
                    new SPO(x,sameAs,y,
                            StatementEnum.Explicit),
                    new SPO(z,sameAs,w,
                            StatementEnum.Explicit),
                    // forward closure
                    new SPO(a,type,property,
                            StatementEnum.Inferred),
                    new SPO(a,subpropof,a,
                            StatementEnum.Inferred),
                    new SPO(b,type,property,
                            StatementEnum.Inferred),
                    new SPO(b,subpropof,b,
                            StatementEnum.Inferred),
                    new SPO(w,sameAs,z,
                            StatementEnum.Inferred),
                    new SPO(y,sameAs,x,
                            StatementEnum.Inferred),
                    // backward chaining        
                    new SPO(x,a,w,
                            StatementEnum.Inferred),
                    new SPO(x,b,z,
                            StatementEnum.Inferred),
                    new SPO(x,b,w,
                            StatementEnum.Inferred),
                    new SPO(y,a,z,
                            StatementEnum.Inferred),
                    new SPO(y,a,w,
                            StatementEnum.Inferred),
                    new SPO(y,b,z,
                            StatementEnum.Inferred),
                };

            
            if (log.isInfoEnabled())
                log.info("\n" +db.dumpStore(true, true, false));
  
            for (Map.Entry<Value, Long> e : termIds.entrySet()) {
                System.err.println(e.getKey() + " = " + e.getValue());
            }
            
            { // test S
            
                final String SPO = db.getSPORelation().getNamespace();
                final IVariableOrConstant<Long> S = new Constant<Long>(x);
                final IVariableOrConstant<Long> P = Var.var("p");
                final IVariableOrConstant<Long> O = Var.var("o");
                final IVariableOrConstant<Long> SAMES = Var.var("sameS");
                final IVariableOrConstant<Long> SAMEO = Var.var("sameO");
                final IVariableOrConstant<Long> SAMEAS = new Constant<Long>(sameAs);
                final IRule rule =
                        new Rule("sameas", new SPOPredicate(SPO, S, P, SAMEO), // head
                                new IPredicate[] {
                                        new SPOPredicate(SPO, SAMES, SAMEAS, S),
                                        new SPOPredicate(SPO, SAMEO, SAMEAS, O, false),
                                        new SPOPredicate(SPO, SAMES, P, SAMEO) },
                                // constraints on the rule.
                                null
                                );
                
                try {
                
                    IChunkedOrderedIterator<ISolution> solutions = runQuery(db, rule);
                    
                    while (solutions.hasNext()) {
                        
                        ISolution solution = solutions.next();
                        
                        System.err.println(solution);
                        
                    }
                    
                } catch(Exception ex) {
                    
                    ex.printStackTrace();
                    
                }
            }

        } finally {
            
            db.__tearDownUnitTest();
            
        }
        
    }

    private IChunkedOrderedIterator<ISolution> runQuery(AbstractTripleStore db, IRule rule)
        throws Exception {
        // run the query as a native rule.
        final IEvaluationPlanFactory planFactory =
                DefaultEvaluationPlanFactory2.INSTANCE;
        final IJoinNexusFactory joinNexusFactory =
                db.newJoinNexusFactory(RuleContextEnum.HighLevelQuery,
                        ActionEnum.Query, IJoinNexus.BINDINGS, null, // filter
                        false, // justify 
                        false, // backchain
                        planFactory);
        final IJoinNexus joinNexus =
                joinNexusFactory.newInstance(db.getIndexManager());
        final IEvaluationPlan plan = planFactory.newPlan(joinNexus, rule);
        StringBuilder sb = new StringBuilder();
        int order[] = plan.getOrder();
        for (int i = 0; i < order.length; i++) {
            sb.append(order[i]);
            if (i < order.length-1) {
                sb.append(",");
            }
        }
        System.err.println("order: [" + sb.toString() + "]");
        IChunkedOrderedIterator<ISolution> solutions = joinNexus.runQuery(rule);
        return solutions;
    }

}
