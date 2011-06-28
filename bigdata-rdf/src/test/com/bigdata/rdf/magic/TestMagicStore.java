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

import java.util.Collection;
import java.util.Properties;

import org.deri.iris.api.factory.IBasicFactory;
import org.deri.iris.api.factory.IBuiltinsFactory;
import org.deri.iris.api.factory.ITermFactory;
import org.deri.iris.basics.BasicFactory;
import org.deri.iris.builtins.BuiltinsFactory;
import org.deri.iris.terms.TermFactory;

import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.MockTermIdFactory;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.rules.AbstractInferenceEngineTestCase;
import com.bigdata.rdf.rules.RuleRdfs11;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.relation.accesspath.IAccessPath;
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
public class TestMagicStore extends AbstractInferenceEngineTestCase {

    final IBasicFactory BASIC = BasicFactory.getInstance();
    
    final ITermFactory TERM = TermFactory.getInstance();
    
    final IBuiltinsFactory BUILTINS = BuiltinsFactory.getInstance();
    
    final org.deri.iris.api.basics.IPredicate EQUAL = BASIC.createPredicate( "EQUAL", 2 );
    
    final org.deri.iris.api.basics.IPredicate NOT_EQUAL = BASIC.createPredicate( "NOT_EQUAL", 2 );
    
    final org.deri.iris.api.basics.IPredicate TRIPLE = BASIC.createPredicate("triple", 3);

    /**
     * Factory for mock {@link MagicTuple}s based on mock {@link TermId}s.
     * @param terms
     * @return
     */
    static private MagicTuple newMagicTuple(MockTermIdFactory f, int... terms) {
        IV[] ivs = new IV[terms.length];
        for (int i = 0; i < terms.length; i++) {
            ivs[i] = f.newTermId(VTE.URI, terms[i]);
        }
//        this.terms = ivs;
        return new MagicTuple(ivs);
    }

    /**
     * 
     */
    public TestMagicStore() {
        super();
    }

    /**
     * @param name
     */
    public TestMagicStore(String name) {
        super(name);
    }

    private MockTermIdFactory f;
    
    protected void setUp() throws Exception {
        super.setUp();
        f = new MockTermIdFactory();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        f = null;
    }

    public void testMagicStore() {
        
        final Properties properties = getProperties();
        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
        properties.setProperty(Options.CLOSURE_CLASS, SimpleClosure.class.getName());
        
        final AbstractTripleStore store = getStore(properties);

        final Properties tmp = store.getProperties();
        tmp.setProperty(AbstractTripleStore.Options.LEXICON, "false");
        tmp.setProperty(AbstractTripleStore.Options.ONE_ACCESS_PATH, "true");

        final TempMagicStore tempStore = new TempMagicStore(
                store.getIndexManager().getTempStore(), tmp, store);

        try {

            // create this: magic_xXx_prefix_xXx_triple_bbb('96', '44', '108')
            // and then run some queries for it. simple enough.  :-)
            
            final String symbol = "magic_xXx_prefix_xXx_triple_bbb";
            final int arity = 3;
            final IMagicTuple tuple = newMagicTuple(f, 96, 44, 108);
            tempStore.createRelation(symbol, arity);
            MagicRelation relation = tempStore.getMagicRelation(symbol);
            tempStore.createRelation("label_xXx_prefix_xXx_triple_1_fbb", 2);
            Collection<String> symbols = tempStore.getMagicSymbols();
            for (String s : symbols) {
                System.out.println(s);
            }
            relation.insert(new IMagicTuple[] { tuple }, 1);
            
            IVariableOrConstant<IV>[] terms = new IVariableOrConstant[arity];
            terms[0] = Var.var("a");
            terms[1] = Var.var("b");
            terms[2] = Var.var("c");
            IPredicate<IMagicTuple> predicate = 
//                new MagicPredicate(relation.getNamespace(), terms);
            new MagicPredicate(terms, NV.asMap(new NV[] { new NV(
                    IPredicate.Annotations.RELATION_NAME, new String[]{relation
                            .getNamespace()}) }));
            IAccessPath<IMagicTuple> accessPath = 
                relation.getAccessPath(predicate);
            String fqn = relation.getFQN(accessPath.getKeyOrder());
            System.err.println("fqn: " + fqn);
            IChunkedOrderedIterator<IMagicTuple> itr = accessPath.iterator();
            while (itr.hasNext()) {
                IMagicTuple next = itr.next();
                System.err.println(next);
            }
            
            tempStore.destroy();
            
        } catch( Exception ex ) {
            
            throw new RuntimeException(ex);
            
        } finally {
            
            store.__tearDownUnitTest();
            
        }
        
    }
    
    public void testQuadStore() {
        
        final Properties properties = getProperties();
        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
        properties.setProperty(Options.CLOSURE_CLASS, SimpleClosure.class.getName());
        
        final AbstractTripleStore store = getStore(properties);

        final Properties tmp = store.getProperties();
        tmp.setProperty(AbstractTripleStore.Options.LEXICON, "false");
        tmp.setProperty(AbstractTripleStore.Options.ONE_ACCESS_PATH, "true");

        final TempMagicStore tempStore = new TempMagicStore(
                store.getIndexManager().getTempStore(), tmp, store);

        try {

            final String symbol = "quad";
            final int arity = 4;
            tempStore.createRelation(symbol, arity);
            MagicRelation relation = tempStore.getMagicRelation(symbol);
            Collection<String> symbols = tempStore.getMagicSymbols();
            for (String s : symbols) {
                System.out.println(s);
            }

            final IMagicTuple[] tuples = {
                newMagicTuple(f, 96, 44, 108, 31),
                newMagicTuple(f, 14, 44, 82, 31),
                newMagicTuple(f, 14, 73, 65, 31),
                newMagicTuple(f, 50, 10, 65, 49),
                newMagicTuple(f, 36, 13, 18, 49)
            };
                
            relation.insert(tuples, tuples.length);
            
            IVariableOrConstant<IV>[] terms = new IVariableOrConstant[arity];
            terms[0] = Var.var("a");
            terms[1] = Var.var("b");
            terms[2] = Var.var("c");
            terms[3] = Var.var("d");
            
            IPredicate<IMagicTuple> predicate = 
//                new MagicPredicate(relation.getNamespace(), terms);
            new MagicPredicate(terms, NV.asMap(new NV[] { new NV(
                    IPredicate.Annotations.RELATION_NAME,
                    new String[] { relation.getNamespace() }) }));
            IAccessPath<IMagicTuple> accessPath = 
                relation.getAccessPath(predicate);
            String fqn = relation.getFQN(accessPath.getKeyOrder());
            System.err.println("fqn: " + fqn);
            IChunkedOrderedIterator<IMagicTuple> itr = accessPath.iterator();
            while (itr.hasNext()) {
                IMagicTuple next = itr.next();
                System.err.println(next);
            }
            
            tempStore.destroy();
            
        } catch( Exception ex ) {
            
            throw new RuntimeException(ex);
            
        } finally {
            
            store.__tearDownUnitTest();
            
        }
        
    }
    
}
