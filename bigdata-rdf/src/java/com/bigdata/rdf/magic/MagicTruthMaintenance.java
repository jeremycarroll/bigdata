
package com.bigdata.rdf.magic;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.inf.SPOAssertionBuffer;
import com.bigdata.rdf.inf.TruthMaintenance;
import com.bigdata.rdf.rules.BaseClosure;
import com.bigdata.rdf.rules.DoNotAddFilter;
import com.bigdata.rdf.rules.FullClosure;
import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.rules.RuleContextEnum;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.Program;
import com.bigdata.relation.rule.QueryOptions;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlanFactory2;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id
 */
public class MagicTruthMaintenance extends TruthMaintenance {

    /**
     * Logger.
     */
    final static protected Logger log = Logger.getLogger(MagicTruthMaintenance.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.isDebugEnabled();
    
    public TempMagicStore newTempMagicStore() {

        final Properties properties = database.getProperties();

        // no justifications please
        properties.setProperty(
                com.bigdata.rdf.store.AbstractTripleStore.Options.JUSTIFY, 
                "false");

        // turn off the lexicon since we will only use the statement indices.
        properties.setProperty(
                com.bigdata.rdf.store.AbstractTripleStore.Options.LEXICON,
                "false");

        // no bloom filter, maybe yes bloom filter though?
        properties.setProperty(
                com.bigdata.rdf.store.AbstractTripleStore.Options.BLOOM_FILTER,
                "false");
        
        final TempMagicStore tempStore = new TempMagicStore(
                database.getIndexManager().getTempStore(), properties, 
                database);
        
        return tempStore;
        
    }

    /**
     * 
     * @param inferenceEngine
     *            The inference engine for the database.
     */
    public MagicTruthMaintenance(InferenceEngine inferenceEngine) {

        super(inferenceEngine);
        
    }

    /**
     * Perform truth maintenance for statement retraction using magic sets.
     * <p>
     * 1. gather all statements about statements in the temp store into the temp store
     * 2. fixed point the temp store against the database and itself, gathering
     * all downstream inferences into the temp store
     * 3. delete everything in the temp store from the database
     * 4. for each explicit non-sid statement in the temp store, do a magic sets query
     * against the database to see if it reappears in the magic store.  if it
     * does, then it's provable.  if it's provable, then add it to a new focus
     * store, the re-add store.  also add any statements about this statement into the re-add store
     * 5. run closure on the re-add store
     * 6. add everything from the re-add store back into the database
     * 
     * @param tempStore
     *            A temporary store containing explicit statements to be
     *            retracted from the database. The tempStore will be closed and
     *            as a post-condition.
     * 
     * @return statistics about the closure operation.
     */
    public ClosureStats retractAll(final TempTripleStore tempStore) {

        final long begin = System.currentTimeMillis();
        
        final ClosureStats stats = new ClosureStats();
        
        if (tempStore == null) {
            
            throw new IllegalArgumentException();
            
        }

        // #of given statements to retract.
        final long ngiven = tempStore.getStatementCount();
        
        if (ngiven == 0) {
            
            // nothing to retract.
            
            return stats;
            
        }
        
        if(INFO) log.info("Computing closure of the temporary store with "
                + ngiven+ " statements");
        
        /*
         * 1. gather all statements about statements in the temp store into the temp store
         */

        if(database.getStatementIdentifiers()) {
            
            AbstractTripleStore.fixPointStatementIdentifiers(database, tempStore);

            if(INFO) log.info("Computing closure of the temporary store with " + ngiven
                    + " statements (after fix point of statement identifiers)");
            
        }

        /*
         * 2. fixed point the temp store against the database and itself, gathering
         * all downstream inferences into the temp store
         */
        {
            
        final boolean justify = false;
        
        stats.add(inferenceEngine.computeClosure(tempStore, justify));
        
        }
        
        /*
         * 3. delete everything in the temp store from the database
         */
        {
            
        final IChunkedOrderedIterator<ISPO> itr = tempStore.getAccessPath(
                SPOKeyOrder.SPO,ExplicitSPOFilter.INSTANCE).iterator();
        
        final boolean computeClosureForStatementIdentifiers = false;
        
        long n = database.removeStatements(itr, computeClosureForStatementIdentifiers);
        
        if (INFO) log.info("removed " + n + " statements from the database");

        }
        
        /*
         * 4. for each explicit non-sid statement in the temp store, do a magic sets query
         * against the database to see if it reappears in the magic store.  if it
         * does, then it's provable.  if it's provable, then add it to a new focus
         * store, the re-add store.  also add any statements about this statement into the re-add store
         */
        final TempTripleStore reAddStore = newTempTripleStore();
        
        final long focusStoreSize = tempStore.getStatementCount();
        
        final int capacity = (int) Math.min(focusStoreSize, 1000000);
        
        final SPOAssertionBuffer reAddBuffer = new SPOAssertionBuffer(
                reAddStore, database, null/* filter */, capacity, false/* justified */);
        
        {
            
        final IChunkedOrderedIterator<ISPO> itr = tempStore.getAccessPath(
                SPOKeyOrder.SPO,ExplicitSPOFilter.INSTANCE).iterator();

        while (itr.hasNext()) {
            
            ISPO spo = itr.next();
            
            final long s = spo.s();
            
            final long p = spo.p();
            
            final long o = spo.o();
            
            TempMagicStore magicStore = newTempMagicStore();
            
            BaseClosure closure = new FullClosure(database);
            
            Program program = closure.getProgram(
                    database.getSPORelation().getNamespace(),
                    null
                    );
            
            IRule query = new Rule(
                    "magic",
                    null, // head
                    new SPOPredicate[] {
                        new SPOPredicate(
                            new String[] {
                                database.getSPORelation().getNamespace(),
                                magicStore.getSPORelation().getNamespace()
                            },
                            new Constant<Long>(spo.s()),
                            new Constant<Long>(spo.p()),
                            new Constant<Long>(spo.o()))
                    },
                    QueryOptions.NONE,
                    null // constraints
                    );

            Program magicProgram =  
                IRISUtils.magicSets(database, magicStore, program, query);

            computeClosure(database, magicStore, magicProgram);
            
            if (!magicStore.getAccessPath(s, p, o).isEmpty()) {
                
                reAddBuffer.add(spo);
                
            }
            
            magicStore.close();
            
        }
        
        long n = reAddBuffer.flush();
        
        if (INFO) log.info("put " + n + " statements back into the re-add buffer");
        
        AbstractTripleStore.fixPointStatementIdentifiers(tempStore, reAddStore);
        
        }
        
        /*
         * 5. run closure on the re-add store
         */
        {
        
        final boolean justify = false;
            
        stats.add(inferenceEngine.computeClosure(reAddStore, justify));
            
        }
        
        /*
         * 6. add everything from the re-add store back into the database
         */
        {
            
        final IChunkedOrderedIterator<ISPO> itr = tempStore.getAccessPath(
                SPOKeyOrder.SPO,ExplicitSPOFilter.INSTANCE).iterator();
        
        long n = database.addStatements(itr, null);
        
        if (INFO) log.info("added " + n + " statements to the database");
            
            
        }
        
        tempStore.close();
        
        assert ! tempStore.isOpen();
       
        final long elapsed = System.currentTimeMillis() - begin;
        
        if(INFO) log.info("Retracted " + ngiven
                + " given and updated closure on the database in " + elapsed
                + " ms");
        
        return stats;
        
    }
    
    private ClosureStats computeClosure(
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

}
