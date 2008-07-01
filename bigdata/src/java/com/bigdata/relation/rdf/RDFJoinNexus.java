/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 25, 2008
 */

package com.bigdata.relation.rdf;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.IRelationFactory;
import com.bigdata.relation.IRelationLocator;
import com.bigdata.relation.IRelationName;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.rule.ArrayBindingSet;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.relation.rule.eval.AbstractSolutionBuffer;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.EmptyProgramTask;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IProgramTask;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.LocalProgramTask;
import com.bigdata.relation.rule.eval.ProgramUtility;
import com.bigdata.relation.rule.eval.Solution;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.DataService;
import com.bigdata.service.EmbeddedFederation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IClientIndex;
import com.bigdata.service.LocalDataServiceFederation;
import com.bigdata.service.LocalDataServiceFederation.LocalDataServiceImpl;

/**
 * {@link IProgram} execution support for the RDF DB.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo handle the lexicon as well.
 * 
 * FIXME Additional metadata is required in order to know what kind of relation
 * to instantiate given the namespace of the relation. For RDF we will have at
 * least two relations (the lexicon and the triples, and possible the full text
 * index - unless that is understood as a secondary index for the lexicon for
 * maintained from the lexicon by triggers).
 * <p>
 * It seems best to explicitly declare the {@link IRelationName} to
 * {@link IRelationLocator} mapping for now and to have the
 * {@link IRelationLocator} know how to create the relation instance. However,
 * that makes it difficult to locate relations that are part of the federation.
 * Maybe an {@link IRelationName} hierarchy would help here with types
 * corresponding to {@link TemporaryStore}, local {@link Journal},
 * {@link IBigdataFederation}, {@link AbstractTask}, and fused view names. A
 * match is then routed based on the type of the name. For local {@link Journal}
 * and {@link TemporaryStore} it would succeed iff there is such a resource w/o
 * serialization. At that point the name is nearly a locator for a specific
 * relation in itself.
 * <p>
 * The {@link RDFJoinNexusFactory} needs to install the appropriate locators
 * when creating a new instance, e.g., after sending a {@link LocalProgramTask}
 * to a remote host.
 * 
 * @todo store the {@link IRelationFactory} in the {@link IndexMetadata}? This
 *       makes it possible to inspect an index and then apply the factory to
 *       consitute the appropriate {@link IRelation} class.
 */
public class RDFJoinNexus implements IJoinNexus {

    protected static Logger log = Logger.getLogger(RDFJoinNexus.class);
    
    private final ExecutorService service;
    
//    private final IIndexManager indexManager;
        
    private final IRelationLocator relationLocator;
    
    private final long timestamp;
    
    private final boolean elementOnly;

    /**
     * 
     * @param service
     *            The service used to executor tasks.
     * @param relationLocator
     *            The object used to resolve {@link IRelation}.
     * @param timestamp
     *            The timestamp of the relation view(s).
     * @param elementOnly
     *            <code>true</code> if only the entailed element should be
     *            materialized in the computed {@link ISolution}s when the
     *            program is executed and <code>false</code> if the
     *            {@link IRule} and {@link IBindingSet} should be materialized
     *            as well.
     */
    public RDFJoinNexus(ExecutorService service,
            IRelationLocator relationLocator, long timestamp,
            boolean elementOnly) {

        if (relationLocator == null)
            throw new IllegalArgumentException();

        if (service == null)
            throw new IllegalArgumentException();

        this.relationLocator = relationLocator;
        
        this.service = service;
        
        this.timestamp = timestamp;

        this.elementOnly = elementOnly;
        
    }

    public long getTimestamp() {
        
        return timestamp;
        
    }
    
    public ExecutorService getExecutorService() {
        
        return service;
        
    }
    
    @SuppressWarnings("unchecked")
    public void copyValues(Object e, IPredicate predicate, IBindingSet bindingSet ) {

        if (e == null)
            throw new IllegalArgumentException();
        
        if (predicate == null)
            throw new IllegalArgumentException();
        
        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final SPO spo = (SPO) e;
        
        final IPredicate<ISPO> pred = (IPredicate<ISPO>)predicate;
        
        {

            final IVariableOrConstant<Long> t = pred.get(0);
            
            if(t.isVar()) {

                bindingSet.set((IVariable<Long>) t, new Constant<Long>(spo.s));
                
            }

        }

        {

            final IVariableOrConstant<Long> t = pred.get(1);
            
            if(t.isVar()) {

                bindingSet.set((IVariable<Long>) t, new Constant<Long>(spo.p));
                
            }

        }

        {

            final IVariableOrConstant<Long> t = pred.get(2);
            
            if(t.isVar()) {

                bindingSet.set((IVariable<Long>) t, new Constant<Long>(spo.o));
                
            }

        }
        
    }

    @SuppressWarnings("unchecked")
    public SPO newElement(IPredicate predicate, IBindingSet bindingSet) {

        if (predicate == null)
            throw new IllegalArgumentException();
        
        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final IPredicate<ISPO> pred = (IPredicate<ISPO>) predicate;

        final long s = asBound(pred, 0, bindingSet);

        final long p = asBound(pred, 1, bindingSet);

        final long o = asBound(pred, 2, bindingSet);

        final SPO spo = new SPO(s, p, o, StatementEnum.Inferred);
        
        if(log.isDebugEnabled()) {
            
            log.debug(spo.toString());
            
        }
        
        return spo;
        
    }

    /**
     * Extract the bound value from the predicate. When the predicate is not
     * bound at that index, then extract its binding from the binding set.
     * 
     * @param pred
     *            The predicate.
     * @param index
     *            The index into that predicate.
     * @param bindingSet
     *            The binding set.
     *            
     * @return The bound value.
     */
    @SuppressWarnings("unchecked")
    private long asBound(IPredicate<ISPO> pred, int index, IBindingSet bindingSet) {

        final IVariableOrConstant<Long> t = pred.get(index);

        final IConstant<Long> c;
        if(t.isVar()) {
            
            c = bindingSet.get((IVariable) t);
            
        } else {
            
            c = (IConstant<Long>)t;
            
        }

        return c.get().longValue();

    }
    
    public ISolution<SPO> newSolution(IRule rule, IBindingSet bindingSet) {

        final SPO spo = newElement(rule.getHead(), bindingSet);

        final Solution<SPO> solution;
        
        if (elementOnly) {

            solution = new Solution<SPO>(spo);

        } else {

            solution = new Solution<SPO>(spo, rule, bindingSet.clone());
            
        }
        
        if(log.isDebugEnabled()) {
            
            log.debug(solution.toString());
            
        }
        
        return solution;

    }

    public IBindingSet newBindingSet(IRule rule) {

        return new ArrayBindingSet(rule.getVariableCount());
        
    }

    public IRelationLocator getRelationLocator() {
        
        return relationLocator;
        
    }
    
    public IBlockingBuffer<ISolution> newQueryBuffer() {

        return new BlockingBuffer<ISolution>();
        
    }
    
    /**
     * The default buffer capacity.
     */
    private final int DEFAULT_BUFFER_CAPACITY = 10000;
    
    @SuppressWarnings("unchecked")
    public IBuffer<ISolution> newInsertBuffer(IMutableRelation relation) {

        return new AbstractSolutionBuffer.InsertSolutionBuffer(
                DEFAULT_BUFFER_CAPACITY, relation);

    }
    
    @SuppressWarnings("unchecked")
    public IBuffer<ISolution> newDeleteBuffer(IMutableRelation relation) {

        return new AbstractSolutionBuffer.DeleteSolutionBuffer(
                DEFAULT_BUFFER_CAPACITY, relation);

    }


    @SuppressWarnings("unchecked")
    public IChunkedOrderedIterator<ISolution> runQuery(IProgram program)
            throws Exception {

        if (program == null)
            throw new IllegalArgumentException();

        if(log.isInfoEnabled())
            log.info("program="+program.getName());

        if(isEmptyProgram(program)) {

            log.warn("Empty program");

            return (IChunkedOrderedIterator<ISolution>) new EmptyProgramTask(
                    ActionEnum.Query, program).call();

        }

        return (IChunkedOrderedIterator<ISolution>) runProgram(
                ActionEnum.Query, program);

    }

    public long runMutation(ActionEnum action, IProgram program)
            throws Exception {

        if (action == null)
            throw new IllegalArgumentException();
        
        if (program == null)
            throw new IllegalArgumentException();
        
        if (!action.isMutation())
            throw new IllegalArgumentException();
        
        if(log.isInfoEnabled())
            log.info("action="+action+", program="+program.getName());
        
        if(isEmptyProgram(program)) {

            log.warn("Empty program");

            return (Long) new EmptyProgramTask(action, program).call();

        }
        
        return (Long) runProgram(action, program);

    }
    
    /**
     * Return true iff the <i>program</i> is empty.
     * 
     * @param program
     *            The program.
     */
    protected boolean isEmptyProgram(IProgram program) {

        if (!program.isRule() && program.stepCount() == 0) {

            return true;

        }

        return false;
        
    }

    /**
     * Core impl. This handles the logic required to execute the program either
     * on a target {@link DataService} (highly efficient) or within the client
     * using the {@link IClientIndex} to submit operations to the appropriate
     * {@link DataService}(s) (not very efficient, even w/o RMI).
     * 
     * @return Either an {@link IChunkedOrderedIterator} (query) or {@link Long}
     *         (mutation count).
     */
    protected Object runProgram(ActionEnum action, IProgram program)
            throws Exception {

        if (action == null)
            throw new IllegalArgumentException();

        if (program == null)
            throw new IllegalArgumentException();

        final ProgramUtility util = new ProgramUtility(this);
        
        final IBigdataFederation fed = util.getFederation(program);

        if (fed == null) {

            // local Journal or TemporaryStore execution.
            return runLocalProgram(action, program);

        } else if (fed instanceof LocalDataServiceFederation) {

            /*
             * FIXME Get running on the LDS case! There is a problem with how
             * the task is setup (how the locators are setup). The locators need
             * to use the AbstractTask#getJournal() as the index manager for
             * resolving the relations. That will give them the indices on which
             * a lock is being held. (It runs queries fine since there are no
             * locks, but it is actually using IClientIndex views for those
             * queries!!!!)
             */
            
            final DataService dataService = ((LocalDataServiceFederation) fed)
                    .getDataService();

            // single data service program execution.
            return runDataServiceProgram(dataService, action, program);

        } else {

            // distributed program execution.
            return runDistributedProgram(fed, action, program);

        }

    }

    /**
     * This variant handles both local indices on a {@link TemporaryStore} or
     * {@link Journal} WITHOUT concurrency controls.
     * 
     * @todo restrict the {@link ExecutorService} so that only allowable
     *       parallelism is used. Concurrent reads are fine as long as there are
     *       no concurrent writers. Perhaps use a lock when we have to flush a
     *       buffer to the {@link IMutableRelation} for insert or delete.
     */
    protected Object runLocalProgram(ActionEnum action, IProgram program) throws Exception {

        if (log.isInfoEnabled())
            log.info("Running local program: action=" + action + ", program="
                    + program.getName());

        final IProgramTask innerTask = new LocalProgramTask(action,
                program, this, getExecutorService());

        return innerTask.call();

    }

    /**
     * Runs a distributed {@link IProgram}. This covers both the
     * {@link EmbeddedFederation} (which uses key-range partitioned indices) and
     * {@link AbstractDistributedFederation}s that are truely multi-machine and
     * use RMI.
     * 
     * FIXME This is not optimized for distributed joins. it is actually using
     * the {@link ClientIndexView} and the {@link LocalProgramTask} - this is
     * NOT efficient!!!
     */
    protected Object runDistributedProgram(IBigdataFederation fed,
            ActionEnum action, IProgram program) throws Exception {

        if (log.isInfoEnabled()) {

            log.info("Running distributed program: action=" + action
                    + ", program=" + program.getName());

        }

        final IProgramTask innerTask = new LocalProgramTask(action, program,
                this, getExecutorService());

        return innerTask.call();

    }

    /**
     * This variant is submitted and executes on the
     * {@link LocalDataServiceImpl} (fast).
     * <p>
     * Note: This can only be done if all indices for the relation(s) are (a)
     * monolithic; and (b) located on the SAME {@link DataService}. This is
     * <code>true</code> for {@link LocalDataServiceFederation}. All other
     * {@link IBigdataFederation} implementations are scale-out (use key-range
     * partitioned indices).
     */
    protected Object runDataServiceProgram(DataService dataService,
            ActionEnum action, IProgram program) throws InterruptedException,
            ExecutionException {

        final IProgramTask innerTask = new LocalProgramTask(action, program,
                this);

        if (log.isInfoEnabled()) {

            log.info("Submitting program to data service: action=" + action
                    + ", program=" + program.getName() + ", dataService="
                    + dataService);

        }
        
        return dataService.submit(innerTask).get();

    }

}
