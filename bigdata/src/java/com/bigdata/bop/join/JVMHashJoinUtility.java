/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Oct 17, 2011
 */

package com.bigdata.bop.join;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HTreeAnnotations;
import com.bigdata.bop.HashMapAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.join.JVMHashJoinUtility.JVMHashIndex.Bucket;
import com.bigdata.bop.join.JVMHashJoinUtility.JVMHashIndex.SolutionHit;
import com.bigdata.counters.CAT;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.util.InnerCause;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;
import cutthecrap.utils.striterators.Visitor;

/**
 * Utility class supporting hash join against a Java hash collection.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JVMHashJoinUtility implements IHashJoinUtility {

    private static final Logger log = Logger.getLogger(JVMHashJoinUtility.class);
    
    public static class JVMHashIndex {

        /**
         * Note: If joinVars is an empty array, then the solutions will all hash to
         * ONE (1).
         */
        private static final int ONE = 1;
        
        /**
         * Return the hash code which will be used as the key given the ordered
         * as-bound values for the join variables.
         * 
         * @param joinVars
         *            The join variables.
         * @param bset
         *            The bindings whose as-bound hash code for the join variables
         *            will be computed.
         * @param ignoreUnboundVariables
         *            If a variable without a binding should be silently ignored.
         * 
         * @return The hash code.
         * 
         * @throws JoinVariableNotBoundException
         *             if there is no binding for a join variable.
         */
        private static int hashCode(final IVariable<?>[] joinVars,
                final IBindingSet bset, final boolean ignoreUnboundVariables)
                throws JoinVariableNotBoundException {

            int h = ONE;

            for (IVariable<?> v : joinVars) {

                final IConstant<?> c = bset.get(v);

                if (c == null) {

                    if(ignoreUnboundVariables)
                        continue;

                    // Reject any solution which does not have a binding for a join
                    // variable.

                    throw new JoinVariableNotBoundException(v.getName());
                    
                }

                h = 31 * h + c.hashCode();
                
            }
            
            if (log.isTraceEnabled())
                log.trace("hashCode=" + h + ", joinVars="
                        + Arrays.toString(joinVars) + " : " + bset);

            return h;

        }

       
        /**
         * Return an array of constants corresponding to the as-bound values of the
         * join variables for the given solution.
         * 
         * @param joinVars
         *            The join variables.
         * @param bset
         *            The solution.
         * @param optional
         *            <code>true</code> iff the hash join is optional.
         * 
         * @return The as-bound values for the join variables for that solution. 
         */
        static private Key makeKey(final IVariable<?>[] joinVars,
                final IBindingSet bset, final boolean optional) {

            final IConstant<?>[] vals = new IConstant<?>[joinVars.length];

            for (int i = 0; i < joinVars.length; i++) {

                final IVariable<?> v = joinVars[i];

                vals[i] = bset.get(v);

            }

            int hashCode = ONE;
            try {

                /*
                 * Note: The original version of this class always throws an
                 * exception for an unbound join variable out of its hashCode() impl
                 * and then handles that exception here.
                 */
                
                hashCode = hashCode(joinVars, bset, false/* ignoreUnboundVariables */);

            } catch (JoinVariableNotBoundException ex) {
                
                if (!optional) {
                    
                    // Drop solution;
                    
                    if (log.isDebugEnabled())
                        log.debug(ex);

                    return null;

                }

            }
            
            return new Key(hashCode, vals);

        }

        /**
         * Wrapper for the keys in the hash table. This is necessary for the hash
         * table to compare the keys as equal and also provides a efficiencies in
         * the hash code and equals() methods.
         */
        static class Key {
            
            private final int hash;

            private final IConstant<?>[] vals;

            private Key(final int hashCode, final IConstant<?>[] vals) {
                this.vals = vals;
                this.hash = hashCode;
            }

            public int hashCode() {
                return hash;
            }

            public boolean equals(final Object o) {
                if (this == o)
                    return true;
                if (!(o instanceof Key)) {
                    return false;
                }
                final Key t = (Key) o;
                if (vals.length != t.vals.length)
                    return false;
                for (int i = 0; i < vals.length; i++) {
                    if (vals[i] == t.vals[i])
                        continue;
                    if (vals[i] == null)
                        return false;
                    if (!vals[i].equals(t.vals[i]))
                        return false;
                }
                return true;
            }
        }
        
        /**
         * An input solution and a hit counter.
         */
        public static class SolutionHit {

            /**
             * The input solution.
             */
            final public IBindingSet solution;

            /**
             * The #of hits on that input solution when processing the join against
             * the subquery.
             */
            public final CAT nhits = new CAT();
            
            private SolutionHit(final IBindingSet solution) {
                
                if(solution == null)
                    throw new IllegalArgumentException();
                
                this.solution = solution;
                
            }
            
            public String toString() {

                return getClass().getName() + "{nhits=" + nhits + ",solution="
                        + solution + "}";

            }
            
        } // class SolutionHit

        /**
         * A group of solutions having the same as-bound values for their join vars.
         * Each solution is paired with a hit counter so we can support OPTIONAL
         * semantics for the join.
         */
        public static class Bucket implements Iterable<SolutionHit>,
                Comparable<Bucket> {

            /** The hash code for this collision bucket. */
            private final int hashCode;
            
            /**
             * A set of solutions (and their hit counters) which have the same
             * as-bound values for the join variables.
             */
            private final List<SolutionHit> solutions = new LinkedList<SolutionHit>(); 

            public String toString() {
                return super.toString()
                        + //
                        "{hashCode=" + hashCode + ",#solutions=" + solutions.size()
                        + "}";
            }
            
            public Bucket(final int hashCode, final IBindingSet solution) {

                this.hashCode = hashCode;
                
                add(solution);
                
            }

            public void add(final IBindingSet solution) {
             
                if (solution == null)
                    throw new IllegalArgumentException();
                
                solutions.add(new SolutionHit(solution));
                
            }
            
            /**
             * Add the solution to the bucket iff the solutions is not already
             * present in the bucket.
             * <p>
             * Note: There is already a hash index in place on the join variables
             * when we are doing a DISTINCT filter. Further, only the "join"
             * variables are "selected" and participate in a DISTINCT filter.
             * Therefore, if we have a hash collision such that two solutions would
             * be directed into the same {@link Bucket} then we can not improve
             * matters but must simply scan the solutions in the bucket to decide
             * whether the new solution duplicates a solution which is already
             * present.
             * 
             * @param solution
             *            The solution.
             * 
             * @return <code>true</code> iff the bucket was modified by this
             *         operation.
             */
            public boolean addDistinct(final IBindingSet solution) {

                if(solutions.isEmpty()) {

                    // First solution.
                    solutions.add(new SolutionHit(solution));
                    
                    return true;
                    
                }

                final Iterator<SolutionHit> itr = solutions.iterator();
                
                while(itr.hasNext()) {
                    
                    final SolutionHit aSolution = itr.next();
                    
                    if(aSolution.solution.equals(solution)) {
                        
                        // Solution already in this bucket.
                        return false;
                        
                    }
                    
                }
                
                // This is a distinct solution.
                solutions.add(new SolutionHit(solution));
                
                return true;
                
            }
            
            public Iterator<SolutionHit> iterator() {
                
//                return Collections.unmodifiableList(solutions).iterator();
                return solutions.iterator();
                
            }

//            @SuppressWarnings("unchecked")
//            public Iterator<IBindingSet> bindingSetIterator() {
//                
//                return new Striterator(solutions.iterator()).addFilter(new Resolver() {
//                    
//                    @Override
//                    protected Object resolve(Object obj) {
//                        return ((SolutionHit)obj).solution;
//                    }
//                });
//                
//            }

            /**
             * Orders the buckets based on their hash codes.
             */
            @Override
            public int compareTo(final Bucket o) {
                if (hashCode > o.hashCode)
                    return 1;
                if (hashCode < o.hashCode)
                    return -1;
                return 0;
            }

        } // Bucket

        /**
         * The backing map - this is NOT thread safe.
         */
        private final Map<Key, Bucket> map;
        private final IVariable<?>[] joinVars;
//        private final boolean optional;

        public JVMHashIndex(final int initialCapacity, final float loadFactor,
                final IVariable<?>[] joinVars) {//, final boolean optional) {

            if (joinVars == null) {
                /*
                 * A ZERO LENGTH joinVars[] means that all solutions will be in
                 * the same hash bucket. This can arise due to poor assignment
                 * of join variables or simply because there are no available
                 * join variables (full cross product join). Such joins are very
                 * expensive.
                 */
                throw new IllegalArgumentException();
            }
            
            this.map = new LinkedHashMap<Key, Bucket>(initialCapacity,
                    loadFactor);

            this.joinVars = joinVars;

            /*
             * TOOD Can we pass this in and remove it from the API? But see
             * filterDistinct().
             */
//            this.optional = optional;

        }

        /**
         * Add the solution to the index.
         * 
         * @param bset
         *            The {@link IBindingSet}.
         * @param optional
         * 
         * @return The {@link Key} iff the solution was added to the index and
         *         <code>null</code> iff the solution was not added (because a
         *         {@link Key} could not be formed for the solution given the
         *         specified {@link #joinVars}).
         * 
         *         TODO javadoc on OPTIONAL
         */
        public Key add(final IBindingSet bset,final boolean optional) {

            final Key key = makeKey(joinVars, bset, optional);

            if (key == null) {

                // Drop solution.
                return null;
                
            }

            Bucket b = map.get(key);

            if (b == null) {

                map.put(key, b = new Bucket(key.hash, bset));

            } else {

                b.add(bset);

            }

            return key;
            
        }
        
        /**
         * Add the solution to the index iff the solution is not already present
         * in the index.
         * 
         * @param solution
         *            The solution.
         * 
         * @return <code>true</code> iff the index was modified by this
         *         operation.
         */
        public boolean addDistinct(final IBindingSet bset) {

            // TODO Review why optional:=true here.
            final Key key = makeKey(joinVars, bset, true/* optional */);

            assert key != null;

            Bucket b = map.get(key);

            if (b == null) {

                // New bucket holding just this solution.
                map.put(key, b = new Bucket(key.hash, bset));

                return true;

            } else {

                if (b.addDistinct(bset)) {

                    // Existing bucket not having this solution.
                    return true;

                }

                // Existing bucket with duplicate solution.
                return false;

            }

        }

        /**
         * Return the hash {@link Bucket} into which the given solution is mapped.
         * <p>
         * Note: The caller must apply an appropriate join constraint in order
         * to correctly reject solutions that (a) violate the join contract; and
         * (b) that are present in the hash bucket due to a hash collection
         * rather than because they have the same bindings for the join
         * variables.
         * 
         * @param left
         *            The probe.
         * @param optional
         * 
         * @return The hash {@link Bucket} into which the given solution is
         *         mapped -or- <code>null</code> if there is no such hash
         *         bucket.
         * 
         *         TODO javadoc [optional].
         */
        public Bucket getBucket(final IBindingSet left, final boolean optional) {

            final Key key = makeKey(joinVars, left, optional);

            if (key == null) {

                return null;

            }

            // Probe the hash map : May return [null]!
            return map.get(key);

        }

        /**
         * Visit all buckets in the hash index.
         */
        public Iterator<Bucket> buckets() {
            
            return map.values().iterator();
            
        }
        
        /**
         * The #of buckets in the hash index. Each bucket has a distinct hash
         * code. Hash collisions can cause solutions that are distinct in their
         * {@link #joinVars} to nevertheless be mapped into the same hash
         * bucket.
         * 
         * @return The #of buckets in the hash index.
         */
        public int bucketCount() {
            
            return map.size();
            
        }
        
        /**
         * Export the {@link Bucket}s as an array.
         */
        public Bucket[] toArray() {

            // source.
            final Iterator<Bucket> bucketIterator = map.values().iterator();

            final Bucket[] a = new Bucket[map.size()];

            int i = 0;

            while (bucketIterator.hasNext()) {

                a[i++] = bucketIterator.next();

            }

            return a;

        }

    }
    
    /**
     * <code>true</code> until the state is discarded by {@link #release()}.
     */
    private final AtomicBoolean open = new AtomicBoolean(true);
    
    /**
     * The operator whose annotations are used to initialize this object.
     * <p>
     * Note: This was added to support the DISTINCT FILTER in
     * {@link #outputSolutions(IBuffer)}.
     */
    private final PipelineOp op;
    
    /**
     * The type of join to be performed.
     */
    private final JoinTypeEnum joinType;
    
    /**
     * <code>true</code> iff the join is OPTIONAL.
     */
    private final boolean optional;
    
    /**
     * <code>true</code> iff this is a DISTINCT filter.
     */
    private final boolean filter;

    /**
     * @see HashJoinAnnotations#ASK_VAR
     */
    private final IVariable<?> askVar;
    
    /**
     * The join variables.
     */
    private final IVariable<?>[] joinVars;

    /**
     * The variables to be retained (aka projected out) (optional, all variables
     * are retained if not specified).
     */
    private final IVariable<?>[] selectVars;

    /**
     * The variables to be projected into a join group. When non-
     * <code>null</code> variables that are NOT in this array are NOT flowed
     * into the join group.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/668" >
     *      JoinGroup optimizations </a>
     */
    private final IVariable<?>[] projectedInVars;
    
    /**
     * The join constraints (optional).
     */
    private final IConstraint[] constraints;

    /**
     * The hash index.
     * <p>
     * Note: There is no separate "joinSet". Instead, the {@link SolutionHit}
     * class provides a join hit counter.
     */
    private final AtomicReference<JVMHashIndex> rightSolutionsRef = new AtomicReference<JVMHashIndex>();
    
    /**
     * The #of solutions accepted into the hash index.
     */
    private final CAT rightSolutionCount = new CAT();
    
    /**
     * The maximum #of (left,right) solution joins that will be considered
     * before failing the join. This is used IFF there are no join variables.
     */
    private final long noJoinVarsLimit = HashJoinAnnotations.DEFAULT_NO_JOIN_VARS_LIMIT;
    
    /**
     * The #of left solutions considered for a join.
     */
    private final CAT nleftConsidered = new CAT();

    /**
     * The #of right solutions considered for a join.
     */
    private final CAT nrightConsidered = new CAT();

    /**
     * The #of solution pairs considered for a join.
     */
    private final CAT nJoinsConsidered = new CAT();
    
    /**
     * Human readable representation of the {@link IHashJoinUtility} metadata
     * (but not the solutions themselves).
     */
    public String toString() {

        final StringBuilder sb = new StringBuilder();
        
        sb.append(getClass().getSimpleName());
        
        sb.append("{open=" + open);
        sb.append(",joinType="+joinType);
//        sb.append(",optional=" + optional);
//        sb.append(",filter=" + filter);
        if (askVar != null)
            sb.append(",askVar=" + askVar);
        sb.append(",joinVars=" + Arrays.toString(joinVars));
        if (selectVars != null)
            sb.append(",selectVars=" + Arrays.toString(selectVars));
        if (constraints != null)
            sb.append(",constraints=" + Arrays.toString(constraints));
        sb.append(",size=" + getRightSolutionCount());
        sb.append(",considered(left=" + nleftConsidered + ",right="
                + nrightConsidered + ",joins=" + nJoinsConsidered + ")");
        sb.append("}");
        
        return sb.toString();
        
    }

    /**
     * 
     * @param op
     *            The operator whose annotation will inform construction the
     *            hash index. The {@link HTreeAnnotations} may be specified for
     *            this operator and will control the initialization of the
     *            various {@link HTree} instances.
     * @param joinType
     *            The type of join to be performed.
     * 
     * @see JVMHashJoinAnnotations
     */
    public JVMHashJoinUtility(final PipelineOp op, final JoinTypeEnum joinType) {
//            final boolean optional,
//            final boolean filter) {

        if (op == null)
            throw new IllegalArgumentException();

        if(joinType == null)
            throw new IllegalArgumentException();
        
        this.op = op;
        this.joinType = joinType;
        this.optional = joinType == JoinTypeEnum.Optional;
        this.filter = joinType == JoinTypeEnum.Filter;

        // Optional variable used for (NOT) EXISTS.
        this.askVar = (IVariable<?>) op
                .getProperty(HashJoinAnnotations.ASK_VAR);
        
        // The join variables (required).
        this.joinVars = (IVariable<?>[]) op
                .getRequiredProperty(HashJoinAnnotations.JOIN_VARS);

        /*
         * The projected OUT variables (optional and equal to the join variables
         * iff this is a DISTINCT filter).
         */
        this.selectVars = filter ? joinVars : (IVariable<?>[]) op
                .getProperty(JoinAnnotations.SELECT);

        /*
         * The variables that are projected IN to the join group.
         */
        this.projectedInVars = (IVariable<?>[]) op
                .getProperty(HashJoinAnnotations.PROJECT_IN_VARS);

        // The join constraints (optional).
        this.constraints = (IConstraint[]) op
                .getProperty(JoinAnnotations.CONSTRAINTS);

//        // Iff the join has OPTIONAL semantics.
//        this.optional = optional;
//        
//        // Iff this is a DISTINCT filter.
//        this.filter = filter;

        /*
         * TODO Parameter for LinkedHashMap versus HashMap. HashMap is going to
         * be faster for insert and search. LinkedHashMap will be faster for the
         * iterator. Some join patterns do not require us to use an iterator at
         * all, in which case HashMap is the clear winner. (For example, a
         * non-optional hash join against an access path never uses the iterator
         * over the hash index.)
         * 
         * TODO There is an opportunity for CONCURRENT hash map for at least the
         * DISTINCT SOLUTIONS filter and perhaps for others as well. However, to
         * do this with the DISTINCT SOLUTIONS filter we would have to make the
         * mutation operations on a Bucket atomic. E.g., using the synchronized
         * keyword. This would give us what amounts to per-hash code striped
         * locks.
         * 
         * Note: the JVMDistinctBindingSetsOp does not use this class right now
         * because it enjoys better concurrency than the JVMHashJoinUtility.
         * Also see JVMDistinctFilter, which is the backing implementation for
         * the JVMDistinctBindingSetsOp.
         */
        rightSolutionsRef.set(new JVMHashIndex(//
                op.getProperty(HashMapAnnotations.INITIAL_CAPACITY,
                        HashMapAnnotations.DEFAULT_INITIAL_CAPACITY),//
                op.getProperty(HashMapAnnotations.LOAD_FACTOR,
                        HashMapAnnotations.DEFAULT_LOAD_FACTOR),//
                        joinVars//
//                        optional//
                ));
        
    }
    
    public JoinTypeEnum getJoinType() {
        return joinType;
    }
    
    public IVariable<?> getAskVar() {
        return askVar;
    }
    
    public IVariable<?>[] getJoinVars() {
        return joinVars;
    }
    
    public IVariable<?>[] getSelectVars() {
        return selectVars;
    }
    
    public IConstraint[] getConstraints() {
        return constraints;
    }

    @Override
    public boolean isEmpty() {

        return getRightSolutionCount() == 0;
        
    }

    private JVMHashIndex getRightSolutions() {
        
        return rightSolutionsRef.get();
        
    }
    
    @Override
    public long getRightSolutionCount() {
        /*
         * Note: This needs to be explicitly tracked and reported. Since each
         * entry in the rightSolutions map is a collision bucket, we can not
         * just report the size of the hash index. Instead we have to track and
         * report the #of solutions entered into the hash index in
         * acceptSolutions() and filterSolutions().
         */
        return rightSolutionCount.get();
    }

    @Override
    public void release() {

        if (open.compareAndSet(true/* expect */, false/* update */)) {
            // Already closed.
            return;
        }

        rightSolutionsRef.set(null);
        
    }

    @Override
    public long acceptSolutions(final ICloseableIterator<IBindingSet[]> itr,
            final BOpStats stats) {

        try {

            final JVMHashIndex index = getRightSolutions();

            final IBindingSet[] all = BOpUtility.toArray(itr, stats);

            if (log.isDebugEnabled())
                log.debug("Materialized: " + all.length + " source solutions.");

            long naccepted = 0;

            for (IBindingSet bset : all) {

                if (index.add(bset, optional) == null) {

                    continue;

                }

                naccepted++;

            }

            if (log.isDebugEnabled())
                log.debug("There are " + index.bucketCount()
                        + " hash buckets, joinVars="
                        + Arrays.toString(joinVars));

            rightSolutionCount.add(naccepted);

            return naccepted;

        } catch (Throwable t) {

            throw launderThrowable(t);
            
        }

    }

    /*
     * FIXME I have observed two apparent bugs in this class. First, it was not
     * assigning the output of [bset.copy(joinVars)] back to bset. Second, it
     * was failing to output the first solution in a given bucket. I suspect
     * that nobody is calling this code and that the JVMDistinctBindingSetOp is
     * being used instead (which is a better choice since it allows full
     * concurrency) - I have verified this.  This method is not called. We might
     * use the method by the same name on the HTreeHashJoinUtility, but not this
     * version.
     */
    @Override
    public long filterSolutions(final ICloseableIterator<IBindingSet[]> itr,
            final BOpStats stats, final IBuffer<IBindingSet> sink) {
        
        try {
        
            final JVMHashIndex index = getRightSolutions();

            final IBindingSet[] all = BOpUtility.toArray(itr, stats);

            if (log.isDebugEnabled())
                log.debug("Materialized: " + all.length + " source solutions.");

            for (IBindingSet bset : all) {

                /*
                 * Note: For a DISTINCT SOLUTIONS filter, we only consider the
                 * variables that are being projected. Further, all variables
                 * are used when computing the hash code. Therefore "joinVars"
                 * == "selectedVars" for a DISTINCT SOLUTIONS filter.
                 */
                bset = bset.copy(joinVars); // only consider the selected variables. 

                /*
                 * Note: Solutions are NOT dropped if a variable is not bound in
                 * a given solution. The variable is simply not used when
                 * computing the hash code. Specifying optional:=true here
                 * causes makeKey() to have this behavior.
                 */
                if (index.addDistinct(bset)) {

                    // Write on the output sink.
                    sink.add(bset);

                }

            }

            if (log.isDebugEnabled())
                log.debug("There are " + index.bucketCount()
                        + " hash buckets, joinVars="
                        + Arrays.toString(joinVars));

            final long naccepted = all.length;

            rightSolutionCount.add(naccepted);

            return naccepted;

        } catch (Throwable t) {

            throw launderThrowable(t);
            
        }

    }

    @Override
    public void hashJoin(//
            final ICloseableIterator<IBindingSet> leftItr,//
            final IBuffer<IBindingSet> outputBuffer//
            ) {
        
        hashJoin2(leftItr, outputBuffer, constraints);
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * For each source solution materialized, the hash table is probed using the
     * as-bound join variables for that source solution. A join hit counter is
     * carried for each solution in the hash index and is used to support
     * OPTIONAL joins.
     */
    @Override
    public void hashJoin2(//
            final ICloseableIterator<IBindingSet> leftItr,//
            final IBuffer<IBindingSet> outputBuffer,//
            final IConstraint[] constraints//
            ) {

        final JVMHashIndex rightSolutions = getRightSolutions();
          
        if (log.isInfoEnabled()) {
            log.info("rightSolutions: #buckets=" + rightSolutions.bucketCount()
                    + ",#solutions=" + getRightSolutionCount());
        }

        // true iff there are no join variables.
        final boolean noJoinVars = joinVars.length == 0;

        try {

            while (leftItr.hasNext()) {

                final IBindingSet left = leftItr.next();

                nleftConsidered.increment();
                
                if (log.isDebugEnabled())
                    log.debug("Considering " + left);

                final Bucket bucket = rightSolutions.getBucket(
                        left, optional);

                if (bucket == null)
                    continue;
                
                final Iterator<SolutionHit> ritr = bucket
                        .iterator();

                while (ritr.hasNext()) {

                    final SolutionHit right = ritr.next();

                    nrightConsidered.increment();
                    
                    if (log.isDebugEnabled())
                        log.debug("Join with " + right);

                    nJoinsConsidered.increment();

                    if (noJoinVars
                            && nJoinsConsidered.get() == noJoinVarsLimit) {

                        if (nleftConsidered.get() > 1
                                && nrightConsidered.get() > 1) {

                            throw new UnconstrainedJoinException();

                        }

                    }

                    // See if the solutions join. 
                    final IBindingSet outSolution = BOpContext.bind(//
                            right.solution,//
                            left,//
                            constraints,//
                            selectVars//
                            );

                    switch(joinType) {
                    case Normal: {
                        if (outSolution != null) {
                            // Output the solution.
                            outputSolution(outputBuffer, outSolution);
                        }
                        break;
                    }
                    case Optional: {
                        if (outSolution != null) {
                            // Output the solution.
                            outputSolution(outputBuffer, outSolution);
                            // Increment counter so we know not to output the
                            // rightSolution as an optional solution.
                            right.nhits.increment();
                        }
                        break;
                    }
                    case Exists: {
                        /*
                         * The right solution is output iff there is at least
                         * one left solution which joins with that right
                         * solution. Each right solution is output at most one
                         * time.
                         */
                        if (outSolution != null) {
//                            if (right.nhits.get() == 0L) {
//                                // Output the solution.
//                                outputSolution(outputBuffer, right.solution);
//                            }
                            // Increment counter so we know this solution joins.
                            right.nhits.increment();
                        }
                        break;
                    }
                    case NotExists: {
                        /*
                         * The right solution is output iff there does not exist
                         * any left solution which joins with that right
                         * solution. This basically an optional join where the
                         * solutions which join are not output.
                         */
                        if (outSolution != null) {
                            // Increment counter so we know not to output the
                            // rightSolution as an optional solution.
                            right.nhits.increment();
                        }
                        break;
                    }
                    default:
                        throw new AssertionError();
                    }

                }

            }

        } catch(Throwable t) {

            throw launderThrowable(t);
            
        } finally {

            leftItr.close();

        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation is a NOP since the underlying Java collection class
     * is thread-safe for concurrent readers.
     */
    @Override
    public void saveSolutionSet() {
        
        // NOP
        
    }
    
    /**
     * Output a solution.
     * 
     * @param outputBuffer
     *            Where to write the solution.
     * @param outSolution
     *            The solution.
     */
    private void outputSolution(final IBuffer<IBindingSet> outputBuffer,
            final IBindingSet outSolution) {

        if (log.isDebugEnabled())
            log.debug("Output solution: " + outSolution);

        // Accept this binding set.
        outputBuffer.add(outSolution);
        
    }

    @Override
    public void outputOptionals(final IBuffer<IBindingSet> outputBuffer) {

        try {

            @SuppressWarnings({ "rawtypes", "unchecked" })
            final Constant f = askVar == null ? null : new Constant(
                    XSDBooleanIV.FALSE);

            final JVMHashIndex rightSolutions = getRightSolutions();

            final IVariable<?>[] selected = getSelectVars();

            if (log.isInfoEnabled())
                log.info("rightSolutions: #buckets="
                        + rightSolutions.bucketCount());

            /*
             * Note: when NO solutions joined for a given source binding set AND
             * the join is OPTIONAL then we output the _original_ binding set to
             * the sink join task(s) and DO NOT apply the CONSTRAINT(s).
             */
            final Iterator<Bucket> bitr = rightSolutions.buckets();
            
            while (bitr.hasNext()) {

                final Bucket b = bitr.next();

                for (SolutionHit hit : b) {

                    if (hit.nhits.get() > 0)
                        continue;

                    IBindingSet bs = hit.solution;

                    if (selected != null) {

                        // Drop variables which are not projected.
                        bs = bs.copy(selected);

                    }

                    if (f != null) {

                        if (bs == hit.solution)
                            bs = bs.clone();

                        bs.set(askVar, f);

                    }

                    outputBuffer.add(bs);

                    if (log.isDebugEnabled())
                        log.debug("Optional solution: " + bs);

                }

            }

        } catch (Throwable t) {
            
            throw launderThrowable(t);
        
        }
        
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ICloseableIterator<IBindingSet> indexScan() {

        try {

//            /*
//             * The selected variables -or- <code>null</code> if all variables
//             * should be projected.
//             */
//            final IVariable<?>[] selected = getSelectVars();

            final JVMHashIndex rightSolutions = getRightSolutions();

            if (log.isInfoEnabled())
                log.info("rightSolutions: #buckets=" + rightSolutions.bucketCount());

            // Visit the buckets.
            IStriterator itr = new Striterator(rightSolutions.buckets());
            
            itr = itr.addFilter(new Expander() {

                private static final long serialVersionUID = 1L;

                /**
                 * Expand the bucket into the solutions in the bucket.
                 */
                @SuppressWarnings("rawtypes")
                @Override
                protected Iterator expand(final Object obj) {

                    final Bucket b = (Bucket) obj;

                    return b.iterator();

                }
            });

            /**
             * Copy only the variables that are projected.
             */
            itr = itr.addFilter(new Resolver() {

                private static final long serialVersionUID = 1L;

                @Override
                protected Object resolve(final Object obj) {

                    final IBindingSet bs = ((SolutionHit) obj).solution;

//                    if (selected != null) {
//
//                        // Drop variables which are not projected.
//                        bs = bs.copy(selected);
//
//                    }

                    return bs;

                }
            });
            
            return (ICloseableIterator<IBindingSet>) itr;

        } catch (Throwable t) {

            throw launderThrowable(t);

        }

    }
    
    @Override
    public void outputSolutions(final IBuffer<IBindingSet> out) {

        /*
         * FIXME Set this to enable "DISTINCT" on the solutions flowing into the
         * join group.
         * 
         * Note: This should be set by the HashIndexOp (or passed in through the
         * interface).
         * 
         * Note: Enabling this causes failures. See the ticket below. I suspect
         * that these failures are related to underproducing solutions, but some
         * of them might also be related to a failure to produce the correct set
         * of variables for [projectedInVars].
         * 
         * TODO I have factored out the JVMHashIndex class. This class tracks
         * the #of hits for each distinct solution. We can use this to correct
         * the output cardinality.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/668" >
         * JoinGroup optimizations </a>
         */
        final boolean distinct = false;
        
        final JVMDistinctFilter distinctFilter;
        
        if (distinct && projectedInVars != null && projectedInVars.length > 0) {

            /*
             * Note: We are single threaded here so we can use a lower
             * concurrencyLevel value.
             * 
             * Note: If necessary, this could be replaced with JVMHashIndex so
             * we get the #of occurrences of each distinct combination of
             * bindings that is projected into the sub-group/-query.
             */
            final int concurrencyLevel = 1;//ConcurrentHashMapAnnotations.DEFAULT_CONCURRENCY_LEVEL;

            distinctFilter = new JVMDistinctFilter(projectedInVars, //
                    op.getProperty(HashMapAnnotations.INITIAL_CAPACITY,
                            HashMapAnnotations.DEFAULT_INITIAL_CAPACITY),//
                    op.getProperty(HashMapAnnotations.LOAD_FACTOR,
                            HashMapAnnotations.DEFAULT_LOAD_FACTOR),//
                            concurrencyLevel
            );
            
        } else {
         
            distinctFilter = null;
            
        }
        
        try {

//            if (true) {
//
//          /*
//          * Code works, uses Striterator pattern.
//          *
//                 * The selected variables -or- <code>null</code> if all
//                 * variables should be projected.
//                 */
//                final IVariable<?>[] selected = getSelectVars();
//                
//                final ICloseableIterator<IBindingSet> itr = indexScan();
//
//                try {
//
//                    while (itr.hasNext()) {
//
//                        IBindingSet bs = itr.next();
//
//                        if (selected != null) {
//
//                            // Drop variables which are not projected.
//                            bs = bs.copy(selected);
//
//                        }
//                        
//                        out.add(bs);
//
//                        if (log.isDebugEnabled())
//                            log.debug("Output solution: " + bs);
//
//                    }
//
//                } finally {
//
//                    itr.close();
//
//                }
//
//            } else {

                /*
                 * Code works, uses nested iterators pattern.
                 */
            
                final JVMHashIndex rightSolutions = getRightSolutions();

                final IVariable<?>[] selected = getSelectVars();

                if (log.isInfoEnabled())
                    log.info("rightSolutions: #buckets="
                            + rightSolutions.bucketCount());

                // source.
                final Iterator<Bucket> bucketIterator = rightSolutions.
                        buckets();

                while (bucketIterator.hasNext()) {

                    final Bucket bucket = bucketIterator.next();

                    for (SolutionHit solutionHit : bucket) {

                        IBindingSet bs = solutionHit.solution;

                        if (distinctFilter != null) {

                            /*
                             * Note: The DISTINCT filter is based on the
                             * variables that are projected INTO the child
                             * join group.  However, those are NOT always
                             * the same as the variables that are projected
                             * OUT of the child join group, so we need to
                             * 
                             */
                            
                            if ((bs = distinctFilter.accept2(bs)) == null) {

                                // Drop duplicate solutions.
                                continue;
                                
                            }

                        } else if (selected != null) {

                        /*
                         * FIXME We should be using projectedInVars here since
                         * outputSolutions() is used to stream solutions into
                         * the child join group (at least for some kinds of
                         * joins, but there might be exceptions for joining with
                         * a named solution set).
                         */
                            
                            // Drop variables which are not projected.
                            bs = bs.copy(selected);

                        }

                        out.add(bs);

                        if (log.isDebugEnabled())
                            log.debug("Output solution: " + bs);

                    }

                }

//            }

        } catch (Throwable t) {
            
            throw launderThrowable(t);
            
        }

    }

    @Override
    public void outputJoinSet(final IBuffer<IBindingSet> outputBuffer) {

        try {

            @SuppressWarnings({ "rawtypes", "unchecked" })
            final Constant t = askVar == null ? null : new Constant(
                    XSDBooleanIV.TRUE);
            
            final JVMHashIndex rightSolutions = getRightSolutions();

            final IVariable<?>[] selected = getSelectVars();

            if (log.isInfoEnabled())
                log.info("rightSolutions: #buckets=" + rightSolutions.bucketCount());

            final Iterator<Bucket> bitr = rightSolutions.buckets();

            while(bitr.hasNext()) {

                final Bucket b = bitr.next();

                for (SolutionHit hit : b) {

                    if (hit.nhits.get() == 0)
                        continue;

                    IBindingSet bs = hit.solution;

                    if (selected != null) {

                        // Drop variables which are not projected.
                        bs = bs.copy(selected);

                    }

                    if (t != null) {

                        if (bs == hit.solution)
                            bs = bs.clone();

                        bs.set(askVar, t);

                    }
                    
                    outputBuffer.add(bs);

                    if (log.isDebugEnabled())
                        log.debug("Output solution: " + bs);

                }

            }

        } catch (Throwable t) {
            
            throw launderThrowable(t);
        
        }
        
    }

    /**
     * Combine constraints for each source with the given constraints.
     * 
     * @param constraints
     *            Explicitly given constraints for this join.
     * @param all
     *            The sources for the join.
     * 
     * @return The combined constraints and <code>null</code> iff there are no
     *         constraints.
     */
    static IConstraint[] combineConstraints(final IConstraint[] constraints,
            final IHashJoinUtility[] all) {

        final List<IConstraint> list = new LinkedList<IConstraint>();

        // For each source.
        for (int i = 0; i < all.length; i++) {

            final IHashJoinUtility tmp = all[i];

            if (tmp.getConstraints() != null) {

                list.addAll(Arrays.asList(tmp.getConstraints()));

            }

        }

        // The join constraints specified by the caller.
        if (constraints != null) {

            list.addAll(Arrays.asList(constraints));

        }

        return list.isEmpty() ? null : list
                .toArray(new IConstraint[list.size()]);
        
    }

    /**
     * Advance each other source to the first hash code GTE the hashCode for the
     * first source.
     * <p>
     * If the source does not have a bucket for the hash code in the first
     * bucket then either (a) if this is a required join, this method will
     * return <code>false</code> and the caller must advance to the next bucket
     * in the first source; or (b) if this is an optional join, there will be a
     * <code>null</code> in the <i>currentBucket[]</i> for that source.
     * 
     * @param sortedSourceBuckets
     *            An array of {@link Bucket}[]s for each source. The vector of
     *            {@link Bucket}s for each source has been sorted. This means
     *            that we can scan down those vectors and observe {@link Bucket}
     *            s having strictly increasing hash codes for each source.
     * @param sourceIndex
     *            The next index into each source.
     * @param currentBucket
     *            The current bucket for each source.
     * @param optional
     *            <code>true</code> iff this is an optional join.
     * 
     * @return <code>true</code> if we are on a bucket which might join. if this
     *         method returns <code>false</code>, then the caller should
     *         immediately advance to the next bucket from the first source
     *         without attempting a join.
     */
    static private boolean advanceOtherSources(//
            final Bucket[][] sortedSourceBuckets,//
            final int[] sourceIndex,//
            final Bucket[] currentBucket,//
            final boolean optional//
            ) {

        // The next collision bucket in hash code order from the 1st source.
        final Bucket firstBucket = sortedSourceBuckets[0][sourceIndex[0]];
        final int hashCode = firstBucket.hashCode;
        currentBucket[0] = firstBucket;

        for (int i = 1; i < sourceIndex.length; i++) {

            // Advance source to first bucket GTE hashCode.
            while (true) {

                // Next bucket index for the other source
                final int j = sourceIndex[i];

                final Bucket otherBucket;
                if (j >= sortedSourceBuckets[i].length) {
                    // This source is exhausted.
                    if (!optional) {
                        // Nothing is left which can join.
                        return false;
                    }
                    otherBucket = null;
                } else {
                    otherBucket = sortedSourceBuckets[i][j];
                }

                if (otherBucket == null) {
                	assert optional;
                    currentBucket[i] = null;
                	break;
                }
                
                if (otherBucket.hashCode < hashCode) {

                    sourceIndex[i]++;

                    continue;

                }

                if (otherBucket.hashCode > hashCode) {

                    if (!optional) {

                        // The bucket on the first source can not join.
                        return false;
                        
                    } else {

                        // The bucket will be ignored.
                        currentBucket[i] = null;
                        
                        // Exit the inner loop.
                        break;
                        
                    }

                }

                currentBucket[i] = otherBucket;

                break;

            }

        }
        
        return true;
        
    }
    
    @Override
    public void mergeJoin(//
            final IHashJoinUtility[] others,//
            final IBuffer<IBindingSet> outputBuffer, //
            final IConstraint[] constraints,//
            final boolean optional//
            ) {

        /*
         * Validate arguments.
         */

        if (others == null)
            throw new IllegalArgumentException();
        
        if (others.length == 0)
            throw new IllegalArgumentException();
        
        if (outputBuffer == null)
            throw new IllegalArgumentException();
        
        final JVMHashJoinUtility[] all = new JVMHashJoinUtility[others.length + 1];
        {
            all[0] = this;
            for (int i = 0; i < others.length; i++) {
                final JVMHashJoinUtility o = (JVMHashJoinUtility) others[i];
                if (o == null)
                    throw new IllegalArgumentException();
                if (!Arrays.equals(this.joinVars, o.joinVars)) {
                    // Must have the same join variables.
                    throw new IllegalArgumentException();
                }
                all[i + 1] = o;
            }

        }

        if(isEmpty()) {
            return;
        }
        
        /*
         * Combine constraints for each source with the given constraints.
         */
        final IConstraint[] c = combineConstraints(constraints, all);

        /*
         * The JVM hash collections do not maintain the data in hash code order.
         * Therefore, we materialize and sort the collision buckets for each
         * hash index.
         */
        final Bucket[][] sortedSourceBuckets = new Bucket[all.length][];
        {

            for (int i = 0; i < all.length; i++) {
                
                // Fully materialize the solution set as a Bucket[].
                final Bucket[] t = all[i].getRightSolutions().toArray();

                /*
                 * Sort the array. It's natural sort order is by the hash code
                 * of the join variables.
                 */
                Arrays.sort(t);
                
                sortedSourceBuckets[i] = t;
                
            }
            
        }

        /*
         * Synchronize each source.
         */

        // The next index into each source (not used for the 1st source).
        final int[] sourceIndex = new int[all.length];

        // The current bucket for each source.
        final Bucket[] currentBucket = new Bucket[all.length];

        while (sourceIndex[0] < sortedSourceBuckets[0].length) {

            if (!optional) {
                /*
                 * If the join is not optional, then we are done as soon as any
                 * source is exhausted.
                 */
                for (int i = 1; i < sourceIndex.length; i++) {
                    if (sourceIndex[i] >= sortedSourceBuckets[i].length) {
                        // All done.
                        return;
                    }
                }
            }
            
            // Synchronous the other sources.
            if (advanceOtherSources(sortedSourceBuckets, sourceIndex,
                    currentBucket, optional)) {

//                log.error("sourceIndex[]=" + Arrays.toString(sourceIndex));
                
                // Join those buckets, outputting solutions which join.
                mergeJoin(currentBucket, c, optional, outputBuffer);

            }

            // Advance the first source to the next bucket.
            sourceIndex[0]++;
            
        }

    }

    /**
     * MERGE JOIN
     * <p>
     * Join the solution sets from each source. This will consider the full
     * cross product of the solutions in each source bucket. All buckets will
     * have the same hash code. If this is an optional join, then some entries
     * in buckets[] MAY be <code>null</code>. However, the first entry is never
     * <code>null</code> since that is the primary source for the join.
     * 
     * @param currentBucket
     *            The current {@link Bucket} from each source. The first entry
     *            in this array is the source from which optional solutions will
     *            be reported if the join is optional.
     * @param constraints
     * @param optional
     *            <code>true</code> iff the join is optional.
     * @param outputBuffer
     */
    static private void mergeJoin(//
            final Bucket[] currentBucket,//
            final IConstraint[] constraints,//
            final boolean optional,//
            final IBuffer<IBindingSet> outputBuffer) {

		final int nsources = currentBucket.length;

		// The bucket for the first source.
		final Bucket firstBucket = currentBucket[0];

		assert firstBucket != null; // never allowed for the 1st source.

		for (int i = 1; i < nsources; i++) {

			// A bucket having the same hash code for another source.
			final Bucket otherBucket = currentBucket[i];

			if (otherBucket == null) {

				assert optional; // only allowed if the join is optional.

				continue;

			}

			// Must be the same hash code.
			assert firstBucket.hashCode == otherBucket.hashCode;

		}

		final SolutionHit[] set = new SolutionHit[nsources];
		
		final Striterator sols1 = new Striterator(firstBucket.solutions
				.listIterator());
		
		sols1.addFilter(new Visitor() {
            private static final long serialVersionUID = 1L;
            @Override
			protected void visit(Object obj) {
				set[0] = (SolutionHit) obj;
			}

		});
		
		// now add in Expanders and Visitors for each Bucket
		for (int i = 1; i < nsources; i++) {
			// A bucket having the same hash code for another source.
			final int slot = i;
			final Bucket otherBucket = currentBucket[i];
			
			// if optional then if there are no solutions don't try and
			// expand further
			if (!(optional && (otherBucket == null || otherBucket.solutions.isEmpty()))) {
				sols1.addFilter(new Expander() {
                    private static final long serialVersionUID = 1L;
                    @Override
					protected Iterator<?> expand(final Object obj) {
						return otherBucket.iterator();
					}
	
				});
				sols1.addFilter(new Visitor() {
                    private static final long serialVersionUID = 1L;
                    @Override
					protected void visit(final Object obj) {
						set[slot] = (SolutionHit) obj;
					}
	
				});
			}
		}

		while (sols1.hasNext()) {
			sols1.next();
            IBindingSet in = set[0].solution;
            for (int i = 1; i < set.length; i++) {

                // See if the solutions join. 
            	if (set[i] != null) {
                in = 
                BOpContext.bind(//
                		in,// 
                        set[i].solution,// 
                        constraints,// TODO constraint[][]
                        null//
                        );
            	}

                if (in == null) {
                    // Join failed.
                    break;
                }

                if (log.isDebugEnabled())
                    log.debug("Output solution: " + in);

            }

            // Accept this binding set.
            if (in != null) {
            	outputBuffer.add(in);
            }
            
//            // now clear set!
//            for (int i = 1; i < set.length; i++) {
//            	set[i] = null;
//            }

		}

	}

    /**
     * Adds metadata about the {@link IHashJoinUtility} state to the stack
     * trace.
     * 
     * @param t
     *            The thrown error.
     * 
     * @return The laundered exception.
     * 
     * @throws Exception
     * 
     * @see http://sourceforge.net/apps/trac/bigdata/ticket/508 (LIMIT causes
     *      hash join utility to log errors)
     */
    private RuntimeException launderThrowable(final Throwable t) {

        final String msg = "cause=" + t + ", state=" + toString();

        if (!InnerCause.isInnerCause(t, InterruptedException.class)
                && !InnerCause.isInnerCause(t, BufferClosedException.class)) {

            /*
             * Some sort of unexpected exception.
             */

            log.error(msg, t);

        }

        return new RuntimeException(msg, t);

    }

}
