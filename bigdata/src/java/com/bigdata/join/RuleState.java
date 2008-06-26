package com.bigdata.join;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * State for a rule execution.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleState {

    protected static final Logger log = Logger.getLogger(RuleState.class);
    
    /**
     * The {@link Rule} being evaluated.
     */
    final protected IRule rule;
    
    /**
     * Helper class for managing predicates, bindings, and solutions.
     */
    final private IJoinNexus joinNexus;

    /**
     * The {@link Rule} that is being executed. 
     */
    final public IRule getRule() {
        
        return rule;
        
    }

    /**
     * A helper class that provides interoperability for the {@link IPredicate}s,
     * {@link IBindingSet}s, and {@link ISolution}s for the evaluation of the
     * <i>rule</i>.
     */
    final public IJoinNexus getElementBinding() {
        
        return joinNexus;
        
    }

//    /**
//     * Statistics for the rule evaluation.
//     */
//    final public RuleStats stats;
    
    /**
     * The {@link IAccessPath} corresponding to each {@link IPredicate} in the
     * tail of the {@link Rule}.
     * <p>
     * Note: The corresponding access path in this array is invalidated by
     * whenever a variable is (re-)bound since that will in general change the
     * selectivity of the access path (it may have a different range count).
     * <p>
     * Note: {@link #resetBindings()} clears all elements of this array.
     */
    final private IAccessPath[] accessPath;
    
    /**
     * The evaluation order for the predicates in the tail of the {@link Rule}.
     */
    final protected int[] order;
    
    /**
     * Map from the variable to the index of the predicate in tail of the
     * {@link Rule} that first binds that variable in terms of the evaluation
     * {@link #order}. This information is used by
     * {@link #clearDownstreamBindings(int)} to decide which variables should be
     * preserved. If a variable is listed here with a lower index than the
     * starting index to be cleared then its binding is NOT cleared.
     */
    final protected Map<Var,Integer> depends;
    
    /**
     * 
     * @param rule
     *            The rule.
     * @param joinNexus
     *            A helper class that provides interoperability for the
     *            {@link IPredicate}s, {@link IBindingSet}s, and
     *            {@link ISolution}s for the evaluation of the <i>rule</i>.
     */
    public RuleState(IRule rule, IJoinNexus joinNexus) {

        if (rule == null)
            throw new IllegalArgumentException();
        
        if (joinNexus== null)
            throw new IllegalArgumentException();
        
        this.rule = rule;

        this.joinNexus = joinNexus;
        
        final int tailCount = rule.getTailCount();
        
        // Allocate access path cache.
        this.accessPath = new IAccessPath[tailCount];

        // The evaluation order.
        this.order = computeEvaluationOrder();

//        if (focusStore != null && focusIndex > 0) {
//        /*
//         * Note: This stuff is specific to the RDF DB, including the ROM
//         * assumptions for the focusStore vs the DB. With the refactor the
//         * focusStore and the fusedView are already encoded into the relation
//         * associated with each tail and the order will therefore reflect the
//         * actual (or approximate) range counts for each access path.
//         */
//            
//            /*
//             * The rule of thumb is to always evaluate the predicate that
//             * will read from the focusStore 1st since we presume that any
//             * triple pattern reading on the [focusStore] will be
//             * significantly smaller than any other triple pattern reading
//             * on the fused view [focusStore + database].
//             * 
//             * However, there are counter examples. Loading schema and then
//             * data into a database is one, e.g., the wordnet ontology
//             * followed by the wordnet nouns data. Another is when the 2nd
//             * predicate in the body is none bound - you are often better
//             * off NOT running the all none bound predicate 1st.
//             * 
//             * Note: All of this logic is executed IFF the focusIndex is >
//             * 0. When the focusIndex is 0, the focusStore is already being
//             * read by the predicate that is first in the evaluation order.
//             */
//
//            /*
//             * In this alternative, we simply refuse to order a 3-unbound
//             * predicate to the 1st position in the evaluation order.
//             */
//            
//            if (false && rule.getTailPredicate(order[focusIndex])
//                    .getVariableCount() < 3) {
//
//                /*
//                 * Note: This does not work since we may have already done a
//                 * rangeCount (w/o bindings) when computing the evaluation
//                 * order so we can not undo that purely by considering the
//                 * variable bindings. An alternative is to mark as "pinned"
//                 * any part of the evaluation order that have already been
//                 * tested in the data, e.g., by retaining the rangeCounts if
//                 * computed by computeEvaluationOrder.  If we do that then
//                 * we could try this simpler test again.
//                 */
//                
////                    /*
////                     * Swap the places of those predicates in the evaluation
////                     * order such that we will evaluate the predicate at the
////                     * focusIndex 1st.
////                     */
////
////                    int tmp = order[0];
////
////                    order[0] = order[focusIndex];
////
////                    order[focusIndex] = tmp;
//
//            } else {
//
//                /*
//                 * In order to catch those counter examples we range count
//                 * the predicate that is already 1st in the evaluate order
//                 * against [focusStore+database] and then the predicate at
//                 * the focusIndex against the [focusStore]. We then reorder
//                 * the predicate at the focusIndex 1st iff it has a smaller
//                 * range count that the predicate that is already first in
//                 * the evaluation order.
//                 * 
//                 * Note: range counts here could be expensive when the index
//                 * is remote or partitioned.
//                 */
//                
//                /*
//                 * Range count for the predicate that is 1st in the
//                 * evaluation order. This will read against [focusStore +
//                 * database].
//                 */
//
//                final long rangeCount1 = getAccessPath(order[0], null/* bindingSet */)
//                        .rangeCount();
//
//                /*
//                 * Range count for the predicate at the focusIndex. This
//                 * will read against [focusStore].
//                 */
//
//                final long rangeCount2 = getAccessPath(order[focusIndex],
//                        null/* bindingSet */).rangeCount();
//
//                if (rangeCount2 < rangeCount1) {
//
//                    /*
//                     * Swap the places of those predicates in the evaluation
//                     * order such that we will evaluate the predicate at the
//                     * focusIndex 1st.
//                     */
//
//                    int tmp = order[0];
//
//                    order[0] = order[focusIndex];
//
//                    order[focusIndex] = tmp;
//
//                }
//
//            }
//            
//        }

        /*
         * The dependency graph for the variables in the tail based on the
         * selected evaluation order.
         */
        this.depends = Collections
                .unmodifiableMap(computeVariableDependencyMap(order));

//        // initialize the bindings from the predicate declarations.
//        resetBindings();

//        // collects statistics on the rule.
//        this.stats = new RuleStats(this);
       
    }

    /**
     * Externalizes the rule and the evaluation order.
     */
    public String toString() {

        return toString(null);
        
    }
    
    /**
     * Externalizes the rule displaying variable names, their bindings,
     * constants, and the evaluation order.
     * 
     * @param bindingSet
     *            When non-<code>null</code>, the current variable bindings
     *            will be displayed. Otherwise, the names of variables will be
     *            displayed rather than their bindings.
     * 
     * @todo There should be a way to resolve the bindings against a dictionary.
     *       perhaps for {@link Rule} but definately for {@link RuleState}.
     *       This makes it MUCH easier to figure out what is going on when the
     *       data are keys in a dictionary, which is the case for the RDF DB. Of
     *       course, doing that efficiently is a JOIN :-) but it can be done
     *       with a cache in front of a point lookup as well.
     */
    public String toString(IBindingSet bindingSet) {

        final StringBuilder sb = new StringBuilder();
        
        sb.append(rule.getName());
        
        sb.append(" : ");
        
        // write out bindings for the tail.
        
        int i = 0;
        
        for(Iterator<IPredicate> itr = rule.getTail(); itr.hasNext(); i++) {

            final IPredicate pred = itr.next();
            
            sb.append(pred.toString(bindingSet));
            
//            if (bindingSet == null) {

                // displays the evaluation order as an index on the predicate.

                sb.append("[" + order[i] + "]");
                
//            }
            
            if (itr.hasNext()) {
                
                sb.append(", ");
                
            }
            
        }

        sb.append(" -> ");
        
        // write out bindings for the head.
        {
            
            sb.append(rule.getHead().toString(bindingSet));

        }
        
        return sb.toString();

    }

    /**
     * Computes and returns the evaluation order (no side-effects).
     * <p>
     * The evaluation {@link #order}[] is determined by analysis of the
     * propagation of bindings. The most selective predicate is choosen first
     * (having the fewest unbound variables with ties broken by a range count on
     * the data) and "fake" bindings are propagated to the other predicates in
     * the tail. This process is repeated until all variables are bound and an
     * evaluation order has been determined.
     * 
     * @return The evaluation order. The indices in this array are correlated
     *         1:1 with the predicates in tail of the {@link Rule}.
     */
    public int[] computeEvaluationOrder() {

        // use private bindingSet to avoid side-effects.
        final IBindingSet bindingSet = joinNexus.newBindingSet(rule);

        final int tailCount = rule.getTailCount();

        final int[] order = new int[tailCount];

        final boolean[] used = new boolean[tailCount];

        // clear array. -1 is used to detect logic errors.
        for (int i = 0; i < order.length; i++)
            order[i] = -1;

        for (int i = 0; i < tailCount; i++) {

            int index = -1;
            int minVarCount = Integer.MAX_VALUE;
            long minRangeCount = Long.MAX_VALUE;
            
            for( int j=0; j<tailCount; j++) {
                
                if(used[j]) continue; // already in the evaluation order. 
                
                final int varCount = rule.getTail(j)
                        .getVariableCount();

                if (varCount < minVarCount) {

                    index = j;

                    minVarCount = varCount;

                } else if (true && varCount == minVarCount) {

                    /*
                     * Tweaks the evaluation order for predicates where the #of
                     * variable bindings is the same by examining the range
                     * counts.
                     * 
                     * Note: In doing this, we disregard the bindings that were
                     * propagated since they are -1 and will NOT match anything
                     * anywhere!
                     * 
                     * Note: In the case where some other predicate is already
                     * first in the evaluation order by the virtue of having
                     * more variables bound this tweak is purely heuristic
                     * regardless of the fact that it considers the data. The
                     * reason is that the actual bindings that are propagated
                     * during the execution of the rule will determine which of
                     * the two predicates under consideration is, in fact, more
                     * selective in the data. However, in the special case where
                     * the two predicates are competing for the 1st position in
                     * the evaluation order, this "tweak" is exact.
                     * 
                     * @todo Some tails use the same constant pattern in both
                     * predicates. E.g., rdfs11 (u subClassOf v) (v subClassOf
                     * x). In these cases comparing range counts is pointless
                     * and could be avoided by testing for this pattern.
                     */
                    
                    if(minRangeCount == Integer.MAX_VALUE) {
                        
                        // range count of the current best choice (computed
                        // lazily).
                        minRangeCount = getAccessPath(index, null/* bindingSet */)
                                .rangeCount();

                    }

                    // range count of the current predicate under
                    // examination.
                    final long rangeCount = getAccessPath(j, null/* bindingSet */)
                            .rangeCount();
                    
                    if (rangeCount < minRangeCount) {

                        /*
                         * choose the predicate that is more selective given
                         * the variable bindings.
                         */
                        
                        index = j;
                        
                        minVarCount = varCount;
                        
                        minRangeCount = rangeCount;
                        
                    }
                    
                }
                
            }

            if (index == -1)
                throw new AssertionError();
            
            if (used[index])
                throw new AssertionError("Attempting to reuse predicate: index="+i+"\n"+this);
            
            order[i] = index;
            
            used[index] = true;
            
            // set fake bindings for this predicate.
            {
                
                final IPredicate pred = rule.getTail(index);

                final int arity = pred.arity();

                for (int z = 0; z < arity; z++) {

                    final IVariableOrConstant v = pred.get(z);

                    if (v.isVar()) {

                        bindingSet.set((IVariable) v, FAKE);

                    }

                }
                
            }
            
        }

        if (log.isInfoEnabled())
            log.info(rule.getName() + ": order=" + Arrays.toString(order));
        
        return order;
        
    }
    /**
     * Used to mark fake bindings by {@link #computeEvaluationOrder()}.
     */
    private static final transient IConstant<String> FAKE = new Constant<String>(
            "Fake Binding");
    
    /**
     * Given an evaluation <i>order</i>, construct the dependency graph for the
     * variables in the tail of the rule.
     * 
     * @param order
     *            The evaluation order.
     * 
     * @return The map from variable to the 1st predicate in evaluation order
     *         where it is bound. The values of this map are indices into
     *         <i>order</i>.
     */
    public Map<Var,Integer> computeVariableDependencyMap(int[] order) {
        
        if (order == null)
            throw new IllegalArgumentException();
        
        if (order.length != rule.getTailCount())
            throw new IllegalArgumentException();
        
        final Map<Var,Integer> depends = new HashMap<Var,Integer>();
        
        final int tailCount = rule.getTailCount();
        
        for(int i=0; i<tailCount; i++) {
            
            final IPredicate pred = rule.getTail(order[i]);
            
            final int arity = pred.arity();
            
            for(int j=0; j<arity; j++) {

                final IVariableOrConstant t = pred.get(j);
                
                if(t.isVar()) {
                
                    final Var var = (Var)t;
                
                    if(!depends.containsKey(var)) {
                        
                        depends.put(var, i);
                        
                    }
                
                }
            
            }
                            
        }
        
        return depends;
        
    }
    
    /**
     * Initialize the bindings from the constants and variables in the rule.
     * <p>
     * Note: This MUST be invoked before the rule is executed. If you do not
     * clear the bindings then the old bindings will make it appear as if your
     * variables are all bound to constants and the rule will NOT select the
     * correct data.
     */
    protected void resetBindings(IBindingSet bindings) {

        bindings.clearAll();

        for (int i = 0; i < accessPath.length; i++) {

            accessPath[i] = null;

        }

    }
    
    /**
     * <p>
     * Return the current binding for the variable or constant.
     * </p>
     * 
     * @param var
     *            The variable or constant.
     * 
     * @return Its binding. The binding will be <code>null</code> if a
     *         variable is not currently bound.
     * 
     * @throws NullPointerException
     *             if <i>var</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the variable is not used in the rule.
     * @throws IllegalArgumentException
     *             if var is a constant.
     * 
     * @see #set(Var, Object)
     * @see #bind(int, SPO)
     */
    public Object get(IBindingSet bindings,IVariableOrConstant var) {
    
        return get(bindings,var, true);
        
    }
    
    /**
     * <p>
     * Return the current binding for the variable or constant.
     * </p>
     * 
     * @param var
     *            The variable or constant.
     * @param required
     *            When <code>true</code> an exception is reported if the
     *            variable is not used in the rule. Note that specializing a
     *            rule often results in variables being replaced by constants
     *            such that an {@link IConstraint} might no longer be evaluable
     *            for the rule in terms of that variable.
     * 
     * @return Its binding. The binding will be <code>null</code> if a
     *         variable is not currently bound or if the variable is not used in
     *         the rule and <code>required := false</code>.
     * 
     * @throws NullPointerException
     *             if <i>var</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <code>required := true</code> and the variable is not
     *             used in the rule.
     * @throws IllegalArgumentException
     *             if var is a constant.
     * 
     * @see #set(Var, Object)
     * @see #bind(int, SPO)
     * 
     * FIXME it is not entirely satisfactory to have variable names disappear
     * from rules as they are specialized. It might be better to define a
     * BoundVar (extends Var) whose id was the bound value and which reported
     * true for both isVariable() and isConstant(). However this would mean that
     * var(x) != var(x,id) where the latter is bound to a constant. Or if
     * {@link Var} is rule-local (or rule execution thread local) then it could
     * define set(Object) and get():Object to access its binding. A rule that
     * was specialized would then flag bound variables as immutable.
     */
    public Object get(IBindingSet bindings,IVariableOrConstant var, boolean required) {
        
//        if (var == null)
//            throw new NullPointerException();

        if (var.isConstant()) {

            return var.get();
            
        }

        if (required && !rule.isDeclared((IVariable)var)) {

            throw new IllegalArgumentException("Not declared: " + var + " by "
                    + rule);
            
        }
        
        return bindings.get((IVariable) var);
                
    }
    
    /**
     * Binds the variable.
     * <p>
     * Note: The cached {@link IAccessPath} for a predicate is cleared if a
     * variable binding is set on that predicate.
     * 
     * @param var
     *            A variable that appears in that predicate.
     * @param val
     *            The value to be bound on the variable.
     * 
     * @throws NullPointerException
     *             if the variable is null.
     * @throws IndexOutOfBoundsException
     *             if the predicate index is out of range.
     * 
     * @see #bind(int, Object)
     */
//    * @throws IllegalArgumentException
//    *             if the variable does not appear in the rule.
    public void set(IBindingSet bindings, IVariable var, Object val) {

//        // verify variable declared by the rule.
//        if(!rule.isDeclared(var)) throw new IllegalArgumentException();

        // bind the variable.
        bindings.set(var, new Constant(val));

        final int tailCount = rule.getTailCount();
        
        // clear cached access path for preds using that variable.
        for (int i = 0; i < tailCount; i++) {

            final IPredicate pred = rule.getTail(i);

            final int arity = pred.arity();

            for (int j = 0; j < arity; j++) {

                if (pred.get(j) == var) {

                    accessPath[i] = null;

                }

            }

        }

    }

    /**
     * Binds variables from a visited element.
     * <p>
     * Note: The bindings are propagated before the constraints are verified so
     * this method will have a side-effect on the bindings even if the
     * constraints were not satisified.
     * 
     * @param index
     *            The index of the {@link IPredicate} in the body of the
     *            {@link Rule}.
     * @param e
     *            An element materialized by the {@link IAccessPath} for that
     *            {@link IPredicate}.
     * 
     * @return <code>true</code> unless the new bindings would violate any of
     *         the {@link IConstraint}s declared for the {@link Rule}).
     * 
     * @throws NullPointerException
     *             if <i>e</i> is <code>null</code>.
     * @throws IndexOutOfBoundsException
     *             if the <i>index</i> is out of bounds.
     * 
     * @see #clearDownstreamBindings(IBindingSet bindingSet, int index)
     */
    @SuppressWarnings("unchecked")
    public boolean bind(IBindingSet bindings,int index, Object e) {
       
        // propagate bindings from the visited object into the binding set.
        joinNexus.copyValues(e, rule.getTail(index), bindings);

        // verify constraints.
        return rule.isLegal(bindings);
        
    }
    
    /**
     * Clear downstream bindings in the evaluation {@link #order}[]. If a
     * variable in a downstream predicate was 1st bound by an upstream predicate
     * as identified by {@link #depends} then its value is NOT cleared.
     * <p>
     * Note: You MUST {@link #resetBindings()} before you evaluate a rule.
     * <p>
     * Note: You MUST {@link #clearDownstreamBindings(IBindingSet,int)} bindings
     * before you (re-)evaluate a subquery. Failure to do so will leave stale
     * bindings in place which will cause {@link #getAccessPath(int)} to
     * identify the wrong access path, and hence select the wrong data.
     * 
     * @param index
     *            The index of the predicate whose values you intend to
     *            {@link #bind(IBindingSet,int, Object)}.
     */
    protected void clearDownstreamBindings(IBindingSet bindings,int index) {

        final int tailCount = rule.getTailCount();
        
        for (int i = index; i < tailCount; i++) {

            final IPredicate pred = rule.getTail(order[index]);

            final int arity = pred.arity();

            for (int j = 0; j < arity; j++) {

                final IVariableOrConstant t = pred.get(j);

                if (t.isVar()) {

                    final int k = depends.get((Var) t);

                    if (k >= index) {

                        set(bindings, (Var) t, null);

                    }

                }

            }

        }
        
    }
    
    /**
     * Return the {@link IAccessPath} that would be used to read from the
     * selected tail {@link IPredicate}.
     * <p>
     * Note: a cache is maintained by the rule for the access paths. If the
     * cache does not have an entry for the desired access path then one is
     * obtained and placed into the cache before being returned to the caller.
     * The cache is invalidated by {@link #resetBindings()} and (on a selective
     * basis) by {@link #bind(int, Object)} and {@link #set(IVariable, Object)}.
     * 
     * @param index
     *            The index into tail of the {@link Rule}.
     * 
     * @return The {@link IAccessPath}.
     * 
     * @throws IndexOutOfBoundsException
     *             if index is out of bounds.
     * 
     * FIXME The side-effect on {@link #accessPath}[] means that this class is
     * NOT thread-safe. A thread-local weak reference to an access path cache
     * might fix that. However, I am not yet sure if this is a problem.
     */
    public IAccessPath getAccessPath(IBindingSet bindingSet, final int index) {
        
        // check the cache.
        IAccessPath accessPath = this.accessPath[index];
        
        if (accessPath == null) {

            accessPath = getAccessPath(index, bindingSet);

            // update the cache.
            this.accessPath[index] = accessPath;
        
        }
    
        return accessPath;

    }

    /**
     * Return the iterator that should be used to read from the selected
     * {@link IPredicate}.
     * 
     * @param index
     *            The index of the {@link IPredicate} in the tail of the
     *            {@link Rule}.
     * 
     * @return The iterator.
     * 
     * @throws IndexOutOfBoundsException
     *             if index is out of bounds.
     */
    public IChunkedOrderedIterator iterator(IBindingSet bindingSet,
            final int index) {

        return getAccessPath(bindingSet, index).iterator();
        
    }
    
    /**
     * Return the {@link IAccessPath} that would be used to read from the
     * selected {@link IPredicate} (no caching).
     * 
     * @param index
     *            The index of the {@link IPredicate} in the tail of the
     *            {@link Rule}.
     * @param bindingSet
     *            When non-<code>null</code>, the bindings will be used to
     *            generate the {@link IAccessPath}. When <code>null</code>
     *            the {@link IAccessPath} will use wildcards in every position
     *            where the predicate declares a variable.
     * 
     * @return The {@link IAccessPath}.
     * 
     * @throws IndexOutOfBoundsException
     *             if index is out of bounds.
     * @throws RuntimeException
     *             if the name of the relation can not be resolved by the
     *             {@link IJoinNexus} to an {@link IRelation} instance.
     */
    public IAccessPath getAccessPath(final int index, final IBindingSet bindingSet) {

        final IPredicate predicate;

        if (bindingSet != null) {

            // based on the current bindings.

            predicate = rule.getTail(index).asBound(bindingSet);

        } else {

            // as declared by the predicate (no bindings).

            predicate = rule.getTail(index);

        }

        // The name of the relation that the predicate will query.
        final IRelationName relationName = predicate.getRelation();
        
        // Resolve the relation name to the IRelation object.
        final IRelation relation = joinNexus.getRelationLocator().getRelation(
                relationName);
        
        assert relation != null;
        
        // find the best access path for the predicate for that relation.
        final IAccessPath accessPath = relation.getAccessPath(predicate);
        
        assert accessPath != null;
        
        // return that access path.
        return accessPath;

    }
    
}
