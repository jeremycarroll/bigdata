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
package com.bigdata.rdf.graph.impl.bd;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.graph.IBindingExtractor;
import com.bigdata.rdf.graph.IBindingExtractor.IBinder;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASScheduler;
import com.bigdata.rdf.graph.IGASSchedulerImpl;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IGASStats;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.IReducer;
import com.bigdata.rdf.graph.impl.GASEngine;
import com.bigdata.rdf.graph.impl.GASState;
import com.bigdata.rdf.graph.impl.bd.BigdataGASEngine.BigdataGraphAccessor;
import com.bigdata.rdf.graph.impl.scheduler.CHMScheduler;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueImpl;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.service.BigdataNativeServiceOptions;
import com.bigdata.rdf.sparql.ast.service.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.service.CustomServiceFactory;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.striterator.ChunkedArrayIterator;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A SERVICE that exposes {@link IGASProgram}s for SPARQL execution.
 * <p>
 * For example, the following would run a depth-limited BFS traversal:
 * 
 * <pre>
 * PREFIX gas: <http://www.bigdata.com/rdf/gas#>
 * #...
 * SERVICE &lt;gas#service&gt; {
 *    gas:program gas:gasClass "com.bigdata.rdf.graph.analytics.BFS" .
 *    gas:program gas:in &lt;IRI&gt; . # one or more times, specifies the initial frontier.
 *    gas:program gas:out ?out . # exactly once - will be bound to the visited vertices.
 *    gas:program gas:maxIterations 4 . # optional limit on breadth first expansion.
 *    gas:program gas:maxVisited 2000 . # optional limit on the #of visited vertices.
 *    gas:program gas:nthreads 4 . # specify the #of threads to use (optional)
 * }
 * </pre>
 * 
 * Or the following would run the FuzzySSSP algorithm.
 * 
 * <pre>
 * PREFIX gas: <http://www.bigdata.com/rdf/gas#>
 * #...
 * SERVICE &lt;gas:service&gt; {
 *    gas:program gas:gasClass "com.bigdata.rdf.graph.analytics.FuzzySSSP" .
 *    gas:program gas:in &lt;IRI&gt; . # one or more times, specifies the initial frontier.
 *    gas:program gas:target &lt;IRI&gt; . # one or more times, identifies the target vertices and hence the paths of interest.
 *    gas:program gas:out ?out . # exactly once - will be bound to the visited vertices laying within N-hops of the shortest paths.
 *    gas:program gas:maxIterations 4 . # optional limit on breadth first expansion.
 *    gas:program gas:maxVisited 2000 . # optional limit on the #of visited vertices.
 * }
 * </pre>
 * 
 * FIXME Also allow the execution of gas workflows, such as FuzzySSSP. A workflow
 * would be more along the lines of a Callable, but one where the initial source
 * and/or target vertices could be identified. Or have an interface that wraps
 * the analytics (including things like FuzzySSSP) so they can declare their own
 * arguments for invocation as a SERVICE.
 * 
 * TODO The input frontier could be a variable, in which case we would pull out
 * the column for that variable rather than running the algorithm once per
 * source binding set, right? Or maybe not.
 * 
 * TODO Allow {@link IReducer} that binds the visited vertex and also the
 * dynamic state associated with that vertex. For BFS and SSSP, this could be
 * depth/distance and the predecessor (for path information). For BFS and SSSP,
 * we could also have a specific target vertex (or vertices) and then report out
 * the path for that vertex/vertices. This would significantly reduce the data
 * reported back. (Could we run SSSP in both directions to accelerate the
 * convergence?)
 * 
 * TODO Also support export. This could be easily done using a SPARQL SELECT
 * 
 * <pre>
 * SELECT ?src ?tgt ?edgeWeight {
 *    <<?src linkType ?tgt> propertyType ?edgeWeight>
 * }
 * </pre>
 * 
 * or (if you have a simple topology without edge weights)
 * 
 * <pre>
 * SELECT ?src ?tgt bind(?edgeWeight,1) {
 *    ?src linkType ?tgt
 * }
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class GASService implements CustomServiceFactory {

    public interface Options {
        
        /**
         * The namespace used for bigdata GAS API.
         */
        String NAMESPACE = "http://www.bigdata.com/rdf/gas#";

        /**
         * The URL at which the {@link GASService} will respond.
         */
        URI SERVICE_KEY = new URIImpl(NAMESPACE + "service");
        
        /**
         * Used as the subject in the GAS SERVICE invocation pattern.
         */
        URI PROGRAM = new URIImpl(NAMESPACE + "program");

        /**
         * Magic predicate identifies the fully qualified class name of the
         * {@link IGASProgram} to be executed.
         */
        URI GAS_CLASS = new URIImpl(NAMESPACE + "gasClass");

        /**
         * The #of threads that will be used to expand the frontier in each
         * iteration of the algorithm (optional, default
         * {@value #DEFAULT_NTHREADS}).
         * 
         * @see #DEFAULT_NTHREADS
         */
        URI NTHREADS = new URIImpl(NAMESPACE + "nthreads");
        
        int DEFAULT_NTHREADS = 4;

        /**
         * This option determines whether the traversal of the graph will
         * interpret the edges as directed or undirected.
         * 
         * @see IGASContext#setDirectedTraversal(boolean)
         */
        URI DIRECTED_TRAVERSAL = new URIImpl(NAMESPACE + "directedTraversal");
        
        boolean DEFAULT_DIRECTED_TRAVERSAL = true;
        
        /**
         * The maximum #of iterations for the GAS program (optional, default
         * {@value #DEFAULT_MAX_ITERATIONS}).
         * 
         * @see #DEFAULT_MAX_ITERATIONS
         * @see IGASContext#setMaxIterations(int)
         */
        URI MAX_ITERATIONS = new URIImpl(NAMESPACE + "maxIterations");
        
        int DEFAULT_MAX_ITERATIONS = Integer.MAX_VALUE;

        /**
         * The maximum #of vertices in the visited set for the GAS program
         * (optional, default {@value #DEFAULT_MAX_VISITED}).
         * 
         * @see #DEFAULT_MAX_VISITED
         * @see IGASContext#setMaxVisited(int)
         */
        URI MAX_VISITED = new URIImpl(NAMESPACE + "maxVisited");
        
        int DEFAULT_MAX_VISITED = Integer.MAX_VALUE;

        /**
         * An optional constraint on the types of links that will be visited by
         * the algorithm.
         * <p>
         * Note: When this option is used, the scatter and gather will not visit
         * the property set for the vertex. Instead, the graph is treated as if
         * it were an unattributed graph and only mined for the connectivity
         * data (which may include a link weight).
         * 
         * @see IGASContext#setLinkType(URI)
         */
        URI LINK_TYPE = new URIImpl(NAMESPACE + "linkType");

        /**
         * An optional constraint on the types of the link attributes that will
         * be visited by the algorithm - the use of this option is required if
         * you want to process some specific link weight rather than the simple
         * topology of the graph.
         * 
         * @see IGASContext#setLinkAttributeType(URI)
         */
        URI LINK_ATTR_TYPE = new URIImpl(NAMESPACE + "linkAttrType");

        /**
         * The {@link IGASScheduler} (default is {@link #DEFAULT_SCHEDULER}).
         * Class must implement {@link IGASSchedulerImpl}.
         */
        URI SCHEDULER_CLASS = new URIImpl(NAMESPACE + "schedulerClass");
 
        Class<? extends IGASSchedulerImpl> DEFAULT_SCHEDULER = CHMScheduler.class;

        /**
         * Magic predicate used to specify a vertex in the initial frontier.
         */
        URI IN = new URIImpl(NAMESPACE + "in");

        /**
         * Magic predicate used to specify a variable that will become bound to
         * each vertex in the visited set for the analytic. {@link #OUT} is
         * always bound to the visited vertices. The other "out" variables are
         * bound to state associated with the visited vertices in an algorithm
         * dependent manner.
         * 
         * @see IGASProgram#getBinderList()
         */
        URI OUT = new URIImpl(NAMESPACE + "out");

        URI OUT1 = new URIImpl(NAMESPACE + "out1");

        URI OUT2 = new URIImpl(NAMESPACE + "out2");

        URI OUT3 = new URIImpl(NAMESPACE + "out3");

        URI OUT4 = new URIImpl(NAMESPACE + "out4");

        URI OUT5 = new URIImpl(NAMESPACE + "out5");

        URI OUT6 = new URIImpl(NAMESPACE + "out6");

        URI OUT7 = new URIImpl(NAMESPACE + "out7");

        URI OUT8 = new URIImpl(NAMESPACE + "out8");

        URI OUT9 = new URIImpl(NAMESPACE + "out9");
        
    }

    static private transient final Logger log = Logger
            .getLogger(GASService.class);

    /**
     * The list of all out variables.
     */
    static private List<URI> OUT_VARS = Collections.unmodifiableList(Arrays
            .asList(new URI[] { Options.OUT, Options.OUT1, Options.OUT2,
                    Options.OUT3, Options.OUT4, Options.OUT5, Options.OUT6,
                    Options.OUT7, Options.OUT8, Options.OUT9 }));

    private final BigdataNativeServiceOptions serviceOptions;

    public GASService() {

        serviceOptions = new BigdataNativeServiceOptions();
        
        /*
         * TODO Review decision to make this a runFirst service. The rational is
         * that this service can only apply a very limited set of restrictions
         * during query, therefore it will often make sense to run it first.
         * However, the fromTime and toTime could be bound by the query and the
         * service can filter some things more efficiently internally than if we
         * generated a bunch of intermediate solutions for those things.
         */
        serviceOptions.setRunFirst(true);
        
    }

    /**
     * The known URIs.
     * <p>
     * Note: We can recognize anything in {@link Options#NAMESPACE}, but the
     * predicate still has to be something that we know how to interpret.
     */
    static final Set<URI> gasUris;
    
    static {
        
        final Set<URI> set = new LinkedHashSet<URI>();
    
        set.add(Options.PROGRAM);
        
        gasUris = Collections.unmodifiableSet(set);
        
    }

    @Override
    public IServiceOptions getServiceOptions() {

        return serviceOptions;
        
    }

    /**
     * NOP
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void startConnection(BigdataSailConnection conn) {
        // NOP
    }

    @Override
    public ServiceCall<?> create(final ServiceCallCreateParams params) {

        if (params == null)
            throw new IllegalArgumentException();

        final AbstractTripleStore store = params.getTripleStore();

        if (store == null)
            throw new IllegalArgumentException();

        /*
         * Create and return the ServiceCall object which will execute this
         * query.
         */

        return new GASServiceCall(store, params.getServiceNode(),
                getServiceOptions());

    }

    /**
     * Execute the service call (run the GAS program).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * 
     *         TODO Validate the service call parameters, including whether they
     *         are understood by the specific algorithm.
     */
    private static class GASServiceCall<VS, ES, ST> implements BigdataServiceCall {

        private final AbstractTripleStore store;
        private final GraphPatternGroup<IGroupMemberNode> graphPattern;
        private final IServiceOptions serviceOptions;
        
        // options extracted from the SERVICE's graph pattern.
        private final int nthreads;
        private final boolean directedTraversal;
        private final int maxIterations;
        private final int maxVisited;
        private final URI linkType, linkAttrType;
        private final Class<IGASProgram<VS, ES, ST>> gasClass;
        private final Class<IGASSchedulerImpl> schedulerClass;
        private final Value[] initialFrontier;
        private final IVariable<?>[] outVars;

        public GASServiceCall(final AbstractTripleStore store,
                final ServiceNode serviceNode,
                final IServiceOptions serviceOptions) {

            if (store == null)
                throw new IllegalArgumentException();

            if (serviceNode == null)
                throw new IllegalArgumentException();

            if (serviceOptions == null)
                throw new IllegalArgumentException();

            this.store = store;

            this.graphPattern = serviceNode.getGraphPattern();

            this.serviceOptions = serviceOptions;

            this.nthreads = ((Literal) getOnlyArg(
                    Options.PROGRAM,
                    Options.NTHREADS,
                    store.getValueFactory().createLiteral(
                            Options.DEFAULT_NTHREADS))).intValue();

            this.directedTraversal = ((Literal) getOnlyArg(Options.PROGRAM,
                    Options.DIRECTED_TRAVERSAL, store.getValueFactory()
                            .createLiteral(Options.DEFAULT_DIRECTED_TRAVERSAL)))
                    .booleanValue();

            this.maxIterations = ((Literal) getOnlyArg(Options.PROGRAM,
                    Options.MAX_ITERATIONS, store.getValueFactory()
                            .createLiteral(Options.DEFAULT_MAX_ITERATIONS)))
                    .intValue();

            this.maxVisited = ((Literal) getOnlyArg(
                    Options.PROGRAM,
                    Options.MAX_VISITED,
                    store.getValueFactory().createLiteral(
                            Options.DEFAULT_MAX_VISITED))).intValue();

            this.linkType = (URI) getOnlyArg(Options.PROGRAM,
                    Options.LINK_TYPE, null/* default */);

            this.linkAttrType = (URI) getOnlyArg(Options.PROGRAM,
                    Options.LINK_ATTR_TYPE, null/* default */);

            // GASProgram (required)
            {
                
                final Literal tmp = (Literal) getOnlyArg(Options.PROGRAM,
                        Options.GAS_CLASS);

                if (tmp == null)
                    throw new IllegalArgumentException(
                            "Required predicate not specified: "
                                    + Options.GAS_CLASS);

                final String className = tmp.stringValue();

                final Class<?> cls;
                try {
                    cls = Class.forName(className);
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException("No such class: "
                            + className);
                }

                if (!IGASProgram.class.isAssignableFrom(cls))
                    throw new IllegalArgumentException(Options.GAS_CLASS
                            + " must extend " + IGASProgram.class.getName());

                this.gasClass = (Class<IGASProgram<VS, ES, ST>>) cls;

            }

            // Scheduler (optional).
            {
                
                final Literal tmp = (Literal) getOnlyArg(Options.PROGRAM,
                        Options.SCHEDULER_CLASS);

                if (tmp == null) {

                    this.schedulerClass = null;

                } else {
                    
                    final String className = tmp.stringValue();

                    final Class<?> cls;
                    try {
                        cls = Class.forName(className);
                    } catch (ClassNotFoundException e) {
                        throw new IllegalArgumentException("No such class: "
                                + className);
                    }

                    if (!IGASSchedulerImpl.class.isAssignableFrom(cls))
                        throw new IllegalArgumentException(
                                Options.SCHEDULER_CLASS + " must extend "
                                        + IGASSchedulerImpl.class.getName());

                    this.schedulerClass = (Class<IGASSchedulerImpl>) cls;

                }

            }
            
            // Initial frontier.
            this.initialFrontier = getArg(Options.PROGRAM, Options.IN);

            /*
             * The output variable (bound to the visited set).
             * 
             * TODO This does too much work. It searches the group graph pattern
             * 10 times, when we could do just one pass and find everything.
             */
            {

                this.outVars = new IVariable[10];

                int i = 0;

                for (URI uri : OUT_VARS) {

                    this.outVars[i++] = getVar(Options.PROGRAM, uri);

                }

            }

        }

        /**
         * Return the variable associated with the first instandce of the
         * specified subject and predicate in the service's graph pattern. Only
         * the simple {@link StatementPatternNode}s are visited.
         * 
         * @param s
         *            The subject.
         * @param p
         *            The predicate.
         * 
         * @return The variable -or- <code>null</code> if the specified subject
         *         and predicate do not appear.
         */
        private IVariable<?> getVar(final URI s, final URI p) {

            if (s == null)
                throw new IllegalArgumentException();
            if (p == null)
                throw new IllegalArgumentException();

            List<Value> tmp = null;

            final Iterator<IGroupMemberNode> itr = graphPattern.getChildren()
                    .iterator();

            while (itr.hasNext()) {

                final IGroupMemberNode child = itr.next();

                if (!(child instanceof StatementPatternNode))
                    continue;

                final StatementPatternNode sp = (StatementPatternNode) child;

                // s and p are constants.
                if (!sp.s().isConstant())
                    continue;
                if (!sp.p().isConstant())
                    continue;

                // constants match.
                if (!s.equals(sp.s().getValue()))
                    continue;
                if (!p.equals(sp.p().getValue()))
                    continue;

                if (tmp == null)
                    tmp = new LinkedList<Value>();

                // found an o.
                return ((VarNode)sp.o()).getValueExpression();

            }

            return null; // not found.

        }
        
        /**
         * Return the object bindings from the service's graph pattern for the
         * specified subject and predicate. Only the simple
         * {@link StatementPatternNode}s are visited.
         * 
         * @param s
         *            The subject.
         * @param p
         *            The predicate.
         * 
         * @return An array containing one or more bindings -or-
         *         <code>null</code> if the specified subject and predicate do
         *         not appear.
         */
        private Value[] getArg(final URI s, final URI p) {

            if (s == null)
                throw new IllegalArgumentException();
            if (p == null)
                throw new IllegalArgumentException();

            List<Value> tmp = null;

            final Iterator<IGroupMemberNode> itr = graphPattern.getChildren()
                    .iterator();

            while (itr.hasNext()) {

                final IGroupMemberNode child = itr.next();

                if (!(child instanceof StatementPatternNode))
                    continue;

                final StatementPatternNode sp = (StatementPatternNode) child;

                // s and p are constants.
                if (!sp.s().isConstant())
                    continue;
                if (!sp.p().isConstant())
                    continue;

                // constants match.
                if (!s.equals(sp.s().getValue()))
                    continue;
                if (!p.equals(sp.p().getValue()))
                    continue;

                if (tmp == null)
                    tmp = new LinkedList<Value>();

                // found an o.
                tmp.add(sp.o().getValue());

            }

            if (tmp == null)
                return null;

            return tmp.toArray(new Value[tmp.size()]);

        }

        /**
         * Return the sole {@link Value} for the given s and p.
         * 
         * @param s
         *            The subject.
         * @param p
         *            The predicate.
         *            
         * @return The sole {@link Value} for that s and p -or-
         *         <code>null</code> if no value was given.
         *         
         * @throws RuntimeException
         *             if there are multiple values.
         */
        private Value getOnlyArg(final URI s, final URI p) {

            final Value[] tmp = getArg(s, p);

            if (tmp == null)
                return null;

            if (tmp.length > 1)
                throw new IllegalArgumentException("Multiple values: s=" + s
                        + ", p=" + p);

            return tmp[0];
            
        }

        /**
         * Return the sole {@link Value} for the given s and p and the default
         * value if no value was explicitly provided.
         * 
         * @param s
         *            The subject.
         * @param p
         *            The predicate.
         * @param def
         *            The default value.
         * 
         * @return The sole {@link Value} for that s and p -or- the default
         *         value if no value was given.
         * 
         * @throws RuntimeException
         *             if there are multiple values.
         */
        private Value getOnlyArg(final URI s, final URI p, final Value def) {

            final Value tmp = getOnlyArg(s, p);

            if (tmp == null)
                return def;

            return tmp;
            
        }
        
        @Override
        public IServiceOptions getServiceOptions() {

            return serviceOptions;
            
        }

        /**
         * Execute the GAS program.
         * <p>
         * {@inheritDoc}
         */
        @Override
        public ICloseableIterator<IBindingSet> call(
                final IBindingSet[] bindingSets) throws Exception {

            /*
             * Try/finally pattern to setup the BigdataGASEngine, execute the
             * algorithm, and return the results.
             */
            IGASEngine gasEngine = null;

            try {

                gasEngine = newGasEngine(store.getIndexManager(), nthreads);

                if (schedulerClass != null) {

                    ((GASEngine) gasEngine).setSchedulerClass(schedulerClass);

                }

                final IGraphAccessor graphAccessor = newGraphAccessor(store);

                final IGASProgram<VS, ES, ST> gasProgram = newGASProgram(gasClass);

                final IGASContext<VS, ES, ST> gasContext = gasEngine.newGASContext(
                        graphAccessor, gasProgram);

                gasContext.setDirectedTraversal(directedTraversal);
                
                gasContext.setMaxIterations(maxIterations);

                gasContext.setMaxVisited(maxVisited);

                // Optional link type constraint.
                if (linkType != null)
                    gasContext.setLinkType(linkType);

                // Optional link attribute constraint.
                if (linkAttrType != null)
                    gasContext.setLinkAttributeType(linkAttrType);

                final IGASState<VS, ES, ST> gasState = gasContext.getGASState();

                // TODO We should look at this when extracting the parameters from the SERVICE's graph pattern.
//                final FrontierEnum frontierEnum = gasProgram
//                        .getInitialFrontierEnum();

                if (initialFrontier != null) {

                    /*
                     * FIXME Why can't we pass in the Value (with a defined IV)
                     * and not the IV? This should work. Passing in the IV is
                     * against the grain of the API and the generalized
                     * abstraction as Values. Of course, having the IV is
                     * necessary since this is an internal, high performance,
                     * and close to the indices operation.
                     */
                    final IV[] tmp = new IV[initialFrontier.length];

                    // Setup the initial frontier.
                    int i = 0;
                    for (Value startingVertex : initialFrontier) {

                        tmp[i++] = ((BigdataValue) startingVertex).getIV();

                    }

                    // set the frontier.
                    gasState.setFrontier(gasContext, tmp);

                }

                // Run the analytic.
                final IGASStats stats = (IGASStats) gasContext.call();

                if (log.isInfoEnabled()) {
                    final StringBuilder sb = new StringBuilder();
                    sb.append("GAS");
                    sb.append(": analytic=" + gasProgram.getClass().getSimpleName());
                    sb.append(", nthreads=" + nthreads);
                    sb.append(", scheduler=" + ((GASState<VS, ES, ST>)gasState).getScheduler().getClass().getSimpleName());
                    sb.append(", gasEngine=" + gasEngine.getClass().getSimpleName());
                    sb.append(", stats=" + stats);
                    log.info(sb.toString());
                }

                /*
                 * Bind output variables (if any).
                 */

                final IBindingSet[] out = gasState
                        .reduce(new BindingSetReducer<VS, ES, ST>(outVars,
                                store, gasProgram, gasContext));

                return new ChunkedArrayIterator<IBindingSet>(out);

            } finally {

                if (gasEngine != null) {

                    gasEngine.shutdownNow();

                    gasEngine = null;

                }

            }

        }

        /**
         * Class used to report {@link IBindingSet}s to the {@link GASService}.
         * {@link IGASProgram}s can customize the way in which they interpret
         * the declared variables by subclassing this class.
         * 
         * @param <VS>
         * @param <ES>
         * @param <ST>
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * 
         *         TODO This should use the TLBFactory when we change to use
         *         stream solutions out of the SERVICE (#507), but the TLB class
         *         is not necessary until the reduce itself runs in concurrent
         *         threads (it is single threaded right now based on the backing
         *         CHM iterator).
         */
        public static class BindingSetReducer<VS, ES, ST> implements
                IReducer<VS, ES, ST, IBindingSet[]> {

            /**
             * The declared output variables (the ones that the caller wants to
             * extract). Any position that will not be extracted is a
             * <code>null</code>.
             */
            private final IVariable<?>[] outVars;

            /**
             * The KB instance.
             */
            private final AbstractTripleStore store;
            
            private final LexiconRelation lex;
            
            /**
             * The object used to create the variable bindings.
             */
            private final ValueFactory vf;
            
            /**
             * The list of objects used to extract the variable bindings.
             */
            private final List<IBindingExtractor.IBinder<VS, ES, ST>> binderList;
            
            /**
             * The collected solutions.
             */
            private final List<IBindingSet> tmp = new LinkedList<IBindingSet>();
            
            /**
             * 
             * @param outVars
             *            The declared output variables (the ones that the
             *            caller wants to extract). Any position that will not
             *            be extracted is a <code>null</code>.
             */
            public BindingSetReducer(//
                    final IVariable<?>[] outVars,
                    final AbstractTripleStore store,
                    final IGASProgram<VS, ES, ST> gasProgram,
                    final IGASContext<VS, ES, ST> ctx) {

                this.outVars = outVars;

                this.store = store;

                this.lex = store.getLexiconRelation();
                
                this.vf = store.getValueFactory();
                
                this.binderList = gasProgram.getBinderList();

            }
            
            @Override
            public void visit(final IGASState<VS, ES, ST> state, final Value u) {

                final IBindingSet bs = new ListBindingSet();
                
                for (IBindingExtractor.IBinder<VS, ES, ST> b : binderList) {

                    // The variable for this binder.
                    final IVariable<?> var = outVars[b.getIndex()];

                    if(var == null)
                        continue;
                    
                    /*
                     * TODO This does too much work. The API is defined in terms
                     * of openrdf Value objects rather than IVs because it is in
                     * a different package (not bigdata specific). The
                     * getBinderList() method should be moved to the code that
                     * exposes the service (this class) so it can do bigdata
                     * specific things and DO LESS WORK. This would be a good
                     * thing to do at the time that we add support for FuzzySSSP
                     * (which is not an IGASProgram and hence breaks the model
                     * anyway).
                     */
                    final Value val = b.bind(vf, state, u);

                    if (val == null)
                        continue;

                    if (val instanceof IV) {

                        // The value is already an IV.
                        bs.set(var, new Constant((IV) val));

                    } else {

                        /*
                         * The Value is a BigdataValueImpl (if the bind() method
                         * used the supplied ValueFactory). We need to convert
                         * it to an IV and this code ASSUMES that we can do this
                         * using an inline IV with the as configured KB. (This
                         * will work for anything numeric, but not for strings.)
                         */
                        final IV<BigdataValueImpl, ?> iv = lex
                                .getLexiconConfiguration().createInlineIV(val);

                        iv.setValue((BigdataValueImpl) val);

                        bs.set(var, new Constant(iv));

                    }

                }

                // Add to the set of generated solutions.
                tmp.add(bs);

            }

            @Override
            public IBindingSet[] get() {

                return tmp.toArray(new IBindingSet[tmp.size()]);

            }

        }
        
        /**
         * Factory for the {@link IGASEngine}.
         */
        private IGASEngine newGasEngine(final IIndexManager indexManager,
                final int nthreads) {

            return new BigdataGASEngine(indexManager, nthreads);

        }

        /**
         * Return an instance of the {@link IGASProgram} to be evaluated.
         */
        private IGASProgram<VS, ES, ST> newGASProgram(
                final Class<IGASProgram<VS, ES, ST>> cls) {

            if (cls == null)
                throw new IllegalArgumentException();

            try {

                final Constructor<IGASProgram<VS, ES, ST>> ctor = cls
                        .getConstructor(new Class[] {});

                final IGASProgram<VS, ES, ST> gasProgram = ctor
                        .newInstance(new Object[] {});

                return gasProgram;

            } catch (Exception e) {
                
                throw new RuntimeException(e);
                
            }
            
        }

        /**
         * Return the object used to access the as-configured graph.
         */
        private IGraphAccessor newGraphAccessor(final AbstractTripleStore kb) {

            /*
             * Use a read-only view (sampling depends on access to the BTree rather
             * than the ReadCommittedIndex).
             */
            final BigdataGraphAccessor graphAccessor = new BigdataGraphAccessor(
                    kb.getIndexManager(), kb.getNamespace(), kb
                            .getIndexManager().getLastCommitTime());

            return graphAccessor;
            
        }

    }

}
