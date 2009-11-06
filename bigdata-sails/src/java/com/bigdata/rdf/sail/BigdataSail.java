/*
 * Copyright SYSTAP, LLC 2006-2007.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
Portions of this code are:

Copyright Aduna (http://www.aduna-software.com/) � 2001-2007

All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    * Neither the name of the copyright holder nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/
/*
 * Created on Jan 2, 2008
 */

package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;
import org.openrdf.OpenRDFUtil;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.BindingAssigner;
import org.openrdf.query.algebra.evaluation.impl.CompareOptimizer;
import org.openrdf.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.openrdf.query.algebra.evaluation.impl.ConstantOptimizer;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.evaluation.impl.FilterOptimizer;
import org.openrdf.query.algebra.evaluation.impl.QueryJoinOptimizer;
import org.openrdf.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.openrdf.query.algebra.evaluation.util.QueryOptimizerList;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailConnectionListener;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.SailBase;

import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.inf.TruthMaintenance;
import com.bigdata.rdf.model.BigdataBNodeImpl;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rules.BackchainAccessPath;
import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.InferredSPOFilter;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOAccessPath;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BNS;
import com.bigdata.rdf.store.BigdataSolutionResolverator;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.rdf.store.BigdataStatementIteratorImpl;
import com.bigdata.rdf.store.BigdataValueIterator;
import com.bigdata.rdf.store.BigdataValueIteratorImpl;
import com.bigdata.rdf.store.EmptyStatementIterator;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.relation.accesspath.EmptyAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IRule;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.CloseableIteratorWrapper;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.Striterator;

/**
 * <p>
 * Sesame <code>2.x</code> integration.
 * </p>
 * <p>
 * Read-write operations use {@link #getConnection()} to obtain a mutable view.
 * {@link #getConnection()} uses a {@link Semaphore} to enforce the constraint
 * that there is only one writable {@link BigdataSailConnection} at a time. SAIL
 * transactions will be serialized (at most one will run at a time).
 * </p>
 * <p>
 * Concurrent readers are possible, and can be very efficient. However, readers
 * MUST use a database commit point corresponding to a desired state of the
 * store, e.g., after loading some data set and (optionally) after computing the
 * closure of that data set. Use {@link #getDatabase()} .
 * {@link AbstractTripleStore#getIndexManager()} .
 * {@link IIndexStore#getLastCommitTime()} to obtain the commit time and then
 * {@link #getReadHistoricalView(long)} to obtain a read-only view as of that
 * commit time. In you are loading data incrementally, then you can use
 * {@link #asReadCommittedView()} instead.
 * </p>
 * <p>
 * The {@link BigdataSail} may be configured as as to provide a triple store
 * with statement-level provenance using <em>statement identifiers</em>. A
 * statement identifier is unique identifier for a <em>triple</em> in the
 * database. Statement identifiers may be used to make statements about
 * statements without using RDF style reification. The statement identifier is
 * bound to the context position during high-level query so you can use
 * high-level query (SPARQL) to obtain statements about statements. See
 * {@link AbstractTripleStore.Options#STATEMENT_IDENTIFIERS}.
 * </p>
 * <p>
 * Quads may be enabled using {@link AbstractTripleStore.Options#QUADS}.
 * However, note that {@link Options#TRUTH_MAINTENANCE} is not supported for
 * {@link AbstractTripleStore.Options#QUADS} at this time. This may change in
 * the future once we decide how to handle eager materialization of entailments
 * with multiple named graphs. The basic problem is that:
 * </p>
 * 
 * <pre>
 * merge(closure(graphA), closure(graphB))
 * </pre>
 * <p>
 * IS NOT EQUALS TO
 * </p>
 * 
 * <pre>
 * closure(merge(graphA, graphB))
 * </pre>
 * 
 * <p>
 * There are two ways to handle this. One is to compute all inferences at query
 * time, in which case we are not doing eager materialization and therefore we
 * are not using Truth Maintenance. The other is to punt and use the merge of
 * their individual closures.
 * </p>
 * 
 * @todo Is there anything to be done with {@link #setDataDir(java.io.File)}?
 *       With {@link #getDataDir()}?
 * 
 * @todo write custom initialization class (Instead of the generic setParameter
 *       method, most Sail implementation now have specific methods for each
 *       parameter. This is much more convenient when creating Sail instances
 *       programmatically. You are free to add whatever initialization method to
 *       your Sail class.)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataSail extends SailBase implements Sail {
    
    /**
     * Additional parameters understood by the Sesame 2.x SAIL implementation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends com.bigdata.rdf.store.AbstractTripleStore.Options {
    
        /**
         * This optional boolean property may be used to specify whether or not
         * RDFS entailments are maintained by eager closure of the knowledge
         * base (the default is <code>true</code>).  This property only effects
         * data loaded through the {@link Sail}.
         */
        public static final String TRUTH_MAINTENANCE = BigdataSail.class
                .getPackage().getName()
                + ".truthMaintenance"; 
    
        public static final String DEFAULT_TRUTH_MAINTENANCE = "true"; 
    
//        /**
//         * The property whose value is the name of the {@link ITripleStore}
//         * implementation that will be instantiated. This may be used to select
//         * either the {@link LocalTripleStore} (default) or the
//         * {@link TempTripleStore} in combination
//         * {@link BigdataSail#BigdataSail(Properties)} ctor.
//         * 
//         * @deprecated 
//         */
//        public static final String STORE_CLASS = "storeClass";
//        
//        /** @deprecated */
//        public static final String DEFAULT_STORE_CLASS = LocalTripleStore.class.getName();
    
        /**
         * The capacity of the statement buffer used to absorb writes.
         * 
         * @see #DEFAULT_BUFFER_CAPACITY
         */
        public static final String BUFFER_CAPACITY = BigdataSail.class
                .getPackage().getName()
                + ".bufferCapacity";
    
        public static final String DEFAULT_BUFFER_CAPACITY = "10000";
        
        /**
         * This boolean may be used to specify whether or not to use "native"
         * joins, bypassing the Sesame join mechanism (default
         * {@value #DEFAULT_NATIVE_JOINS}). Native joins translate high-level
         * query from an {@link TupleExpr} into an {@link IRule} and then
         * execute the {@link IRule}. The generated {@link IRule}s operate
         * directly with the {@link SPORelation} and {@link SPO} objects using
         * internal 64-bit term identifiers rather than RDF {@link Value}s.
         * When query results are externalized, efficient batch resolution is
         * used to translate from those 64-bit term identifiers to
         * {@link BigdataValue}s.
         * 
         * @see #DEFAULT_NATIVE_JOINS
         */
        public static final String NATIVE_JOINS = BigdataSail.class
                .getPackage().getName()
                + ".nativeJoins";
        
        public static final String DEFAULT_NATIVE_JOINS = "true";
        
        /**
         * Option (default <code>true</code>) may be used to explicitly
         * disable query-time expansion for entailments NOT computed during
         * closure. In particular, this may be used to disable the query time
         * expansion of (x rdf:type rdfs:Resource) and owl:sameAs. (Those
         * entailments are fast for the {@link LocalTripleStore} but have not
         * been optimized for scale-out deployments.)
         * 
         * @see #DEFAULT_QUERY_TIME_EXPANDER
         */
        public static final String QUERY_TIME_EXPANDER = BigdataSail.class
                .getPackage().getName()
                + ".queryTimeExpander";
        
        public static final String DEFAULT_QUERY_TIME_EXPANDER = "true";
        
        
        /**
         * Option (default <code>false</code>) determines whether the {@link
         * SailConnection#size(Resource[])} method returns an exact size or
         * an upper bound.  Exact size is a very expensive operation.
         */
        public static final String EXACT_SIZE = BigdataSail.class
                .getPackage().getName()
                + ".exactSize";

        public static final String DEFAULT_EXACT_SIZE = "false";
        
        /**
         * Options (default <code>false</code>) added only to pass the Sesame
         * test suites.  DO NOT EVER USE AUTO-COMMIT WITH BIGDATA!
         */
        public static final String ALLOW_AUTO_COMMIT = BigdataSail.class
                .getPackage().getName()
                + ".allowAutoCommit";
        
        public static final String DEFAULT_ALLOW_AUTO_COMMIT = "false";

    }

    /**
     * Logger.
     */
    final protected static Logger log = Logger.getLogger(BigdataSail.class);

//    final protected static boolean INFO = log.isInfoEnabled();

//    final protected static boolean DEBUG = log.isDebugEnabled();

    /**
     * Sesame has the notion of a "null" graph which we use for the quad store
     * mode. Any time you insert a statement into a quad store and the context
     * position is not specified, it is actually inserted into this "null"
     * graph. If SPARQL <code>DATASET</code> is not specified, then all contexts
     * are queried and you will see statements from the "null" graph as well as
     * from any other context.
     * {@link BigdataSailConnection#getStatements(Resource, URI, Value, boolean, Resource...)}
     * will return statements from the "null" graph if the context is either
     * unbound or is an array whose sole element is <code>null</code>.
     * 
     * @see BigdataSailConnection#addStatement(Resource, URI, Value,
     *      Resource...)
     * @see BigdataSailConnection#getStatements(Resource, URI, Value, boolean,
     *      Resource...)
     */
    public static final transient URI NULL_GRAPH = new URIImpl(BNS.NULL_GRAPH);

    /**
     * The equivalent of a null identifier for an internal RDF Value.
     */
    protected static final long NULL = IRawTripleStore.NULL;

    final protected AbstractTripleStore database;

    final protected Properties properties;
    
    /**
     * The inference engine if the SAIL is using one.
     * <p>
     * Note: Requesting this object will cause the axioms to be written onto the
     * database if they are not already present. If this is a read-only view and
     * the mutable view does not already have the axioms defined then this will
     * cause an exception to be thrown since the indices are not writable by the
     * read-only view.
     */
    public InferenceEngine getInferenceEngine() {

        return database.getInferenceEngine();
        
    }
    
    /**
     * Return <code>true</code> if the SAIL is using a "quads" mode database.
     * 
     * @see AbstractTripleStore.Options#QUADS
     */
    public boolean isQuads() {

        return quads;
        
    }
    
    /**
     * The configured capacity for the statement buffer(s).
     * 
     * @see Options#BUFFER_CAPACITY
     */
    final private int bufferCapacity;
    
    /**
     * When true, the RDFS closure will be maintained.
     * 
     * @see Options#TRUTH_MAINTENANCE
     */
    final private boolean truthMaintenance;
    
    /**
     * When true, the SAIL is in the "quads" mode.
     * 
     * @see AbstractTripleStore.Options#QUADS
     */
    final private boolean quads;
    
    /**
     * When true, SAIL will delegate joins to bigdata internal joins.
     * 
     * @see Options#NATIVE_JOINS
     */
    final private boolean nativeJoins;
    
    /**
     * When true, SAIL will compute entailments at query time that were excluded
     * from forward closure.
     * 
     * @see Options#QUERY_TIME_EXPANDER
     */
    final private boolean queryTimeExpander;
    
    /**
     * When true, SAIL will compute an exact size in the {@link
     * SailConnection#size(Resource[])} method.
     * 
     * @see Options#EXACT_SIZE
     */
    final private boolean exactSize;
    
    /**
     * When true, auto-commit is allowed. Do not ever use auto-commit, this is
     * purely used to pass the Sesame test suites.
     * 
     * @see Options#ALLOW_AUTO_COMMIT
     */
    final private boolean allowAutoCommit;
    
    /**
     * <code>true</code> iff the {@link BigdataSail} has been
     * {@link #initialize()}d and not {@link #shutDown()}.
     */
    private boolean open;
    
    /**
     * Set <code>true</code> by ctor variants that open/create the database
     * but not by those that connect to an existing database. This helps to
     * provide the illusion of a dedicated purpose SAIL for those ctor variants.
     */
    private boolean closeOnShutdown;
    
    /**
     * When true, the RDFS closure will be maintained by the <em>SAIL</em>
     * implementation (but not by methods that go around the SAIL).
     */
    public boolean getTruthMaintenance() {
        
        return truthMaintenance;
        
    }
    
    /**
     * The implementation object.
     */
    public AbstractTripleStore getDatabase() {
        
        return database;
        
    }

    /**
     * Defaults various properties.
     */
    private static Properties getDefaultProperties() {
        
        Properties properties = new Properties();
        
        properties.setProperty(Options.FILE, "bigdata" + Options.JNL);
        
        return properties;
        
    }
    
//    /**
//     * Create/re-open the database identified by the properites.
//     * <p>
//     * Note: This can only be used for {@link AbstractLocalTripleStore}s. The
//     * {@link ScaleOutTripleStore} uses the {@link DefaultResourceLocator}
//     * pattern and does not have a constructor suitable for just a
//     * {@link Properties} object.
//     * 
//     * @see Options
//     */
//    @SuppressWarnings("unchecked")
//    private static AbstractLocalTripleStore setUp(Properties properties) {
//
//        final String val = properties.getProperty(
//                BigdataSail.Options.STORE_CLASS,
//                BigdataSail.Options.DEFAULT_STORE_CLASS);
//
//        try {
//
//            final Class storeClass = Class.forName(val);
//
//            if (!AbstractLocalTripleStore.class.isAssignableFrom(storeClass)) {
//
//                throw new RuntimeException("Must extend "
//                        + AbstractLocalTripleStore.class.getName() + " : "
//                        + storeClass.getName());
//
//            }
//
//            final Constructor<AbstractLocalTripleStore> ctor = storeClass
//                    .getConstructor(new Class[] { Properties.class });
//
//            final AbstractLocalTripleStore database = ctor
//                    .newInstance(new Object[] { properties });
//
//            return database;
//
//        } catch (Exception t) {
//
//            throw new RuntimeException(t);
//
//        }
//
//    }

    /**
     * Create or re-open a database instance configured using defaults.
     */
    public BigdataSail() {
        
//        this(setUp(getDefaultProperties()));
        this(new LocalTripleStore(getDefaultProperties()));
        
        closeOnShutdown = true;
        
    }
    
    /**
     * Create or open a database instance configured using the specified
     * properties.
     * 
     * @see Options
     */
    public BigdataSail(Properties properties) {
        
//        this(setUp(properties));
        
        this(new LocalTripleStore(properties));

        closeOnShutdown = true;

    }
    
    /**
     * Core ctor.  You must use this variant for a scale-out triple store.
     * <p>
     * To create a {@link BigdataSail} backed by an
     * {@link IBigdataFederation} use the {@link ScaleOutTripleStore} ctor and
     * then {@link AbstractTripleStore#create()} the triple store if it does not
     * exist.
     * 
     * @param database
     *            An existing {@link AbstractTripleStore}.
     */
    public BigdataSail(final AbstractTripleStore database) {
        
        if (database == null)
            throw new IllegalArgumentException();
    
        // default to false here and overwritten by some ctor variants.
        this.closeOnShutdown = false;
        
        this.database = database;
        
        this.properties = database.getProperties();

        this.quads = database.isQuads();
        
        if (quads) {

            if (properties.getProperty(BigdataSail.Options.TRUTH_MAINTENANCE) != null
                    && Boolean
                            .parseBoolean(properties
                                    .getProperty(BigdataSail.Options.TRUTH_MAINTENANCE))) {

                /*
                 * Note: Truth maintenance is not supported for quads at this
                 * time.
                 */
                throw new UnsupportedOperationException(
                        Options.TRUTH_MAINTENANCE
                                + " is not supported with quads ("
                                + Options.QUADS + ")");
            }

        }

        // truthMaintenance
        if (database.getAxioms() instanceof NoAxioms || quads) {

            /*
             * If there is no axioms model then inference is not enabled and
             * truth maintenance is disabled automatically.
             */

            truthMaintenance = false;
            
        } else {
            
            truthMaintenance = Boolean.parseBoolean(properties.getProperty(
                    BigdataSail.Options.TRUTH_MAINTENANCE,
                    BigdataSail.Options.DEFAULT_TRUTH_MAINTENANCE));

            if (log.isInfoEnabled())
                log.info(BigdataSail.Options.TRUTH_MAINTENANCE + "="
                        + truthMaintenance);
            
        }
        
        // bufferCapacity
        {
            
            bufferCapacity = Integer.parseInt(properties.getProperty(
                    BigdataSail.Options.BUFFER_CAPACITY,
                    BigdataSail.Options.DEFAULT_BUFFER_CAPACITY));

            if (log.isInfoEnabled())
                log.info(BigdataSail.Options.BUFFER_CAPACITY + "="
                        + bufferCapacity);

        }

        // nativeJoins
        {
            
//            nativeJoins = false;
//            System.err.println("native joins are disabled.");
            nativeJoins = Boolean.parseBoolean(properties.getProperty(
                    BigdataSail.Options.NATIVE_JOINS,
                    BigdataSail.Options.DEFAULT_NATIVE_JOINS));

            if (log.isInfoEnabled())
                log.info(BigdataSail.Options.NATIVE_JOINS + "=" + nativeJoins);

        }

        // queryTimeExpander
        {
            
            queryTimeExpander = Boolean.parseBoolean(properties.getProperty(
                    BigdataSail.Options.QUERY_TIME_EXPANDER,
                    BigdataSail.Options.DEFAULT_QUERY_TIME_EXPANDER));

            if (log.isInfoEnabled())
                log.info(BigdataSail.Options.QUERY_TIME_EXPANDER + "="
                        + queryTimeExpander);

        }
        
        // exactSize
        {
            
            exactSize = Boolean.parseBoolean(properties.getProperty(
                    BigdataSail.Options.EXACT_SIZE,
                    BigdataSail.Options.DEFAULT_EXACT_SIZE));

            if (log.isInfoEnabled())
                log.info(BigdataSail.Options.EXACT_SIZE + "="
                        + exactSize);
            
        }

        // allowAutoCommit
        {
            
            allowAutoCommit = Boolean.parseBoolean(properties.getProperty(
                    BigdataSail.Options.ALLOW_AUTO_COMMIT,
                    BigdataSail.Options.DEFAULT_ALLOW_AUTO_COMMIT));

            if (log.isInfoEnabled())
                log.info(BigdataSail.Options.ALLOW_AUTO_COMMIT + "="
                        + allowAutoCommit);
            
        }

    }
    
    /**
     * 
     * @throws IllegalStateException
     *             if the {@link BigdataSail} has not been {@link #initialize()}d
     *             or has been {@link #shutDown()}.
     */
    protected void assertOpen() {

        if (!open)
            throw new IllegalStateException();
        
    }

    /**
     * Return <code>true</code> if the {@link BigdataSail} has been
     * {@link #initialize()}d and has not been {@link #shutDown()}.
     */
    public boolean isOpen() {
        
        return open;
        
    }
    
    /**
     * @throws IllegalStateException
     *             if the sail is already open.
     */
    public void initialize() throws SailException {

        if (open)
            throw new IllegalStateException();
        
        /*
         * NOP (nothing to invoke in the SailBase).
         */
        
        if(log.isInfoEnabled()) {
            
            log.info("closeOnShutdown=" + closeOnShutdown);
            
        }
        
        open = true;
        
    }
    
    /**
     * Invokes {@link #shutDown()}.
     */
    protected void finalize() throws Throwable {
        
        if(isOpen()) {
            
            if (log.isInfoEnabled())
                log.info("");
            
            shutDown();
            
        }
        
        super.finalize();
        
    }

    public void shutDown() throws SailException {
        
        assertOpen();
        
        super.shutDown();
        
    }
    
    /**
     * If the backing database was created/opened by the {@link BigdataSail}
     * then it is closed.  Otherwise this is a NOP.
     */
    protected void shutDownInternal() throws SailException {
        
        if (open) {
            
            try {
            
                if (closeOnShutdown) {

                    if (log.isInfoEnabled())
                        log.info("Closing the backing database");

                    database.close();

                }
                
            } finally {

                open = false;

            }

        }
        
    }

    /**
     * <strong>DO NOT INVOKE FROM APPLICATION CODE</strong> - this method
     * deletes the KB instance and destroys the backing database instance. It is
     * used to help tear down unit tests.
     */
    void __tearDownUnitTest() {
        
        closeOnShutdown = false;
        
        try {

            shutDown();

            database.__tearDownUnitTest();

        } catch (Throwable t) {

            log.error("Problem during shutdown: " + t, t);

        }
        
    }
    
    /**
     * A factory returning the singleton read-committed view of the database.
     * This view is safe for concurrent query operations.
     * <p>
     * Note: The returned object is thread-safe and the same instance is used by
     * concurrent readers. Therefore {@link SailConnection#close()} is a NOP for
     * the returned object and {@link SailConnection#isOpen()} will always
     * return <code>true</code> for the returned object. A weak reference is
     * maintained to the read-committed view and the view will be reclaimed by
     * the garbage collector once the application no longer holds a hard
     * reference to the returned object.
     */
    public BigdataSailConnection asReadCommittedView() {
        
        synchronized(this) {
        
            BigdataSailConnection view = readCommittedRef == null ? null
                    : readCommittedRef.get();
            
            if (view == null) {

                /*
                 * Create the singleton using a read-committed view of the
                 * database.
                 */

                view = new BigdataSailConnection(database.asReadCommittedView()) {

                    public void close() throws SailException {

                        // NOP - close is ignored.

                    }

                };

                readCommittedRef = new WeakReference<BigdataSailConnection>(
                        view);
                
            }
            
            return view; 
        
        }
        
    }
    private WeakReference<BigdataSailConnection> readCommittedRef;
        
    /**
     * Obtain a read-historical view that reads from the specified commit point.
     * This view is safe for concurrent readers and will not update if there are
     * concurrent writes.
     * 
     * @param commitTime
     *            The commit point.
     * 
     * @return The view.
     * 
     * @todo both {@link #asReadCommittedView()} and
     *       {@link #getReadHistoricalView(long)} do not register the connection
     *       with the {@link SailBase} which leads to the following message.
     * 
     * <pre>
     *      SailBase.connectionClosed(SailBase.java:204): tried to remove unknown connection object from store.
     * </pre>
     * 
     * This message (probably) can be ignored since these are read-only views.
     * However, since the {@link SailBase} wants to "close" the connections, the
     * read-committed view should not be a singleton - it needs to be a distinct
     * {@link BigdataSailConnection} instance. Also, both views need to get
     * registered for this message to go away.
     * <p>
     * One way to handle this is to obtain a {@link BigdataSail} subclass that
     * operates as a factory for those views.
     */
    public BigdataSailConnection getReadHistoricalView(final long commitTime) {
       
        AbstractTripleStore tmp = (AbstractTripleStore) database
                .getIndexManager().getResourceLocator().locate(
                        database.getNamespace(),
                        TimestampUtility.asHistoricalRead(commitTime));
        
        return new BigdataSailConnection(tmp);
        
    }
    
    /**
     * A {@link BigdataValueFactory}
     */
    final public ValueFactory getValueFactory() {
        
        return database.getValueFactory();
        
    }

    final public boolean isWritable() throws SailException {

        return ! database.isReadOnly();
        
    }

    /**
     * Used to serialize concurrent writers.
     */
    final private Semaphore semaphore = new Semaphore(1);

    /**
     * Return a read-write {@link SailConnection}. There is only one writable
     * connection and this method will block until the connection is available.
     * 
     * @see #asReadCommittedView() for a read-only connection.
     * 
     * @todo many of the stores can support concurrent writers, but there is a
     *       requirement to serialize writers when truth maintenance is enabled.
     */
    protected NotifyingSailConnection getConnectionInternal() throws SailException {

        // this will block until the semaphore is available.
        try {

            semaphore.acquire();
            
        } catch(InterruptedException ex) {
            
            throw new SailException(ex);
            
        }

        // new writable connection.
        final BigdataSailConnection conn = new BigdataSailConnection(database);
        
        return conn;
        
    }
    
    /**
     * Strengthens the return type.
     */
    @Override
    public BigdataSailConnection getConnection() throws SailException {
        
        return (BigdataSailConnection) super.getConnection();
        
    }
    
    /**
     * Return a query connection based on a the last commit point.
     */
    public BigdataSailConnection getQueryConnection() throws SailException {
        
        final long timestamp = database.getIndexManager().getLastCommitTime();
        
        return getReadHistoricalView(timestamp);
        
    }
    
    
    /**
     * Inner class implements the {@link SailConnection}. Some additional
     * functionality is available on this class, including
     * {@link #computeClosure()}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class BigdataSailConnection implements NotifyingSailConnection {

        /**
         * The database view.
         */
        final protected AbstractTripleStore database;

        /**
         * True iff the database view is read-only.
         */
        final protected boolean readOnly;

        /**
         * True iff the {@link SailConnection} is open.
         */
        private boolean open = true;
        
        /**
         * non-<code>null</code> iff truth maintenance is being performed.
         */
        private final TruthMaintenance tm;
        
        /**
         * Used to buffer statements that are being asserted.
         * 
         * @see #getAssertionBuffer()
         */
        private StatementBuffer<Statement> assertBuffer = null;
        
        /**
         * Used to buffer statements being retracted.
         * 
         * @see #getRetractionBuffer()
         */
        private StatementBuffer<Statement> retractBuffer = null;

        /**
         * A canonicalizing mapping for blank nodes whose life cycle is the same
         * as that of the {@link SailConnection}.
         * 
         * FIXME bnodes : maintain the dual of this map, {@link #bnodes2}.
         * 
         * FIXME bnodes : resolution of term identifiers to blank nodes for
         * access path scans and CONSTRUCT in {@link BigdataStatementIterator}.
         * 
         * FIXME bnodes : resolution of term identifiers to blank nodes for
         * JOINs in {@link BigdataSolutionResolverator}.
         */
        private final Map<String, BigdataBNodeImpl> bnodes;

        /**
         * A reverse mapping from the assigned term identifiers for blank nodes
         * to the {@link BigdataBNodeImpl} object. This is used to resolve blank
         * nodes recovered during query from within the same
         * {@link SailConnection} without loosing the blank node identifier.
         * This behavior is required by the contract for {@link SailConnection}.
         */
        private final Map<Long, BigdataBNodeImpl> bnodes2;

        /**
         * Return the assertion buffer.
         * <p>
         * The assertion buffer is used to buffer statements that are being
         * asserted so as to maximize the opportunity for batch writes. Truth
         * maintenance (if enabled) will be performed no later than the commit
         * of the transaction.
         * <p>
         * Note: When non-<code>null</code> and non-empty, the buffer MUST be
         * flushed (a) if a transaction completes (otherwise writes will not be
         * stored on the database); or (b) if there is a read against the
         * database during a transaction (otherwise reads will not see the
         * unflushed statements).
         * <p>
         * Note: if {@link #truthMaintenance} is enabled then this buffer is
         * backed by a temporary store which accumulates the {@link SPO}s to be
         * asserted. Otherwise it will write directly on the database each time
         * it is flushed, including when it overflows.
         */
        synchronized protected StatementBuffer<Statement> getAssertionBuffer() {

            if (assertBuffer == null) {

                if (truthMaintenance) {

                    assertBuffer = new StatementBuffer<Statement>(tm
                            .newTempTripleStore(), database, bufferCapacity);

                } else {

                    assertBuffer = new StatementBuffer<Statement>(database,
                            bufferCapacity);

                }

                // FIXME bnodes : must also track the reverse mapping [bnodes2].
                assertBuffer.setBNodeMap(bnodes);

            }

            return assertBuffer;
            
        }

        /**
         * Return the retraction buffer.
         * <p>
         * The retraction buffer is used by the {@link SailConnection} API IFF
         * truth maintenance is enabled since the only methods available on the
         * {@link Sail} to delete statements,
         * {@link #removeStatements(Resource, URI, Value)} and
         * {@link #removeStatements(Resource, URI, Value, Resource[])}, each
         * accepts a triple pattern rather than a set of statements. The
         * {@link AbstractTripleStore} directly supports removal of statements
         * matching a triple pattern, so we do not buffer retractions for those
         * method UNLESS truth maintenance is enabled.
         * <p>
         * Note: you CAN simply obtain the retraction buffer, write on it the
         * statements to be retracted, and the {@link BigdataSailConnection}
         * will do the right thing whether or not truth maintenance is enabled.
         * <p>
         * When non-<code>null</code> and non-empty the buffer MUST be
         * flushed (a) if a transaction completes (otherwise writes will not be
         * stored on the database); or (b) if there is a read against the
         * database during a transaction (otherwise reads will not see the
         * unflushed statements).
         * <p>
         * Note: if {@link #truthMaintenance} is enabled then this buffer is
         * backed by a temporary store which accumulates the SPOs to be
         * retracted. Otherwise it will write directly on the database each time
         * it is flushed, including when it overflows.
         * <p>
         */
        synchronized protected StatementBuffer<Statement> getRetractionBuffer() {

            if (retractBuffer == null) {

                if (truthMaintenance) {

                    retractBuffer = new StatementBuffer<Statement>(tm
                            .newTempTripleStore(), database, bufferCapacity);

                } else {

                    /*
                     * Note: The SailConnection API will not use the
                     * [retractBuffer] when truth maintenance is disabled, but
                     * one is returned anyway so that callers may buffer
                     * statements which they have on hand for retraction rather
                     * as a complement to using triple patterns to describe the
                     * statements to be retracted (which is how you do it with
                     * the SailConnection API).
                     */

                    retractBuffer = new StatementBuffer<Statement>(database,
                            bufferCapacity);

                }

                // FIXME bnodes : Must also track the reverse mapping [bnodes2].
                retractBuffer.setBNodeMap(bnodes);
                
            }
            
            return retractBuffer;

        }
        
        /**
         * Create a {@link SailConnection} for the database.
         * 
         * @param database
         *            The database. If this is a read-only view, then the
         *            {@link SailConnection} will not support update.
         */
        protected BigdataSailConnection(final AbstractTripleStore database) {

            BigdataSail.this.assertOpen();
            
            this.database = database;
            
            readOnly = database.isReadOnly();            
         
            if (database.isReadOnly()) {
                
                if (log.isInfoEnabled())
                    log.info("Read-only view");
                
                tm = null;
                
                bnodes = null;

                bnodes2 = null;
                
            } else {

                if (log.isInfoEnabled())
                    log.info("Read-write view");

                /*
                 * Note: A ConcurrentHashMap is used in case the SAIL has
                 * concurrent threads which are processing RDF/XML statements
                 * and therefore could in principle require concurrent access to
                 * this map. ConcurrentHashMap is preferred to
                 * Collections#synchronizedMap() since the latter requires
                 * explicit synchronization during iterators, which breaks
                 * encapsulation.
                 */
                bnodes = new ConcurrentHashMap<String, BigdataBNodeImpl>();
                bnodes2 = new ConcurrentHashMap<Long, BigdataBNodeImpl>();
//                bnodes = Collections
//                        .synchronizedMap(new HashMap<String, BigdataBNodeImpl>(
//                                bufferCapacity));
                
                if (truthMaintenance) {

                    /*
                     * Setup the object that will be used to maintain the
                     * closure of the database.
                     */

                    tm = new TruthMaintenance(getInferenceEngine());
                    
                } else {
                    
                    tm = null;
                    
                }
                    
            }

        }
        
        /**
         * The implementation object.
         */
        public AbstractTripleStore getTripleStore() {
            
            return database;
            
        }

        /**
         * When true, SAIL will compute entailments at query time that were excluded
         * from forward closure.
         * 
         * @see Options#QUERY_TIME_EXPANDER
         */
        public final boolean isQueryTimeExpander() {

            return queryTimeExpander;
            
        }
        
        /**
         * Used by the RepositoryConnection to determine whether or not to allow
         * auto-commits.
         * 
         * @see Options#ALLOW_AUTO_COMMIT
         */
        public boolean getAllowAutoCommit() {
            
            return allowAutoCommit;
            
        }
        
        /*
         * SailConnectionListener support.
         * 
         * FIXME There is _ALSO_ a SailChangedListener that only has booleans
         * for added and removed and which reports the Sail instance that was
         * modified (outside of the SailConnection). I need to restore the old
         * logic that handled this sort of thing and carefully verify that the
         * boolean flags are being set and cleared as appropriate.
         * 
         * It used to be that those notices were deferred until the next commit.
         * Does that still apply for Sesame 2.x?
         * 
         * Note: SailBase appears to handle the SailChangedListener api.
         */

        /**
         * Vector of transient {@link SailConnectionListener}s registered with
         * this SAIL. The field is set back to <code>null</code> whenever
         * there are no more listeners.
         */
        private Vector<SailConnectionListener> m_listeners = null;
        
        /**
         * Note: This method is <strong>strongly discouraged</strong> as it
         * imposes an extremely high burden on the database requiring the
         * materialization at the client of every statement to be added or
         * removed from the database in the scope of this {@link SailConnection}.
         * Further, while the client is only notified for explicit statements
         * added or removed, it is possible that a statement remains entailed in
         * the database regardless of its removal.
         */
        synchronized public void addConnectionListener(SailConnectionListener listener) {

            if( m_listeners == null ) {
                
                m_listeners = new Vector<SailConnectionListener>();
                
                m_listeners.add( listener );
                
            } else {
                
                if( m_listeners.contains( listener ) ) {
                    
                    throw new IllegalStateException
                        ( "Already registered: listener="+listener
                          );
                    
                }
                
                m_listeners.add( listener );
                
            }

            log.warn("Adding SailConnectionListener - performance will suffer!");
            
        }

        synchronized public void removeConnectionListener(SailConnectionListener listener) {

            if( m_listeners == null ) {
                
                throw new IllegalStateException
                    ( "Not registered: listener="+listener
                      );
                
            }
            
            if( ! m_listeners.remove( listener ) ) {
                
                throw new IllegalStateException
                    ( "Not registered: listener="+listener
                      );
                
            }
            
            if(m_listeners.isEmpty()) {
                
                /*
                 * Note: Since the price of notifying listeners is so high the
                 * listeners vector is explicitly set to null so that we can
                 * test whether or not listeners need to be notified simply by
                 * testing m_listeners != null.
                 */
                
                m_listeners = null;
                
            }

        }

        /**
         * Notifies {@link SailConnectionListener}s if one or more statements have
         * been added to or removed from the repository using the SAIL methods:
         * <ul>
         * 
         * <li> {@link #addStatement(Resource, URI, Value)}
         * <li> {@link #removeStatements(Resource, URI, Value)}
         * <li> {@link #clearRepository()}
         * </ul>
         * 
         * @todo javadoc update.
         */
        synchronized protected void fireSailChangedEvent(final boolean added,
                final Statement stmt) {

            /*
             * Make sure that you test on m_listeners BEFORE hand so that you
             * can avoid the work required to materialize the statements if
             * there are no listeners registered! (The burden is especially high
             * when removing statements).
             */

            if( m_listeners == null ) return;

            /*
             * FIXME Since we are calling the listener for every single
             * statement we really need to eagerly materialize the array of
             * listeners whenever the listeners are added or removed so that we
             * can efficiently process that array here. as long as this method
             * and the add/remove methods are all synchronized then the result
             * will be coherent.
             */
            
            final SailConnectionListener[] listeners = (SailConnectionListener[]) m_listeners
                    .toArray(new SailConnectionListener[] {});

            for (int i = 0; i < listeners.length; i++) {
                
                final SailConnectionListener l = listeners[ i ];
                
                if(added) {
                    
                    l.statementAdded(stmt);
                    
                } else {
                
                    l.statementRemoved(stmt);
                    
                }
                
            }
            
        }
        
        /*
         * Namespace map CRUD.
         * 
         * @todo thread-safety for modifications to the namespaces. Either the
         * namespaces are local to a SailConnection, in which case the need to
         * be moved into this class or into a helper class, or they are shared,
         * in which case the methods need to be synchronized on the Sail.
         * 
         * @todo the namespace API suggests a bi-directional map from namespace
         * to prefix and from prefix to namespace for efficiency. only one
         * direction is actually backed by a Map object - the other uses a scan.
         */
        
        public void setNamespace(final String prefix, final String namespace)
                throws SailException {

            assertWritable();

            database.addNamespace(namespace,prefix);
            
        }

        public String getNamespace(final String prefix) {
            
            return database.getNamespace(prefix);
            
        }
        
        public void removeNamespace(final String prefix) {
            
            database.removeNamespace(prefix);
            
        }

        public void clearNamespaces() {
            
            database.clearNamespaces();
            
        }
        
        public NamespaceIterator getNamespaces() {

            /*
             * Note: You do NOT need to flush the buffer since this does not read
             * statements.
             */

            return new NamespaceIterator(database.getNamespaces().entrySet().iterator());

        }

        /**
         * Namespace iterator.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        private class NamespaceIterator implements CloseableIteration<Namespace,SailException> {

            private final Iterator<Map.Entry<String/*namespace*/,String/*prefix*/>> src;
            
            public NamespaceIterator(Iterator<Map.Entry<String/*namespace*/,String/*prefix*/>> src) {
                
                assert src != null;
                
                this.src = src;
                
            }
            
            public boolean hasNext() {
                
                return src.hasNext();
                
            }

            public Namespace next() {
             
                final Map.Entry<String/*namespace*/,String/*prefix*/> current = src.next();
                
                return new NamespaceImpl(current.getValue(), current.getKey());
                
            }

            public void remove() {
                
                // @todo implement?
                throw new UnsupportedOperationException();
                
            }
            
            public void close() {
                
                // NOP.
                
            }
            
        }
        
        /*
         * Statement CRUD
         */

        /**
         * Sesame has a concept of a "null" graph. Any statement inserted whose
         * context position is NOT bound will be inserted into the "null" graph.
         * Statements inserted into the "null" graph are visible from the SPARQL
         * default graph, when no data set is specified (in this case all
         * statements in all contexts are visible).
         * 
         * @see BigdataSail#NULL_GRAPH
         */
        public void addStatement(final Resource s, final URI p, final Value o,
                final Resource... contexts) throws SailException {

            if (log.isInfoEnabled())
                log.info("s=" + s + ", p=" + p + ", o=" + o + ", contexts="
                        + Arrays.toString(contexts));

            OpenRDFUtil.verifyContextNotNull(contexts);

            if (contexts.length == 0) {

                /*
                 * Operate on just the nullGraph.
                 * 
                 * Note: When no contexts are specified, the intention for
                 * addStatements() is that a statement with no associated
                 * context is added to the store.
                 */

                addStatement(s, p, o, (Resource) null/* c */);

            }

            if (contexts.length == 1 && contexts[0] == null) {

                // Operate on just the nullGraph.

                addStatement(s, p, o, (Resource) null/* c */);

            }

            for (Resource c : contexts) {

                addStatement(s, p, o, c);

            }
            
        }

        private void addStatement(final Resource s, final URI p, final Value o,
                final Resource c) throws SailException {

            if(!isOpen()) {

                /*
                 * Note: While this exception is not declared by the javadoc,
                 * it is required by the Sesame TCK.
                 */
                throw new IllegalStateException();
                
            }
            
            assertWritable();

            // flush any pending retractions first!
            flushStatementBuffers(false/* flushAssertBuffer */, true/* flushRetractBuffer */);

            /*
             * Buffer the assertion.
             * 
             * Note: If [c] is null and we are in quads mode, then we set
             * [c==NULL_GRAPH]. Otherwise we leave [c] alone. For the triple
             * store mode [c] will be null. For the provenance mode, [c] will
             * set to a statement identifier (SID) when the statement(s) are
             * flushed to the database.
             */
            getAssertionBuffer().add(s, p, o,
                    c == null && quads ? NULL_GRAPH : c);

            if (m_listeners != null) {

                // Notify listener(s).
                
                fireSailChangedEvent(true, new ContextStatementImpl(s, p, o, c));
                
            }

        }

        public void clear(final Resource... contexts) throws SailException {

            OpenRDFUtil.verifyContextNotNull(contexts);
            
            if (log.isInfoEnabled())
                log.info("contexts=" + Arrays.toString(contexts));
            
            assertWritable();

            // discard any pending writes.
            clearBuffers();
            
            /*
             * @todo if listeners are registered then we need to materialize
             * everything that is going to be removed....
             */
            
            if (contexts.length == 0) {

                /*
                 * Operates on all contexts.
                 * 
                 * Note: This deliberately removes the statements from each
                 * access path rather than doing a drop/add on the triple/quad
                 * store instance.
                 */
                
                database.getAccessPath(null/* s */, null/* p */, null/* o */,
                        null/* c */, null/* filter */).removeAll();

                return;
                
            }

            if (contexts.length == 1 && contexts[0] == null) {

                /*
                 * Operate on just the nullGraph, or on the sole graph if not in
                 * quads mode.
                 */

                database.getAccessPath(null/* s */, null/* p */, null/* o */,
                        quads ? NULL_GRAPH : null, null/* filter */)
                        .removeAll();

                return;

            }

            // FIXME parallelize this in chunks as per getStatements()
            long size = 0;
            for (Resource c : contexts) {

                size += database.getAccessPath(null/* s */, null/* p */,
                        null/* o */, (c == null && quads) ? NULL_GRAPH : c,
                        null/* filter */).removeAll();

            }

            return;
            
        }
        
        /**
         * Clears all buffered statements in the {@link #assertBuffer} and in
         * the optional {@link #retractBuffer}. If {@link #truthMaintenance} is
         * enabled, then the backing tempStores are also closed and deleted. The
         * buffer references are set to <code>null</code> and the buffers must
         * be re-allocated on demand.
         */
        private void clearBuffers() {

            if(assertBuffer != null) {
                
                // discard all buffered data.
                assertBuffer.reset();
                
                if(truthMaintenance) {
                    
                    // discard the temp store that buffers assertions.
                    assertBuffer.getStatementStore().close();
                    
                    // must be re-allocated on demand.
                    assertBuffer = null;
                    
                }
                
            }

            if (retractBuffer != null) {

                // discard all buffered data.
                retractBuffer.reset();

                if (truthMaintenance) {

                    // discard the temp store that buffers retractions.
                    retractBuffer.getStatementStore().close();

                    // must be re-allocated on demand.
                    retractBuffer = null;

                }

            }
            
            bnodes.clear();
            
            bnodes2.clear();

        }

        /**
         * Count the statements in the specified contexts.  Returns an exact
         * size or an upper bound depending on the value of {@link
         * Options#EXACT_SIZE}.  Exact size is an extremely expensive operation,
         * which we turn off by default.  In default mode, an upper bound is
         * given for the total number of statements in the database, explicit
         * and inferred.  In exact size mode, the entire index will be visited
         * and materialized and each explicit statement will be counted.
         * 
         * @see Options#EXACT_SIZE
         */
        public long size(final Resource... contexts) throws SailException {
            
            return size(exactSize, contexts);
            
        }
        
        /**
         * Note: This method is quite expensive since it must materialize all
         * statement in either the database or in the specified context(s) and
         * then filter for the explicit statements in order to obtain an exact
         * count. See {@link AbstractTripleStore#getStatementCount()} or
         * {@link IAccessPath#rangeCount()} for efficient methods for reporting
         * on the #of statements in the database or within a specific context.
         * 
         * @see Options#EXACT_SIZE
         */
        public long exactSize(final Resource... contexts) throws SailException {
            
            return size(true, contexts);
            
        }
        
        private long size(final boolean exactSize,
                final Resource... contexts) throws SailException {

            flushStatementBuffers(true/* assertions */, true/* retractions */);

            OpenRDFUtil.verifyContextNotNull(contexts);
            
            if (log.isInfoEnabled())
                log.info("contexts=" + Arrays.toString(contexts));

            if (exactSize) {
            
                if (contexts.length == 0 ) {
                    
                    // Operates on all contexts.
                    
                    return database.getExplicitStatementCount(null/* c */);
    
                }
    
                if (contexts.length == 1 && contexts[0] == null) {
    
                    /*
                     * Operate on just the nullGraph (or on the sole graph if not in
                     * quads mode).
                     */
    
                    return database.getExplicitStatementCount(quads ? NULL_GRAPH
                            : null/* c */);
    
                }
    
                // FIXME parallelize this in chunks as per getStatements()
                long size = 0;
    
                for (Resource c : contexts) {
    
                    size += database.getExplicitStatementCount(
                            (c == null && quads) ? NULL_GRAPH : c);
    
                }

                return size;
                
            } else { //exactSize == false
                
                if (contexts.length == 0 ) {
                    
                    // Operates on all contexts.
                    
                    return database.getStatementCount(null/* c */, false/*exact*/);
    
                }
    
                if (contexts.length == 1 && contexts[0] == null) {
    
                    /*
                     * Operate on just the nullGraph (or on the sole graph if not in
                     * quads mode).
                     */
    
                    return database.getStatementCount(quads ? NULL_GRAPH
                            : null/* c */, false/*exact*/);
    
                }
    
                // FIXME parallelize this in chunks as per getStatements()
                long size = 0;
    
                for (Resource c : contexts) {
    
                    size += database.getStatementCount(
                            (c == null && quads) ? NULL_GRAPH : c, false/*exact*/);
    
                }

                return size;
                
            }
            
        }

        public void removeStatements(final Resource s, final URI p,
                final Value o, final Resource... contexts) throws SailException {

            OpenRDFUtil.verifyContextNotNull(contexts);
            
            if (log.isInfoEnabled())
                log.info("s=" + s + ", p=" + p + ", o=" + o + ", contexts="
                        + Arrays.toString(contexts));

            if (contexts.length == 0 ) {
                
                // Operates on all contexts.
                
                removeStatements(s, p, o, (Resource) null/* c */);

            }

            if (contexts.length == 1 && contexts[0] == null) {

                /*
                 * Operate on just the nullGraph, or on the sole graph if not in
                 * quads mode.
                 */

                removeStatements(s, p, o, quads ? NULL_GRAPH : null/* c */);

            }

            // FIXME parallelize this in chunks as per getStatements()
            for (Resource c : contexts) {

                removeStatements(s, p, o, (c == null && quads) ? NULL_GRAPH : c);

            }
            
        }

        /**
         * Note: The CONTEXT is ignored when in statementIdentifier mode!
         */
        private int removeStatements(final Resource s, final URI p,
                final Value o, final Resource c) throws SailException {
            
            assertWritable();

            flushStatementBuffers(true/* flushAssertBuffer */, false/* flushRetractBuffer */);

            if (m_listeners != null) {

                /*
                 * FIXME to support the SailConnectionListener we need to
                 * pre-materialize the explicit statements that are to be
                 * deleted and then notify the listener for each such explicit
                 * statement. Since that is a lot of work, make sure that we do
                 * not generate notices unless there are registered listeners!
                 */

                throw new UnsupportedOperationException();
                
            }

            // #of explicit statements removed.
            final long n;

            if (getTruthMaintenance()) {

                /*
                 * Since we are doing truth maintenance we need to copy the
                 * matching "explicit" statements into a temporary store rather
                 * than deleting them directly. This uses the internal API to
                 * copy the statements to the temporary store without
                 * materializing them as Sesame Statement objects.
                 */

                /*
                 * Obtain a chunked iterator using the triple pattern that
                 * visits only the explicit statements.
                 */
                final IChunkedOrderedIterator<ISPO> itr = database
                        .getAccessPath(s, p, o, ExplicitSPOFilter.INSTANCE)
                        .iterator();

                // The tempStore absorbing retractions.
                final AbstractTripleStore tempStore = getRetractionBuffer()
                        .getStatementStore();

                // Copy explicit statements to tempStore.
                n = tempStore.addStatements(tempStore, true/* copyOnly */,
                        itr, null/* filter */);

                /*
                 * Nothing more happens until the commit or incremental write
                 * flushes the retraction buffer and runs TM.
                 */
                
            } else {

                /*
                 * Since we are not doing truth maintenance, just remove the
                 * statements from the database (synchronous, batch api, not
                 * buffered).
                 */
                
                n = database.removeStatements(s, p, o, c);

            }

            // avoid overflow.
            return (int) Math.min(Integer.MAX_VALUE, n);
            
        }

        public CloseableIteration<? extends Resource, SailException> getContextIDs()
                throws SailException {
            
            if (!database.isQuads())
                throw new UnsupportedOperationException();

            if(database.getSPORelation().oneAccessPath) {

                /*
                 * The necessary index does not exist (we would have to scan
                 * everything and filter to obtain a distinct set).
                 */
 
                throw new UnsupportedOperationException();

            }

            // flush before query.
            flushStatementBuffers(true/* assertions */, true/* retractions */);
            
            // Visit the distinct term identifiers for the context position.
            final IChunkedIterator<Long> itr = database.getSPORelation()
                    .distinctTermScan(SPOKeyOrder.CSPO);

            // Resolve the term identifiers to terms efficiently during iteration.
            final BigdataValueIterator itr2 = new BigdataValueIteratorImpl(
                    database, itr);
            
//            return new CloseableIteration</*? extends*/ Resource, SailException>() {
            return new CloseableIteration<Resource, SailException>() {
                Resource next = null;

                public void close() throws SailException {
                    next = null;
                    itr2.close();
                }

                public boolean hasNext() throws SailException {
                    if (next != null)
                        return true;
                    while (itr2.hasNext()) {
                        next = (Resource) itr2.next();
                        if (next.stringValue().equals(BNS.NULL_GRAPH)) {
                            next = null;
                            continue;
                        }
                        return true;
                    }
                    return false;
                }

                public Resource next() throws SailException {
                    if (next == null)
                        throw new SailException();
                    final Resource tmp = next;
                    next = null;
                    return tmp;
                }

                public void remove() throws SailException {
                    /*
                     * Note: remove is not supported. The semantics would
                     * require that we removed all statements for the last
                     * visited context.
                     */
                    throw new UnsupportedOperationException();
//                    itr2.remove();
                }
            };

//            return (CloseableIteration<? extends Resource, SailException>) itr2;

        }

        /*
         * transaction support.
         */
        
        /**
         * Note: The semantics depend on the {@link Options#STORE_CLASS}. See
         * {@link ITripleStore#abort()}.
         */
        public void rollback() throws SailException {

            assertWritable();

            // discard buffered assertions and/or retractions.
            clearBuffers();

            // discard the write set.
            database.abort();
            
        }
        
        /**
         * Commit the write set.
         * <p>
         * Note: The semantics depend on the {@link Options#STORE_CLASS}.  See
         * {@link ITripleStore#commit()}.
         */
        public void commit() throws SailException {

            assertWritable();

            /*
             * Flush any pending writes.
             * 
             * Note: This must be done before you compute the closure so that the
             * pending writes will be read by the inference engine when it computes
             * the closure.
             */
            
            flushStatementBuffers(true/* assertions */, true/* retractions */);
            
            database.commit();
            
        }

        final public boolean isOpen() throws SailException {

            return open;
            
        }

        /**
         * Note: This does NOT implicitly {@link #rollback()} the
         * {@link SailConnection}. If you are doing error handling do NOT
         * assume that {@link #close()} will discard all writes.<p>
         * 
         * @todo Since there is a moderate amount of state (the buffers) it
         *       would be nice to not have to reallocate those. In order to
         *       reuse the buffer for writable connections we need to separate
         *       the concept of whether or not the connection is opened from its
         *       buffer state. Note that the scale-out triple store allows
         *       concurrent writers, so each writer needs its own buffers for
         *       that scenario.
         */
        public void close() throws SailException {

            assertOpen();

            /*
             * Note: I have commented out the implicit [rollback]. It causes the
             * live indices to be discarded by the backing journal which is a
             * significant performance hit. This means that if you write on a
             * SailConnection and do NOT explicitly rollback() the writes then
             * any writes that were flushed through to the database will remain
             * there and participate in the next commit.
             * 
             * @todo we could notice if there were writes and only rollback the
             * store when there were uncommitted writes.  this scenario can only
             * arise for the Journal.  Any federation based system will be using
             * unisolated operations with auto-commit.
             */
            
//            * Note: Since {@link #close()} discards any uncommitted writes it is
//            * important to commit the {@link #getDatabase()} made from OUTSIDE of
//            * the {@link BigdataSail} before opening a {@link SailConnection},
//            * even if the connection does not write on the database (this artifact
//            * arises because the {@link SailConnection} is using unisolated writes
//            * on the database).
//            * 
//            // discard any changes that might be lying around.
//            rollback();

            // notify the SailBase that the connection is no longer in use.
            BigdataSail.this.connectionClosed(this);

            // release the semaphore so that another writer may run.
            semaphore.release();
            
            open = false;
            
        }

        /**
         * Flush the statement buffers. The {@link BigdataSailConnection}
         * heavily buffers assertions and retractions. Either a {@link #flush()}
         * or a {@link #commit()} is required before executing any operations
         * directly against the backing {@link AbstractTripleStore} so that the
         * buffered assertions or retractions will be written onto the KB and
         * become visible to other methods. This is not a transaction issue --
         * just a buffer issue. The public methods on the
         * {@link BigdataSailConnection} all flush the buffers before performing
         * any queries against the underlying {@link AbstractTripleStore}.
         */
        public void flush() {

            flushStatementBuffers(true/* flushAssertBuffer */, true/* flushRetractBuffer */);
            
        }
        
        /**
         * Flush pending assertions and/or retractions to the database using
         * efficient batch operations. If {@link #getTruthMaintenance()} returns
         * <code>true</code> this method will also handle truth maintenance.
         * <p>
         * Note: This MUST be invoked within any method that will read on the
         * database to ensure that any pending writes have been flushed
         * (otherwise the read operation will not be able to see the pending
         * writes). However, methods that assert or retract statements MUST only
         * flush the buffer on which they will NOT write. E.g., if you are going
         * to retract statements, then first flush the assertions buffer and
         * visa versa.
         */
        protected void flushStatementBuffers(final boolean flushAssertBuffer,
                final boolean flushRetractBuffer) {

            if (readOnly) return;

            if (flushAssertBuffer && assertBuffer != null) {

                // flush statements
                assertBuffer.flush();
                
                if(getTruthMaintenance()) {

                    // do TM, writing on the database.
                    tm.assertAll((TempTripleStore)assertBuffer.getStatementStore());

                    // must be reallocated on demand.
                    assertBuffer = null;

                }
                
            }

            if (flushRetractBuffer && retractBuffer != null) {

                // flush statements.
                retractBuffer.flush();
                
                if(getTruthMaintenance()) {
                
                    // do TM, writing on the database.
                    tm.retractAll((TempTripleStore)retractBuffer.getStatementStore());

                    // must be re-allocated on demand.
                    retractBuffer = null;

                }
                
            }
            
        }
        
        protected void assertOpen() throws SailException {

            if(!open) {
                
                throw new SailException("Closed");
                
            }

        }
        
        protected void assertWritable() throws SailException {

            assertOpen();
            
            if (readOnly) {

                throw new SailException("Read-only");

            }
            
        }

        /**
         * Note: if the context is <code>null</code>, then you will see data
         * from each context in a quad store, including anything in the
         * {@link BigdataSail#NULL_GRAPH}.
         */
        @SuppressWarnings("unchecked")
        public CloseableIteration<? extends Statement, SailException> getStatements(
                final Resource s, final URI p, final Value o,
                final boolean includeInferred, final Resource... contexts)
                throws SailException {

            OpenRDFUtil.verifyContextNotNull(contexts);
            
            if (log.isInfoEnabled())
                log.info("s=" + s + ", p=" + p + ", o=" + o
                        + ", includeInferred=" + includeInferred
                        + ", contexts=" + Arrays.toString(contexts));

            if (contexts.length == 0) {
                
                // Operates on all contexts.
                
                return new Bigdata2SesameIteration<Statement, SailException>(
                        getStatements(s, p, o, null/* c */, includeInferred));

            }

            if (contexts.length == 1 && contexts[0] == null) {

                /*
                 * Operate on just the nullGraph, or on the sole graph if not in
                 * quads mode.
                 */
                
                return new Bigdata2SesameIteration<Statement, SailException>(
                        getStatements(s, p, o,
                                quads ? NULL_GRAPH : null/* c */,
                                includeInferred));

            }

            /*
             * Note: The Striterator pattern expands each context in turn. The
             * Striterator itself is wrapped as ICloseableIterator, and that
             * gets wrapped for the Sesame CloseableIteration and returned.
             * 
             * FIXME parallelize this in chunks using a thread pool. See
             * DefaultGraphSolutionExpander for examples. Or we could have a
             * thread pool specifically for this purpose on the Sail, which
             * tends to have a nice long life cycle so that could make sense.
             */

            // See QueryEvaluationIterator for an example of how to do this.
            return new Bigdata2SesameIteration<Statement, SailException>(
                    new CloseableIteratorWrapper<Statement>(
                    new Striterator(Arrays
                    .asList(contexts).iterator()).addFilter(new Expander() {
                private static final long serialVersionUID = 1L;
                @Override
                protected Iterator expand(final Object c) {
                    return getStatements(//
                            s, p, o,//
                            (Resource) ((c == null && quads) ? NULL_GRAPH :c),//
                            includeInferred//
                            );
                }
            })));

        }

        /**
         * Returns an iterator that visits {@link BigdataStatement} objects.
         */
        @SuppressWarnings("unchecked")
        private BigdataStatementIterator getStatements(final Resource s,
                final URI p, final Value o, final Resource c,
                final boolean includeInferred) {

            flushStatementBuffers(true/* assertions */, true/* retractions */);

            /*
             * When includedInferred is false we set a filter that causes the
             * access path to only visit the Explicit statements.
             */
            final IElementFilter<ISPO> filter = includeInferred ? null
                    : ExplicitSPOFilter.INSTANCE;

            final IAccessPath<ISPO> accessPath = database.getAccessPath(s, p,
                    o, c, filter);

            if(accessPath instanceof EmptyAccessPath) {
                
                /*
                 * One of the Values was unknown so the access path will be empty.
                 * 
                 * Note: This is true even if we are doing some backchaining.
                 */
                
                return EmptyStatementIterator.INSTANCE;
                
            }
            
            /*
             * Some valid access path.
             */
            
            final IChunkedOrderedIterator<ISPO> src;

            final boolean backchain = database.getAxioms().isRdfSchema()
                    && includeInferred && isQueryTimeExpander(); 
            
//            System.err.println("s=" + s + ", p=" + p + ", o=" + o
//                    + ",\nincludeInferred=" + includeInferred + ", backchain="
//                    + backchain
//                    + ",\nrawAccessPath="
//                    + accessPath
//                    + "\nrangeCount(exact)="
//                    + accessPath.rangeCount(true/*exact*/));
  
            if (backchain) {

                /*
                 * Obtain an iterator that will generate any missing entailments
                 * at query time. The behavior of the iterator depends on how
                 * the InferenceEngine was configured.
                 */
                
                src = new BackchainAccessPath(database, accessPath).iterator();

//                System.err.print("backchainAccessPath");
//                System.err.println(": rangeCount="
//                        + new BackchainAccessPath(database, accessPath)
//                                .rangeCount(true/* exact */));

            } else {

                /*
                 * Otherwise we only return the statements actually present in
                 * the database.
                 * 
                 * Note: An ExplicitSPOFilter is set above that enforces this.
                 */

                src = accessPath.iterator();
                
            }

            /*
             * Resolve SPOs containing term identifiers to BigdataStatementImpls
             * containing BigdataValue objects. The blank node term identifiers
             * will be recognized if they have been seen in the context of this
             * session and will be resolved to the corresponding blank node
             * object in order to preserve their blank node IDs across the scope
             * of the connection.
             * 
             * FIXME bnodes : Also fix in BigdataConstructIterator.
             * 
             * FIXME bnodes : Consider simplifying by passing along the desired
             * valueFactory with the forward and reverse bnode mappings into
             * database.asStatementIterator(src). Note that this is essentially
             * a transactional isolation issue.
             */
            return new BigdataStatementIteratorImpl(database, bnodes2, src)
                    .start(database.getExecutorService());

//            return database.asStatementIterator(src);

        }

        // Note: not part of the Sesame 2 API.
//        public boolean hasStatement(Resource s, URI p, Value o) {
//
//            flushStatementBuffers();
//
//            if( RDF.TYPE.equals(p) && RDFS.RESOURCE.equals(o) ) {
//                
//                if (database.getTermId(s) != NULL) {
//                    
//                    return true;
//                    
//                }
//                
//            }
//            
//            return database.hasStatement(s, p, o);
//            
//        }

        /**
         * Computes the closure of the triple store for RDF(S)+ entailments.
         * <p>
         * This computes the closure of the database. This can be used if you do
         * NOT enable truth maintenance and choose instead to load up all of
         * your data first and then compute the closure of the database. Note
         * that some rules may be computed by eager closure while others are
         * computed at query time.
         * <p>
         * Note: If there are already entailments in the database AND you have
         * retracted statements since the last time the closure was computed
         * then you MUST delete all entailments from the database before
         * re-computing the closure.
         * <p>
         * Note: This method does NOT commit the database. See
         * {@link ITripleStore#commit()} and {@link #getTripleStore()}.
         * 
         * @see #removeAllEntailments()
         */
        public void computeClosure() throws SailException {

            assertWritable();

            flushStatementBuffers(true/* assertions */, true/* retractions */);

            getInferenceEngine().computeClosure(null/* focusStore */);

        }
        
        /**
         * Removes all "inferred" statements from the database (does NOT commit
         * the database).
         */
        public void removeAllEntailments() throws SailException {
            
            assertWritable();
            
            flushStatementBuffers(true/* assertions */, true/* retractions */);

            if (quads) {

                // quads materalized inferences not supported yet. 
                throw new UnsupportedOperationException();
                
            }
            
            database
                    .getAccessPath(NULL, NULL, NULL, InferredSPOFilter.INSTANCE)
                    .removeAll();
            
        }

        /*
         * High-level query.
         */

        /**
         * Note: The <i>includeInferred</i> argument is applied in two ways.
         * First, inferences are stripped out of the {@link SPOAccessPath}.
         * Second, query time expansion of
         * <code>foo rdf:type rdfs:Resource</code>, owl:sameAs, etc.
         * <p>
         * Note: Query time expansion can be disabled independently using
         * {@link Options#QUERY_TIME_EXPANDER}, but not on a per-query basis.
         * 
         * @todo The [bindings] are supposed to be inputs to the query
         *       evaluation, but I am still not quite clear what the role of the
         *       "input" binding set is here. Based on their own code, e.g.,
         *       MemoryStore, and the Sesame TCK, it is clear that evaluation
         *       needs to proceed against an empty binding set once it gets
         *       started.
         */
        public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(
                TupleExpr tupleExpr, Dataset dataset,
                final BindingSet bindings, final boolean includeInferred)
                throws SailException {

            if (log.isInfoEnabled())
                log.info("Evaluating query: " + tupleExpr + ", dataSet="
                        + dataset + ", includeInferred=" + includeInferred);

            flushStatementBuffers(true/* assertions */, true/* retractions */);

            // Clone the tuple expression to allow for more aggressive optimizations
            tupleExpr = tupleExpr.clone();

            if (!(tupleExpr instanceof QueryRoot)) {
                // Add a dummy root node to the tuple expressions to allow the
                // optimizers to modify the actual root node
                tupleExpr = new QueryRoot(tupleExpr);
            }

            try {

                /*
                 * Convert the terms in the query with BigdataValues. Both the
                 * native joins and the BigdataEvaluationStatistics rely on
                 * this.
                 */
                    dataset = replaceValues(dataset, tupleExpr);

                final TripleSource tripleSource = new BigdataTripleSource(this,
                        includeInferred);

                final EvaluationStrategyImpl strategy = new BigdataEvaluationStrategyImpl(
                        (BigdataTripleSource) tripleSource, dataset,
                        nativeJoins);

                final QueryOptimizerList optimizerList = new QueryOptimizerList();
                optimizerList.add(new BindingAssigner());
                optimizerList.add(new ConstantOptimizer(strategy));
                optimizerList.add(new CompareOptimizer());
                optimizerList.add(new ConjunctiveConstraintSplitter());
                optimizerList.add(new SameTermFilterOptimizer());
                // only need to optimize the join order this way if we are not
                // using native joins
                if (nativeJoins == false) {
                    optimizerList.add(new QueryJoinOptimizer(
                            new BigdataEvaluationStatistics(this)));
                }
                optimizerList.add(new FilterOptimizer());

                optimizerList.optimize(tupleExpr, dataset, bindings);

                if (log.isInfoEnabled())
                    log.info("Optimized query: " + tupleExpr);

                // Note: evaluation begins with an empty binding set NOT the
                // caller's bindingSet.
                final CloseableIteration<BindingSet, QueryEvaluationException> itr = strategy
                        .evaluate(tupleExpr,
                                org.openrdf.query.impl.EmptyBindingSet
                                        .getInstance());

                return itr;

            } catch (QueryEvaluationException e) {

                throw new SailException(e);

            }

        }

        /**
         * Batch resolve and replace all {@link Value} objects stored in
         * variables or in the {@link Dataset} with {@link BigdataValue}
         * objects, which have access to the 64-bit internal term identifier
         * associated with each value in the database.
         * <p>
         * Note: The native rule execution must examine the resulting
         * {@link BigdataValue}s. If any value does not exist in the lexicon
         * then its term identifier will be ZERO (0L). {@link StatementPattern}s
         * with term identifiers of ZERO (0L) WILL NOT match anything in the
         * data and MUST NOT be executed since a ZERO (0L) will be interpreted
         * as a variable!
         */
        protected Dataset replaceValues(final Dataset dataset,
                final TupleExpr tupleExpr) throws SailException {

            /*
             * Resolve the values used by this query.
             * 
             * Note: If any value can not be resolved, then its term identifer
             * will remain ZERO (0L) (aka NULL). Except within OPTIONALs, this
             * indicates that the query CAN NOT be satisified by the data since
             * one or more required terms are unknown to the database.
             */
            final HashMap<Value, BigdataValue> values = new HashMap<Value, BigdataValue>();

            final BigdataValueFactory valueFactory = database.getValueFactory();

            if (dataset != null) {

                for(URI uri : dataset.getDefaultGraphs())
                    values.put(uri, valueFactory.asValue(uri));
                
                for(URI uri : dataset.getNamedGraphs())
                    values.put(uri, valueFactory.asValue(uri));
                
            }

            tupleExpr.visit(new QueryModelVisitorBase<SailException>() {

                @Override
                public void meet(final Var var) {
                    
                    if (var.hasValue()) {

                        final Value val = var.getValue();

                        // add BigdataValue variant of the var's Value.
                        values.put(val, valueFactory.asValue(val));
                        
                    }
                    
                }
                
            });

            /*
             * Batch resolve term identifiers for those BigdataValues.
             * 
             * Note: If any value does not exist in the lexicon then its term
             * identifier will be ZERO (0L).
             */
            {
                 
                 final BigdataValue[] terms = values.values().toArray(
                        new BigdataValue[] {});
    
                 database.getLexiconRelation().addTerms(terms, terms.length,
                         true/* readOnly */);

            }
            
            /*
             * Replace the values with BigdataValues having their resolve term
             * identifiers.
             */
            tupleExpr.visit(new QueryModelVisitorBase<SailException>() {

                @Override
                public void meet(Var var) {
                    
                    if (var.hasValue()) {

                        // the Sesame Value object.
                        final Value val = var.getValue();

                        // Lookup the resolve BigdataValue object.
                        final BigdataValue val2 = values.get(val);

                        assert val2 != null : "value not found: "+var.getValue();
                        
                        if (log.isDebugEnabled())
                            log.debug("value: " + val + " : " + val2 + " ("
                                    + val2.getTermId() + ")");

                        if (val2.getTermId() == NULL) {

                            /*
                             * Since the term identifier is NULL this value is
                             * not known to the kb.
                             */
                            
                            if(log.isInfoEnabled())
                                log.info("Not in knowledge base: " + val2);
                            
                        }
                        
                        // replace the constant in the query.
                        var.setValue(val2);

                    }
                }
                
            });
            
            if (dataset != null) {
                
                final DatasetImpl dataset2 = new DatasetImpl();
                
                for(URI uri : dataset.getDefaultGraphs())
                    dataset2.addDefaultGraph((URI)values.get(uri));
                
                for(URI uri : dataset.getNamedGraphs())
                    dataset2.addNamedGraph((URI)values.get(uri));
                
                return dataset2;
                
            }
            
            return dataset;

        }

    }
   
}
