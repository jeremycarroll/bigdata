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
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.StatementImpl;
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
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailConnectionListener;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.SailBase;

import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.inf.TruthMaintenance;
import com.bigdata.rdf.model.BigdataBNodeImpl;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.InferredSPOFilter;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.rdf.store.BigdataStatementIteratorImpl;
import com.bigdata.rdf.store.EmptyStatementIterator;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.relation.accesspath.EmptyAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.striterator.IChunkedOrderedIterator;


/**
 * Sesame <code>2.x</code> integration.
 * <p>
 * 
 * This SAIL enforces the constraint that there may be at most one writer. This
 * is done in {@link #getConnection()}. If a 2nd thread attempts to obtain a
 * {@link SailConnection} then that thread will block until the open
 * {@link SailConnection} is {@link SailConnection#close()}d.
 * 
 * <h2>Concurrent Query</h2>
 * 
 * While this class, by itself, is NOT safe for readers that run concurrently
 * with a writer, it provides a read-committed view that is safe for concurrent
 * readers and that is safe to use while a writer is concurrently running a
 * transaction. See {@link #asReadCommittedView()}.
 * <p>
 * In order to use concurrent query you basically have to decide whether an
 * operation is read-only or read-write. A read-only operation should be
 * directed to the {@link #asReadCommittedView()} while read-write operations
 * should obtain a lock on the mutable SAIL using {@link #startTransaction()}
 * before doing any other work. When approached in this manner, you will be able
 * to execute read operations (for example, high-level query) with full
 * concurrency and SAIL transactions will be serialized (at most one will run at
 * a time) but they will not block readers.
 * 
 * @todo update javadoc to describe new concurrency options.
 * 
 * @todo update javadoc to describe the "provenance" mode for the database,
 *       where the context is always a statement identifier and can be bound in
 *       query to recover statements about statements. Also document the
 *       extension required for a client to make use of the resulting RDF/XML,
 *       e.g., if a CONSTRUCT query is used to recover statements about
 *       statements.
 * 
 * @todo support a "quadStore" mode with named graphs. I'm not clear if we need
 *       3 or 6 statement indices for this.
 * 
 * @todo Get basic query optimizations working and test w/ modified LUBM.
 *       <p>
 *       (a) Use {@link BigdataValue} and avoid lookup of terms in the lexicon
 *       staying within out native API for JOINs.
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
 * FIXME add a magic predicate for free text search <code>bigdata:search</code>.
 * This will need to get hooked in as an alternative to a standard join,
 * returning a set of term identifiers that are then used to select matching
 * statements in the triple pattern within which it is embedded.
 * 
 * FIXME run against the "Technology Compatibilty Kit"
 * https://src.aduna-software.org/svn/org.openrdf/projects/sesame2-tck/
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataSail extends SailBase implements Sail {

    /**
     * Additional parameters understood by the Sesame 2.x SAIL implementation.
     * 
     * @todo also extend Options for the scale-out triple store once defined.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends com.bigdata.rdf.store.LocalTripleStore.Options {
    
        /**
         * This optional boolean property may be used to specify whether or not
         * RDFS entailments are maintained by eager closure of the knowledge
         * base (the default is <code>true</code>).  This property only effects
         * data loaded through the {@link Sail}.
         */
        public static final String TRUTH_MAINTENANCE = "truthMaintenance"; 
    
        public static final String DEFAULT_TRUTH_MAINTENANCE = "true"; 
    
        /**
         * The property whose value is the name of the {@link ITripleStore} 
         * implementation that will be instantiated.  An {@link InferenceEngine} 
         * will be used to wrap that {@link ITripleStore}.
         */
        public static final String STORE_CLASS = "storeClass";
        
        public static final String DEFAULT_STORE_CLASS = LocalTripleStore.class.getName();
    
        /**
         * The capacity of the buffers used to absorb writes.
         * 
         * @todo name conflict?
         */
        public static final String BUFFER_CAPACITY = "bufferCapacity";
    
        public static final String DEFAULT_BUFFER_CAPACITY = "10000";
        
        /**
         * This boolean may be used to specify whether or not to use "native"
         * joins, bypassing the Sesame join mechanism. 
         */
        public static final String NATIVE_JOINS = "nativeJoins";
        
        public static final String DEFAULT_NATIVE_JOINS = "false";
    }

    /**
     * Logger.
     */
    private static final Logger log = Logger.getLogger(BigdataSail.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();
    
    /**
     * The equivilent of a null identifier for an internal RDF Value.
     */
    protected static final long NULL = IRawTripleStore.NULL;

    final protected AbstractTripleStore database;

    final protected Properties properties;

//    final private ValueFactory valueFactory = BigdataValueFactory.INSTANCE;
    
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
        
        if(_inf==null) {
        
            _inf = database.getInferenceEngine();
            
        }
        
        return _inf;
        
    }
    
    // hard reference cache.
    private InferenceEngine _inf = null;
    
    /**
     * The configured capacity for the statement buffer(s).
     */
    final private int bufferCapacity;
    
    /**
     * When true, the RDFS closure will be maintained.
     */
    final private boolean truthMaintenance;
    
    /**
     * When true, SAIL will delegate joins to bigdata internal joins.
     */
    final private boolean nativeJoins;
    
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
        
        properties.setProperty(Options.FILE,"bigdata"+Options.JNL);
        
        return properties;
        
    }
    
    /**
     * Create/re-open the database identified by the properites.
     * 
     * @see Options
     */
    private static AbstractTripleStore setUp(Properties properties) {

        final String val = properties.getProperty(
                BigdataSail.Options.STORE_CLASS,
                BigdataSail.Options.DEFAULT_STORE_CLASS);

        try {

            final Class storeClass = Class.forName(val);

            if (!ITripleStore.class.isAssignableFrom(storeClass)) {

                throw new RuntimeException("Must extend "
                        + ITripleStore.class.getName() + " : "
                        + storeClass.getName());

            }

            final Constructor ctor = storeClass
                    .getConstructor(new Class[] { Properties.class });

            final AbstractTripleStore database = (AbstractTripleStore) ctor
                    .newInstance(new Object[] { properties });

            return database;

        } catch (Exception t) {

            throw new RuntimeException(t);

        }

    }

    /**
     * Create or re-open a database instance configured using defaults.
     */
    public BigdataSail() {
        
        this(setUp(getDefaultProperties()));
    }
    
    /**
     * Create or open a database instance configured using the specified
     * properties.
     * <p>
     * Note: you MUST still invoke {@link #initialize()}.
     * 
     * @see Options
     */
    public BigdataSail(Properties properties) {
        
        this(setUp(properties));
        
    }
    
    /**
     * Variant constructor used to wrap an exiting database.
     * <p>
     * Note: you MUST still invoke {@link #initialize()}.
     * 
     * @param database
     *            A reference for an existing database instance.
     * 
     * @see Options
     */
    public BigdataSail(AbstractTripleStore database) {

        if (database == null)
            throw new IllegalArgumentException();
        
        this.database = database;
        
        this.properties = database.getProperties();
        
        // truthMaintenance
        {
            
            truthMaintenance = Boolean.parseBoolean(properties.getProperty(
                    BigdataSail.Options.TRUTH_MAINTENANCE,
                    BigdataSail.Options.DEFAULT_TRUTH_MAINTENANCE));

            log.info(BigdataSail.Options.TRUTH_MAINTENANCE + "="
                    + truthMaintenance);
            
        }
        
        // bufferCapacity
        {
            
            bufferCapacity = Integer.parseInt(properties.getProperty(
                    BigdataSail.Options.BUFFER_CAPACITY,
                    BigdataSail.Options.DEFAULT_BUFFER_CAPACITY));

            log
                    .info(BigdataSail.Options.BUFFER_CAPACITY + "="
                            + bufferCapacity);

        }

        // nativeJoins
        {
            
            nativeJoins = Boolean.parseBoolean(properties.getProperty(
                    BigdataSail.Options.NATIVE_JOINS,
                    BigdataSail.Options.DEFAULT_NATIVE_JOINS));

            log
                    .info(BigdataSail.Options.NATIVE_JOINS + "="
                            + nativeJoins);

        }

    }

    public void initialize() throws SailException {
        
        // NOP (nothing to invoke in the SailBase).
        
    }
    
    private WeakReference<BigdataSailConnection> readCommittedRef;
    
    /**
     * A factory returning the singleton read-committed view of the database.
     * This view is safe for concurrent query operations.
     * <p>
     * Note: This is only supported when the {@link Options#STORE_CLASS}
     * specifies {@link LocalTripleStore} or {@link ScaleOutTripleStore}.
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
    public BigdataSailConnection getReadHistoricalView(long commitTime) {
       
        AbstractTripleStore tmp = (AbstractTripleStore) database
                .getIndexManager().getResourceLocator().locate(
                        database.getNamespace(),
                        TimestampUtility.asHistoricalRead(commitTime));
        
        return new BigdataSailConnection(tmp);
        
    }
    
    /**
     * A {@link BigdataValueFactoryImpl}
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
     * @todo many of the store can support concurrent writers, but there is a
     *       requirement to serialize writers when truth maintenance is enabled.
     */
    protected BigdataSailConnection getConnectionInternal() throws SailException {

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
    public BigdataSailConnection getConnection() throws SailException {
        
        return (BigdataSailConnection)super.getConnection();
        
    }

    /**
     * Note: This is an immediate shutdown - any running transactions will not
     * complete.
     */
    protected void shutDownInternal() throws SailException {
        
        database.close();
        
    }

    /**
     * Inner class implements the {@link SailConnection}. Some additional
     * functionality is available on this class, including
     * {@link #computeClosure()}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class BigdataSailConnection implements SailConnection {

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
        private StatementBuffer assertBuffer = null;
        
        /**
         * Used to buffer statements being retracted.
         * 
         * @see #getRetractionBuffer()
         */
        private StatementBuffer retractBuffer = null;
        
        /**
         * A canonicalizing mapping for blank nodes whose life cycle is the same
         * as that of the {@link SailConnection}.
         * 
         * @todo share with {@link BigdataStatementIteratorImpl}s returned by
         *       the {@link SailConnection}? Make concurrency safe if I do
         *       that?
         */
        private final Map<String,BigdataBNodeImpl> bnodes;
        
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
        synchronized protected StatementBuffer getAssertionBuffer() {

            if (assertBuffer == null) {

                if (truthMaintenance) {

                    assertBuffer = new StatementBuffer(tm.newTempTripleStore(),
                            database, bufferCapacity);
                    
                } else {

                    assertBuffer = new StatementBuffer(database, bufferCapacity);

                }

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
        synchronized protected StatementBuffer getRetractionBuffer() {
            
            if (retractBuffer == null) {

                if (truthMaintenance) {

                    retractBuffer = new StatementBuffer(tm.newTempTripleStore(),
                            database, bufferCapacity);

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
                    
                    retractBuffer = new StatementBuffer(database, bufferCapacity);

                }

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
        protected BigdataSailConnection(AbstractTripleStore database) {

            this.database = database;
            
            readOnly = database.isReadOnly();            
         
            if (database.isReadOnly()) {
                
                log.info("Read-only view");
                
                tm = null;
                
                bnodes = null;
                
            } else {

                log.info("Read-write view");

                bnodes = new HashMap<String,BigdataBNodeImpl>(bufferCapacity);
                
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
        public AbstractTripleStore getDatabase() {
            
            return database;
            
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
         * imposes an extremely high burden on the database reqiring the
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
        synchronized protected void fireSailChangedEvent(boolean added, Statement stmt)
        {
            
            /*
             * FIXME make sure that you test on m_listeners BEFORE hand so that
             * you can avoid the work required to materialize the statements if
             * there are no listeners registered!  (The burden is especially high
             * when removing statements).
             */

            if( m_listeners == null ) return;
            
            /*
             * @todo since we are calling the listener for every single
             * statement we really need to eagerly materialize the array of
             * listeners whenever the listeners are added or removed so that we
             * can efficiently process that array here. as long as this method
             * and the add/remove methods are all synchronized then the result
             * will be coherent.
             */
            
            SailConnectionListener[] listeners = (SailConnectionListener[]) 
                m_listeners.toArray( new SailConnectionListener[]{} );

            for( int i=0; i<listeners.length; i++ ) {
                
                SailConnectionListener l = listeners[ i ];
                
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
        
        public void setNamespace(String prefix, String namespace)
                throws SailException {

            assertWritable();

            database.addNamespace(namespace,prefix);
            
        }

        public String getNamespace(String prefix) {
            
            return database.getNamespace(prefix);
            
        }
        
        public void removeNamespace(String prefix) {
            
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
        
        public void addStatement(Resource s, URI p, Value o, Resource... contexts) throws SailException {

            if (contexts == null || contexts.length == 0 || contexts[0] == null) {

                addStatement(s, p, o, (Resource) null);
                
            } else {

                for (Resource c : contexts) {

                    addStatement(s, p, o, c);

                }
                
            }
            
        }

        private void addStatement(Resource s, URI p, Value o, Resource c)
                throws SailException {

            assertWritable();

            // flush any pending retractions first!
            flushStatementBuffers(false/* flushAssertBuffer */, true/* flushRetractBuffer */);
            
            // buffer the assertion.
            getAssertionBuffer().add(s, p, o, c);
            
            if (m_listeners != null) {

                // Notify listener(s).
                
                fireSailChangedEvent(true, new StatementImpl(s, p, o));
                
            }

        }

        public void clear(Resource... contexts) throws SailException {
            
            assertWritable();

            // discard any pending writes.
            clearBuffers();
            
            /*
             * @todo if listeners are registered then we need to materialize
             * everything that is going to be removed....
             */
            
            if (contexts == null || contexts.length == 0 || contexts[0] == null) {
                
                // clear the database.

                // @todo obtain exclusive lock 1st.
                database.destroy();
                database.create();
                
            } else {

                /*
                 * FIXME It is no longer true that there is only one graph per database
                 * so we have to actually delete the statements in the graph by writing
                 * those deletes on the database.
                 */

                throw new UnsupportedOperationException();
                
            }
                
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
                    assertBuffer.getStatementStore().closeAndDelete();
                    
                    // must be re-allocated on demand.
                    assertBuffer = null;
                    
                }
                
            }

            if (retractBuffer != null) {

                // discard all buffered data.
                retractBuffer.reset();

                if (truthMaintenance) {

                    // discard the temp store that buffers retractions.
                    retractBuffer.getStatementStore().closeAndDelete();

                    // must be re-allocated on demand.
                    retractBuffer = null;

                }

            }

        }

        /**
         * Note: This method is quite expensive since it must materialize all
         * statement in either the database or in the specified context(s) and
         * then filter for the explicit statements in order to obtain an exact
         * count. See {@link AbstractTripleStore#getStatementCount()} or
         * {@link IAccessPath#rangeCount()} for efficient methods for reporting
         * on the #of statements in the database or within a specific context.
         */
        public long size(Resource... contexts) throws SailException {

            flushStatementBuffers(true/* assertions */, true/* retractions */);
            
            if (contexts == null || contexts.length == 0 || contexts[0] == null) {

                return database.getExplicitStatementCount();
                
            } else {
                
                throw new UnsupportedOperationException();
                
            }
            
        }

        public void removeStatements(Resource s, URI p, Value o,
                Resource... contexts) throws SailException {

            if (contexts == null || contexts.length == 0 || contexts[0] == null) {

                removeStatements(s, p, o, (Resource) null);

            } else {

                for (Resource c : contexts) {

                    removeStatements(s, p, o, c);

                }
                    
            }
            
        }

        /**
         * Note: The CONTEXT is ignored when in statementIdentifier mode!
         * 
         * @param s
         * @param p
         * @param o
         * @param c
         * @return
         * @throws SailException
         */
        private int removeStatements(Resource s, URI p, Value o, Resource c)
                throws SailException {
            
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
                 * obtain a chunked iterator using the triple pattern that
                 * visits only the explicit statements.
                 */
                final IChunkedOrderedIterator<ISPO> itr = database
                        .getAccessPath(s, p, o, ExplicitSPOFilter.INSTANCE)
                        .iterator();

                // the tempStore absorbing retractions.
                final AbstractTripleStore tempStore = getRetractionBuffer()
                        .getStatementStore();

                // copy explicit statements to tempStore.
                n = tempStore.addStatements(tempStore, true/* copyOnly */,
                        itr, null/* filter */);
                
            } else {

                /*
                 * Since we are not doing truth maintenance, just remove the
                 * statements from the database (synchronous, batch api, not
                 * buffered).
                 */
                
                n = database.removeStatements(s, p, o);

            }

            // avoid overflow.
            return (int) Math.min(Integer.MAX_VALUE, n);
            
        }
        
        /**
         * @todo implement {@link #getContextIDs()}
         */
        public CloseableIteration<? extends Resource, SailException> getContextIDs() throws SailException {

            throw new UnsupportedOperationException();
            
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

        public CloseableIteration<? extends Statement, SailException> getStatements(
                Resource s, URI p, Value o, boolean includeInferred,
                Resource... contexts) throws SailException {
            
            if (INFO)
                log.info("s=" + s + ", p=" + p + ", o=" + o
                        + ", includeInferred=" + includeInferred
                        + ", contexts=" + Arrays.toString(contexts));
            
            if (contexts == null || contexts.length == 0 || contexts[0] == null) {
                
                // Operates on all contexts.
                
                return getStatements(s, p, o, includeInferred);
                
            }
                
            /*
             * FIXME the context is being ignored but it should operate on the
             * specified contexts.
             */
            
            return getStatements(s, p, o, includeInferred);
            
        }

        /**
         * Returns an iterator that visits {@link BigdataStatement} objects.
         */
        private BigdataStatementIterator getStatements(Resource s, URI p, Value o, boolean includeInferred) {

            flushStatementBuffers(true/* assertions */, true/* retractions */);

            final IElementFilter<ISPO> filter = includeInferred ? null
                    : ExplicitSPOFilter.INSTANCE;

            final IAccessPath<ISPO> accessPath = database.getAccessPath(s, p, o,
                    filter);

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
            
            if(/*getTruthMaintenance() &&*/ includeInferred) {

                /*
                 * FIXME Do this if TM is enabled OR the database closure has
                 * been updated, but we have no way to detect the latter.
                 * 
                 * Obtain an iterator that will generate any missing entailments
                 * at query time. The behavior of the iterator depends on how
                 * the InferenceEngine was configured.
                 */
                
                src = getInferenceEngine().backchainIterator(
                        accessPath.getPredicate());
                
            } else {

                /*
                 * Otherwise NO entailments are permitted and we only return the
                 * statements actually present in the database.
                 */
                
//                // Read straight from the database.
//                if(includeInferred) {

                    // include statements in the database that are axioms or inferences.
                    src = accessPath.iterator();
                    
//                } else {
//                    
//                    // filter such that the caller only sees the explicit statements.
//                    src = accessPath.iterator(ExplicitSPOFilter.INSTANCE);
//                    
//                }
                
            }
            
//            return new BigdataStatementIteratorImpl(this, src);
            return database.asStatementIterator(src);

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
         * {@link ITripleStore#commit()} and {@link #getDatabase()}.
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

            // @todo write unit test - make sure the filter is applied before remove!
            database
                    .getAccessPath(NULL, NULL, NULL, InferredSPOFilter.INSTANCE)
                    .removeAll();
            
        }

        /*
         * High-level query.
         */
        
        /**
         * @todo the {@link Dataset} is the RDF merge of graphs. How does that
         *       interact with inference? It looks like the
         *       {@link StatementPattern} is evaluated against either the entire
         *       database or some set of enumerated contexts. Those evaluations
         *       should of course be done in parallel.
         */
        public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(
                TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred)
                throws SailException {
           
            if (INFO)
                log.info("Evaluating query: "+tupleExpr+", dataSet="+dataset+", includeInferred="+includeInferred);
            
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
                replaceValues(tupleExpr);

                final TripleSource tripleSource = new BigdataTripleSource(this,
                        includeInferred);

                final EvaluationStrategyImpl strategy = new BigdataEvaluationStrategyImpl(
                        (BigdataTripleSource) tripleSource, dataset, nativeJoins);

                QueryOptimizerList optimizerList = new QueryOptimizerList();
                optimizerList.add(new BindingAssigner());
                optimizerList.add(new ConstantOptimizer(strategy));
                optimizerList.add(new CompareOptimizer());
                optimizerList.add(new ConjunctiveConstraintSplitter());
                optimizerList.add(new SameTermFilterOptimizer());
                // only need to optimize the join order this way if we are not
                // using native joins
                if (nativeJoins == false) {
                    optimizerList.add(new QueryJoinOptimizer(new BigdataEvaluationStatistics(this)));
                }
                optimizerList.add(new FilterOptimizer());

                optimizerList.optimize(tupleExpr, dataset, bindings);

                if (INFO)
                    log.info("Optimized query: "+tupleExpr);

                final CloseableIteration<BindingSet, QueryEvaluationException> itr = strategy
                        .evaluate(tupleExpr, bindings);
                
                return itr;
                
            } catch (QueryEvaluationException e) {
             
                throw new SailException(e);
                
            }
            
        }

        /**
         * Replace all Value objects stored in variables with
         * {@link BigdataValue} objects, which have access to the 64-bit
         * internal term identifier associated with each value in the database.
         * 
         * FIXME Batch resolve the term identifiers. This will benefit from the
         * term cache, but does an index hit per unknown term.
         */
        protected void replaceValues(TupleExpr tupleExpr) throws SailException {
            
            tupleExpr.visit(new QueryModelVisitorBase<SailException>() {

                @Override
                public void meet(Var var) {
                    
                    if (var.hasValue()) {

                        Value val = var.getValue();

                        // convert to a BigdataValue object.
                        val = database.getValueFactory().asValue(val);

                        // resolve the termId.
//                        final long termId = 
                        database.getTermId(val);
                        
                        if (DEBUG)
                            log.debug("value: "+val+" : "+((BigdataValue)val).getTermId());
                        
                        // replace the constant.
                        var.setValue(val);

                    }
                }
                
            });
            
        }

    }
   
}
