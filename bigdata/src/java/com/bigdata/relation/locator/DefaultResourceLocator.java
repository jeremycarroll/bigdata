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
 * Created on Jun 30, 2008
 */

package com.bigdata.relation.locator;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.concurrent.NamedLock;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.relation.AbstractResource;
import com.bigdata.relation.IRelation;
import com.bigdata.service.IBigdataFederation;

/**
 * Generic implementation relies on a ctor for the resource with the following
 * method signature:
 * 
 * <pre>
 * public NAME ( IIndexManager indexManager, String namespace, Long timestamp, Properties properties )
 * </pre>
 * 
 * <p>
 * A relation is located using the {@link IIndexStore#getGlobalRowStore()} and
 * materialized by supplying an {@link IIndexManager} that will be able to
 * resolve the indices for the relation's view. Several different contexts are
 * handled:
 * <dl>
 * 
 * <dt>{@link IBigdataFederation}</dt>
 * 
 * <dd>The {@link IRelation} will be resolved using the
 * {@link IBigdataFederation#getGlobalRowStore()} and the
 * {@link IBigdataFederation} as its {@link IIndexManager}. The makes access to
 * a remote and potentially distributed {@link IIndex} transparent to the
 * {@link IRelation}. However, it is NOT possible to resolve local resources on
 * other JVMs - only scale-out indices registered against the
 * {@link IBigdataFederation}.</dd>
 * 
 * <dt>{@link Journal}</dt>
 * 
 * <dd>The {@link IRelation} will be resolved using the
 * {@link Journal#getGlobalRowStore()} and will use the local index objects
 * directly.</dd>
 * 
 * <dt>{@link AbstractTask}</dt>
 * 
 * <dd>If the index is local and monolithic then you can declare the index to
 * the {@link AbstractTask} and <em>override</em> the locator to use
 * {@link AbstractTask#getJournal()} as its index manager. This will give you
 * access to the local index object from within the concurrency control
 * mechanism</dd>
 * 
 * <dt>{@link TemporaryStore}</dt>
 * 
 * <dd>When used by itself, this is just like a {@link Journal}. However,
 * {@link TemporaryStore}s are also used to provide local resources for more
 * efficient data storage for a variety of purposes. When used in this manner,
 * you must explicitly notify the locator of the existence of the
 * {@link TemporaryStore} in order to be able to resolve {@link IRelation}s on
 * the {@link TemporaryStore}. It is prudent to use a prefix for such local
 * resources that guarentees uniqueness, e.g., an {@link UUID}</dd>
 * 
 * </dl>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 *            The generic type of the [R]elation.
 */
public class DefaultResourceLocator<T extends ILocatableResource> extends
        AbstractCachingResourceLocator<T> implements IResourceLocator<T> {

    protected static final transient Logger log = Logger
            .getLogger(DefaultResourceLocator.class);

    protected final transient IIndexManager indexManager;

    private final IResourceLocator<T> delegate;
    
    /**
     * Provides locks on a per-namespace basis for higher concurrency.
     */
    private final transient NamedLock<String> namedLock = new NamedLock<String>();

    /**
     * Designated constructor.
     * 
     * @param indexManager
     * @param delegate
     *            Optional {@link IResourceLocator} to which unanswered requests
     *            are then delegated.
     */
    public DefaultResourceLocator(final IIndexManager indexManager,
            final IResourceLocator<T> delegate) {

        if (indexManager == null)
            throw new IllegalArgumentException();

        this.indexManager = indexManager;

        this.delegate = delegate;// MAY be null.

    }

    public T locate(final String namespace, final long timestamp) {

        if (namespace == null)
            throw new IllegalArgumentException();

        if (log.isInfoEnabled()) {

            log.info("namespace=" + namespace+", timestamp="+timestamp);

        }

        // lock is only for the named relation.
        final Lock lock = namedLock.acquireLock(namespace);

        try {

            // test cache.
            T resource = get(namespace, timestamp);

            if (resource != null) {

                if (log.isDebugEnabled()) {

                    log.debug("cache hit: " + resource);

                }

                return resource;

            }
            
            if (log.isInfoEnabled())
                log.info("cache miss: namespace=" + namespace + ", timestamp="
                        + timestamp);
          
            /*
             * First, test this locator, including any [seeAlso] IIndexManager
             * objects.
             */
            final AtomicReference<IIndexManager> foundOn = new AtomicReference<IIndexManager>();

            final Properties properties = locateResource(namespace, foundOn);
            
            if (properties == null) {

                // Not found by this locator.
                
                if(delegate != null) {
                    
                    /*
                     * A delegate was specified, so see if the delegate can
                     * resolve this request.
                     */
                    
                    if(log.isInfoEnabled()) {
                        
                        log.info("Not found - passing to delegate: namespace="
                                + namespace + ", timestamp=" + timestamp);
                        
                    }
                    
                    // pass request to delegate.
                    resource = delegate.locate(namespace, timestamp);
                    
                    if (resource != null) {

                        if (log.isInfoEnabled()) {

                            log.info("delegate answered: " + resource);

                        }

                        return resource;

                    }

                }
                
                throw new IllegalStateException("Not found: namespace="
                        + namespace+", timestamp="+timestamp);

            }

            if (log.isDebugEnabled()) {

                log.debug(properties.toString());

            }

            // can throw a ClassCastException.
            final String className = properties.getProperty(RelationSchema.CLASS);

            if (className == null) {

                throw new IllegalStateException("Required property not found: namespace="
                        + namespace + ", property=" + RelationSchema.CLASS);

            }

            final Class<? extends T> cls;
            try {

                cls = (Class<? extends T>) Class.forName(className);

            } catch (ClassNotFoundException e) {

                throw new RuntimeException(e);

            }

            if (log.isDebugEnabled()) {

                log.debug("Implementation class=" + cls.getName());

            }

            // create a new instance of the relation.
            resource = newInstance(cls, foundOn.get(), namespace, timestamp,
                    properties);

            {
                
                /*
                 * Add to the cache.
                 * 
                 * Note: There is a HUGE performance penalty for the federation
                 * based modes if we do not allow read-committed views into the
                 * cache!!!
                 * 
                 * FIXME There is a problem when we let in the read-committed
                 * view of a BTree. Each time there is a commit for a given
                 * BTree, the READ_COMMITTED view of that BTree needs to be
                 * replaced by the most recently committed view, which is a
                 * different BTree object and is loaded from a different
                 * checkpoint record.
                 * 
                 * The problem with READ_COMMITTED BTree instances in the
                 * locator cache does not arise with the federation based modes
                 * as they submit requests to a DataService in which the view is
                 * identified by a timestamp.
                 * 
                 * The problem would go aways if we modified BTree such that it
                 * was directly aware of read-committed semantics. In that case
                 * the BTree would have to re-load its state from the backing
                 * store if there had been an intervening checkpoint on the
                 * corresponding live BTree.
                 * 
                 * For now, we are letting READ_COMMITTED resources into the
                 * cache since it is a huge performance penalty otherwise (3x)
                 * and since there does not appear to be anything in the RDF DB
                 * that depends on cached read-committed resources that use
                 * BTree (vs IIndex) objects.  If there were it would wind up
                 * with a STALE view. 
                 */

//                if (timestamp != ITx.READ_COMMITTED)
                {

                    if (log.isDebugEnabled()) {

                        log.debug("Caching: namespace=" + namespace
                                + ", timestamp=" + timestamp);

                    }

                    put(resource);

                }
                
            }

            return resource;

        } finally {

            lock.unlock();

        }

    }

    /**
     * Note: Caller is synchronized for this <i>namespace</i>.
     * 
     * @param namespace
     *            The namespace for the resource.
     * @param foundOn
     *            Used to pass back the {@link IIndexManager} on which the
     *            resource was found as a side-effect.
     * 
     * @return The properties for that resource.
     */
    protected Properties locateResource(String namespace,
            AtomicReference<IIndexManager> foundOn) {

        synchronized (seeAlso) {

            for (IIndexManager indexManager : seeAlso.keySet()) {

                /*
                 * read properties from the global row store for the default
                 * index manager.
                 */
                Properties properties = null;

                try {
                
                    properties = locateResourceOn(indexManager, namespace);
                    
                } catch(IllegalStateException t) {
                    
                    log.warn("Closed? " + indexManager);

                    continue;
                    
                } catch (Throwable t) {

                    log.error(t, t);
                    
                    continue;
                    
                }

                if (properties != null) {

                    if (log.isInfoEnabled()) {

                        log.info("Found: namespace=" + namespace + " on "
                                + indexManager);

                    }

                    // tell the caller _where_ we found the resource.
                    foundOn.set(indexManager);
                    
                    return properties;

                }

            }

        }

        /*
         * read properties from the global row store for the default index
         * manager.
         */
        
        final Properties properties = locateResourceOn(indexManager, namespace);

        if (properties != null) {

            if (log.isInfoEnabled()) {

                log.info("Found: namespace=" + namespace + " on "
                        + indexManager);

            }

            // tell the caller _where_ we found the resource.
            foundOn.set(indexManager);

            return properties;

        }
        
        // will be null.
        return properties;

    }

    /**
     * Return the {@link Properties} that will be used to configure the
     * {@link IRelation} instance. The {@link RelationSchema#CLASS} property
     * MUST be defined and specified the runtime class that will be
     * instantiated.
     * <p>
     * Note: A <code>null</code> return is an indication that the resource was
     * NOT FOUND on this {@link IIndexManager} and the caller SHOULD try another
     * {@link IIndexManager}.
     * 
     * @param namespace
     *            The resource identifier - this is the primary key.
     * 
     * @return The {@link Properties} iff there is a logical row for the given
     *         namespace.
     */
    protected Properties locateResourceOn(IIndexManager indexManager,
            String namespace) {

        if (log.isInfoEnabled()) {

            log.info("indexManager="+indexManager+", namespace=" + namespace);

        }

        /*
         * Note: This is a READ_COMMITTED request. The [timestamp] does not get
         * passed in as it has different semantics for the row store! (Caching
         * here may be useful.)
         */
        final Map<String, Object> map = indexManager.getGlobalRowStore().read(
                RelationSchema.INSTANCE, namespace);

        if (map == null) {

            if (log.isDebugEnabled()) {

                log.debug("No properties: indexManager=" + indexManager
                        + ", namespace=" + namespace);

            }

            return null;

        }

        final Properties properties = new Properties();

        properties.putAll(map);

        if (log.isDebugEnabled()) {

            log.debug("Read properties: indexManager=" + indexManager
                    + ", namespace=" + namespace + " :: " + properties);

        }

        return properties;

    }

    /**
     * Create a new view of the relation.
     * 
     * @param indexManager
     *            The {@link IIndexManager} that will be used to resolve the
     *            named indices for the relation.
     * @param namespace
     *            The namespace for the relation.
     * @param timestamp
     *            The timestamp for the view of the relation.
     * @param properties
     *            Configuration properties for the relation.
     * 
     * @return A new instance of the identifed resource.
     */
    protected T newInstance(Class<? extends T> cls, IIndexManager indexManager,
            String namespace, long timestamp, Properties properties) {

        if (cls == null)
            throw new IllegalArgumentException();
        
        if (indexManager == null)
            throw new IllegalArgumentException();
        
        if (namespace == null)
            throw new IllegalArgumentException();
        
        if (properties == null)
            throw new IllegalArgumentException();

        final Constructor<? extends T> ctor;
        try {

            ctor = cls.getConstructor(new Class[] {//
                            IIndexManager.class,//
                            String.class,// relation namespace
                            Long.class, // timestamp of the view
                            Properties.class // configuration properties.
                    });

        } catch (Exception e) {

            throw new RuntimeException("No appropriate ctor?: cls="
                    + cls.getName() + " : " + e, e);

        }

        final T r;
        try {

            r = ctor.newInstance(new Object[] {//
                    indexManager,//
                    namespace, //
                    timestamp, //
                    properties //
                    });

            if(log.isInfoEnabled()) {
                
                log.info("new instance: "+r);
                
            }
            
            return r;

        } catch (Exception ex) {

            throw new RuntimeException("Could not instantiate relation: " + ex,
                    ex);

        }

    }

    /**
     * Places the instance into the cache iff there is no existing instance in
     * the cache for the same resource and timestamp.
     * <p>
     * Note: This is done automatically by {@link AbstractResource}.
     * 
     * @param instance
     *            The instance.
     */
    public T putInstance(T instance) {
      
        if (instance == null)
            throw new IllegalArgumentException();

        final String namespace = instance.getNamespace();
        
        final long timestamp = instance.getTimestamp();
        
        if (log.isInfoEnabled()) {

            log.info("namespace=" + namespace+", timestamp="+timestamp);

        }

        // lock is only for the named relation.
        final Lock lock = namedLock.acquireLock(namespace);

        try {

            final T tmp = get(namespace, timestamp);
            
            if (tmp != null) {

                if(log.isInfoEnabled()) {
                    
                    log.info("Existing instance already in cache: "+tmp);
                    
                }
                
                return tmp;
                
            }
            
            put(instance);
            
            if (log.isInfoEnabled()) {

                log.info("Instance added to cache: " + instance);
                
            }
            
            return instance;
            
        } finally {
            
            lock.unlock();
            
        }
        
    }
    
    /**
     * Resources that hold hard references to local index objects MUST discarded
     * during abort processing. Otherwise the same resource objects will be
     * returned from the cache and buffered writes on the indices for those
     * relations (if they are local index objects) will still be visible, this
     * defeating the abort semantics.
     */
    public void discard(ILocatableResource<T> instance) {
        
        if (instance == null)
            throw new IllegalArgumentException();

        final String namespace = instance.getNamespace();
        
        final long timestamp = instance.getTimestamp();
        
        if (log.isInfoEnabled()) {

            log.info("namespace=" + namespace+", timestamp="+timestamp);

        }

        // lock is only for the named relation.
        final Lock lock = namedLock.acquireLock(namespace);

        try {

            final boolean found = clear(namespace, timestamp);
            
            if (log.isInfoEnabled()) {

                log.info("instance=" + instance + ", found=" + found);
                
            }
            
        } finally {
            
            lock.unlock();
            
        }        
        
    }
    
    /**
     * Causes the {@link IIndexManager} to be tested when attempting to resolve
     * a resource identifiers. The {@link IIndexManager} will be automatically
     * cleared from the set of {@link IIndexManager}s to be tested if its
     * reference is cleared by the JVM. If it becomes closed asynchronously then
     * a warning will be logged until its reference is cleared.
     * <p>
     * Note: The default {@link IIndexManager} specified to the ctor normally
     * ensures that the global namespace is consistent (it is not possible to
     * have two indices or {@link ILocatableResource}s with the same name).
     * When you add additional {@link IIndexManager}s, the opportunity for an
     * <em>inconsistent</em> unified namespace is introduced. You can protect
     * yourself by ensuring that resources located on {@link TemporaryStore}s
     * and the like are always created with a unique prefix, e.g., the
     * {@link UUID} of the {@link TemporaryStore} itself or a {@link UUID} for
     * each container that is allocated on the {@link TemporaryStore}.
     * 
     * @param indexManager
     *            Typically a {@link TemporaryStore} or {@link Journal}
     *            containing local resources.
     * 
     * @see #locateResource(String)
     */
    public void add(IIndexManager indexManager) {

        if (indexManager == null)
            throw new IllegalArgumentException();

        synchronized (seeAlso) {

            /*
             * Note: weak reference keys, nothing stored under the key.
             */
            seeAlso.put(indexManager, null);

            if (log.isInfoEnabled()) {

                log.info("size=" + seeAlso.size() + ", added indexManager="
                        + indexManager);

            }

        }

    }

    private final WeakHashMap<IIndexManager, Void> seeAlso = new WeakHashMap<IIndexManager, Void>();

}
