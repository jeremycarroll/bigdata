package com.bigdata.rdf.magic;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IResourceLock;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.RelationSchema;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.journal.IConcurrencyManager;

public class TempMagicStore extends TempTripleStore {

    protected static final Logger log = Logger.getLogger(TempMagicStore.class);
    
    protected static final boolean INFO = log.isInfoEnabled();
    
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    public TempMagicStore(IIndexManager indexManager, String namespace,
//BTM - PRE_CLIENT_SERVICE            Long timestamp, Properties properties) {
//BTM - PRE_CLIENT_SERVICE        super(indexManager, namespace, timestamp, properties);
//BTM - PRE_CLIENT_SERVICE    }
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE    public TempMagicStore(Properties properties) {
//BTM - PRE_CLIENT_SERVICE        super(properties);
//BTM - PRE_CLIENT_SERVICE    }
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE    public TempMagicStore(TemporaryStore store, Properties properties,
//BTM - PRE_CLIENT_SERVICE            AbstractTripleStore db) {
//BTM - PRE_CLIENT_SERVICE        super(store, properties, db);
//BTM - PRE_CLIENT_SERVICE    }
    public TempMagicStore(IIndexManager indexManager,
                          IConcurrencyManager concurrencyManager,
                          IBigdataDiscoveryManagement discoveryManager,
                          String namespace,
                          Long timestamp,
                          Properties properties)
    {
        super(indexManager,
              concurrencyManager, discoveryManager,
              namespace, timestamp, properties);
    }

    public TempMagicStore(IConcurrencyManager concurrencyManager,
                          IBigdataDiscoveryManagement discoveryManager,
                          Properties properties)
    {
        super(concurrencyManager, discoveryManager, properties);
    }

    public TempMagicStore(TemporaryStore store,
                          IConcurrencyManager concurrencyManager,
                          IBigdataDiscoveryManagement discoveryManager,
                          Properties properties,
                          AbstractTripleStore db)
    {
        super(store, concurrencyManager, discoveryManager, properties, db);
    }
//BTM - PRE_CLIENT_SERVICE - END

    @Override
    public Iterator<IRelation> relations() {

        Collection<String> symbols = getMagicSymbols();
        IRelation[] relations = 
            new IRelation[symbols.size()+ (lexicon ? 2 : 1)];
        int i = 0; 
        relations[i++] = getSPORelation();
        if (lexicon) {
            relations[i++] = getLexiconRelation();
        }
        for (String symbol : symbols) {
            relations[i++] = getMagicRelation(symbol);
        }
        return Collections.unmodifiableList(
            Arrays.asList(relations)).iterator();
        
    }
    
    public MagicRelation createRelation(String symbol, int arity) {
        
        MagicRelation relation = getMagicRelation(symbol);
        if (relation != null) {
            return relation;
        }
        
        assertWritable();
        
        final Properties tmp = getProperties();
        
        // set property that will let the contained relations locate their container.
        tmp.setProperty(RelationSchema.CONTAINER, getNamespace());
        // set the arity (REQUIRED)
        tmp.setProperty(MagicSchema.ARITY, String.valueOf(arity));
        
        final IResourceLock resourceLock = acquireExclusiveLock();

        try {

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE            relation = new MagicRelation(getIndexManager(), 
//BTM - PRE_CLIENT_SERVICE                getNamespace() + "." + symbol, getTimestamp(), tmp);
            relation = new MagicRelation(getIndexManager(),
                                         getConcurrencyManager(),
                                         getDiscoveryManager(),
                                         getNamespace() + "." + symbol,
                                         getTimestamp(),
                                         tmp);
//BTM - PRE_CLIENT_SERVICE - END

            relation.create();

            /*
             * Update the global row store to set the axioms and the
             * vocabulary objects.
             */
            {

                Collection<String> symbols = getMagicSymbols();
                
                symbols.add(symbol);
                
                final Map<String, Object> map = new HashMap<String, Object>();

                // primary key.
                map.put(RelationSchema.NAMESPACE, getNamespace());

                // symbols.
                map.put(MagicSchema.SYMBOLS, symbols);

                // Write the map on the row store.
                getIndexManager().getGlobalRowStore().write(
                        RelationSchema.INSTANCE, map);

            }
        
            /*
             * Note: A commit is required in order for a read-committed view to
             * have access to the registered indices.
             * 
             * @todo have the caller do this? It does not really belong here
             * since you can not make a large operation atomic if you do a
             * commit here.
             */

            commit();

        } finally {

            unlock(resourceLock);

        }
        
        return relation;

    }
    
    final public synchronized Collection<String> getMagicSymbols() {
        
        /*
         * Extract the de-serialized symbols from the global row
         * store.
         */
        
        Collection<String> symbols = (Collection<String>) 
            getIndexManager().getGlobalRowStore().get(
                RelationSchema.INSTANCE, getNamespace(),
                MagicSchema.SYMBOLS);
        
        if (symbols == null) {
            symbols = new LinkedList<String>();
        }

        return symbols;
        
    }
    
    final public synchronized MagicRelation getMagicRelation(String symbol) {
        
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE        return (MagicRelation) getIndexManager().getResourceLocator()
//BTM - PRE_CLIENT_SERVICE                .locate(getNamespace() + "." + symbol,
//BTM - PRE_CLIENT_SERVICE                        getTimestamp());
        return (MagicRelation) getIndexManager().getResourceLocator()
                                   .locate(getIndexManager(),
                                           getConcurrencyManager(),
                                           getDiscoveryManager(),
                                           getNamespace() + "." + symbol,
                                           getTimestamp());
//BTM - PRE_CLIENT_SERVICE - END

    }
    
    @Override
    public void destroy() {
    
        assertWritable();

        final IResourceLock resourceLock = acquireExclusiveLock();

        try {

            Collection<String> symbols = getMagicSymbols();
            
            for (String symbol : symbols) {
                
                MagicRelation relation = getMagicRelation(symbol);
                
                if (INFO) log.info("destroying relation: " + relation.getNamespace());
                
                relation.destroy();
                
            }
            
            super.destroy();
            
        } finally {

            unlock(resourceLock);
            
        }
        
    }
    
    @Override
    public StringBuilder dumpStore(
            final AbstractTripleStore resolveTerms, final boolean explicit,
            final boolean inferred, final boolean axioms,
            final boolean justifications, final IKeyOrder<ISPO> keyOrder) {
        
        StringBuilder sb = super.dumpStore(
            resolveTerms, explicit, inferred, axioms, justifications, keyOrder);
        
        Collection<String> symbols = getMagicSymbols();
        for (String symbol : symbols) {
            MagicRelation relation = getMagicRelation(symbol);
            MagicAccessPath accessPath = 
                relation.getAccessPath(relation.getPrimaryKeyOrder());
            IChunkedOrderedIterator<IMagicTuple> itr = accessPath.iterator();
            int i = 0;
            while (itr.hasNext()) {
                IMagicTuple tuple = itr.next();
                sb.append("\n").append(relation.getNamespace()).append("#").append(i++)
                  .append("\t").append(tuple);
            }
        }
        
        return sb;
        
    }
    
}
