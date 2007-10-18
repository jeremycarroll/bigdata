/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on May 21, 2007
 */

package com.bigdata.rdf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.vocabulary.OWL;
import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;
import org.openrdf.vocabulary.XmlSchema;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.UnicodeKeyBuilder;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.SPO;
import com.bigdata.rdf.model.OptimizedValueFactory.OSPComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.POSComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.SPOComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.rio.Buffer;
import com.bigdata.rdf.rio.IRioLoader;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.RioLoaderEvent;
import com.bigdata.rdf.rio.RioLoaderListener;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.rdf.util.RdfKeyBuilder;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;

/**
 * Abstract base class that implements logic for the {@link ITripleStore}
 * interface that is invariant across the choice of the backing store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractTripleStore implements ITripleStore {

    /**
     * Used to generate the compressed sort keys for the
     * {@link #getTermIdIndex()}.
     */
    final protected RdfKeyBuilder keyBuilder;
    
    /**
     * A copy of properties used to configure the {@link ITripleStore}.
     */
    final protected Properties properties;
    
    final public Properties getProperties() {
        
        /*
         * wrap them up so that people can not easily mess with the initial
         * properties.
         */
        return new Properties(properties);
        
    }
    
    protected AbstractTripleStore(Properties properties) {
        
        // Copy the properties object.
        this.properties = (Properties)properties.clone();
        
        // setup namespace mapping for serialization utility methods.
        addNamespace(RDF.NAMESPACE, "rdf");
        addNamespace(RDFS.NAMESPACE, "rdfs");
        addNamespace(OWL.NAMESPACE, "owl");
        addNamespace(XmlSchema.NAMESPACE, "xsd");

        keyBuilder = new RdfKeyBuilder(new UnicodeKeyBuilder(createCollator(),
                Bytes.kilobyte32));
        
    }
    
    final public RdfKeyBuilder getKeyBuilder() {
        
        return keyBuilder;
        
    }
    
    /**
     * Create and return a new collator object responsible for encoding unicode
     * strings into sort keys.
     * 
     * @return A new collator object.
     * 
     * @todo define the means to configure the key builder for desired unicode
     *       support. the configuration should be restart-safe and must be
     *       shared by all clients for the same triple store.
     */
    final protected RuleBasedCollator createCollator() {
        
        // choose a collator for the default locale.
        RuleBasedCollator collator = (RuleBasedCollator) Collator
                .getInstance(Locale.getDefault());

        /*
         * Primary uses case folding and produces smaller sort strings.
         * 
         * Secondary does not fold case.
         * 
         * Tertiary is the default.
         * 
         * Identical is also allowed.
         * 
         * @todo handle case folding - currently the indices complain, e.g., for
         * wordnet that a term already exists with a given id "Yellow Pages" vs
         * "yellow pages". Clearly the logic to fold case needs to extend
         * further if it is to work.
         */
//        collator.setStrength(Collator.PRIMARY);
//        collator.setStrength(Collator.SECONDARY);

        return collator;
        
    }
    
    ExecutorService indexWriteService = Executors.newFixedThreadPool(3,
            DaemonThreadFactory.defaultThreadFactory());

    /**
     * Adds the statements to each index (batch api).
     * 
     * @param stmts
     *            An array of statements
     */
    final public void addStatements(_Statement[] stmts, int numStmts) {
        
        /*
         * Note: The statements are inserted into each index in parallel. We
         * clone the statement[] and sort and bulk load each index in parallel
         * using a thread pool.
         */

        if( numStmts == 0 ) return;

        long begin = System.currentTimeMillis();
        final AtomicLong sortTime = new AtomicLong(0); // time to sort terms by assigned byte[] keys.
        final AtomicLong insertTime = new AtomicLong(0); // time to insert terms into the forward and reverse index.

        /**
         * Writes on one of the statement indices.
         * 
         * @return The elapsed time for the operation.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        class IndexWriter implements Callable<Long> {

            private final _Statement[] stmts;
            private final int numStmts; // @todo not needed - always equals stmts.length
            private final Comparator<_Statement> comparator;
            private final IIndex ndx;
            private final KeyOrder keyOrder;
            
            /*
             * Private key builder for the SPO, POS, and OSP keys.
             */
            private final RdfKeyBuilder keyBuilder = new RdfKeyBuilder(new KeyBuilder(3 * Bytes.SIZEOF_LONG));
            
            private final byte[][] keys;

            IndexWriter(_Statement[] stmts, int numStmts, Comparator<_Statement> comparator, IIndex ndx, KeyOrder keyOrder) {
                
                this.stmts = new _Statement[numStmts];
                
                System.arraycopy(stmts, 0, this.stmts, 0, numStmts);

                this.numStmts = numStmts;
                
                this.comparator = comparator;
                
                this.ndx = ndx;
                
                this.keys = new byte[numStmts][];
                
                this.keyOrder = keyOrder;
                
            }
            
            public Long call() throws Exception {

                final long beginIndex = System.currentTimeMillis();

                { // sort

                    long _begin = System.currentTimeMillis();

                    Arrays.sort(stmts, 0, numStmts, comparator);

                    sortTime.addAndGet(System.currentTimeMillis() - _begin);

                }

                { // load

                    long _begin = System.currentTimeMillis();

                    for (int i = 0; i < numStmts; i++) {

                        final _Statement stmt = stmts[i];

                        switch (keyOrder) {
                        case SPO:
                            keys[i] = keyBuilder.statement2Key(stmt.s.termId,
                                    stmt.p.termId, stmt.o.termId);
                            break;
                        case OSP:
                            keys[i] = keyBuilder.statement2Key(stmt.o.termId,
                                    stmt.s.termId, stmt.p.termId);
                            break;
                        case POS:
                            keys[i] = keyBuilder.statement2Key(stmt.p.termId,
                                    stmt.o.termId, stmt.s.termId);
                            break;
                        default:
                            throw new UnsupportedOperationException();
                        }

                    }

                    /*
                     * @todo allow client to send null for the values when (a)
                     * they are inserting [null] values under the keys; and (b)
                     * they do not need the old values back.
                     */
                    BatchInsert op = new BatchInsert(numStmts, keys,
                            new byte[numStmts][]);

                    ndx.insert(op);

                    insertTime.addAndGet(System.currentTimeMillis() - _begin);

                }

                long elapsed = System.currentTimeMillis() - beginIndex;

                return elapsed;

            }
            
        }

        List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(3);
        
        tasks.add(new IndexWriter(stmts,numStmts,SPOComparator.INSTANCE,getSPOIndex(),KeyOrder.SPO));
        tasks.add(new IndexWriter(stmts,numStmts,POSComparator.INSTANCE,getPOSIndex(),KeyOrder.POS));
        tasks.add(new IndexWriter(stmts,numStmts,OSPComparator.INSTANCE,getOSPIndex(),KeyOrder.OSP));

        System.err.print("Writing " + numStmts + " statements...");
        
        final List<Future<Long>> futures;
        final long elapsed_SPO;
        final long elapsed_POS;
        final long elapsed_OSP;
        
        try {

            futures = indexWriteService.invokeAll( tasks );

            elapsed_SPO = futures.get(0).get();
            elapsed_POS = futures.get(1).get();
            elapsed_OSP = futures.get(2).get();

        } catch(InterruptedException ex) {
            
            throw new RuntimeException(ex);
            
        } catch(ExecutionException ex) {
        
            throw new RuntimeException(ex);
        
        }
        
        long elapsed = System.currentTimeMillis() - begin;

        System.err.println("in " + elapsed + "ms; sort=" + sortTime
                + "ms, keyGen+insert=" + insertTime + "ms; spo=" + elapsed_SPO
                + "ms, pos=" + elapsed_POS + "ms, osp=" + elapsed_OSP + "ms");
        
    }

    /**
     * Adds the statements to each index (batch api).
     * 
     * @param stmts
     *            An array of statements
     */
    final public void addStatements_singleThread(_Statement[] stmts, int numStmts) {

        if( numStmts == 0 ) return;

        long begin = System.currentTimeMillis();
//        long keyGenTime = 0; // time to convert unicode terms to byte[] sort keys.
        long sortTime = 0; // time to sort terms by assigned byte[] keys.
        long insertTime = 0; // time to insert terms into the forward and reverse index.
        
        final long elapsed_SPO;
        final long elapsed_POS;
        final long elapsed_OSP;

        final byte[][] keys = new byte[numStmts][];
        
        System.err.print("Writing " + numStmts + " statements...");
        
        { // SPO

            final long beginIndex = System.currentTimeMillis();

            IIndex ndx_spo = getSPOIndex();

            { // sort

                long _begin = System.currentTimeMillis();
                
                Arrays.sort(stmts, 0, numStmts, SPOComparator.INSTANCE);
                
                sortTime += System.currentTimeMillis() - _begin;
                
            }
            
            { // load

                long _begin = System.currentTimeMillis();

                for (int i = 0; i < numStmts; i++) {

                    final _Statement stmt = stmts[i];

                    keys[i] = keyBuilder.statement2Key(stmt.s.termId,
                            stmt.p.termId, stmt.o.termId);

                }

                /*
                 * @todo allow client to send null for the values when (a) they
                 * are inserting [null] values under the keys; and (b) they do
                 * not need the old values back.
                 */
                BatchInsert op = new BatchInsert(numStmts, keys,
                        new byte[numStmts][]);
                
                ndx_spo.insert( op );
                
                insertTime += System.currentTimeMillis() - _begin;

            }

            elapsed_SPO = System.currentTimeMillis() - beginIndex;
            
        }

        { // POS

            final long beginIndex = System.currentTimeMillis();
            
            IIndex ndx_pos = getPOSIndex();

            { // sort

                long _begin = System.currentTimeMillis();

                Arrays.sort(stmts, 0, numStmts, POSComparator.INSTANCE);

                sortTime += System.currentTimeMillis() - _begin;

            }

            { // load

                long _begin = System.currentTimeMillis();
                
                for (int i = 0; i < numStmts; i++) {

                    final _Statement stmt = stmts[i];
                    
                    keys[i] = keyBuilder.statement2Key(stmt.p.termId,
                            stmt.o.termId, stmt.s.termId);

//                    ndx_pos.insert(keyBuilder.statement2Key(stmt.p.termId,
//                            stmt.o.termId, stmt.s.termId), null);

                }

                BatchInsert op = new BatchInsert(numStmts, keys,
                        new byte[numStmts][]);

                ndx_pos.insert(op);
                
                insertTime += System.currentTimeMillis() - _begin;
                
            }

            elapsed_POS = System.currentTimeMillis() - beginIndex;

        }

        { // OSP

            final long beginIndex = System.currentTimeMillis();
            
            IIndex ndx_osp = getOSPIndex();

            { // sort

                long _begin = System.currentTimeMillis();

                Arrays.sort(stmts, 0, numStmts, OSPComparator.INSTANCE);
             
                sortTime += System.currentTimeMillis() - _begin;

            }

            { // load

                long _begin = System.currentTimeMillis();
                
                for (int i = 0; i < numStmts; i++) {

                    final _Statement stmt = stmts[i];

                    keys[i] = keyBuilder.statement2Key(stmt.o.termId, stmt.s.termId,
                            stmt.p.termId);
                    
//                    ndx_osp.insert(keyBuilder.statement2Key(stmt.o.termId, stmt.s.termId,
//                            stmt.p.termId), null);

                }
                
                BatchInsert op = new BatchInsert(numStmts, keys,
                        new byte[numStmts][]);
                
                ndx_osp.insert( op );
                
                insertTime += System.currentTimeMillis() - _begin;

            }

            elapsed_OSP = System.currentTimeMillis() - beginIndex;

        }

        long elapsed = System.currentTimeMillis() - begin;

        System.err.println("in " + elapsed + "ms; sort=" + sortTime
                + "ms, keyGen+insert=" + insertTime + "ms; spo=" + elapsed_SPO
                + "ms, pos=" + elapsed_POS + "ms, osp=" + elapsed_OSP + "ms");
        
    }

    /**
     * Performs an efficient scan of a statement index returning the distinct
     * term identifiers found in the first key component for the named access
     * path. Depending on which access path you are using, this will be the term
     * identifiers for the distinct subjects, predicates, or values in the KB.
     * 
     * @param keyOrder
     *            Names the access path. Use {@link KeyOrder#SPO} to get the
     *            term identifiers for the distinct subjects,
     *            {@link KeyOrder#POS} to get the term identifiers for the
     *            distinct predicates, and {@link KeyOrder#OSP} to get the term
     *            identifiers for the distinct objects
     * 
     * @return The distinct term identifiers in the first key slot for the
     *         triples in that index.
     * 
     * @todo This will need to be modified to return an iterator that
     *       encapsulates the logic so that the distinct term scan may be
     *       applied when very large #s of terms would be visited. For this case
     *       it is also possible to parallelize the scan IFF the index partition
     *       boundaries are choosen such that the entried for a term never cross
     *       an index partition bounary.
     */
    final public ArrayList<Long> distinctTermScan(KeyOrder keyOrder) {

        /*
         * The implementation uses a key scan to find the first term identifer
         * for the given index. It then forms a fromKey that starts at the next
         * possible term identifier and does another scan, thereby obtaining the
         * 2nd distinct term identifier for that position on that index. This
         * process is repeated iteratively until the key scan no longer
         * identifies a match. This approach skips quickly over regions of the
         * index which have many statements for the same term and makes N+1
         * queries to identify N distinct terms.  Note that there is no way to
         * pre-compute the #of distinct terms that will be identified short of
         * running the queries.
         */
        ArrayList<Long> ids = new ArrayList<Long>(1000);
        
        byte[] fromKey = null;
        
        final byte[] toKey = null;
        
        IIndex ndx = getStatementIndex(keyOrder);
        
        IEntryIterator itr = ndx.rangeIterator(fromKey, toKey);
        
        long[] tmp = new long[3];
        
        while(itr.hasNext()) {
            
            itr.next();
            
            // extract the term ids from the key. 
            keyBuilder.key2Statement(itr.getKey(), tmp); 

            final long id = tmp[0];
            
            // append tmp[0] to the output list.
            ids.add(id);

//            System.err.println(ids.size() + " : " + id + " : "
//                    + toString(id));
            
            // restart scan at the next possible term id.

            final long nextId = id + 1;
            
            fromKey = keyBuilder.statement2Key(nextId, NULL, NULL);
            
            // new iterator.
            itr = ndx.rangeIterator(fromKey, toKey);
            
        }
        
//        System.err.println("Distinct key scan: KeyOrder=" + keyOrder
//                + ", #terms=" + ids.size());
        
        return ids;
        
    }
    
    final public void generateSortKeys(RdfKeyBuilder keyBuilder,
            _Value[] terms, int numTerms) {
        
        for (int i = 0; i < numTerms; i++) {

            _Value term = terms[i];

            if (term.key == null) {

                term.key = keyBuilder.value2Key(term);

            }

        }

    }
    
    final public IIndex getStatementIndex(KeyOrder keyOrder) {

        switch (keyOrder) {
        case SPO:
            return getSPOIndex();
        case POS:
            return getPOSIndex();
        case OSP:
            return getOSPIndex();
        default:
            throw new IllegalArgumentException("Unknown: " + keyOrder);
        }

    }

    final public _Value getTerm(long id) {

        byte[] data = (byte[]) getIdTermIndex().lookup(keyBuilder.id2key(id));

        if (data == null)
            return null;

        return _Value.deserialize(data);

    }

    final public long getTermId(Value value) {

        _Value val = (_Value) value;
        
        if( val.termId != ITripleStore.NULL ) return val.termId; 

        byte[] tmp = (byte[]) getTermIdIndex().lookup(keyBuilder.value2Key(value));
        
        if( tmp == null ) return ITripleStore.NULL;
        
        try {
            
            val.termId = new DataInputBuffer(tmp).unpackLong();
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }

        return val.termId;

    }

    final public int getStatementCount() {
        
        return getSPOIndex().rangeCount(null,null);
        
    }
    
    final public int getTermCount() {
        
        return getTermIdIndex().rangeCount(null,null);
        
    }
    
    final public int getURICount() {
        
        byte[] fromKey = keyBuilder.keyBuilder.reset().append(
                RdfKeyBuilder.CODE_URI).getKey();

        byte[] toKey = keyBuilder.keyBuilder.reset().append(
                RdfKeyBuilder.CODE_LIT).getKey();
        
        return getTermIdIndex().rangeCount(fromKey,toKey);
        
    }
    
    final public int getLiteralCount() {
        
        byte[] fromKey = keyBuilder.keyBuilder.reset().append(
                RdfKeyBuilder.CODE_LIT).getKey();

        byte[] toKey = keyBuilder.keyBuilder.reset().append(
                RdfKeyBuilder.CODE_BND).getKey();
        
        return getTermIdIndex().rangeCount(fromKey,toKey);
        
    }
    
    final public int getBNodeCount() {
        
        byte[] fromKey = keyBuilder.keyBuilder.reset().append(
                RdfKeyBuilder.CODE_BND).getKey();

        byte[] toKey = keyBuilder.keyBuilder.reset().append(
                (byte) (RdfKeyBuilder.CODE_BND + 1)).getKey();
        
        return getTermIdIndex().rangeCount(fromKey,toKey);
        
    }
    
    /**
     * The capacity (in triples or terms, which ever comes first) of the
     * {@link Buffer} used when reading RDF data. The default (1M) is good for
     * the {@link LocalTripleStore}.
     * 
     * @return The buffer capacity.
     */
    protected int getDataLoadBufferCapacity() {
        
        return 1000000;
        
    }
    
    final public LoadStats loadData(File file, String baseURI, RDFFormat rdfFormat,
            boolean verifyData, boolean commit) throws IOException {

        final long begin = System.currentTimeMillis();
        
        LoadStats stats = new LoadStats();
        
        log.debug( "loading: " + file.getAbsolutePath() );
        
        IRioLoader loader = new PresortRioLoader(this, rdfFormat, verifyData,
                getDataLoadBufferCapacity(), false /* distinct */);

        loader.addRioLoaderListener( new RioLoaderListener() {
            
            public void processingNotification( RioLoaderEvent e ) {
                
                log.info
                    ( e.getStatementsProcessed() + 
                      " stmts added in " + 
                      ((double)e.getTimeElapsed()) / 1000d +
                      " secs, rate= " + 
                      e.getInsertRate() 
                      );
                
            }
            
        });
        
        /*
         * @todo change to use correct Parser method depending on Reader vs
         * InputStream (SAX Source).  Changing this means updating all of
         * the parser implementations, not just the PresortRioLoader.
         */
        Reader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(file)));
        
        try {
            
            loader.loadRdf( reader, baseURI );
            
            long nstmts = loader.getStatementsAdded();
            
            stats.toldTriples += nstmts;
            
            stats.loadTime = System.currentTimeMillis() - begin;
            
            // commit the data.
            if(commit) {
                
                long beginCommit = System.currentTimeMillis();
                
                commit();

                stats.commitTime = System.currentTimeMillis() - beginCommit;

                log.info("commit: latency="+stats.commitTime+"ms");
                
            }
            
            stats.totalTime = System.currentTimeMillis() - begin;
            
            log.info( nstmts + 
                    " stmts added in " + 
                    ((double)loader.getInsertTime()) / 1000d +
                    " secs, rate= " + 
                    loader.getInsertRate()
                    );

            return stats;
            
        } catch ( Exception ex ) {
            
            throw new RuntimeException("While loading: "+file, ex);
            
        } finally {
            
            reader.close();
            
        }
        
//        long elapsed = System.currentTimeMillis() - begin;
//
//        log
//                .info(total_stmts
//                        + " stmts added in "
//                        + ((double) elapsed)
//                        / 1000d
//                        + " secs, rate= "
//                        + ((long) (((double) total_stmts) / ((double) elapsed) * 1000d)));

    }

    /**
     * Add a single statement by lookup and/or insert into the various indices
     * (non-batch api). The terms are inserted into the database iff they are
     * not already defined. The statement is inserted into the database iff it
     * is not already defined.
     * 
     * @todo rewrite to use batch lookup on the N>1 terms, where N is the #of
     *       terms that do not have their termId fields already set.
     */
    final public void addStatement(Resource s, URI p, Value o) {
        
        // assume until disproven.
        boolean termsExist = true;
        
        long _s = getTermId(s);
        
        long _p = getTermId(p);
        
        long _o = getTermId(o);
        
        if( _s == NULL ) {
            
            _s = addTerm(s);
            
            termsExist = false;
            
        }
        
        if( _p == NULL ) {
            
            _p = addTerm(p);
            
            termsExist = false;
            
        }
        
        if( _o == NULL ) {
            
            _o = addTerm(o);
            
            termsExist = false;
            
        }
        
        if (!termsExist || !containsStatement(_s, _p, _o)) {

            addStatement(_s, _p, _o);

        }
        
    }

    /**
     * Add a single statement by lookup and/or insert into the various indices
     * (non-batch api). This does NOT test for existence so it will cause IO if
     * the statement is already present in the database.
     */
    final public void addStatement(long s, long p, long o) {

        getSPOIndex().insert(keyBuilder.statement2Key(s, p, o), null);

        getPOSIndex().insert(keyBuilder.statement2Key(p, o, s), null);

        getOSPIndex().insert(keyBuilder.statement2Key(o, s, p), null);

    }

    /**
     * Return true if the statement exists in the store (non-batch API).
     * 
     * @param s
     *            The term identifier for the subject and not
     *            {@link ITripleStore#NULL}
     * @param p
     *            The term identifier for the predicate and not
     *            {@link ITripleStore#NULL}
     * @param o
     *            The term identifier for the object and not
     *            {@link ITripleStore#NULL}
     */
    final public boolean containsStatement(long s, long p, long o) {

        assert s != NULL;
        assert p != NULL;
        assert o != NULL;
        
        return getSPOIndex().contains(keyBuilder.statement2Key(s, p, o));
        
    }

    final public boolean containsStatement(Resource s, URI p, Value o) {

        /*
         * Resolve each term to its term identifier.
         * 
         * Note: If a value was specified and it is not in the terms index then
         * the statement can not exist in the KB and the code will return
         * [false] immediately.
         */
        final long _s = (s == null ? NULL : getTermId(s));
        if (_s == NULL && s != null) return false;

        final long _p = (p == null ? NULL : getTermId(p));
        if (_p == NULL && p != null) return false;
        
        final long _o = (o == null ? NULL : getTermId(o));
        if (_o == NULL && o != null) return false;
        
        /*
         * if all bound, then a slight optimization.
         */
        if (_s != NULL && _p != NULL && _o != NULL) {

            return getSPOIndex().contains(keyBuilder.statement2Key(_s, _p, _o));
            
        }
        
        /*
         * Choose the access path and test to see if any statements would be
         * visited for that triple pattern.
         * 
         * @todo We really need to send a "limit" parameter along with this
         * request or we could pull back a full buffer of data just to perform
         * an existence test.
         */
        return rangeQuery(_s,_p,_o).hasNext();
        
    }

    /**
     * @todo write tests.
     */
    final public IEntryIterator rangeQuery(long s, long p, long o) {

        if (s != NULL && p != NULL && o != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(s, p, o);

            byte[] toKey = keyBuilder.statement2Key(s, p, o + 1);

            return getSPOIndex().rangeIterator(fromKey, toKey);

        } else if (s != NULL && p != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(s, p, NULL);

            byte[] toKey = keyBuilder.statement2Key(s, p + 1, NULL);

            return getSPOIndex().rangeIterator(fromKey, toKey);

        } else if (s != NULL && o != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(o, s, NULL);

            byte[] toKey = keyBuilder.statement2Key(o, s + 1, NULL);

            return getOSPIndex().rangeIterator(fromKey, toKey);

        } else if (p != NULL && o != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(p, o, NULL);

            byte[] toKey = keyBuilder.statement2Key(p, o + 1, NULL);

            return getPOSIndex().rangeIterator(fromKey, toKey);

        } else if (s != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(s, NULL, NULL);

            byte[] toKey = keyBuilder.statement2Key(s + 1, NULL, NULL);

            return getSPOIndex().rangeIterator(fromKey, toKey);

        } else if (p != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(p, NULL, NULL);

            byte[] toKey = keyBuilder.statement2Key(p + 1, NULL, NULL);

            return getPOSIndex().rangeIterator(fromKey, toKey);

        } else if (o != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(o, NULL, NULL);

            byte[] toKey = keyBuilder.statement2Key(o + 1, NULL, NULL);

            return getOSPIndex().rangeIterator(fromKey, toKey);

        } else {

            return getSPOIndex().rangeIterator(null, null);

        }

    }
    
    /**
     * @todo write tests.
     */
    final public int rangeCount(long s, long p, long o) {

        if (s != NULL && p != NULL && o != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(s, p, o);

            byte[] toKey = keyBuilder.statement2Key(s, p, o + 1);

            return getSPOIndex().rangeCount(fromKey, toKey);

        } else if (s != NULL && p != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(s, p, NULL);

            byte[] toKey = keyBuilder.statement2Key(s, p + 1, NULL);

            return getSPOIndex().rangeCount(fromKey, toKey);

        } else if (s != NULL && o != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(o, s, NULL);

            byte[] toKey = keyBuilder.statement2Key(o, s + 1, NULL);

            return getOSPIndex().rangeCount(fromKey, toKey);

        } else if (p != NULL && o != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(p, o, NULL);

            byte[] toKey = keyBuilder.statement2Key(p, o + 1, NULL);

            return getPOSIndex().rangeCount(fromKey, toKey);

        } else if (s != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(s, NULL, NULL);

            byte[] toKey = keyBuilder.statement2Key(s + 1, NULL, NULL);

            return getSPOIndex().rangeCount(fromKey, toKey);

        } else if (p != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(p, NULL, NULL);

            byte[] toKey = keyBuilder.statement2Key(p + 1, NULL, NULL);

            return getPOSIndex().rangeCount(fromKey, toKey);

        } else if (o != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(o, NULL, NULL);

            byte[] toKey = keyBuilder.statement2Key(o + 1, NULL, NULL);

            return getOSPIndex().rangeCount(fromKey, toKey);

        } else {

            return getSPOIndex().rangeCount(null, null);

        }

    }
    
    /**
     * @todo write tests.
     */
    final public int removeStatements(Resource s,URI p,Value o) {

        /*
         * convert our object types to internal identifiers.
         */
        long _s, _p, _o;

        _s = (s == null ? NULL : getTermId(s));
        _p = (p == null ? NULL : getTermId(p));
        _o = (o == null ? NULL : getTermId(o));

        /*
         * If a value was specified and it is not in the terms index then the
         * statement can not exist in the KB.
         */
        if (_s == NULL && s != null) {

            return 0;
            
        }
        
        if (_p == NULL && p != null) {
        
            return 0;
            
        }
        
        if (_o == NULL && o != null) {
            
            return 0;
            
        }
    
        return removeStatements(_s,_p,_o);
        
    }
    
    /**
     * @todo Modify to not materalize the statements (since the indices do not
     *       support modification with concurrent traversal the statements are
     *       materialized before they are deleted). An interim approach is to
     *       materialize the statements into a {@link TempTripleStore} so that
     *       they are less likley to overflow RAM. We could also use an iterator
     *       based on a historical state (unisolated reader) and make the
     *       changes on the live version (unisolated writer). There might be a
     *       general mechanisms similar to isolation that could be used to
     *       support the appearence of traversal with concurrent modification.
     *       The iterator reads from the committed state of the index (just like
     *       an isolated transaction) and the modifications are written into the
     *       live version of the index.
     * 
     * @todo the {@link #keyBuilder} is being used, which means that this is NOT
     *       thread safe.
     * 
     * @todo In order to return the exact #of statements that are being removed
     *       we have to execute and aggregate a server-side procedure since a
     *       simple rangeCount will only tell us the upper bound on the #of
     *       matching statements when using an {@link UnisolatedBTree}.
     * 
     * FIXME Use the batch btree api.
     * 
     * @todo write tests.
     */
    final public int removeStatements(long _s, long _p, long _o) {
    
        /*
         * if all bound, then a slight optimization.
         */
        if (_s != NULL && _p != NULL && _o != NULL) {

            byte[] key = keyBuilder.statement2Key(_s, _p, _o);

            if (getSPOIndex().contains(key)) {

                getSPOIndex().remove(key);
                getPOSIndex().remove(keyBuilder.statement2Key(_p, _o, _s));
                getOSPIndex().remove(keyBuilder.statement2Key(_o, _s, _p));
                
                return 1;
                
            } else {
                
                return 0;
                
            }
            
        }
        
        /*
         * Choose the access path, count the #of statements that match the
         * triple pattern, and the materalize those statements (since traversal
         * with concurrent modification is not supported).
         */
        
        KeyOrder keyOrder = KeyOrder.getKeyOrder(_s, _p, _o);
        
        // The upper bound on the #of matching statements.
        final int rangeCount = rangeCount(_s, _p, _o);
        
        SPO[] stmts = new SPO[rangeCount];

        // materialize the matching statements.
        final int nremoved;
        {
            IEntryIterator itr1 = rangeQuery(_s, _p, _o);

            int i = 0;

            while (itr1.hasNext()) {

                itr1.next();

                stmts[i++] = new SPO(keyOrder, keyBuilder, itr1.getKey());

            }

//            assert i == rangeCount; // Note: Not true with UnisolatedBTree
            nremoved = i; // The actual #of statements being removed.
        }

        /*
         * Remove the statements from each of the access paths.
         */
        {

            {
                IIndex ndx = getSPOIndex();

                // Place statements in SPO order.
                Arrays.sort(stmts, 0, nremoved, com.bigdata.rdf.inf.SPOComparator.INSTANCE);

                // remove statements from SPO index.
                for (int i = 0; i < nremoved; i++) {

                    SPO spo = stmts[i];

                    ndx.remove(keyBuilder.statement2Key(spo.s, spo.p, spo.o));

                }
            }

            {

                IIndex ndx = getPOSIndex();
                
                // Place statements in POS order.
                Arrays.sort(stmts, 0, nremoved, com.bigdata.rdf.inf.POSComparator.INSTANCE);

                // Remove statements from POS index.
                for (int i = 0; i < nremoved; i++) {

                    SPO spo = stmts[i];

                    ndx.remove(keyBuilder.statement2Key(spo.p, spo.o, spo.s));

                }
                
            }

            {

                IIndex ndx = getOSPIndex();

                // Place statements in OSP order.
                Arrays.sort(stmts, 0, nremoved, com.bigdata.rdf.inf.OSPComparator.INSTANCE);

                // Remove statements from OSP index.
                for (int i = 0; i < nremoved; i++) {

                    SPO spo = stmts[i];

                    ndx.remove(keyBuilder.statement2Key(spo.o, spo.s, spo.p));

                }

            }
            
        }

        return nremoved;

    }

    /*
     * @todo move this serialization stuff into a utility class.
     */
    
    // namespace to prefix.
    private final Map<String, String> uriToPrefix = new HashMap<String, String>();
    
    /**
     * Defines a transient mapping from a URI to a namespace prefix that will be
     * used for that URI by {@link #toString()}.
     * 
     * @param namespace
     * 
     * @param prefix
     */
    final protected void addNamespace(String namespace, String prefix) {
    
        uriToPrefix.put(namespace, prefix);

    }

    final public String toString( long s, long p, long o ) {
        
        return ("< " + toString(s) + ", " + toString(p) + ", " + toString(o) +" >");
        
    }

    final public String toString( long termId ) {
        
        IIndex ndx = getIdTermIndex();
        
        byte[] tmp = (byte[]) ndx.lookup(keyBuilder.id2key(termId));
        
        if(tmp == null) return TERM_NOT_FOUND;

        _Value v = _Value.deserialize(tmp);
        
        return (v instanceof URI ? abbrev((URI) v) : v.toString());
        
    }
    private final String TERM_NOT_FOUND = "<NOT_FOUND>";
    
    /**
     * Substitutes in well know namespaces (rdf, rdfs, etc).
     */
    final private String abbrev( URI uri ) {
        
        String uriString = uri.getURI();
        
//        final int index = uriString.lastIndexOf('#');
//        
//        if(index==-1) return uriString;
//
//        final String namespace = uriString.substring(0, index);
        
        final String namespace = uri.getNamespace();
        
        final String prefix = uriToPrefix.get(namespace);
        
        if(prefix != null) {
            
            return prefix+":"+uri.getLocalName();
            
        } else return uriString;
        
    }

    final public void dumpStore() {

        final int nstmts = getStatementCount();
        
        System.err.println("#statements="+nstmts);
        
        IEntryIterator itr = getSPOIndex().rangeIterator(null, null);

        int i = 0;
        
        while (itr.hasNext()) {

            itr.next();
            
            i++;
            
            SPO spo = new SPO(KeyOrder.SPO,keyBuilder,itr.getKey());

            System.err.println("#" + i + "\t" + toString(spo.s, spo.p, spo.o));
            
        }
        
    }
    
    /**
     * Writes out some usage details on System.err.
     */
    final public void usage() {

        usage("termId", getTermIdIndex());
        usage("idTerm", getIdTermIndex());
        usage("spo", getSPOIndex());
        usage("pos", getPOSIndex());
        usage("osp", getOSPIndex());
        
    }

    /**
     * Writes out some usage information on the named index. More information is
     * available for local indices. Information for scale-out indices is both
     * less detailed and more approximate.
     * 
     * @param name
     *            The index name.
     * 
     * @param ndx
     *            The index.
     */
    final public void usage(String name,IIndex ndx) {
        
        if (ndx instanceof BTree) {

            BTree btree = (BTree) ndx;
            
            final int nentries = btree.getEntryCount();
            final int height = btree.getHeight();
            final int nleaves = btree.getLeafCount();
            final int nnodes = btree.getNodeCount();
            final int ndistinctOnQueue = btree.getNumDistinctOnQueue();
            final int queueCapacity = btree.getHardReferenceQueueCapacity();

            System.err.println(name + ": #entries=" + nentries + ", height="
                    + height + ", #nodes=" + nnodes + ", #leaves=" + nleaves
                    + ", #(nodes+leaves)=" + (nnodes + nleaves)
                    + ", #distinctOnQueue=" + ndistinctOnQueue
                    + ", queueCapacity=" + queueCapacity);
        } else {

            // Note: this is only an estimate if the index is a view.
            final int nentries = ndx.rangeCount(null, null);

            System.err.println(name+": #entries(est)="+nentries);
            
        }
        
    }

}
