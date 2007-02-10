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
 * Created on Jan 3, 2007
 */

package com.bigdata.rdf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.journal.ICommitter;
import com.bigdata.journal.Journal;
import com.bigdata.journal.RootBlockView;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.BTreeMetadata;
import com.bigdata.objndx.IIndex;
import com.bigdata.objndx.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.OptimizedValueFactory.OSPComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.POSComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.SPOComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.TermIdComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.model.OptimizedValueFactory._ValueSortKeyComparator;
import com.bigdata.rdf.rio.IRioLoader;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.RioLoaderEvent;
import com.bigdata.rdf.rio.RioLoaderListener;
import com.bigdata.rdf.serializers.RdfValueSerializer;
import com.bigdata.rdf.serializers.StatementSerializer;
import com.bigdata.rdf.serializers.TermIdSerializer;
import com.bigdata.scaleup.PartitionedJournal;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;

/**
 * A triple store based on the <em>bigdata</em> architecture.
 * 
 * @todo refactor to use a delegation mechanism so that we can run with or
 *       without partitioned indices.
 * 
 * @todo persue some alternative strategies for restart safety and scale: (1)
 *       TripleStore extends Journal and manages its indices directly as
 *       {@link ICommitter}s without transaction support. This can be a scale
 *       up solution using a disk-only buffer and the LRUs internal to the
 *       different indices. I would expect fair performance. (2) A partitioned
 *       index variant.; (3) a transactional variant, but see various notes
 *       below about isolation for rdfs stores; and (4) a distributed database
 *       (scale out) variant.
 * 
 * @todo try loading some very large data sets; try Transient vs Disk vs Direct
 *       modes. If Transient has significantly better performance then it
 *       indicates that we are waiting on IO so introduce AIO support in the
 *       Journal and try Disk vs Direct with aio. Otherwise, consider
 *       refactoring the btree to have the values be variable length byte[]s
 *       with serialization in the client and other tuning focused on IO (the
 *       only questions with that approach are appropriate compression
 *       techniques and handling transparently timestamps as part of the value
 *       when using an isolated btree in a transaction).
 * 
 * @todo the only added cost for a quad store is the additional statement
 *       indices. There are only three more statement indices in a quad store.
 *       Since statement indices are so cheap, it is probably worth implementing
 *       them now, even if only as a configuration option.
 * 
 * @todo verify read after commit (restart safe) for large data sets and test
 *       re-load rate for a data set and verify that no new statements are
 *       added.
 * 
 * @todo add bulk data export (buffering statements and bulk resolving term
 *       identifiers).
 * 
 * @todo The use of long[] identifiers for statements also means that the SPO
 *       and other statement indices are only locally ordered so they can not be
 *       used to perform a range scan that is ordered in the terms without
 *       joining against the various term indices and then sorting the outputs.
 * 
 * @todo possibly save frequently seen terms in each batch for the next batch in
 *       order to reduce unicode conversions.
 * 
 * @todo support metadata about the statement, e.g., whether or not it is an
 *       inference.
 * 
 * @todo compute the MB/sec rate at which this test runs and compare it with the
 *       maximum transfer rate for the journal without the btree and the maximum
 *       transfer rate to disk. this will tell us the overhead of the btree
 *       implementation.
 * 
 * @todo Try a variant in which we have metadata linking statements and terms
 *       together. In this case we would have to go back to the terms and update
 *       them to have metadata about the statement. it is a bit circular since
 *       we can not create the statement until we have the terms and we can not
 *       add the metadata to the terms until we have the statement.
 * 
 * @todo Note that a very interesting solution for RDF places all data into a
 *       statement index and then use block compression techniques to remove
 *       frequent terms, e.g., the repeated parts of the value. Also note that
 *       there will be no "value" for an rdf statement since existence is all.
 *       The key completely encodes the statement. So, another approach is to
 *       bit code the repeated substrings found within the key in each leaf. *
 *       This way the serialized key size reflects only the #of distinctions.
 * 
 * @todo I've been thinking about rdfs stores in the light of the work on
 *       bigdata. Transactional isolation for rdf is really quite simple. Since
 *       lexicons (uri, literal or bnode indices) do not (really) support
 *       deletion, the only acts are asserting term and asserting and retracting
 *       statements. since assertion terms can lead to write-write conflicts,
 *       which must be resolved and can cascade into the statement indices since
 *       the statement key depends directly on the assigned term identifiers. a
 *       statement always merges with an existing statement, inserts never cause
 *       conflicts. Hence the only possible write-write conflict for the
 *       statement indices is a write-delete conflict. quads do not really make
 *       this more complex (or expensive) since merges only occur when there is
 *       a context match. however entailments can cause twists depending on how
 *       they are realized.
 * 
 * If we do a pure RDF layer (vs RDF over GOM over bigdata), then it seems that
 * we could simple use a statement index (no lexicons for URIs, etc). Normally
 * this inflates the index size since you have lots of duplicate strings, but we
 * could just use block compression to factor out those strings when we evict
 * index leaves to disk. Prefix compression of keys will already do great things
 * for removing repetitive strings from the index nodes and block compression
 * will get at the leftover redundancy.
 * 
 * So, one dead simple architecture is one index per access path (there is of
 * course some index reuse across the access paths) with the statements inline
 * in the index using prefix key compression and block compression to remove
 * redundancy. Inserts on this architecture would just send triples to the store
 * and the various indices would be maintained by the store itself. Those
 * indices could be load balanced in segments across a cluster.
 * 
 * Since a read that goes through to disk reads an entire leaf at a time, the
 * most obvious drawback that I see is caching for commonly used assertions, but
 * that is easy to implement with some cache invalidation mechanism coupled to
 * deletes.
 * 
 * I can also see how to realize very large bulk inserts outside of a
 * transactional context while handling concurrent transactions -- you just have
 * to reconcile as of the commit time of the bulk insert and you get to do that
 * using efficient compacting sort-merges of "perfect" bulk index segments. The
 * architecture would perform well on concurrent apstars style document loading
 * as well as what we might normally consider a bulk load (a few hundred
 * megabytes of data) within the normal transaction mechanisms, but if you
 * needed to ingest uniprot you would want to use a different technique :-)
 * outside of the normal transactional isolation mechanisms.
 * 
 * I'm not sure what the right solution is for entailments, e.g., truth
 * maintenance vs eager closure. Either way, you would definitely want to avoid
 * tuple at a time processing and batch things up so as to minimize the #of
 * index tests that you had to do. So, handling entailments and efficient joins
 * for high-level query languages would be the two places for more thought. And
 * there are little odd spots in RDF - handling bnodes, typed literals, and the
 * concept of a total sort order for the statement index.
 * 
 * @todo Note that the target platform will use a 200M journal extent and freeze
 *       the extent when it gets full, opening a new extent. Once a frozen
 *       journal contains committed state, we can evict the indices from the
 *       journal into perfect index range files. Reads then read through the
 *       live journal, any frozen journals that have not been evicted yet, and
 *       lastly the perfect index range segments on the disk. For the latter,
 *       the index nodes are buffered so that one read is require to get a leaf
 *       from the disk. Perfect index range segments get compacted from time to
 *       time. A bloom filter in front of an index range (or a segment of an
 *       index range) could reduce IOs further.
 * 
 * @todo test with the expected journal modes, e.g., direct buffered, once the
 *       page buffering feature is in place.
 * 
 * @todo a true bulk loader does not need transactional isolation and this class
 *       does not use transactional isolation. When using isolation there are
 *       two use cases - small documents where the differential indicies will be
 *       small (document sized) and very large loads where we need to use
 *       persistence capable differential indices and the load could even span
 *       more than one journal extent.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TripleStore extends /*Partitioned*/Journal {
    
    static transient public Logger log = Logger.getLogger(TripleStore.class);

    /*
     * Declare indices for root addresses for the different indices maintained
     * by the store.
     */
//    static public transient final int ROOT_TERM_ID = RootBlockView.FIRST_USER_ROOT;
//    static public transient final int ROOT_ID_TERM = RootBlockView.FIRST_USER_ROOT + 1;
//    static public transient final int ROOT_SPO     = RootBlockView.FIRST_USER_ROOT + 2;
//    static public transient final int ROOT_POS     = RootBlockView.FIRST_USER_ROOT + 3;
//    static public transient final int ROOT_OSP     = RootBlockView.FIRST_USER_ROOT + 4;
    static public transient final int ROOT_COUNTER = RootBlockView.FIRST_USER_ROOT + 5;
    
    public RdfKeyBuilder keyBuilder;

    /*
     * Note: You MUST NOT retain hard references to these indices across
     * operations since they may be discarded and re-loaded.
     */
    private IIndex ndx_termId;
    private IIndex ndx_idTerm;
    private IIndex ndx_spo;
    private IIndex ndx_pos;
    private IIndex ndx_osp;
    private AutoIncCounter counter;

    final String name_termId = "termId";
    final String name_idTerm = "idTerm";

    final String name_spo = "spo";
    final String name_pos = "pos";
    final String name_osp = "osp";

    /*
     * Note: access to the indices through these methods is required to support
     * partitioned indices since the latter introduce an indirection to: (a)
     * periodically replace a mutable btree with a fused view of the btree on
     * the journal and one or more index segments; (b) periodically split or
     * join partitions of the index based on the size on disk of that partition.
     * 
     * At this time is is valid to hold onto a reference during a given load or
     * query operation but not across commits (more accurately, not across
     * overflow() events). Eventually we will have to put a client api into
     * place that hides the routing of btree api operations to the journals and
     * segments for each index partition.
     */
    public IIndex getTermIdIndex() {

        if(ndx_termId!=null) return ndx_termId;
        
        IIndex ndx = getIndex(name_termId);
        
        if(ndx==null) {
            
            ndx_termId = ndx = registerIndex(name_termId, new BTree(this,
                    BTree.DEFAULT_BRANCHING_FACTOR, TermIdSerializer.INSTANCE));
            
        }
        
        return ndx;
        
    }
    
    public IIndex getIdTermIndex() {

        if(ndx_idTerm!=null) return ndx_idTerm;
        
        IIndex ndx = getIndex(name_idTerm);

        if (ndx == null) {

            ndx_idTerm = ndx = registerIndex(name_idTerm,
                    new BTree(this, BTree.DEFAULT_BRANCHING_FACTOR,
                            RdfValueSerializer.INSTANCE));

        }

        return ndx;

    }

    protected IIndex getStatementIndex(String name) {

        IIndex ndx = getIndex(name);

        if (ndx == null) {

            ndx = registerIndex(name, new BTree(this,
                    BTree.DEFAULT_BRANCHING_FACTOR,
                    StatementSerializer.INSTANCE));

        }

        return ndx;

    }

    public IIndex getSPOIndex() {

        if(ndx_spo!=null) return ndx_spo;

        return ndx_spo = getStatementIndex(name_spo);
        
    }
    
    public IIndex getPOSIndex() {

        if(ndx_pos!=null) return ndx_pos;

        return ndx_pos = getStatementIndex(name_pos);

    }
    
    public IIndex getOSPIndex() {
        
        if(ndx_osp!=null) return ndx_osp;

        return ndx_osp = getStatementIndex(name_osp);

    }

    /**
     * The counter used to assign term identifiers.
     */
    public AutoIncCounter getCounter() {return counter;}
    
    /**
     * 
     */
    public TripleStore(Properties properties) throws IOException {

        super(properties);

        // setup key builder that handles unicode and primitive data types.
        KeyBuilder _keyBuilder = new KeyBuilder(createCollator(), Bytes.kilobyte32 * 4);
        
        // setup key builder for RDF Values and Statements.
        keyBuilder = new RdfKeyBuilder(_keyBuilder);

    }

    /**
     * Extends the default behavior to create/re-load the indices defined by the
     * {@link TripleStore} and to cache hard references to those indices.
     */
    public void setupCommitters() {

        super.setupCommitters();
        
//        assert ndx_termId == null;
//        assert ndx_idTerm == null;
//        assert ndx_spo == null;
//        assert ndx_pos == null;
//        assert ndx_osp == null;
        assert counter == null;
//
        long addr;
//        
//        if((addr=getAddr(ROOT_TERM_ID))==0L) {
//
//            ndx_termId = new BTree(this, BTree.DEFAULT_BRANCHING_FACTOR,
//                    TermIdSerializer.INSTANCE);
//            
//        } else {
//            
//            ndx_termId = new BTree(this, BTreeMetadata.read(this, addr));
//            
//        }
//
//        if((addr=getAddr(ROOT_ID_TERM))==0L) {
//
//            ndx_idTerm = new BTree(this, BTree.DEFAULT_BRANCHING_FACTOR,
//                    RdfValueSerializer.INSTANCE);
//            
//        } else {
//            
//            ndx_idTerm = new BTree(this, BTreeMetadata.read(this, addr));
//            
//        }
//        
//        if((addr=getAddr(ROOT_SPO))==0L) {
//
//            ndx_spo = new BTree(this, BTree.DEFAULT_BRANCHING_FACTOR,
//                    StatementSerializer.INSTANCE);
//            
//        } else {
//            
//            ndx_spo = new BTree(this, BTreeMetadata.read(this, addr));
//            
//        }
//        
//        if ((addr = getAddr(ROOT_POS)) == 0L) {
//
//            ndx_pos = new BTree(this, BTree.DEFAULT_BRANCHING_FACTOR,
//                    StatementSerializer.INSTANCE);
//
//        } else {
//
//            ndx_pos = new BTree(this, BTreeMetadata.read(this, addr));
//
//        }
//
//        if ((addr = getAddr(ROOT_OSP)) == 0L) {
//
//            ndx_osp = new BTree(this, BTree.DEFAULT_BRANCHING_FACTOR,
//                    StatementSerializer.INSTANCE);
//
//        } else {
//
//            ndx_osp = new BTree(this, BTreeMetadata.read(this, addr));
//
//        }

        if ((addr = getAddr(ROOT_COUNTER)) == 0L) {

            // Note: first termId is ONE (1).  Zero is reserved.
            counter = new AutoIncCounter(this,1L);

        } else {

            counter = AutoIncCounter.read(this,addr);

        }
        
        /*
         * declare the comitters.
         */
//        setCommitter(ROOT_TERM_ID, (BTree) ndx_termId);
//        setCommitter(ROOT_ID_TERM, (BTree) ndx_idTerm);
//        setCommitter(ROOT_SPO, (BTree) ndx_spo);
//        setCommitter(ROOT_POS, (BTree) ndx_pos);
//        setCommitter(ROOT_OSP, (BTree) ndx_osp);
        setCommitter(ROOT_COUNTER, counter );

    }
    
    /**
     * Extends the default behavior to discard hard references to the indices
     * defined by the {@link TripleStore}.
     */
    public void discardCommitters() {
        
        super.discardCommitters();
        
//        ndx_termId = null;
//        ndx_idTerm = null;
//        ndx_spo = null;
//        ndx_pos = null;
//        ndx_osp = null;
        counter = null;
        
    }
    
    /**
     * Create and return a new collator object responsible for encoding unicode
     * strings into sort keys.
     * 
     * @return A new collator object.
     * 
     * @todo define properties for configuring the collator.
     */
    public RuleBasedCollator createCollator() {
        
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
    
    /**
     * The #of triples in the store.
     * <p>
     * This may be an estimate when using partitioned indices.
     */
    public int getStatementCount() {
        
        return getSPOIndex().rangeCount(null,null);
        
    }
    
    /**
     * Add a single statement by lookup and/or insert into the various indices
     * (non-batch api).
     */
    public void addStatement(Resource s, URI p, Value o) {

        long _s = addTerm(s);
        long _p = addTerm(p);
        long _o = addTerm(o);

        getSPOIndex().insert(keyBuilder.statement2Key(_s, _p, _o),null);
        getPOSIndex().insert(keyBuilder.statement2Key(_p, _o, _s),null);
        getOSPIndex().insert(keyBuilder.statement2Key(_p, _s, _p),null);
        
    }

    /**
     * Return true if the statement exists in the store (non-batch API).
     */
    public boolean containsStatement(Resource s, URI p, Value o) {

        long _s, _p, _o;
        
        if( (_s = getTerm(s)) == 0L ) return false;
        if( (_p = getTerm(p)) == 0L ) return false;
        if( (_o = getTerm(o)) == 0L ) return false;
        
        return getSPOIndex().contains(keyBuilder.statement2Key(_s, _p, _o));
        
    }

    /**
     * Adds the statements to each index (batch api).
     * <p> 
     * Note: this is not sorting by the generated keys so the sort order may not
     *       perfectly reflect the natural order of the index. however, i
     *       suspect that it simply creates a few partitions out of the natural
     *       index order based on the difference between signed and unsigned
     *       interpretations of the termIds when logically combined into a
     *       statement identifier.
     * 
     * @param stmts
     *            An array of statements
     */
    public void addStatements(_Statement[] stmts, int numStmts) {

        if( numStmts == 0 ) return;

        long begin = System.currentTimeMillis();
//        long keyGenTime = 0; // time to convert unicode terms to byte[] sort keys.
        long sortTime = 0; // time to sort terms by assigned byte[] keys.
        long insertTime = 0; // time to insert terms into the forward and reverse index.

        System.err.print("Writing " + numStmts + " statements...");

        { // SPO
            
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

                    ndx_spo.insert(keyBuilder.statement2Key(stmt.s.termId, stmt.p.termId,
                            stmt.o.termId), null);
                    
                }

                insertTime += System.currentTimeMillis() - _begin;

            }

        }

        { // POS

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
                    
                    ndx_pos.insert(keyBuilder.statement2Key(stmt.p.termId,
                            stmt.o.termId, stmt.s.termId), null);

                }

                insertTime += System.currentTimeMillis() - _begin;
                
            }

        }

        { // OSP
            
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

                    ndx_osp.insert(keyBuilder.statement2Key(stmt.o.termId, stmt.s.termId,
                            stmt.p.termId), null);

                }
                
                insertTime += System.currentTimeMillis() - _begin;

            }

        }

        long elapsed = System.currentTimeMillis() - begin;

        System.err.println("in " + elapsed + "ms; sort=" + sortTime
                + "ms, keyGen+insert=" + insertTime + "ms");
        
    }

    /**
     * Generate the sort keys for the terms.
     * 
     * @param keyBuilder
     *            The object used to generate the sort keys - <em>this is not
     *            safe for concurrent writers</em>
     * @param terms
     *            The terms whose sort keys will be generated.
     * @param numTerms
     *            The #of terms in that array.
     * 
     * @see #createCollator()
     * @see KeyBuilder
     */
    public void generateSortKeys(RdfKeyBuilder keyBuilder, _Value[] terms, int numTerms) {
        
        for (int i = 0; i < numTerms; i++) {

            _Value term = terms[i];

            if (term.key == null) {

                term.key = keyBuilder.value2Key(term);

            }

        }

    }
    
    /**
     * Batch insert of terms into the database.
     * 
     * @param terms
     *            An array whose elements [0:nterms-1] will be inserted.
     * @param numTerms
     *            The #of terms to insert.
     * @param haveKeys
     *            True if the terms already have their sort keys.
     * @param sorted
     *            True if the terms are already sorted by their sort keys (in
     *            the correct order for a batch insert).
     * 
     * @exception IllegalArgumentException
     *                if <code>!haveKeys && sorted</code>.
     */
    public void insertTerms( _Value[] terms, int numTerms, boolean haveKeys, boolean sorted ) {

        if (numTerms == 0)
            return;

        if (!haveKeys && sorted)
            throw new IllegalArgumentException("sorted requires keys");
        
        long begin = System.currentTimeMillis();
        long keyGenTime = 0; // time to convert unicode terms to byte[] sort keys.
        long sortTime = 0; // time to sort terms by assigned byte[] keys.
        long insertTime = 0; // time to insert terms into the forward and reverse index.
        
        System.err.print("Writing "+numTerms+" terms ("+terms.getClass().getSimpleName()+")...");

        {

            /*
             * First make sure that each term has an assigned sort key.
             */
            if(!haveKeys) {

                long _begin = System.currentTimeMillis();
                
                generateSortKeys(keyBuilder, terms, numTerms);
                
                keyGenTime = System.currentTimeMillis() - _begin;

            }
            
            /* 
             * Sort terms by their assigned sort key.  This places them into
             * the natural order for the term:id index.
             */

            if (!sorted ) {
            
                long _begin = System.currentTimeMillis();
                
                Arrays.sort(terms, 0, numTerms, _ValueSortKeyComparator.INSTANCE);

                sortTime += System.currentTimeMillis() - _begin;
                
            }

            {
            
                /*
                 * Lookup the term in the term:id index. If it is there then
                 * take its termId and mark it as 'known' so that we can avoid
                 * testing the reverse index. Otherwise, insert the term into
                 * the term:id index which gives us its termId.
                 * 
                 * @todo use batch api?
                 */

                IIndex termId = getTermIdIndex();
                
                long _begin = System.currentTimeMillis();
                
                for (int i = 0; i < numTerms; i++) {

                    _Value term = terms[i];

                    if (!term.known) {

                        assert term.termId==0L;
                        assert term.key != null;

                        // Lookup in the forward index.
                        Long tmp = (Long)termId.lookup(term.key);
                        
                        if(tmp == null) { // not found.

                            // assign termId.
                            term.termId = counter.nextId();
                        
                            // insert into index.
                            if(termId.insert(term.key, Long.valueOf(term.termId))!=null) {
                                
                                throw new AssertionError();
                                
                            }
                            
                        } else { // found.
                        
                            term.termId = tmp.longValue();
                            
                            term.known = true;
                        
                        }
                        
                    } else assert term.termId != 0L;
                    
                }

                insertTime += System.currentTimeMillis() - _begin;
                
            }
            
        }
        
        {
            
            /*
             * Sort terms based on their assigned termId.
             * 
             * FIXME The termId should be converted to a byte[8] for this sort
             * order since that is how the termId is actually encoded when we
             * insert into the termId:term (reverse) index.  We could also 
             * fake this with a comparator that compared the termIds as if they
             * were _unsigned_ long integers.
             */

            long _begin = System.currentTimeMillis();

            Arrays.sort(terms, 0, numTerms, TermIdComparator.INSTANCE);

            sortTime += System.currentTimeMillis() - _begin;

        }
        
        {
        
            /*
             * Add unknown terms to the reverse index, which is the index that
             * we use to lookup the RDF value by its termId to serialize some
             * data as RDF/XML or the like.
             * 
             * Note: We only insert terms that were reported as "not found" when
             * we inserted them into the forward mapping. This reduces the #of
             * index tests that we perform.
             * 
             * @todo use batch api?
             */
            
            IIndex idTerm = getIdTermIndex();
            
            long _begin = System.currentTimeMillis();
            
            for (int i = 0; i < numTerms; i++) {

                _Value term = terms[i];
                
                assert term.termId != 0L;
                
                if (!term.known) {
                    
                    final byte[] idKey = keyBuilder.id2key(term.termId);

                    if(idTerm.insert(idKey, term) != null) {

                        // term was already in this index.
                        
                        throw new AssertionError();
                        
                    }
                    
                    term.known = true; // now in the fwd and rev indices.
                    
                }

            }

            insertTime += System.currentTimeMillis() - _begin;

        }

        long elapsed = System.currentTimeMillis() - begin;
        
        System.err.println("in " + elapsed + "ms; keygen=" + keyGenTime
                + "ms, sort=" + sortTime + "ms, insert=" + insertTime + "ms");
        
    }
    
    /**
     * Add a term into the term:id index and the id:term index, returning the
     * assigned term identifier (non-batch API).
     * 
     * @param value
     *            The term.
     * 
     * @return The assigned term identifier.
     */
    public long addTerm(Value value) {

        final _Value val = (_Value) value;
        
        if(val.known) {
            
            assert val.termId !=0L;
            
            return val.termId;

        }

        /*
         * The forward mapping assigns the identifier.
         * 
         * Note: this code tests for existance based on the foward mapping so
         * that we can avoid the use of the reverse mapping if we know that the
         * term exists.
         */

        final IIndex termId = getTermIdIndex();
        final IIndex idTerm = getIdTermIndex();

        // formulate key from the RDF value.
        final byte[] termKey = keyBuilder.value2Key(val);

        // test the forward index.
        Long tmp = (Long) termId.lookup(termKey);

        if (tmp != null) {

            /*
             * the term was found on the forward lookup, so we are done.
             */

            val.termId = tmp.longValue();
            
        } else {
            
            /*
             * the term was not in the database.
             */

            // assign the next term identifier from the persistent counter.
            val.termId = counter.nextId();
            
            // insert into the forward mapping.
            termId.insert(termKey, Long.valueOf(val.termId));

            /*
             * Insert into the reverse mapping from identifier to term.
             */

            final byte[] idKey = keyBuilder.id2key(val.termId);

            if (idTerm.insert(idKey, val) != null) {
    
                throw new AssertionError();
                        
            }

        }

        val.known = true;
        
        return val.termId;

    }

    /**
     * Return the pre-assigned termId for the value (non-batch API).
     * 
     * @param value
     *            The value.
     * 
     * @return The pre-assigned termId -or- 0L iff the term is not known to the
     *         database.
     */
    public long getTerm(Value value) {

        _Value val = (_Value) value;
        
        if( val.termId != 0l ) return val.termId; 

        Long id = (Long)getTermIdIndex().lookup(keyBuilder.value2Key(value));
        
        if( id == null ) return 0L;

        val.termId = id.longValue();

        return val.termId;

    }
    
    public void commit() {

        final long begin = System.currentTimeMillis();

        super.commit();

        final long elapsed = System.currentTimeMillis() - begin;
        
        System.err.println("commit: commit latency="+elapsed+"ms");

        usage();
        
    }

    public void overflow() {
        
        System.err.println("*** Overflow *** ");

        // the current state.
        final long counter = getCounter().getCounter();
        
        super.overflow();
        
        // setup the counter on the new journal.
        setCommitter( ROOT_COUNTER, new AutoIncCounter(this,counter) );
        
    }
    
    /**
     * Writes out some usage details on System.err.
     */
    public void usage() {

        usage("termId", getTermIdIndex());
        usage("idTerm", getIdTermIndex());
        usage("spo", getSPOIndex());
        usage("pos", getPOSIndex());
        usage("osp", getOSPIndex());
        
    }

    private void usage(String name,IIndex ndx) {
        
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
    
    /**
     * Load a file into the triple store.
     *
     * @param file The file.
     * 
     * @param baseURI The baseURI or "" if none is known.
     * 
     * @throws IOException
     */
    public void loadData(File file, String baseURI ) throws IOException {
        
        loadData(file,baseURI,true);
        
    }

    /**
     * Used to report statistics when loading data.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class LoadStats {

        public long toldTriples;
        public long totalTime;
        public long loadTime;
        public long commitTime;
        
    }
    
    /**
     * Load a file into the triple store.
     * 
     * @param file
     *            The file.
     * @param baseURI
     *            The baseURI or "" if none is known.
     * @param commit
     *            A {@link #commit()} will be performed IFF true.
     * @return Statistics about the file load.
     * @throws IOException
     */
    public LoadStats loadData(File file, String baseURI, boolean commit ) throws IOException {

        final long begin = System.currentTimeMillis();
        
        LoadStats stats = new LoadStats();
        
        log.debug( "loading: " + file.getAbsolutePath() );
        
        Reader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(file)));

        IRioLoader loader = new PresortRioLoader( this );

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
        
        try {
            
            loader.loadRdfXml( reader, baseURI );
            
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
    
}
