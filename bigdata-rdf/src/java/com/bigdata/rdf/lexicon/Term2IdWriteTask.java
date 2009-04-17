package com.bigdata.rdf.lexicon;

import java.util.Arrays;
import java.util.concurrent.Callable;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.rdf.lexicon.Term2IdWriteProc.Term2IdWriteProcConstructor;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.service.Split;

/**
 * Synchronous RPC write on the TERM2ID index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Term2IdWriteTask implements
        Callable<KVO<BigdataValue>[]> {

    private final LexiconRelation r;
    private final boolean readOnly;
    private final int numTerms;
    private final BigdataValue[] terms;
    private final WriteTaskStats stats;
    
    public Term2IdWriteTask(final LexiconRelation r,
            final boolean readOnly, final int numTerms,
            final BigdataValue[] terms, final WriteTaskStats stats) {

        if (r == null)
            throw new IllegalArgumentException();

        if (terms == null)
            throw new IllegalArgumentException();

        if (numTerms < 0 || numTerms > terms.length)
            throw new IllegalArgumentException();

        if (stats == null)
            throw new IllegalArgumentException();

        this.r = r;

        this.readOnly = readOnly;
        
        this.numTerms = numTerms;
        
        this.terms = terms;
        
        this.stats = stats;
        
    }
    
    /**
     * Unify the {@link BigdataValue}s with the TERM2ID index, setting the
     * term identifiers (TIDs) on those values as a side-effect.
     * 
     * @return A dense {@link KVO}[] chunk consisting of only those
     *         distinct terms whose term identifier was not already known.
     *         (This may be used to write on the reverse index).
     * 
     * @throws Exception
     */
    public KVO<BigdataValue>[] call() throws Exception {
        
        /*
         * Insert into the forward index (term -> id). This will either assign a
         * termId or return the existing termId if the term is already in the
         * lexicon.
         */
        
        // The #of distinct terms lacking a pre-assigned term identifier in [a].
        int ndistinct = 0;

        // A dense array of correlated tuples.
        final KVO<BigdataValue>[] a;
        {
            
            final KVO<BigdataValue>[] b;

            /*
             * First make sure that each term has an assigned sort key.
             */
            {

                long _begin = System.currentTimeMillis();
                
                final Term2IdTupleSerializer tupleSer = (Term2IdTupleSerializer) r.getIndex(
                        LexiconKeyOrder.TERM2ID).getIndexMetadata()
                        .getTupleSerializer();

                // may contain duplicates and/or terms with pre-assigned term identifiers.
                b = r.generateSortKeys(tupleSer.getLexiconKeyBuilder(), terms, numTerms);

                stats.keyGenTime = System.currentTimeMillis() - _begin;

            }

            /*
             * Sort by the assigned sort key. This places the array into the
             * natural order for the term:id index.
             */
            {

                final long _begin = System.currentTimeMillis();

                Arrays.sort(b);

                stats.sortTime += System.currentTimeMillis() - _begin;

            }

            /*
             * For each distinct term that does not have a pre-assigned term
             * identifier, add it to a remote unisolated batch operation that
             * assigns term identifiers.
             * 
             * Note: Both duplicate term references and terms with their term
             * identifiers already assigned are dropped out in this step.
             */
            {

                final long _begin = System.currentTimeMillis();

                final IIndex termIdIndex = r.getTerm2IdIndex();

                /*
                 * Create a key buffer holding the sort keys. This does not
                 * allocate new storage for the sort keys, but rather aligns the
                 * data structures for the call to splitKeys(). This also makes
                 * a[] into a dense copy of the references in b[], but without
                 * duplicates and without terms that already have assigned term
                 * identifiers. Note that keys[] and a[] are correlated.
                 * 
                 * @todo Could be restated as an IDuplicateRemover, but note
                 * that this case is specialized since it can drop terms whose
                 * term identifier is known (they do not need to be written on
                 * T2ID, but they still need to be written on the reverse index
                 * to ensure a robust and consistent mapping).
                 */
                final byte[][] keys = new byte[numTerms][];
                a = new KVO[numTerms];
                {

                    for (int i = 0; i < numTerms; i++) {

                        if (b[i].obj.getTermId() != IRawTripleStore.NULL) {
                            
                            if (LexiconRelation.DEBUG)
                                LexiconRelation.log.debug("term identifier already assigned: "
                                        + b[i].obj);
                            
                            // term identifier already assigned.
                            continue;
                            
                        }
                        
                        if (i > 0 && b[i - 1].obj == b[i].obj) {

                            if (LexiconRelation.DEBUG)
                                LexiconRelation.log.debug("duplicate term reference: "
                                        + b[i].obj);
                            
                            // duplicate reference.
                            continue;
                            
                        }

                        // assign to a[] (dense variant of b[]).
                        a[ndistinct] = b[i];
                        
                        // assign to keys[] (dense and correlated with a[]).
                        keys[ndistinct] = b[i].key;
                        
                        ndistinct++;

                    }

                }

                if (ndistinct == 0) {
                    
                    /*
                     * Nothing to be written.
                     */
                    
                    return new KVO[0];
                    
                }
                
                // run the procedure.
                termIdIndex.submit(0/* fromIndex */, ndistinct/* toIndex */,
                        keys, null/* vals */, new Term2IdWriteProcConstructor(
                                readOnly, r.storeBlankNodes, //scaleOutTermIds,
                                r.termIdBitsToReverse),
                        new IResultHandler<Term2IdWriteProc.Result, Void>() {

                            /**
                             * Copy the assigned / discovered term identifiers
                             * onto the corresponding elements of the terms[].
                             */
                            public void aggregate(
                                    final Term2IdWriteProc.Result result,
                                    final Split split) {

                                for (int i = split.fromIndex, j = 0; i < split.toIndex; i++, j++) {

                                    final long termId = result.ids[j];
                                    
                                    if (termId == IRawTripleStore.NULL) {

                                        if (!readOnly)
                                            throw new AssertionError();
                                        
                                        stats.nunknown.incrementAndGet();
                                        
                                    } else {
                                        
                                        a[i].obj.setTermId(termId);
                                        
                                        if(LexiconRelation.log.isDebugEnabled()) {
                                            LexiconRelation.log.debug("termId="+termId+", term="+a[i].obj);
                                        }
                                        
                                    }

                                }

                            }

                            public Void getResult() {

                                return null;

                            }

                        });

                stats.indexTime = stats.forwardIndexTime = System.currentTimeMillis()
                        - _begin;

            }

        }
        
        stats.ndistinct = ndistinct;

        return KVO.dense(a, ndistinct);
        
    } // call
    
}
