package com.bigdata.rdf.store;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.openrdf.model.Value;

import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.AbstractChunkedResolverator;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Efficiently resolve term identifiers in Bigdata {@link ISolution}s to RDF
 * {@link BigdataValue}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataStatementIteratorImpl
        extends
        AbstractChunkedResolverator<ISPO, BigdataStatement, AbstractTripleStore>
        implements BigdataStatementIterator {

//    final protected static Logger log = Logger.getLogger(BigdataStatementIteratorImpl.class);
//
//    /**
//     * True iff the {@link #log} level is INFO or less.
//     */
//    final protected static boolean INFO = log.isInfoEnabled();
//
//    /**
//     * True iff the {@link #log} level is DEBUG or less.
//     */
//    final protected static boolean DEBUG = log.isDebugEnabled();

    /**
     * 
     * @param db
     *            Used to resolve term identifiers to {@link Value} objects.
     * @param src
     *            The source iterator (will be closed when this iterator is
     *            closed).
     */
    public BigdataStatementIteratorImpl(final AbstractTripleStore db,
            final IChunkedOrderedIterator<ISPO> src) {

        super(db, src, new BlockingBuffer<BigdataStatement[]>(
                db.getChunkOfChunksCapacity(), 
                db.getChunkCapacity(),
                db.getChunkTimeout(),
                TimeUnit.MILLISECONDS));
        
    }

    /**
     * Strengthens the return type.
     */
    public BigdataStatementIteratorImpl start(ExecutorService service) {

        return (BigdataStatementIteratorImpl) super.start(service);
        
    }

    /**
     * Resolve a chunk of {@link ISPO}s into a chunk of
     * {@link BigdataStatement}s.
     */
    protected BigdataStatement[] resolveChunk(final ISPO[] chunk) {

        if (DEBUG)
            log.debug("chunkSize=" + chunk.length);

        /*
         * Create a collection of the distinct term identifiers used in this
         * chunk.
         * 
         * @todo Long.valueOf(long) is a hot spot here. However, I probably need
         * to drive the native long hash map awareness into
         * getTerms(Collection<Long>) in order for the use of the
         * LongOpenHashMap here to be of benefit.
         */

//        final LongOpenHashSet ids = new LongOpenHashSet(chunk.length*4);
        
        final Collection<Long> ids = new LinkedHashSet<Long>(chunk.length * 4);

        for (ISPO spo : chunk) {

            ids.add(spo.s());

            ids.add(spo.p());

            ids.add(spo.o());

            if (spo.hasStatementIdentifier()) {

                ids.add(spo.getStatementIdentifier());

            }

        }

        if (DEBUG)
            log.debug("Resolving " + ids.size() + " term identifiers");
        
        /*
         * Batch resolve term identifiers to BigdataValues, obtaining the
         * map that will be used to resolve term identifiers to terms for
         * this chunk.
         */
        final Map<Long, BigdataValue> terms = state.getLexiconRelation().getTerms(ids);

        final BigdataValueFactory valueFactory = state.getValueFactory();
        
        /*
         * The chunk of resolved statements.
         */
        final BigdataStatement[] stmts = new BigdataStatement[chunk.length];
        
        int i = 0;
        for (ISPO spo : chunk) {

            /*
             * Resolve term identifiers to terms using the map populated
             * when we fetched the current chunk.
             */
            final BigdataResource s = (BigdataResource) terms.get(spo.s());
            final BigdataURI p = (BigdataURI) terms.get(spo.p());
            final BigdataValue o = terms.get(spo.o());
            final BigdataResource c = (spo.hasStatementIdentifier() ? (BigdataResource) terms
                    .get(spo.getStatementIdentifier())
                    : null);

            if (spo.hasStatementType() == false) {

                log.error("statement with no type: "
                        + valueFactory.createStatement(s, p, o, c, null));

            }

            // the statement.
            final BigdataStatement stmt = valueFactory.createStatement(s, p, o,
                    c, spo.getStatementType());

            // save the reference.
            stmts[i++] = stmt;

        }

        return stmts;

    }

}
