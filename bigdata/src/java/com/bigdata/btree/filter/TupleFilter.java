package com.bigdata.btree.filter;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.AbstractTuple;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.Tuple;

import cutthecrap.utils.striterators.Filter;

/**
 * <p>
 * Filter supporting {@link ITupleIterator}s.
 * </p>
 * <p>
 * <strong>Warning: Unlike {@link Filter}, this class correctly uses a second
 * {@link Tuple} instance to perform filtering.<strong> This is necessary since
 * the {@link Tuple} instance for the base {@link ITupleIterator}
 * implementations for the {@link AbstractBTree} is reused by next() on each
 * call and the {@link TupleFilter} uses one-step lookahead. Failure to use a
 * second {@link Tuple} instance will result in <em>overwrite</em> of the
 * current {@link Tuple} with data from the lookahead {@link Tuple}.
 * </p>
 * <p>
 * Note: You must specify {@link IRangeQuery#KEYS} and/or
 * {@link IRangeQuery#VALS} in order to filter on the keys and/or values
 * associated with the visited tuples.
 * </p>
 * <p>
 * Note: YOu must specify {@link IRangeQuery#CURSOR} to enabled
 * {@link Iterator#remove()} for a <em>local</em> {@link BTree}
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The type of the elements visited by the iterator (tuples of some
 *            sort).
 */
abstract public class TupleFilter<E> implements ITupleFilter<E> {

    protected static transient final Logger log = Logger.getLogger(TupleFilter.class);
    
    /**
     * Optional state specified by the ctor.
     */
    protected Object state;
   
    public TupleFilter() {

        this(null/* state */);
        
    }

    public TupleFilter(Object state) {
                
        this.state = state;
        
    }
    
    @SuppressWarnings("unchecked")
    public ITupleIterator<E> filter(Iterator src) {

        return new TupleFilter.Filterator((ITupleIterator) src);

    }

    abstract protected boolean isValid(ITuple<E> tuple);

    /**
     * Implementation class knows how to avoid side-effects from the reuse of
     * the same {@link Tuple} instance by the base {@link ITupleIterator} impls.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    protected class Filterator implements ITupleIterator<E> {

        /**
         * The source iterator.
         */
        protected final ITupleIterator<E> src;

        /**
         * The next value to be returned by {@link #next()}.
         */
        private ITuple nextValue = null;

        /**
         * The {@link ITuple} instance that will actually be returned to the
         * caller. The data from {@link #nextValue} is <em>copied</em> into this
         * {@link #returnValue} in order to avoid side-effects from
         * {@link #getNext()}. Those side-effects would otherwise arise because
         * the base {@link ITupleIterator} implementations reuse the same
         * {@link Tuple} instance for each tuple visited by the iterator. A copy
         * of the data must be made in order to avoid side-effects from the
         * one-step lookahead used by the filter.
         */
        final private AbstractTuple returnValue;
        
        public Filterator(ITupleIterator<E> src) {

            this.src = src;

            /*
             * One step lookahead.
             * 
             * Note: This cases to a Tuple because it presumes that we are
             * running directly against an AbstractBTree rather than filtering
             * tuples buffered in a ResultSet on the client. A class cast
             * exception will be thrown in the latter case.
             */
            this.nextValue = getNext();

            if (this.nextValue != null) {
                
                final int sourceIndex = nextValue.getSourceIndex();
                
                final ITupleSerializer tupleSer = nextValue.getTupleSerializer();
                
                // private buffer used to avoid side-effects from getNext()
                this.returnValue = new AbstractTuple<E>(nextValue.flags()) {

                    public int getSourceIndex() {
                        return sourceIndex;
                    }

                    public ITupleSerializer getTupleSerializer() {
                        return tupleSer;
                    }
                };

            } else {

                // nothing to be returned.
                this.returnValue = null;
                
            }
            
        }

        public boolean hasNext() {

            return nextValue != null;

        }

        @SuppressWarnings("unchecked")
        public ITuple<E> next() {

            if (!hasNext())
                throw new NoSuchElementException();

            // copy data from the lookahead tuple instance.
            returnValue.copyTuple( nextValue );
            
            // one step lookahead.
            nextValue = getNext();

            if(log.isInfoEnabled()) {
                
                log.info("returning: "+returnValue);
                
            }

            visit(returnValue);
            
            // return the private instance containing a copy of the data.
            return (ITuple<E>)returnValue;

        }

        /**
         * Hook for subclasses. This is invoked immediately before
         * {@link #next()} returns. The default implementation is a NOP.
         * 
         * @param tuple
         *            The tuple that will be visited.
         */
        protected void visit(ITuple<E> tuple) {
           
            // NOP
            
        }
        
        /**
         * Note: {@link #remove()} is supported iff the source iterator is an
         * {@link ITupleCursor} and the underlying {@link AbstractBTree} allows
         * modification.
         * <p>
         * Note: The filter imposes a one-step lookahead means that invoking
         * {@link #remove()} on the source iterator would cause the wrong
         * element to be removed from the source iterator. Therefore this
         * operation is disabled unless the {@link ITupleCursor}.
         * {@link ITupleCursor} is safe for traversal with concurrent
         * modification, so we can just remove the key from the source index.
         * <p>
         * Note: An {@link ITupleCursor} can be requested either at the
         * top-level or by specifying {@link IRangeQuery#CURSOR} to
         * {@link TupleFilter#TupleFilter(int)}.
         * 
         * @throws UnsupportedOperationException
         *             unless the source iterator is an {@link ITupleCursor}.
         */
        public void remove() {

            if(src instanceof ITupleCursor) {

                /*
                 * The ITupleCursor supports traversal with concurrent
                 * modification. Therefore we can remove the correct entry from
                 * the underlying B+Tree by directing the remove(key) request to
                 * the index implementation itself.
                 */
                
                final byte[] key = returnValue.getKey();
                
                if(log.isInfoEnabled()) {
                    
                    log.info("key="+key);
                    
                }
                
                ((ITupleCursor<E>) src).getIndex().remove(key);
                
            } else {

                /*
                 * Otherwise the one step lookahead imposed by the filter means
                 * that the source iterator is already positioned on a successor
                 * of the current element. If we were to use [src.remove()]
                 * here, it would cause a successor of the current element to be
                 * removed which is not at all what we want.
                 */
                
                throw new UnsupportedOperationException(
                        "Source iterator does not implement "
                                + ITupleCursor.class.getName());
                
            }

        }

        /**
         * One step look ahead.
         * 
         * @return The next object to be visited.
         */
        protected ITuple getNext() {

            while (src.hasNext()) {

                final ITuple<E> next = src.next();

                if (!isValid(next)) {

                    if(log.isInfoEnabled()) {
                        
                        log.info("rejected  : "+next);
                        
                    }
                    
                    continue;
                    
                }
                
                if(log.isInfoEnabled()) {
                    
                    log.info("will visit: "+next);
                    
                }
                
                return (ITuple) next;
                
            }

            if(log.isInfoEnabled()) {
                
                log.info("Source is exhausted.");
                
            }
            
            // source is exhausted.
            return null;

        }

    }

}
