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
package com.bigdata.rdf.rio;

import java.io.Reader;
import java.util.Iterator;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.Parser;
import org.openrdf.rio.StatementHandler;
import org.openrdf.rio.rdfxml.RdfXmlParser;

import com.bigdata.journal.Bytes;
import com.bigdata.objndx.KeyBuilder;
import com.bigdata.rdf.RdfKeyBuilder;
import com.bigdata.rdf.TripleStore;
import com.bigdata.rdf.model.OptimizedValueFactory;

/**
 * Statement handled for the RIO RDF Parser that collects values and statements
 * in batches and inserts ordered batches into the {@link TripleStore}.
 * 
 * @todo (I've done this but the consumer is lagging far behind.) setup a
 *       producer-consumer queue. that creates a more or less even division of
 *       labor so as to maximize load rate on a platform with at least 2 cpus.
 *       The producer runs rio fills in buffers, generates keys for terms, and
 *       sorts terms by their keys and then places the bufferQueue onto the
 *       queue. The consumer accepts a bufferQueue with pre-generated term keys
 *       and terms in sorted order by those keys and (a) inserts the terms into
 *       the database; and (b) generates the statement keys for each of the
 *       statement indices, ordered the statement for each index in turn, and
 *       bulk inserts the statements into each index in turn. <br>
 *       pre-generate keys before handing over the bufferQueue to the consumer.
 *       <br>
 *       pre-sort terms by their keys before handing over the bufferQueue to the
 *       consumer. <br>
 *       if an LRU cache is to be used, then allow it to cross bufferQueue
 *       boundaries always returning the same value. such values will have a
 *       non-null key so their key will only be generated once. see if i can
 *       leverage the existing cweb LRU classes for this. <br>
 *       sort does not remove duplicate. therefore if we do not use some sort of
 *       cache in the term factory then it may be worth testing if terms in a
 *       sequence are the same term.
 * 
 * @todo compare a bulk insert for each batch with a "perfect" index build and a
 *       compacting merge (i.e., a bulk load that runs outside of the
 *       transaction mechanisms). small documents can clearly fit inside of a
 *       single bufferQueue and batch insert. Up to N buffers could be collected
 *       before deciding whether the document is moderate in size vs very large.
 *       Very large documents can combine N buffers into an index segment and
 *       collect a set of index segments. the commit would merge those index
 *       segments with the database.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MultiThreadedPresortRioLoader implements IRioLoader, StatementHandler
{

    public static Logger log = Logger.getLogger(ConsumerThread.class);

    /**
     * Note: I am seeing a 1000 tps performance boost at 1M vs 100k for this
     * value.
     */
    final int BUFFER_SIZE = 1000000;
    
    TripleStore store;
    
    long stmtsAdded;
    
    long insertTime;
    
    long insertStart;

    /**
     * The capacity of the bufferQueue queue.
     */
    final int queueCapacity = 10;
    
    /**
     * A queue of {@link Buffer} objects that have been populated by the RIO
     * parser and are awaiting further processing.
     */
    final BlockingQueue<Buffer> bufferQueue = new ArrayBlockingQueue<Buffer>(
            queueCapacity); 
    
    ConsumerThread consumer;
    
    /**
     * Setup key builder that handles unicode and primitive data types.
     * <p>
     * Note: this is used when the main parser thread needs to encode term keys
     * at the same time as the {@link ConsumerThread} needs to encode statement
     * keys.
     */
    final RdfKeyBuilder keyBuilder;

    Vector<RioLoaderListener> listeners;
    
//    /**
//     * When true, each term in a type specific bufferQueue will be placed into a
//     * cannonicalizing map so that we never have more than once instance for
//     * the same term in a bufferQueue at a time.  When false those transient term
//     * maps are not used.
//     * 
//     * Note: this does not show much of an effect.
//     */
//    final boolean useTermMaps = false;

    /**
     * Used to bufferQueue RDF {@link Value}s and {@link Statement}s emitted by
     * the RDF parser.
     */
    Buffer buffer;
    
    public MultiThreadedPresortRioLoader( TripleStore store ) {
    
        this.store = store;

        this.buffer = new Buffer(store, BUFFER_SIZE);
       
        this.keyBuilder = new RdfKeyBuilder(new KeyBuilder(store
                .createCollator(), Bytes.kilobyte32 * 4));
    }
    
    public long getStatementsAdded() {
        
        return stmtsAdded;
        
    }
    
    public long getInsertTime() {
        
        return insertTime;
        
    }
    
    public long getTotalTime() {
        
        return insertTime;
        
    }
    
    public long getInsertRate() {
        
        return (long) 
            ( ((double)stmtsAdded) / ((double)getTotalTime()) * 1000d );
        
    }
    
    public void addRioLoaderListener( RioLoaderListener l ) {
        
        if ( listeners == null ) {
            
            listeners = new Vector<RioLoaderListener>();
            
        }
        
        listeners.add( l );
        
    }
    
    public void removeRioLoaderListener( RioLoaderListener l ) {
        
        listeners.remove( l );
        
    }
    
    protected void notifyListeners() {
        
        RioLoaderEvent e = new RioLoaderEvent
            ( stmtsAdded,
              System.currentTimeMillis() - insertStart
              );
        
        for ( Iterator<RioLoaderListener> it = listeners.iterator(); 
              it.hasNext(); ) {
            
            it.next().processingNotification( e );
            
        }
        
    }
    
    /**
     * We need to collect two (three including bnode) term arrays and one
     * statement array.  These should be buffers of a settable size.
     * <p>
     * Once the term buffers are full (or the data is exhausted), the term 
     * arrays should be sorted and batch inserted into the TripleStore.
     * <p>
     * As each term is inserted, its id should be noted in the Value object,
     * so that the statement array is sortable based on term id.
     * <p>
     * Once the statement bufferQueue is full (or the data is exhausted), the 
     * statement array should be sorted and batch inserted into the
     * TripleStore.  Also the term buffers should be flushed first.
     * 
     * @param reader
     *                  the RDF/XML source
     */

    public void loadRdfXml( Reader reader ) throws Exception {
        
        OptimizedValueFactory valueFac = new OptimizedValueFactory();
        
        Parser parser = new RdfXmlParser( valueFac );
        
        parser.setVerifyData( false );
        
        parser.setStatementHandler( this );
        
        // Note: reset to that rates reflect load times not clock times.
        insertStart = System.currentTimeMillis();
        insertTime = 0; // clear.
        
        // Note: reset so that rates are correct for each source loaded.
        stmtsAdded = 0;
        
        // Allocate the initial bufferQueue for parsed data.
        buffer = new Buffer(store,BUFFER_SIZE);

        // Start thread to consume buffered data.
        consumer = new ConsumerThread(bufferQueue);

        try {

            log.info("Starting parse.");
            
            consumer.start();
            
            // Parse the data.
            parser.parse(reader, "");
            
            log.info("parse complete: elapsed="
                    + (System.currentTimeMillis() - insertStart) + "ms");
            
            /*
             * Typically the last buffer will be non-empty so we put it on the
             * queue.
             */
            if(buffer != null) {

                putBufferOnQueue(buffer);
                
            }

            /*
             * Put a poison object on the queue to cause the ConsumerThread to
             * shutdown once it has processed everything on the queue. 
             */

            putBufferOnQueue(new Buffer(store,-1));
            
            /* wait until the queue is empty.
             * 
             * @todo alternatively, wait until the ConsumerThread quits.
             */
            while(!bufferQueue.isEmpty()) {
                try {
                    Thread.sleep(100); // yeild for a bit.
                    checkConsumer(); // look for an error condition.
                } catch(InterruptedException ex) {
                    continue;
                }
            }
            log.info("data loaded: elapsed="
                    + (System.currentTimeMillis() - insertStart) + "ms");

            // check for an error condition.
            checkConsumer();

            // commit the data.
            store.commit();
            
            log.info("data committed: elapsed="
                    + (System.currentTimeMillis() - insertStart) + "ms");
            
        } catch (RuntimeException ex) {

            log.error("While parsing data: "+ex, ex);
            
            // shutdown the consumer.
            consumer.shutdown();

            // clear the queue.
            bufferQueue.clear();
            
            // discard the buffer.
            buffer = null;
            
            throw ex;
            
        }
        
        insertTime += System.currentTimeMillis() - insertStart;

    }

    /**
     * Checks the consumer for an error condition and if there is one then
     * throws an exception in the main thread to halt processing.
     */
    protected void checkConsumer() {

        if(consumer.error!=null) {

            throw new RuntimeException("Consumer reports error: "
                    + consumer.error);
            
        }

    }
    
    /**
     * Put the buffer onto the queue, blocking if necessary.
     * 
     * @param buffer The buffer.
     */
    protected void putBufferOnQueue(Buffer buffer) {

        assert buffer != null;

        // check for an error condition.
        checkConsumer();
        
        try {

            if (buffer.capacity > 0) {

                // pre-generate the sort keys.
                buffer.generateSortKeys(keyBuilder);

                // pre-sort the terms.
                buffer.sortTerms();
                
            }
            
            log.info("putting "
                    + (buffer.capacity == -1 ? "poison object"
                            : "buffer(haveKeys=" + buffer.haveKeys + ",sorted="
                                    + buffer.sorted + ")")
                    + " on the queue; queue length=" + bufferQueue.size());

            /* 
             * Put the buffer onto the queue, blocking if necessary.
             */
            
            bufferQueue.put(buffer);
        
        } catch (InterruptedException ex) {
            
            /*
             * @todo What is the right way to handle this?
             * 
             * @todo What happens if the ConsumerThread stops running while
             * we are blocked here? Should the ConsumerThread hold a
             * reference to this thread and interrupt it so that it can
             * terminate as well?
             */
            
            throw new RuntimeException(
                    "Interrupted while waiting to put data on the queue.",
                    ex);

        }

    }
    
    /**
     * Consumers populated {@link Buffer}s placed on a {@link BlockingQueue}
     * and causes their data to be inserted into the store.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ConsumerThread extends Thread {
        
        final BlockingQueue<Buffer> bufferQueue;
        
        boolean shutdown = false;
        
        volatile Throwable error = null;
        
        public ConsumerThread(BlockingQueue<Buffer> bufferQueue) {
            
            assert bufferQueue != null;
            
            this.bufferQueue = bufferQueue;
            
        }
        
        public void run() {
         
            log.info("Starting consumer.");
            
            Buffer buffer = null;
            
            while(!shutdown) {

                try {

                    /* 
                     * obtain a buffer from the queue.
                     */
                    buffer = bufferQueue.take();

                    log.info("Consumer accepting buffer: queue length="
                            + bufferQueue.size());

                } catch (InterruptedException ex) {
                    
                    /*
                     * If we are interrupted then continue from the top of the
                     * loop. This gives a caller a means to force shutdown by
                     * causing [shutdown] to be true and interrupting this
                     * thread.
                     */
                    
                    continue;
                    
                }
                
                if(buffer.capacity == -1 ) {
                    
                    /*
                     * This indicates that no more buffers will be placed onto
                     * the queue and that this thread should quit immediately.
                     */ 
                    
                    log.info("Consumer accepted poison object - quitting now.");

                    return;
                    
                }
                
                try {

                    buffer.insert();
                    
                } catch(RuntimeException ex) {
                    
                    /*
                     * Set [error] to inform the producer that this thread has
                     * quit.
                     * 
                     * FIXME We may need to have a reference to the producer
                     * thread so that we can interrupt it.
                     */
                    
                    log.error("While loading data: "+ex, ex);
                 
                    error = ex;
                    
                    return;
                    
                }
                
                buffer = null;
                
            }
            
        }
        
        /**
         * Shutdown the thread (abnormal termination).
         */
        public void shutdown() {

            log.info("will shutdown.");
            
            // mark thread for shutdown.
            this.shutdown = true;

            while( isAlive() ) {
                
                yield(); // yield so that the ConsumerThread can run().
                
//                System.err.print('.');
                
            }
            
            log.info("did shutdown.");
            
        }

    }
    
    public void handleStatement( Resource s, URI p, Value o ) {

        /* 
         * When the bufferQueue could not accept three of any type of value plus 
         * one more statement then it is considered to be near capacity and
         * is flushed to prevent overflow. 
         */
        if(buffer.nearCapacity()) {

            putBufferOnQueue(buffer);
            
            // allocate a new bufferQueue.
            buffer = new Buffer(store,BUFFER_SIZE);
            
            // fall through.
            
        }
        
        // add the terms and statement to the bufferQueue.
        buffer.handleStatement(s,p,o);
        
        stmtsAdded++;
        
        if ( stmtsAdded % 100000 == 0 ) {
            
            notifyListeners();
            
        }
        
    }
    
}
