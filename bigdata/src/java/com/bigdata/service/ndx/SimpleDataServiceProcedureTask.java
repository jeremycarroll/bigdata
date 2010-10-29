package com.bigdata.service.ndx;

import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.service.Split;

/**
 * Class handles stale locators by finding the current locator for the
 * <i>key</i> and redirecting the request to execute the procedure on the
 * shard service identified by that locator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
class SimpleDataServiceProcedureTask extends AbstractDataServiceProcedureTask {

    protected final byte[] key;
    
    private int ntries = 1;

    /**
     * Always reports ONE (1).
     */
    protected int getElementCount() {
        
        return 1;
        
    }
    
    /**
     * @param key
     * @param split
     * @param proc
     * @param resultHandler
     */
    public SimpleDataServiceProcedureTask(final IScaleOutClientIndex ndx,
            final byte[] key, long ts, final Split split,
            final ISimpleIndexProcedure proc,
            final IResultHandler resultHandler) {

        super(ndx, ts, split, proc, resultHandler);
        
        if (key == null)
            throw new IllegalArgumentException();
    
        this.key = key;
        
        synchronized(taskCountersByIndex) {
            taskCountersByIndex.pointRequestCount++;
        }
        
    }

    /**
     * The locator is stale. We locate the index partition that spans the
     * {@link #key} and re-submit the request.
     */
    @Override
    protected void retry() throws Exception {
        
        synchronized(taskCountersByIndex) {
            taskCountersByIndex.redirectCount++;
        }

//BTM - PRE_CLIENT_SERVICE  if (ntries++ > ndx.getFederation().getClient().getMaxStaleLocatorRetries()) {
        if (ntries++ > ndx.getMaxStaleLocatorRetries()) {

            throw new RuntimeException("Retry count exceeded: ntries="
                    + ntries);

        }

        /*
         * Note: uses the metadata index for the timestamp against which the
         * procedure is running.
         */
//BTM - PRE_CLIENT_SERVICE  final PartitionLocator locator = ndx.getFederation().getMetadataIndex(ndx.getName(), ts).find(key);
        final PartitionLocator locator = 
            (ndx.getIndexStore()).getMetadataIndex(ndx.getName(), ts).find
                                                                        (key);

        if (log.isInfoEnabled())
            log.info("Retrying: proc=" + proc.getClass().getName()
                    + ", locator=" + locator + ", ntries=" + ntries);

        /*
         * Note: In this case we do not recursively submit to the outer
         * interface on the client since all we need to do is fetch the
         * current locator for the key and re-submit the request to the data
         * service identified by that locator.
         */

        submit(locator);
        
    }
    
}
