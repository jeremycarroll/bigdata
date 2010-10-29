package com.bigdata.service.ndx.pipeline;

//BTM import com.bigdata.service.AbstractFederation;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Statistics for asynchronous index writes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexAsyncWriteStats<L, HS extends IndexPartitionWriteStats> extends
        AbstractRunnableMasterStats<L, HS> {

//BTM    public IndexAsyncWriteStats(final AbstractFederation<?> fed) {
//BTM
//BTM        super(fed);
public IndexAsyncWriteStats(final ScheduledExecutorService scheduledExecutor) {
        super(scheduledExecutor);

    }

}
