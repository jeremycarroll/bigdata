package com.bigdata.service;

import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.DropIndexTask;
import com.bigdata.journal.RegisterIndexTask;

//BTM
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.resources.ResourceManager;

//BTM - PRE_FRED_3481
import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * Interface for procedures that require access to the {@link IDataService} and
 * or the federation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @todo register index and drop index could be rewritten as submitted
 *       procedures derived from this class. This would simplify the
 *       {@link IDataService} API and metrics collection further. The
 *       implementations would have to be distinct from
 *       {@link RegisterIndexTask} and {@link DropIndexTask} since those extend
 *       {@link AbstractTask} - that class does not implement
 *       {@link IIndexProcedure} and can not be sent across the wire.
 * 
 * @see IFederationCallable
 */
//BTM - PRE_FRED_3481 public interface IDataServiceCallable extends IFederationCallable {
public interface IDataServiceCallable<T> extends Serializable {

//BTM - PRE_FRED_3481    /**
//BTM - PRE_FRED_3481     * Invoked before the task is executed to provide a reference to the
//BTM - PRE_FRED_3481     * {@link IDataService} on which it is executing. This method is also
//BTM - PRE_FRED_3481     * responsible for setting the {@link IBigdataFederation} reference using
//BTM - PRE_FRED_3481     * {@link IFederationCallable#setFederation(IBigdataFederation)}.
//BTM - PRE_FRED_3481     * 
//BTM - PRE_FRED_3481     * @param dataService
//BTM - PRE_FRED_3481     *            The data service.
//BTM - PRE_FRED_3481     * 
//BTM - PRE_FRED_3481     * @throws IllegalArgumentException
//BTM - PRE_FRED_3481     *             if the argument is <code>null</code>
//BTM - PRE_FRED_3481     * @throws IllegalStateException
//BTM - PRE_FRED_3481     *             if {@link #setDataService(DataService)} has already been
//BTM - PRE_FRED_3481     *             invoked and was set with a different value.
//BTM - PRE_FRED_3481     */

//BTM    void setDataService(DataService dataService);

//BTM - PRE_FRED_3481 void setDataService(ShardService dataService);
    
//BTM - PRE_FRED_3481    /**
//BTM - PRE_FRED_3481     * Return the {@link DataService}.
//BTM - PRE_FRED_3481     * 
//BTM - PRE_FRED_3481     * @return The data service and never <code>null</code>.
//BTM - PRE_FRED_3481     * 
//BTM - PRE_FRED_3481     * @throws IllegalStateException
//BTM - PRE_FRED_3481     *             if {@link #setDataService(DataService)} has not been invoked.
//BTM - PRE_FRED_3481     */

//BTM    DataService getDataService();

//BTM - PRE_FRED_3481ResourceManager getResourceManager();
//BTM - PRE_FRED_3481ConcurrencyManager getConcurrencyManager();
//BTM - PRE_FRED_3481IIndexManager getIndexManager();
//BTM - PRE_FRED_3481Session getSession();
//BTM - PRE_FRED_3481//for logging output
//BTM - PRE_FRED_3481String getHostname();
//BTM - PRE_FRED_3481String getServiceName();

    /**
     * Computes a result on a shard (data) service, or throws an
     * exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    T startDataTask(IIndexManager indexManager,
                    ResourceManager resourceManager,
                    IConcurrencyManager concurrencyManager,
                    Session session,
                    String hostname,
                    String serviceName)
            throws Exception;    
}
