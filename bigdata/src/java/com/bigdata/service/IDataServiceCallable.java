package com.bigdata.service;

import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.resources.ILocalResourceManagement;
import com.bigdata.resources.ResourceManager;

import java.io.Serializable;

/**
 * Interface for procedures that require access to a shard service.
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
 */
public interface IDataServiceCallable<T> extends Serializable {

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
                    ILocalResourceManagement localResourceManager,
                    IBigdataDiscoveryManagement discoveryManager)
            throws Exception;    
}
