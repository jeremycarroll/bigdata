package com.bigdata.service;

import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.DropIndexTask;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.RegisterIndexTask;
import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * Interface for procedures that require access to the {@link IDataService} and
 * or the federation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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
public interface IDataServiceCallable<T>
        extends Serializable {

    /**
     * Computes a result on a {@link IClientService}, or throws an
     * exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    T startDataTask(IIndexManager indexManager, DataService dataService)
            throws Exception;
}
