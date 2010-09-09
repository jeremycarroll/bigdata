package com.bigdata.bop.engine;

import java.io.Serializable;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.fed.FederatedRunningQuery;
import com.bigdata.relation.accesspath.IAsynchronousIterator;

/**
 * An non-{@link Serializable} chunk of intermediate results which are ready to
 * be consumed by some {@link BOp} in a specific query (this is only used in
 * query evaluation for the standalone database).
 */
public class BindingSetChunk implements IChunkMessage {

    /** The query controller. */
    private final IQueryClient clientProxy;

    /**
     * The query identifier.
     */
    private final long queryId;
    
    /**
     * The target {@link BOp}.
     */
    private final int bopId;

    /**
     * The index partition which is being targeted for that {@link BOp}.
     */
    private final int partitionId;
    
    /**
     * The binding sets to be consumed by that {@link BOp}.
     */
    private IAsynchronousIterator<IBindingSet[]> source;

    public IQueryClient getQueryController() {
        return clientProxy;
    }

    public long getQueryId() {
        return queryId;
    }

    public int getBOpId() {
        return bopId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public boolean isMaterialized() {
        return true;
    }

    /**
     * 
     * @todo constructor to accept the BlockingBuffer instead as part of
     *       {@link IChunkMessage} harmonization (or an "IChunk" API).
     */
    public BindingSetChunk(final IQueryClient clientProxy, final long queryId,
            final int bopId, final int partitionId,
            final IAsynchronousIterator<IBindingSet[]> source) {

        if (clientProxy == null)
            throw new IllegalArgumentException();
        
        if (source == null)
            throw new IllegalArgumentException();
        
        this.clientProxy = clientProxy;
        
        this.queryId = queryId;
        
        this.bopId = bopId;
        
        this.partitionId = partitionId;
        
        this.source = source;
        
    }

    public String toString() {

        return getClass().getName() + "{queryId=" + queryId + ",bopId=" + bopId
                + ",partitionId=" + partitionId + "}";
        
    }

    public void materialize(FederatedRunningQuery runningQuery) {
        // NOP
    }
    
    public IAsynchronousIterator<IBindingSet[]> iterator() {
        return source;
    }

}
