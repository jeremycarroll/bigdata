package com.bigdata.bop.engine;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.UUID;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.fed.FederatedRunningQuery;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

/**
 * An non-{@link Serializable} chunk of intermediate results which are ready to
 * be consumed by some {@link BOp} in a specific query (this is only used in
 * query evaluation for the standalone database).
 */
public class LocalChunkMessage implements IChunkMessage<IBindingSet> {

    /** The query controller. */
    private final IQueryClient queryController;

    /** The service {@link UUID} for the {@link IQueryClient query controller}. */
    private final UUID queryControllerId;
    
    /**
     * The query identifier.
     */
    private final UUID queryId;
    
    /**
     * The target {@link BOp}.
     */
    private final int bopId;

    /**
     * The index partition which is being targeted for that {@link BOp}.
     */
    private final int partitionId;

    /**
     * The #of solutions in the chunk.
     */
    private final int solutionCount;
    
    /**
     * Chunks of binding sets.
     */
    private IBindingSet[][] bindingSetChunks;

    public IQueryClient getQueryController() {
        return queryController;
    }

    public UUID getQueryControllerId() {
        return queryControllerId;
    }
    
    public UUID getQueryId() {
        return queryId;
    }

    public int getBOpId() {
        return bopId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public boolean isLastInvocation() {
        return false; // Never.
    }

    public boolean isMaterialized() {
        return true;
    }

    public int getSolutionCount() {
        return solutionCount;
    }

    public LocalChunkMessage(final IQueryClient queryController,
            final UUID queryId, final int bopId, final int partitionId,
            final IBindingSet bset) {

        this(queryController, queryId, bopId, partitionId,
                new IBindingSet[][] { new IBindingSet[] { bset } });

    }

    public LocalChunkMessage(final IQueryClient queryController,
            final UUID queryId, final int bopId, final int partitionId,
            final IBindingSet[] bsets) {

        this(queryController, queryId, bopId, partitionId,
                new IBindingSet[][] { bsets });

    }

    /**
     * 
     * TODO There are a few unit tests which rely on the ability to push
     * multiple chunks through the query engine, otherwise this could be changed
     * to an IBindingSet[] by flattening the caller's IBindingSet[][] when
     * given. Doing this could simplify some other things in the API since any
     * chunk would just be an IBindingSet[].
     * <p>
     * See {@link IChunkAccessor} which has some notes concerning this and
     * related matters. It might be useful to preserve the ability to have a
     * message with multiple chunks, ie., to deal with data on different driect
     * buffers.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/475">
     *      Optimize serialization for query messages on cluster </a>
     */
    public LocalChunkMessage(final IQueryClient queryController,
            final UUID queryId, final int bopId, final int partitionId,
            final IBindingSet[][] bindingSetChunks) {

        if (queryController == null)
            throw new IllegalArgumentException();
        
        if (queryId == null)
            throw new IllegalArgumentException();
        
        if (bindingSetChunks == null)
            throw new IllegalArgumentException();
        
        this.queryController = queryController;
        try {
            this.queryControllerId = queryController.getServiceUUID();
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
        
        this.queryId = queryId;
        
        this.bopId = bopId;
        
        this.partitionId = partitionId;
        
        this.solutionCount = solutionCount(bindingSetChunks);
        
        this.bindingSetChunks = bindingSetChunks;
        
    }

    public String toString() {

        return getClass().getName() + "{queryId=" + queryId + ",bopId=" + bopId
                + ",partitionId=" + partitionId + ", solutionCount="
                + solutionCount + "}";

    }

    public void materialize(FederatedRunningQuery runningQuery) {
        // NOP
    }

    public void release() {
        final ChunkAccessor tmp = chunkAccessor;
        if (tmp != null) {
            // Close the iterator.
            tmp.close();
        }
        // Clear the array contents.
        for (int i = 0; i < bindingSetChunks.length; i++) {
            bindingSetChunks[i] = null;
        }
    }
    
    public IChunkAccessor<IBindingSet> getChunkAccessor() {
        if (chunkAccessor == null) {
            chunkAccessor = new ChunkAccessor();
        }
        return chunkAccessor;
    }
    
    private volatile transient ChunkAccessor chunkAccessor = null;
    
    private class ChunkAccessor implements IChunkAccessor<IBindingSet> {

//        public IAsynchronousIterator<IBindingSet[]> iterator() {
//
//            return newBindingSetIterator(bindingSetChunks);
//            
//        }
        private final IAsynchronousIterator<IBindingSet[]> source;

        public ChunkAccessor() {
            source = newBindingSetIterator(bindingSetChunks);
        }

        public IAsynchronousIterator<IBindingSet[]> iterator() {
            return source;
        }

        public void close() {
            source.close();
        }

    }

//    /**
//     * Return an {@link IAsynchronousIterator} that will read a single, empty
//     * {@link IBindingSet}.
//     */
//    private static ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator() {
//
//        return newBindingSetIterator(new ListBindingSet());
//
//    }

//    /**
//     * Return an {@link IAsynchronousIterator} that will read a single
//     * {@link IBindingSet}.
//     * 
//     * @param bindingSet
//     *            the binding set.
//     */
//    private static ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
//            final IBindingSet bindingSet) {
//
//        return new ThickAsynchronousIterator<IBindingSet[]>(
//                new IBindingSet[][] { new IBindingSet[] { bindingSet } });
//
//    }

//    /**
//     * Return an {@link IAsynchronousIterator} that will read the source
//     * {@link IBindingSet}s.
//     * 
//     * @param bsets
//     *            The source binding sets.
//     */
//    private static ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
//            final IBindingSet[] bsets) {
//     
//        return new ThickAsynchronousIterator<IBindingSet[]>(
//                new IBindingSet[][] { bsets });
//        
//    }

    /**
     * Return an {@link IAsynchronousIterator} that will read a single, chunk
     * containing all of the specified {@link IBindingSet}s.
     * 
     * @param bindingSetChunks
     *            the chunks of binding sets.
     */
    private static ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet[][] bindingSetChunks) {

        return new ThickAsynchronousIterator<IBindingSet[]>(bindingSetChunks);

    }

    private static int solutionCount(final IBindingSet[][] bindingSetChunks) {
        
        int solutionCount = 0;
        
        for (int i = 0; i < bindingSetChunks.length; i++) {
        
            solutionCount += bindingSetChunks[i].length;
            
        }

        return solutionCount;
        
    }

}
