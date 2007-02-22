package com.bigdata.journal;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * Transient buffer strategy uses a direct buffer but never writes on disk.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BufferMode#Transient
 */
public class TransientBufferStrategy extends BasicBufferStrategy {
    
    private boolean open = false;

    /**
     * The root blocks.
     */
    final private IRootBlockView rootBlocks[] = new IRootBlockView[2];
    
    /**
     * Either zero (0) or one (1).
     */
    private int currentRootBlock = 0;
    
    TransientBufferStrategy(long initialExtent, long maximumExtent,
            boolean useDirectBuffers) {
        
        /*
         * Note: I have not observed much performance gain from the use of
         * a direct buffer for the transient mode.
         */
        super(  maximumExtent,
                0/* nextOffset */, //
                0/*headerSize*/, //
                initialExtent, //
                BufferMode.Transient, //
                (useDirectBuffers ? ByteBuffer
                        .allocateDirect((int) initialExtent) : ByteBuffer
                        .allocate((int) initialExtent)));
    
        open = true;
        
    }
    
    public void deleteFile() {
        
        if( open ) throw new IllegalStateException();

        // NOP.
        
    }
    
    public void force(boolean metadata) {
        
        // NOP.
        
    }
    
    /**
     * Always returns <code>null</code>.
     */
    public File getFile() {
        
        return null;
        
    }

    public void close() {
        
        if( ! isOpen() ) {
            
            throw new IllegalStateException();
            
        }

        open = false;
        
    }

    public void closeAndDelete() {
        
        close();

    }

    final public boolean isOpen() {

        return open;
        
    }

    final public boolean isStable() {
        
        return false;
        
    }

    public boolean isFullyBuffered() {
        
        return true;
        
    }
    
    public void writeRootBlock(IRootBlockView rootBlock, ForceEnum forceOnCommit) {
        
        if(rootBlock == null) throw new IllegalArgumentException();
        
        currentRootBlock = rootBlock.isRootBlock0() ? 0 : 1;

        rootBlocks[currentRootBlock] = rootBlock;
        
    }

}
