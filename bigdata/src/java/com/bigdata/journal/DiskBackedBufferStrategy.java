package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;


/**
 * Abstract base class for implementations that use a direct buffer as a
 * write through cache to an image on the disk. This covers both the
 * {@link BufferMode#Direct}, where we use explicit IO operations, and the
 * {@link BufferMode#Mapped}, where we memory-map the image. Common
 * features shared by these implementations deal mainly with initialization
 * of a new disk image.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class DiskBackedBufferStrategy extends BasicBufferStrategy {

    /**
     * The name of the backing file.
     */
    final File file;

    /**
     * Interface for random access on the backing file.
     */
    final RandomAccessFile raf;

    /**
     * True iff the channel is open.
     */
    private boolean open = false;

    public boolean isOpen() {
        
        return open;
        
    }

    /**
     * Forces the data to disk.
     */
    public void force( boolean metadata ) {
        
        try {

            raf.getChannel().force( metadata );
            
        } catch( IOException ex ) {
            
            throw new RuntimeException(ex);
            
        }           
        
    }
    
    /**
     * Closes the file.
     */
    public void close() {
        
        if( ! open ) throw new IllegalStateException();

        try {

            force( false );
            
            raf.close();
            
        } catch( IOException ex ) {
            
            throw new RuntimeException(ex);
            
        }
        
        open = false;
        
    }

    public void deleteFile() {
        
        if( open ) throw new IllegalStateException();
        
        if( ! file.delete() ) {
            
            throw new RuntimeException("Could not delete file: "
                    + file.getAbsoluteFile());
            
        }
        
    }
    
    DiskBackedBufferStrategy(BufferMode bufferMode, FileMetadata fileMetadata, SlotMath slotMath) {

        super(fileMetadata.journalHeaderSize, bufferMode,slotMath,fileMetadata.buffer);

        this.file = fileMetadata.file;
        
        this.raf = fileMetadata.raf;
        
        this.open = true;

    }
   
    public void writeRootBlock(IRootBlockView rootBlock) {

        if( rootBlock == null ) throw new IllegalArgumentException();

        try {

            FileChannel channel = raf.getChannel();

            channel.write(rootBlock.asReadOnlyBuffer(), rootBlock
                    .isRootBlock0() ? FileMetadata.OFFSET_ROOT_BLOCK0
                    : FileMetadata.OFFSET_ROOT_BLOCK1);

            force(false);

        }

        catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

}
