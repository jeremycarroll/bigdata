package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.bigdata.rawstore.Addr;

/**
 * Disk-based journal strategy.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BufferMode#Disk
 */
public class DiskOnlyStrategy extends AbstractBufferStrategy {

    /**
     * The file.
     */
    final File file;

    /**
     * The IO interface for the file.
     */
    final RandomAccessFile raf;

    /**
     * The size of the journal header, including MAGIC, version, and both root
     * blocks. This is used as an offset when computing the address of a record
     * in an underlying file and is ignored by buffer modes that are not backed
     * by a file (e.g., transient) or that are memory mapped (since the map is
     * setup to skip over the header)
     */
    final int headerSize;
    
    /**
     * Extent of the file. This value should be valid since we obtain an
     * exclusive lock on the file when we open it.
     */
    final long extent;

    final long userExtent;
    
    /**
     * The backing channel.
     */
    final FileChannel channel;

    private boolean open;

    DiskOnlyStrategy(FileMetadata fileMetadata) {

        super(fileMetadata.nextOffset, BufferMode.Disk);

        this.file = fileMetadata.file;

        this.raf = fileMetadata.raf;

        this.extent = fileMetadata.extent;

        this.headerSize = fileMetadata.headerSize0;
        
        this.userExtent = extent - headerSize;
        
        this.channel = raf.getChannel();
        
        open = true;

    }

    final public boolean isOpen() {

        return open;

    }

    final public boolean isStable() {
        
        return true;
        
    }

    /**
     * Forces the data to disk.
     */
    public void force(boolean metadata) {

        try {

            raf.getChannel().force(metadata);

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    /**
     * Closes the file.
     */
    public void close() {

        if (!isOpen())
            throw new IllegalStateException();

        try {

            raf.close();

        } catch (IOException ex) {

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
    
    final public long getExtent() {

        return extent;

    }

    final public long getUserExtent() {
        
        return userExtent;
        
    }
    
    public ByteBuffer read(long addr, ByteBuffer dst) {

        if (addr == 0L)
            throw new IllegalArgumentException("Address is 0L");

        final int offset = Addr.getOffset(addr);

        final int nbytes = Addr.getByteCount(addr);

        if (nbytes == 0) {

            throw new IllegalArgumentException(
                    "Address encodes record length of zero");

        }

        try {

            if (offset + nbytes > nextOffset) {

                throw new IllegalArgumentException("Address never written.");

            }

            if (dst != null && dst.remaining() >= nbytes) {

                // copy exactly this many bytes.
                
                dst.limit(dst.position() + nbytes);
                
                // copy into the caller's buffer.

                raf.getChannel().read(dst, (long) offset + headerSize);

                // flip for reading.

                dst.flip();

                // the caller's buffer.

                return dst;

            } else {

                // allocate a new buffer of the exact capacity.

                dst = ByteBuffer.allocate(nbytes);

                // copy the data into the buffer.

                raf.getChannel().read(dst, (long) offset + headerSize);

                // flip for reading.

                dst.flip();

                // return the buffer.

                return dst;

            }

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }
        
    }

    public long write(ByteBuffer data) {

        if (data == null)
            throw new IllegalArgumentException("Buffer is null");

        // #of bytes to store.
        final int nbytes = data.remaining();

        if (nbytes == 0)
            throw new IllegalArgumentException("No bytes remaining in buffer");

        try {

            long pos = (long) nextOffset + (long) headerSize;

            if (pos + nbytes > Integer.MAX_VALUE) {

                throw new IOException("Would exceed int32 bytes in file.");

            }

            // the offset at which the record will be written (not adjusted for the root blocks).
            final int offset = nextOffset;

            // write the data onto the end of the file.
            final int count = raf.getChannel().write(data, pos);
            
            if(count != nbytes) {
            
                throw new IOException("Expecting to write " + nbytes
                        + " bytes, but wrote " + count + " bytes.");
                
            }

            // increment by the #of bytes written.
            nextOffset += nbytes;
            
            // formulate the address that can be used to recover that record.
            return Addr.toLong(nbytes, offset);

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }
    
    public void writeRootBlock(IRootBlockView rootBlock,ForceEnum forceOnCommit) {

        if (rootBlock == null)
            throw new IllegalArgumentException();
        
        try {

            FileChannel channel = raf.getChannel();

            final int count = channel.write(rootBlock.asReadOnlyBuffer(),
                    rootBlock.isRootBlock0() ? FileMetadata.OFFSET_ROOT_BLOCK0
                            : FileMetadata.OFFSET_ROOT_BLOCK1);
            
            if(count != RootBlockView.SIZEOF_ROOT_BLOCK) {
                
                throw new IOException("Expecting to write "
                        + RootBlockView.SIZEOF_ROOT_BLOCK + " bytes, but wrote"
                        + count + " bytes.");
                
            }

            if( forceOnCommit != ForceEnum.No ) {

                force(forceOnCommit==ForceEnum.ForceMetadata);
            
            }

        }

        catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    public void truncate(long newExtent) {

        long newUserExtent =  newExtent - headerSize;
        
        if (newUserExtent < getNextOffset() ) {
           
            throw new IllegalArgumentException("Would truncate written data.");
            
        }

        if (newUserExtent > Integer.MAX_VALUE) {

            throw new IllegalArgumentException("User extent would exceed int32 bytes");
            
        }
        
        if(newUserExtent == getUserExtent()) {
            
            // NOP.
            return;
            
        }
        
        try {

            raf.setLength(newExtent);

            System.err.println("Disk file: newLength="+newExtent);
                        
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }

    }

}
