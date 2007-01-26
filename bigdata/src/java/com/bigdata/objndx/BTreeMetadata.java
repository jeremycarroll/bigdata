package com.bigdata.objndx;

import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Comparator;

import com.bigdata.journal.Bytes;

/**
 * Used to persist metadata for a {@link BTree} so that a historical state may
 * be re-loaded from the store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo the metadata record is extensible since it uses default java
 *       serialization but that makes it a bit fat. this is probably not much of
 *       an issue.
 */
public class BTreeMetadata implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 4370669592664382720L;

    /**
     * The address of the root node or leaf.
     */
    public final long addrRoot;
    
    public final int branchingFactor;

    public final int height;
    
    public final int nnodes;
    
    public final int nleaves;
    
    public final int nentries;
    
    public final IValueSerializer valueSer;
    
    public final RecordCompressor recordCompressor;

    public final boolean useChecksum;
    
    /**
     * Address that can be used to read this metadata record from the store.
     */
    public final long addrMetadata;
    
//    /**
//     * The #of bytes in the metadata record written by {@link #writeMetadata()}.
//     */
//    public static final int SIZEOF_METADATA = Bytes.SIZEOF_LONG
//            + Bytes.SIZEOF_INT * 6;

    /**
     * Constructor used to write out a metadata record.
     * 
     * @param btree
     *            The btree.
     */
    protected BTreeMetadata(BTree btree) {
        
        this.addrRoot = btree.root.getIdentity();
        
        this.branchingFactor = btree.branchingFactor;
        
        this.height = btree.height;
        
        this.nnodes = btree.nnodes;
        
        this.nleaves = btree.nleaves;

        this.nentries = btree.nentries;
        
        this.valueSer = btree.nodeSer.valueSerializer;
        
        this.recordCompressor = btree.nodeSer.recordCompressor;

        this.useChecksum = btree.nodeSer.useChecksum;
        
        this.addrMetadata = write(btree.store);
        
    }
    
    /**
     * Write out the metadata record for the btree on the store and return the
     * {@link Addr address}.
     * 
     * @return The address of the metadata record.
     */
    protected long write(IRawStore2 store) {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {

            ObjectOutputStream oos = new ObjectOutputStream(baos);

            oos.writeObject(this);

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        ByteBuffer buf = ByteBuffer.wrap(baos.toByteArray());

        return store.write(buf);

    }

    /**
     * Read the metadata record from the store.
     * 
     * @param store
     *            the store.
     * @param addr
     *            the address of the metadata record.
     *            
     * @return the metadata record.
     */
    public static BTreeMetadata read(IRawStore2 store, long addr) {
        
        ByteBuffer buf = store.read(addr,null);
        
        ByteBufferInputStream bais = new ByteBufferInputStream(buf);
        
        try {

            ObjectInputStream ois = new ObjectInputStream(bais);

            BTreeMetadata metadata = (BTreeMetadata) ois.readObject();

            return metadata;
            
        } catch (Exception ex) {

            throw new RuntimeException(ex);
            
        }
        
    }
    
//    /**
//     * Write out the metadata record for the btree on the store and return the
//     * {@link Addr address}.
//     * 
//     * @return The address of the metadata record.
//     */
//    protected long write(IRawStore2 store) {
//
//        ByteBuffer buf = ByteBuffer.allocate(SIZEOF_METADATA);
//
//        buf.putLong(addrRoot);
//        buf.putInt(branchingFactor);
//        buf.putInt(height);
//        buf.putInt(nnodes);
//        buf.putInt(nleaves);
//        buf.putInt(nentries);
//        buf.putInt(keyType.intValue());
//        
//        buf.flip(); // prepare for writing.
//
//    }

//    /**
//     * Read the persistent metadata record for the btree.
//     * 
//     * @param addrMetadta
//     *            The address from which the metadata record will be read.
//     * 
//     * @return The persistent identifier of the root of the btree.
//     */
//    public BTreeMetadata(IRawStore2 store,long addrMetadata) {
//
//        assert store != null;
//        
//        assert addrMetadata != 0L;
//        
//        this.addrMetadata = addrMetadata;
//        
//        ByteBuffer buf = store.read(addrMetadata,null);
//        assert buf.position() == 0;
//        assert buf.limit() == Addr.getByteCount(addrMetadata);
//        
//        addrRoot = buf.getLong();
//        
//        branchingFactor = buf.getInt();
//        
//        height = buf.getInt();
//
//        nnodes = buf.getInt();
//        
//        nleaves = buf.getInt();
//        
//        nentries = buf.getInt();
//        
//        keyType = ArrayType.parseInt( buf.getInt() );
//        
//        assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;
//        assert height >= 0;
//        assert nnodes >= 0;
//        assert nleaves >= 0;
//        assert nentries >= 0;
//
//        BTree.log.info(toString());
//
//    }

    /**
     * A human readable representation of the metadata record.
     */
    public String toString() {
        
        StringBuilder sb = new StringBuilder();

        sb.append("addrRoot=" + Addr.toString(addrRoot));
        sb.append(", branchingFactor=" + branchingFactor);
        sb.append(", height=" + height);
        sb.append(", nnodes=" + nnodes);
        sb.append(", nleaves=" + nleaves);
        sb.append(", nentries=" + nentries);
        sb.append(", addrMetadata=" + Addr.toString(addrMetadata));
        
        return sb.toString();
        
    }
    
}
