/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Dec 5, 2006
 */

package com.bigdata.objndx;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import com.bigdata.journal.Bytes;
import com.bigdata.journal.IRawStore;
import com.bigdata.journal.Journal;

/**
 * Class supports a post-order construction of a "perfect" b+tree given sorted
 * records. There are two main use cases:
 * <ol>
 * <li>Evicting a key range of an index into an optimized on-disk index. In
 * this case, the input is a btree that is ideally backed by a fully buffered
 * {@link IRawStore} so that no random reads are required.</li>
 * <li>Merging index segments. In this case, the input is typically records
 * emerging from a merge-sort. There are two distinct cases here. In one, we
 * simply have raw records that are being merged into an index. This might occur
 * when merging two key ranges or when external data are being loaded. In the
 * other case we are processing two timestamped versions of an overlapping key
 * range. In this case, the more recent version may have "delete" markers
 * indicating that a key present in an older version has been deleted in the
 * newer version. Also, key-value entries in the newer version replaced (rather
 * than are merged with) key-value entries in the older version. If an entry
 * history policy is defined, then it must be applied here to cause key-value
 * whose retention is no longer required by that policy to be dropped.</li>
 * </ol>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see "Post-order B-Tree Construction" by Lawerence West, ACM 1992. Note that
 *      West's algorithm is for a b-tree (values are stored on internal nodes as
 *      well as leaves), not a b+-tree (values are stored only on the leaves).
 *      Our implementation is therefore an adaptation.
 * 
 * @see "Batch-Construction of B+-Trees" by Kim and Won, ACM 2001. The approach
 *      outlined by Kim and Won is designed for B+-Trees, but it appears to be
 *      less efficient on first glance.
 * 
 * FIXME Implement the post-order builder. Support each of the use cases,
 * including external sorting. Write a package to support lookups on the
 * generated perfect indices, including buffering the nodes in memory and
 * optionally buffering the leaves as well. To what extent can we reuse the
 * btree package for these read-only indices and just swap out the
 * {@link IRawStore} implementation?
 */
public class PostOrderBuilder {

    /**
     * @todo consider the file mode and buffering. We should at least buffer
     *       several pages of data per write and can experiment with writing
     *       through (vs caching writes in the OS layer). The file does not need
     *       to be "live" until it is completely written, so there is no need to
     *       update file metadata until the end of the build process.
     */
    final String mode = "rw"; // also rws or rwd
    
    /**
     * <p>
     * Builds an index segment on the disk from a {@link BTree}. The index
     * segment will be written on the identified file, which must not exist. The
     * caller is responsible for updating the metadata required to locate the
     * index segment.
     * </p>
     * <p>
     * The store on which the btree exists should be read-only, e.g., a frozen
     * {@link Journal}. The typical scenario is that a {@link Journal} is
     * frozen when it overflows and a new direct buffer and backing file are
     * opened to absorb writes. In this scenario frozen journal is still fully
     * buffered, which means that the index build will perform zero IOs when
     * reading from the source {@link BTree}. Once all indices on the frozen
     * journal have been externalized as {@link IndexSegment}s the frozen
     * journal can be discarded.
     * </p>
     * <p>
     * With a branching factor of 4096 a tree of height 2 (three levels) could
     * address 68,719,476,736 entries - well beyond what we want in a given
     * index segment! Well before that the index segment should be split into
     * multiple files. The split point should be determined by the size of the
     * serialized leaves and nodes, e.g., the amount of data on disk required by
     * the index segment and the amount of memory required to fully buffer the
     * index nodes. While the size of a serialized node can be estimated easily,
     * the size of a serialized leaf depends on the kinds of values stored in
     * that index.  The actual sizes are recorded in the {@link IndexSegmentMetadata} record
     * in the header of the {@link IndexSegment}.
     * </p>
     * 
     * @param outFile
     *            The file on which the index segment is written.
     * @param tmpDir
     *            The temporary directory in which the index nodes are buffered
     *            during the build (optional - the default temporary directory
     *            is used if this is <code>null</code>).
     * @param btree
     *            The btree.
     * @param m
     *            The branching factor for the generated tree. This can be
     *            choosen with an eye to minimizing the height of the generated
     *            tree.
     * 
     * @throws IOException
     */
    public PostOrderBuilder(File outFile, File tmpDir, BTree btree, int m)
            throws IOException {

        /*
         * The buffer used to serialize individual nodes and leaves before they
         * are written onto the file.
         * 
         * @todo The capacity of this buffer is a SWAG. It is too large for most
         * purposes but it is unlikely to be too small for most purposes. If you
         * see a buffer overflow exception then you may have extremely long keys
         * and/or very large values. The BTree class has the same problem with
         * the buffer that it uses to serialize nodes and leaves, but its nodes
         * and leaves are smaller since it uses a smaller branching factor.
         */
        final ByteBuffer buf = ByteBuffer.allocateDirect(1*Bytes.megabyte32);

        /*
         * Buffer for compressed records.  The capacity is a SWAG.
         */
        final ByteBuffer cbuf = ByteBuffer.allocateDirect(Bytes.megabyte32/2);
        
        /*
         * The minimum #of values that may be placed into non-root leaf (and
         * also the minimum #of children that may be placed into a non-root
         * node). (the minimum capacity).
         */
        final int m2 = (m+1)/2; 
        
        /*
         * The #of entries in the btree.
         */
        final int nentries = btree.nentries;
        assert nentries>0;
        
        /*
         * The #of leaves that will exist in the output tree. When nleaves == 1
         * the output tree will consist of a root leaf. In this case we do not
         * open a temporary file for the nodes since there will not be any.
         */
        final int nleaves = (int)Math.ceil((double)nentries / (double)m); 
        
        /*
         * The height of the output tree. This is used to maintain a stack of
         * the nodes that we are creating as we build the leaves under those
         * nodes. The space requirement is only height nodes and one leaf.
         * 
         * Note: In order for the nodes to be written in a contiguous block we
         * either have to write them onto a temporary file and then copy them
         * into place after the last leaf has been processed or we have to
         * buffer them in memory. The code currently uses a temporary file for
         * this purpose.
         */
        final int height = getMinimumHeight(m,nleaves);

        /*
         * We want to fill up every leaf, but we have to make sure that the last
         * leaf is not under capacity. To that end, we calculate the #of entries
         * that would remain if we filled up n-1 leaves completely. If the #of
         * remaining entries is greater than or equal to the minimum capacity of
         * a leaf, then we have to adjust the allocation of entries such that
         * the last leaf is full. This is done by computing the shortage and
         * then writing one fewer entries into the first shortage leaves. Once
         * we have deferred enough entries we are guarenteed that the final leaf
         * will not be under capacity.
         */
        final int shortage; 
        {
        
            // #of entries remaining for the last leaf.
            int remaining = nentries - ((nleaves-1) * m);
            
            /*
             * if the #of entries remainin would put the leaf under capacity
             * then we compute the shortage.
             */
            shortage = remaining < m2 ? m2 - remaining : 0;

        }

        /*
         * The data type used for the keys in the btree.
         */
        final ArrayType keyType = btree.keyType;
        
        /*
         * setup for IO.
         */

        if (outFile.exists()) {
            throw new IllegalArgumentException("File exists: "
                    + outFile.getAbsoluteFile());
        }

        final File tmpFile = File.createTempFile("index", ".seg", tmpDir);

        RandomAccessFile out = null;

        RandomAccessFile tmp = null;
        
        /*
         * Compressor used for nodes and leaves.
         * 
         * @todo review choice of best speed vs best compression and possibly
         * elevate to a constuctor parameter.
         */
        final RecordCompressor compressor = new RecordCompressor();
        
        try {

            /*
             * Open the output channel and get an exclusive lock.
             */
            
            out = new RandomAccessFile(outFile, mode);
            
            final FileChannel outChannel = out.getChannel();
            
            if (outChannel.tryLock() == null) {
                
                throw new IOException("Could not lock file: "
                        + outFile.getAbsoluteFile());
                
            }

            /*
             * Open the temporary channel and get an exclusive lock. We only do
             * this if there will be at least one node written, i.e., the output
             * tree will consist of more than just a root leaf.
             */

            final FileChannel tmpChannel;
            
            if (nleaves > 1) {

                tmp = new RandomAccessFile(tmpFile, mode);

                tmpChannel = tmp.getChannel();

                if (tmpChannel.tryLock() == null) {

                    throw new IOException("Could not lock file: "
                            + tmpFile.getAbsoluteFile());

                }

            } else {
                
                /*
                 * Set to null iff temp file not opened because there are no
                 * nodes to write.
                 */

                tmpChannel = null;
                
            }

            /*
             * Skip over the metadata record, which we have to write once we
             * know how the leaves and nodes fit once serialized onto the file.
             */
            outChannel.position(IndexSegmentMetadata.SIZE);
            
            /*
             * Scan the btree leaves in key order writing out leaves onto the
             * index segment file with the new branching factor.
             * 
             * @todo we also have to track a stack of nodes that are being
             * written out concurrently on the temporary channel. Each time we
             * write out a leaf we place the separator key for that leaf into
             * its immediate parent node. If the parent becomes full then we
             * write out the node to the tmpChannel and insert the appropriate
             * separatorKey into its parent.
             * 
             * It is an error if the root becomes full.
             * 
             * The separator key for the parent is always a key in a leaf. In
             * fact it is the first key that goes into the next leaf. This means
             * that either we need to read the keys ahead or that we need to
             * fill in the separator key and evict the node behind. Note that we
             * need to evict the last node after we finish the last leaf. Note
             * that we could enter the next leaf address before we actually
             * write the leaf since it is simply the current position of the
             * outChannel. However, this does not generalize to nodes trivially
             * since we have potentially a stack of N nodes that are being
             * written out.  This implies that we defer adding the child to the
             * parent until the child has been written out.
             * 
             * Note that the root may be a leaf as a degenerate case.
             */

            final SimpleLeafData leaf = new SimpleLeafData(m,keyType);

            Iterator sourceLeaves = btree.leafIterator();

            long lastLeafOffset = -1L;
            int lastLeafSize = -1;
            long nextLeafOffset = outChannel.position();

            // track the maximum length of any serialized node or leaf.
            int maxNodeOrLeafLength = 0;

            /*
             * The #of entries consumed in the current source leaf. when this
             * reaches the #of keys in the source leaf then we have to read the
             * next source leaf from the source leaf iterator.
             */
            Leaf sourceLeaf = (Leaf)sourceLeaves.next();
            
            int nconsumed = 0;
            
            int nsourceKeys = sourceLeaf.getKeyCount();
            
            int nnodesWritten = 0;
            int nleavesWritten = 0;
            
            for (int i = 0; i < nleaves; i++) {

                leaf.nkeys = 0;

                /*
                 * @todo the shortage can require that we short early leaves by
                 * more than one key.  This shows up in our test examples when
                 * m := 8 requiring us to short the first leaf by 2 keys so that
                 * the 2nd leaf will not be under capacity.
                 */
                int limit = (shortage > 0 && i < shortage ? m - 1 : m);

                /*
                 * fill in defined keys and values for this leaf.
                 */
                for (int j = 0; j < limit; j++) {

                    if( nconsumed == nsourceKeys ) {
                    
                        // Read another leaf from the source tree.
                        
                        sourceLeaf = (Leaf)sourceLeaves.next();
                        
                        nconsumed = 0;
                        
                        nsourceKeys = sourceLeaf.getKeyCount();
                                                
                    }
                    
                    leaf.copyKey(leaf.nkeys, sourceLeaf, nconsumed);
                    
                    leaf.vals[j] = sourceLeaf.values[nconsumed];
                    
                    leaf.nkeys++;
                    
                    nconsumed++;

                }
                
//                /*
//                 * Clear out remaining elements of arrays.
//                 * 
//                 * @todo This is not required the way that things are setup
//                 * since only the first few leaves can be under capacity, and
//                 * then will always be under capacity by the same amount. Once
//                 * the shortage has been made up the rest of the leaves will be
//                 * fully populated.
//                 */
//                for (int j = limit; j < m; j++) {
//                    
//                    keys[j] = null;
//                    
//                    vals[j] = null;
//                    
////                    System.err.println("Clearing unused element: index="+j);
//                    
//                }

                // verify leaf not under capacity
                assert leaf.nkeys >= m2;

                /*
                 * Serialize, compress, and write the leaf onto the output
                 * channel.
                 * 
                 * @todo write prior; compute next from offset+size during
                 * IndexSegment scans. Basically, we can only record one of the
                 * references in the leaf data structure since we do not know
                 * its size until it has been compressed, at which point we can
                 * no longer set the size field on the record. However, we do
                 * know that offset of the next leaf simply from the reference
                 * of the current leaf, which encodes {offset,size}. The other
                 * way is to write out the next reference on the leaf after it
                 * has been compressed. Review {@link NodeSerializer} with
                 * regard to this issue again.
                 */

                // clear the serialization buffer.
                buf.clear();

                // serialize the leaf onto the buffer.
                btree.nodeSer.putLeaf(buf, leaf);
                
                // flip for reading.
                buf.flip();
                
                // clear compression buffer.
                cbuf.clear();
                
                // setup writer onto compression buffer.
                ByteBufferOutputStream bbos = new ByteBufferOutputStream(cbuf);
                
                // compress the serialized leaf.
                compressor.compress(buf, bbos);
                
                // flush the compression buffer.
                bbos.flush();
                
                bbos.close();
                
                // flip the compressed buffer to prepare for writing.
                cbuf.flip();

                // write leaf on the channel.
                final int nbytes = outChannel.write(cbuf);
                
                assert nbytes == cbuf.limit();
                
                if( nbytes > maxNodeOrLeafLength ) { 
                 
                    // track the largest node or leaf written.
                    maxNodeOrLeafLength = nbytes;
                    
                }

                // the offset where we just wrote the last leaf.
                lastLeafOffset = nextLeafOffset;

                // the size of the compressed leaf that we just wrote out.
                lastLeafSize = nbytes;

                // the offset where we will write the next leaf.
                nextLeafOffset = outChannel.position();

                // the #of leaves written so far.
                nleavesWritten++;
                
                System.err.print("."); // wrote a leaf.

            }

            final long offsetLeaves = IndexSegmentMetadata.SIZE;
            final long offsetNodes;
            final long addrRoot;
            
            if (tmpChannel != null) {

                /*
                 * Direct copy the node index from the temporary file into the
                 * output file and clear the reference to the temporary file.
                 * The temporary file itself will be deleted as a post-condition
                 * of the index build operation.
                 */
                
                offsetNodes = outChannel.position();
                
                outChannel.transferFrom(tmpChannel, 0L, tmp.length());
                
                tmp.close(); // also releases the lock.
                
                tmp = null;

                /*
                 * FIXME The addrRoot should be computed from the offset on the
                 * tmp channel at which we wrote the root node plus the offset
                 * on the output channel to which we transferred the contents of
                 * the temporary channel.
                 */
                addrRoot = 0l;
                
            } else {

                /*
                 * The tree consists of just a root leaf.
                 */
                
                offsetNodes = -1L; // @todo -1L or 0L?

                addrRoot = Addr.toLong(lastLeafSize, (int)lastLeafOffset);
                
            }

            /*
             * Seek to the start of the file and write out the metadata record.
             */
            {

                // timestamp for the index segment.
                final long now = System.currentTimeMillis();
                
                // @todo name of the index segment - drop this field?
                final String name = "<no name>";
                
                outChannel.position(0);
                
                new IndexSegmentMetadata(m, height, keyType, nleaves, nnodesWritten,
                        nentries, maxNodeOrLeafLength, offsetLeaves,
                        offsetNodes, addrRoot, out.length(), now, name)
                        .write(out);
                
            }
                        
            /*
             * Flush this channel to disk, close the channel, and clear the
             * reference. This also releases our lock. We are done and the index
             * segment is ready for use.
             */
            outChannel.force(true);
            out.close(); // also releases the lock.
            out = null;

        } catch (Throwable ex) {

            /*
             * make sure that the output file is deleted unless it was
             * successfully processed.
             */
            if (out != null) {
                try {
                    out.close();
                } catch (Throwable t) {
                }
            }
            outFile.delete();
            throw new RuntimeException(ex);

        }
        
        finally {

            /*
             * make sure that the temporary file gets deleted regardless.
             */
            if (tmp != null) {
                try {tmp.close();}
                catch (Throwable t) {}
            }
            tmpFile.delete();

        }
        
    }

    /**
     * A class that can be used to (de-)serialize the data for a leaf without
     * any of the logic for operations on the leaf.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class SimpleLeafData implements ILeafData {

        int m;
        int nkeys;
        ArrayType keyType;
        Object keys;
        Object[] vals;
        
        public SimpleLeafData(int m,ArrayType keyType) {
            this.m = m;
            this.keyType = keyType;
            this.nkeys = 0;
            this.keys = ArrayType.alloc(keyType, m);
            this.vals = new Object[m];
        }
        
        public int getValueCount() {
            return nkeys;
        }

        public Object[] getValues() {
            return vals;
        }

        public int getBranchingFactor() {
            return m;
        }

        public int getKeyCount() {
            return nkeys;
        }

        public ArrayType getKeyType() {
            return keyType;
        }

        public Object getKeys() {
            return keys;
        }

        public boolean isLeaf() {
            return true;
        }
    
        /**
         * Copy a key from the source node into this node. This method does not
         * modify the source node. This method does not update the #of keys in
         * this node. This method has the substantial advantage that primitive
         * keys are not boxed and unboxed solely to perform the cop.
         * 
         * @param dstpos
         *            The index position to which the key will be copied on this
         *            node.
         * @param src
         *            The source node from which the key will be copied.
         * @param srcpos
         *            The index position from which the key will be copied.
         */
        final protected void copyKey(int dstpos,AbstractNode src,int srcpos) {
              switch(keyType) {
              case BYTE: ((byte[])keys)[dstpos] = ((byte[])src.keys)[srcpos]; break;
              case SHORT: ((short[])keys)[dstpos] = ((short[])src.keys)[srcpos]; break;
              case CHAR: ((char[])keys)[dstpos] = ((char[])src.keys)[srcpos]; break;
              case INT: ((int[])keys)[dstpos] = ((int[])src.keys)[srcpos]; break;
              case LONG: ((long[])keys)[dstpos] = ((long[])src.keys)[srcpos]; break;
              case FLOAT: ((float[])keys)[dstpos] = ((float[])src.keys)[srcpos]; break;
              case DOUBLE: ((double[])keys)[dstpos] = ((double[])src.keys)[srcpos]; break;
              case OBJECT: ((Object[])keys)[dstpos] = ((Object[])src.keys)[srcpos]; break;
              default: throw new UnsupportedOperationException();
              }     
        }
       
    }
    
    /**
     * Chooses the minimum height for a tree having a specified branching factor
     * and a specified #of leaves.
     * 
     * @param m
     *            The branching factor.
     * @param nleaves
     *            The #of leaves that must be addressable by the tree.
     */
    public static int getMinimumHeight(int m, int nleaves) {
        
        final int maxHeight = 10;
        
        for (int h = 0; h <= maxHeight; h++) {
        
            /*
             * The maximum #of leaves addressable by a tree of height h and the
             * given branching factor.
             * 
             * Note: Java guarentees that Math.pow(int,int) produces the exact
             * result iff that result can be represented as an integer. This
             * useful feature lets us avoid having to deal with precision issues
             * or write our own integer version of pow (computing m*m h times).
             */
            final double d = (double)Math.pow(m,h);
            
            if( d >= nleaves ) {
            
                /*
                 * h is the smallest height tree of the specified branching
                 * factor m capable of addressing the specified #of leaves.
                 */
                return h;
                
            }
            
        }
        
        throw new UnsupportedOperationException(
                "Can not build a tree for that many leaves: m=" + m
                        + ", nleaves=" + nleaves + ", maxHeight=" + maxHeight);
    }
   
//    /**
//     * Choose the height and branching factor (aka order) of the generated tree.
//     * This choice is made by choosing a height and order for the tree such
//     * that:
//     * 
//     * <pre>
//     *   2(d + 1)&circ;(h-l) - 1 &lt;= N &lt;= (2d + 1)&circ;h - l
//     * </pre>
//     * 
//     * where
//     * <ul>
//     * <li>d := the minimum #of keys in a node of the generated tree (m/2).</li>
//     * <li>h := the height of the generated tree (origin one (1)).</li>
//     * <li>N := the #of entries (rows of data)</li>
//     * </ul>
//     * 
//     * This can be restated as:
//     * 
//     * <pre>
//     *  
//     *   2(m/2 + 1)&circ;h - 1 &lt;= N &lt;= (m + 1)&circ;(h+1) - l
//     * </pre>
//     * 
//     * where
//     * <ul>
//     * <li>m := the branching factor of the generated tree.</li>
//     * <li>h := the height of the generated tree (origin zero(0)).</li>
//     * <li>N := the #of entries (rows of data)</li>
//     * </ul>
//     * 
//     * @todo The #of entries to be placed into the generated perfect index must
//     *       be unchanging during this process. This suggests that it is best to
//     *       freeze the journal, opening a new journal for continued writes, and
//     *       then evict all index ranges in the frozen journal into perfect
//     *       indices.
//     * 
//     * @todo Note that this routine is limited to an index subrange with no more
//     *       entries than can be represented in an int32 signed value.
//     */
//    protected void phase1(int nentries) {
//
//        /*
//         * @todo solve for the desired height, where h is the #of non-leaf nodes
//         * and is zero (0) if the tree consists of only a root leaf.  We want to
//         * minimize the height as long as the node/leaf size is not too great.
//         */
//        int h = 0;
//        
//        /*
//         * @todo solve for the desired branching factor (#of children for a node
//         * or the #of values for a leaf. The #of keys for a node is m-1. The
//         * branching factor has to be bounded since there is some branching
//         * factor at which any btree fits into the root leaf. Therefore an
//         * allowable upper range for m should be an input, e.g., m = 4096. This
//         * can be choosen with an eye to the size of a leaf on the disk since we
//         * plan to have the index nodes in memory but to read the leaves from
//         * disk. Since the size of a leaf varies by the key and value types this
//         * can either be a SWAG, e.g., 1024 or 4096, or it can be computed based
//         * on the actual average size of the leaves as written onto the store.
//         * 
//         * Note that leaves in the journal index ranges will typically be much
//         * smaller since the journal uses smaller branching factors to minimize
//         * the cost of insert and delete operations on an index.
//         * 
//         * In order to minimize IO we probably want to write the leaves onto the
//         * output file as we go (blocking them in a page buffer of at least 32K)
//         * and generate the index nodes in a big old buffer (since we do not
//         * know their serialized size in advance) and then write them out all at
//         * once.
//         */
//        int m = 4;
//        
//        /*
//         * The minimum #of entries that will fit in a btree given (m,h).
//         */
//        int min = (int) Math.pow(m+1, h)-1;
//        
//        /*
//         * The maximum #of entries that will fit in a btree given (m,h).
//         */
//        int max = (int) Math.pow(m, h+1);
//        
//        if( min <= nentries && nentries<= max ) {
//            
//            /*
//             * A btree may be constructed for this many entries with height := h
//             * and branching factor := m. There will be many such solutions and
//             * one needs to be choosen before building the tree.
//             * 
//             * To build the tree with the fewest possible nodes, select the
//             * combination of (m,h) with the smallest value of h. This is what
//             * we are looking for since there will be no further inserted into
//             * the generated index (it is read-only for our purposes). Other
//             * applications might want to "dial-in" some sparseness to the index
//             * by choosing a larger value of h so that more inserts could be
//             * absorbed without causing nodes split.  This is not an issue for
//             * us since we never insert into the generated index file.
//             */
//            
//        }
//
//        /*
//         * compute per-level values given (h,m).
//         * 
//         * Note: our h is h-1 for West (we count the root as h == 0 and West
//         * counts it as h == 1).  While West defines the height in terms of
//         * the #of nodes in a path to a leaf from the root, West is working
//         * with a b-tree and there is no distinction between nodes and leaves.
//         */
//        int r[] = new int[h-1];
//        int n[] = new int[h-1];
//        
//    }
//
//    /**
//     *
//     */
//    protected void phase2() {
//
//    }

}
