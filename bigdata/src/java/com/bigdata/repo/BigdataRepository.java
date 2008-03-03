/*
 * Created on Jan 17, 2008
 */
package com.bigdata.repo;

import java.io.DataInput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.ILinearList;
import com.bigdata.btree.IOverflowHandler;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.IIndexProcedure.ISimpleIndexProcedure;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.service.DataServiceIndex;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.sparse.ITPS;
import com.bigdata.sparse.ITPV;
import com.bigdata.sparse.KeyType;
import com.bigdata.sparse.Schema;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.sparse.ValueType.AutoIncCounter;
import com.bigdata.text.FullTextIndex;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * A distributed file system with extensible metadata and atomic append
 * implemented using the bigdata scale-out architecture. Files have a client
 * assigned identifier, which is a Unicode string. The file identifier MAY be
 * structured so as to look like a hierarchical file system using any desired
 * convention. Files are versioned and historical versions MAY be accessed until
 * the next compacting merge discards their data. File data is stored in large
 * {@link #BLOCK_SIZE} blocks. Partial and even empty blocks are allowed and
 * only the data written will be stored. <code>2^63-1</code> distinct blocks
 * may be written per file version, making the maximum possible file size
 * <code>536,870,912</code> exabytes. Files may be used as queues, in which
 * case blocks containing new records are atomically appended while a map/reduce
 * style master consumes the head block of the file.
 * <p>
 * Efficient method are offered for streaming and block oriented IO. All block
 * read and write operations are atomic, including block append. Files may be
 * easily written such that records never cross a block boundary by the
 * expediency of flushing the output stream if a record would overflow the
 * current block. A flush forces the atomic write of a partial block. Partial
 * blocks are stored efficiently - only the bytes actually written are stored.
 * Blocks are large enough that most applications can safely store a large
 * number of logical records in each block. Files comprised of application
 * defined logical records organized into a sequence of blocks are well-suited
 * to map/reduce processing. They may be efficiently split at block boundaries
 * and references to the blocks distributed to clients. Likewise, reduce clients
 * can aggregate data into large files suitable for further map/reduce
 * processing.
 * <p>
 * The distributed file system uses two scale-out indices to support ACID
 * operations on file metadata and atomic file append. These ACID guarentees
 * arise from the use of unisolated operations on the respective indices and
 * therefore apply only to the individual file metadata or file block
 * operations. In particular, file metadata read and write are atomic and all
 * individual file block IO (read, write, and append) operations are atomic.
 * Atomicity is NOT guarenteed when performing more than a single file block IO
 * operation, e.g., multiple appends MIGHT NOT write sequential blocks since
 * other block operations could have intervened.
 * <p>
 * The content length of the file is not stored as file metadata. Instead it MAY
 * be estimated by a range count of the index entries spanned by the file's
 * data. The exact file size may be readily determined when reading small files
 * by the expediency of sucking the entire file into a buffer - all reads are at
 * least one block. Streaming processing is advised in all cases when handling
 * large files, including when the file is to be delivered via HTTP.
 * <p>
 * The {@link #getMetadataIndex() metadata index} uses a {@link SparseRowStore}
 * design, similar to Google's bigtable or Hadoop's HBase. All updates to file
 * version metadata are atomic. The primary key in the metadata index for every
 * file is its {@link MetadataSchema#ID}. In addition, each version of a file
 * has a distinct {@link MetadataSchema#VERSION} property. File creation time,
 * version creation time, and file version metadata update timestamps may be
 * recovered from the timestamps associated with the properties in the metadata
 * index. The use of the {@link MetadataSchema#CONTENT_TYPE} and
 * {@link MetadataSchema#CONTENT_ENCODING} properties is enforced by the
 * high-level {@link Document} interface. Applications are free to define
 * additional properties.
 * <p>
 * Each time a file is created a new version number is assigned. The data index
 * uses the {@link MetadataSchema#ID} as the first field in a compound key. The
 * second field is the {@link MetadataSchema#VERSION} - a 32-bit integer. The
 * remainder of the key is a 64-bit signed block identifier (2^63-1 distinct
 * block identifiers). The block identifiers are strictly monotonic (e.g., up
 * one) and their sequence orders the blocks into the logical byte order of the
 * file.
 * <p>
 * Operations that create a new file actually create a new file version. The old
 * file version will eventually be garbage collected depending on the policy in
 * effect for compacting merges. Likewise, operations that delete a file simply
 * mark the metadata for the file version as deleted and the file version will
 * be eventually reclaimed. The high-level {@link #update(Document)} operation
 * in fact simply creates a new file version.
 * <p>
 * <h2>Use cases</h2>
 * <p>
 * Use case: A REST-ful repository. Documents may be stored, updated, read,
 * deleted, and searched using a full text index.
 * <p>
 * Use case: A map/reduce master reads document metadata using an index scan. It
 * examines the data index's {@link MetadataIndex} (that is, the index that
 * knows where each partition of the scale-out data index is stored) and
 * determines which map clients are going to be "close" to each document and
 * then hands off the document to one of those map clients.
 * <p>
 * Use case: The same as the use case above, but large files are being processed
 * and there is a requirement to "break" the files into splits and hand off the
 * splits. This can be achieved by estimating the file system using a range
 * count and multiplying through by the block size. Blocks may be handed off to
 * the clients in parallel (of course, clients need to deal with the hassle of
 * processing files where records will cross split boundaries unless they always
 * pad out with unused bytes to the next {@link #BLOCK_SIZE} boundary).
 * <p>
 * Use case: A reduce client wants to write a very large files so it creates a
 * metadata record for the file and then does a series of atomic appears to the
 * file. The file may grow arbitrarily large. Clients may begin to read from the
 * file as soon as the first block has been flushed.
 * <p>
 * Use case: Queues MAY be built from the operations to atomically read or
 * delete the first block for the file version. The "design pattern" is to have
 * clients append blocks to the file version, taking care that logical rows
 * never cross a block boundary (e.g., by flushing partial blocks). A master
 * then reads the head block from the file version, distributing the logical
 * records therein to consumers and providing fail safe processing in case
 * consumers die or take too long. Once all records for the head block have been
 * processed the master simply deletes the head block. This "pattern" is quite
 * similar to map/reduce and, like map/reduce, requires that the consumer
 * operations may be safely re-run.
 * <p>
 * Use case: File replication, retention of deleted versions, and media indexing
 * are administered by creating "zones" comprising one or more index partitions
 * with a shared file identifier prefix, e.g., /tmp or /highly-available, or
 * /deployment-text-index. All files in a given zone share the same policy for
 * file replication, compacting merges (determining when a deleted or even a
 * non-deleted file version will be discarded), and media indexing.
 * <p>
 * Use case: File rename is NOT a cheap operation. It essentially creates a new
 * file version with the desired name and copies the data from the old file
 * version to the new file version. Finally the old file version is "deleted".
 * This approach is necessary since files may moved from one "zone" to another
 * and since the file data must reside on the index partition(s) identified by
 * its file version.
 * 
 * @todo it should be possible to run against a {@link DataServiceIndex} and not
 *       only against an {@link IBigdataFederation}. I will have to modify the
 *       setup to support both.
 * 
 * @todo journal size and index segment sizes should be at least 500M when 64M
 *       blocks being stored - perhaps raise that threshold throughout?
 * 
 * @todo implement "zones" and their various policies (replication, retention,
 *       and media indexing). access control could also be part of the zones.
 * 
 * @todo should compression be applied? applications are obviously free to apply
 *       their own compression, but it could be convienent to stored compressed
 *       blocks. the caller could specify the compression method on a per block
 *       basis (we don't want to lookup the file metadata for this). the
 *       compression method would be written into a block header. blocks can
 *       always be decompressed by examining the header.
 * 
 * @todo there should be some constraints on the file identifier but it general
 *       it represents a client determined absolute file path name. It is
 *       certainly possible to use a flat file namespace, but you can just as
 *       readily use a hierarchical one. Unicode characters are supported in the
 *       file identifiers.
 * 
 * @todo do we need a global lock mechanism to prevent concurrent high-level
 *       create/update/delete of the same file? a distributed lease-based lock
 *       system derived from jini or built ourselves? Can this be supported with
 *       the historical and not yet purged timestamped metadata for the file?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataRepository implements ContentRepository {

    protected static Logger log = Logger.getLogger(BigdataRepository.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final public static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final public static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * Configuration options.
     * 
     * @todo block size as config option?
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends com.bigdata.journal.Options, KeyBuilder.Options {

    }

    /**
     * The size of a file block (default is 64M). Block identifiers are 64-bit
     * signed integers. With 64M file blocks the maximum file length is
     * <code>2^63 - 1 </code> blocks ( 536,870,912 Exabytes).
     */
    protected final int BLOCK_SIZE;

    /**
     * The size of a file block (64M). Block identifiers are 64-bit signed
     * integers. The maximum file length is <code>2^63 - 1 </code> blocks (
     * 536,870,912 Exabytes).
     */
    public final int getBlockSize() {
        
        return BLOCK_SIZE;
        
    }
    
    /**
     * The maximum block identifier that can be assigned to a file version.
     * <p>
     * Note: This is limited to {@value Long#MAX_VALUE}-1 so that we can always
     * form the key greater than any valid key for a file version. This is
     * required by the atomic append logic when it seeks the next block
     * identifier. See {@link AtomicBlockAppendProc}.
     */
    protected static final long MAX_BLOCK = Long.MAX_VALUE - 1;
    
    /**
     * The connection to the bigdata federation.
     */
    private IBigdataFederation fed;
    
    /**
     * The name of the scale-out index in which the metadata are stored. This is
     * a {@link SparseRowStore} governed by the {@link #metadataSchema}.
     */
    private static final String METADATA_NAME = BigdataRepository.class.getSimpleName()+"#metadata";
    
    /**
     * The name of the scale-out index in which the data are stored. The entries
     * in this index are a series of blocks for a file. Blocks are
     * {@link #BLOCK_SIZE} bytes each and are assigned monotonically increasing
     * block numbers by the atomic append operation. The final block may be
     * smaller (there is no need to pad out the data with nulls). The keys are
     * formed from two fields - a field containing the content identifier
     * followed by an integer field containing the sequential block number. A
     * range scan with a fromKey of the file identifier and a toKey computed
     * using the successor of the file identifier will naturally visit all
     * blocks in a file in sequence.
     */
    private static final String DATA_NAME = BigdataRepository.class.getSimpleName()+"#data";
    
    /**
     * A {@link ThreadLocal} variable providing access to thread-specific
     * instances of a configured {@link IKeyBuilder}.
     * <p>
     * Note: this {@link ThreadLocal} is not static since we need configuration
     * properties from the constructor - those properties can be different for
     * different {@link Journal}s on the same machine.
     */
    private ThreadLocal<IKeyBuilder> threadLocalKeyBuilder = new ThreadLocal<IKeyBuilder>() {

        protected synchronized IKeyBuilder initialValue() {

            return KeyBuilder.newUnicodeInstance(properties);

        }

    };

    /**
     * Return a {@link ThreadLocal} {@link IKeyBuilder} instance configured
     * using the properties specified to the journal constructor.
     */
    public IKeyBuilder getKeyBuilder() {
        
        return threadLocalKeyBuilder.get();
        
    }
        
    /**
     * The schema for metadata about file versions stored in the repository.
     * Some well known properties are always defined, but any property may be
     * stored - ideally within their own namespace!
     * <p>
     * Note: File version creation time and update times are available using the
     * {@link SparseRowStore}, which stores and reports the timestamp for each
     * property value. Convenience methods are available on
     * {@link RepositoryDocumentImpl} to report those timestamps. Timestamps for
     * file blocks can NOT be obtained.
     * <p>
     * Note: A content length property was deliberately NOT defined. The design
     * is geared towards very large file and asynchronous read/write of file
     * blocks. The length of short files may be readily computed by the
     * expediency of sucking their contents into a buffer. Large files should
     * always be processed using a stream-oriented technique or distributed to
     * concurrent clients in block sized pieces.
     * 
     * @todo other obvious metadata would include the user identifier associated
     *       with each update request.
     */
    public static class MetadataSchema extends Schema {
        
        /**
         * 
         */
        private static final long serialVersionUID = 2908749650061841935L;

        /**
         * The content identifer is an arbitrary Unicode {@link String} whose
         * value may be defined by the client.
         */
        public static transient final String ID = "Id";
        
        /**
         * The MIME type associated with the content (the same semantics as the
         * HTTP <code>Content-Type</code> header).
         */
        public static transient final String CONTENT_TYPE = "ContentType";

        /**
         * The encoding, if any, used to convert the byte[] content to
         * characters.
         * <p>
         * Note: This is typically deduced from an analysis of the MIME Type in
         * <code>Content-Type</code> header and at times the leading bytes of
         * the response body itself.
         */
        public static transient final String CONTENT_ENCODING = "ContentEncoding";

        /**
         * The file version number. Together the file {@link #ID} and the file
         * {@link #VERSION} form the primary key for the data index.
         */
        public static transient final String VERSION = "Version";
        
        public MetadataSchema() {
            
            super("metadata", ID, KeyType.Unicode);
            
        }
        
    }

    public static final MetadataSchema metadataSchema = new MetadataSchema();
    
    /**
     * A copy of the {@link Properties} specified to the ctor.
     */
    private final Properties properties;
    
    private SparseRowStore metadataIndex;
    
    private IIndex dataIndex;
    
    // @todo unique or not?
    final protected long AUTO_TIMESTAMP = SparseRowStore.AUTO_TIMESTAMP_UNIQUE;
        
    protected static void assertString(Map<String, Object> properties, String name) {

        Object val = properties.get(name);

        if (val == null)
            throw new IllegalArgumentException(name + " is null");

        if (!(val instanceof String))
            throw new IllegalArgumentException(name + " must be String");

    }
    
    protected static void assertLong(Map<String, Object> properties, String name) {

        Object val = properties.get(name);

        if (val == null)
            throw new IllegalArgumentException(name + " is null");

        if (!(val instanceof Long))
            throw new IllegalArgumentException(name + " must be Long");

    }

    /**
     * @param client
     *            The client.
     * @param properties
     *            See {@link Options}.
     */
    public BigdataRepository(IBigdataFederation fed, Properties properties) {
        
        this.fed = fed;
        
        // clone the properties to keep them immutable.
        this.properties = (Properties) properties.clone();
     
        /*
         * FIXME The {@link #BLOCK_SIZE} is set to 4M-1 bytes right now. In
         * order to increase the block size to 64M a different value would have
         * to be specified for the
         * {@link WormAddressManager#DEFAULT_OFFSET_BITS}. 4M is [0:4,194,303]
         * (42 offset bits) while 64M is 67,108,864 bytes (38 offset bits).
         */
        BLOCK_SIZE = WormAddressManager
                .getMaxByteCount(WormAddressManager.DEFAULT_OFFSET_BITS) - 1;
        
    }

    /**
     * An object wrapping the properties provided to the ctor.
     */
    protected Properties getProperties() {
        
        return new Properties(properties);
        
    }

    /**
     * The index in which the file metadata is stored (the index must exist).
     */
    public SparseRowStore getMetadataIndex() {

        if (metadataIndex == null) {

            IIndex ndx = fed.getIndex(METADATA_NAME,ITx.UNISOLATED);

            metadataIndex = new SparseRowStore(ndx);
            
        }

        return metadataIndex;

    }

    /**
     * The index in which the file data is stored (the index must exist).
     * 
     * @todo pass in the timestamp?
     */
    public IIndex getDataIndex() {

        if (dataIndex == null) {

            dataIndex = fed.getIndex(DATA_NAME,ITx.UNISOLATED);

        }

        return dataIndex;

    }
    
    /**
     * Registers the scale-out indices.
     * 
     * @todo make this an atomic operation.
     * 
     * @todo parameterize the namespace for the file metadata and file data
     *       indices so that you can have more than one repository instance.
     */
    public void registerIndices() {

        final int branchingFactor = Integer.parseInt(properties.getProperty(
                Options.BRANCHING_FACTOR, Options.DEFAULT_BRANCHING_FACTOR));

        // setup metadata index.
        {

            /*
             * FIXME specify an appropriate split handler (keeps the row
             * together). This is a hard requirement. The atomic read/update
             * guarentee depends on this.
             */

            IndexMetadata md = new IndexMetadata(METADATA_NAME, UUID
                    .randomUUID());

            md.setBranchingFactor(branchingFactor);

            md.setDeleteMarkers(true);

            fed.registerIndex(md);

            IIndex ndx = fed.getIndex(METADATA_NAME,ITx.UNISOLATED);

            metadataIndex = new SparseRowStore(ndx);

        }

        // setup data index.
        {

            /*
             * @todo specify split handler that tends to keep the blocks for a
             * file together (soft requirement).
             */

            IndexMetadata md = new IndexMetadata(DATA_NAME, UUID.randomUUID());

            md.setBranchingFactor(branchingFactor);

            md.setDeleteMarkers(true);
            
            /*
             * @todo unit tests for correct copying of blobs during overflow.
             * See {@link IOverflowHandler}.
             */
            md.setOverflowHandler(new BlobOverflowHandler());
            
            // register the index.
            fed.registerIndex(md);

            dataIndex = fed.getIndex(DATA_NAME,ITx.UNISOLATED);

        }

    }

    /**
     * NOP - the caller should disconnect their client from the federation when
     * they are no longer using that connection.
     */
    public void close() {
        
    }

    /**
     * Creates a new file version from the specified metadata. The new file
     * version will not have any blocks. You can use either stream-oriented or
     * block oriented IO to write data on the newly created file version.
     * 
     * @param metadata
     *            The file metadata.
     * 
     * @return The new version identifier.
     */
    public int create(Map<String, Object> metadata) {

        if (metadata == null)
            throw new IllegalArgumentException();

        // check required properties.
        assertString(metadata, MetadataSchema.ID);

        // clone the map since it may be unmodifiable.
        metadata = new HashMap<String, Object>(metadata);
        
        // auto-increment the last defined version counter.
        metadata.put(MetadataSchema.VERSION, AutoIncCounter.INSTANCE);
        
        // write the metadata (atomic operation).
        final ITPS tps = getMetadataIndex().write(getKeyBuilder(),
                metadataSchema, metadata, AUTO_TIMESTAMP, null/* filter */);

        final int version = (Integer) tps.get(MetadataSchema.VERSION).getValue();

        log.info("Created new version: id=" + metadata.get(MetadataSchema.ID)
                + ", version=" + version);
        
        return version;
        
    }
    
    public int create(Document doc) {
        
        if (doc == null)
            throw new IllegalArgumentException();
        
        final String id = doc.getId(); 
        
        if (id == null)
            throw new RuntimeException("The " + MetadataSchema.ID
                    + " property must be defined.");

        final Map<String,Object> metadata = doc.asMap();

//        /*
//         * Verify content type was specified since we will write on the file
//         * version.
//         */
//        assertString(metadata, MetadataSchema.CONTENT_TYPE);

        /*
         * Vreate new file version.
         */
        final int version = create( metadata );

        /*
         * Copy data from the document.
         */
        copyStream(id, version, doc.getInputStream());
        
        return version;
        
    }
    
    /**
     * Reads the document metadata for the current version of the specified
     * file.
     * 
     * @param id
     *            The file identifier.
     * 
     * @return A read-only view of the file version that is capable of reading
     *         the content from the repository -or- <code>null</code> iff
     *         there is no current version for that file identifier.
     */
    public Document read(String id) {

        RepositoryDocumentImpl doc = new RepositoryDocumentImpl(this, id);

        if (!doc.exists()) {

            // no current version for that document.
            
            log.info("No current version: id="+id);
            
            return null;
            
        }
        
        return doc;
        
    }

    /**
     * Return the file metadata for the version of the file associated with the
     * specified timestamp.
     * 
     * @param id
     *            The file identifier.
     * @param timestamp
     *            The timestamp.
     * 
     * @return A read-only view of the logical row of metadata for that file as
     *         of that timestamp.
     * 
     * @see ITPS
     * @see SparseRowStore#read(IKeyBuilder, Schema, Object, long, com.bigdata.sparse.INameFilter)
     */
    public ITPS readMetadata(String id, long timestamp) {

        return getMetadataIndex().read(getKeyBuilder(), metadataSchema, id,
                timestamp, null/* filter */);

    }
    
    /**
     * A read-only view of a {@link Document} that has been read from a
     * {@link BigdataRepository}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class RepositoryDocumentImpl implements DocumentHeader, Document 
    {
        
        final private BigdataRepository repo;
        
        final private String id;
        
        /**
         * The result of the atomic read on the file's metadata. This
         * representation is significantly richer than the current set of
         * property values.
         */
        final ITPS tps;

        /**
         * The current version identifer -or- <code>-1</code> iff there is no
         * current version for the file (including when there is no record of
         * any version for the file).
         */
        final int version;
        
        /**
         * The property set for the current file version.
         */
        final private Map<String,Object> metadata;

        /**
         * Read the metadata for the current version of the file from the
         * repository.
         * 
         * @param id
         *            The file identifier.
         * @param tps
         *            The logical row describing the metadata for some file in
         *            the repository.
         */
        public RepositoryDocumentImpl(BigdataRepository repo, String id,
                ITPS tps) {
            
            if (repo == null)
                throw new IllegalArgumentException();

            if (id == null)
                throw new IllegalArgumentException();
            
            this.repo = repo;
            
            this.id = id;
            
            this.tps = tps;
            
            if (tps != null) {

                ITPV tmp = tps.get(MetadataSchema.VERSION);
                
                if (tmp.getValue() != null) {

                    /*
                     * Note the current version identifer.
                     */
                    
                    this.version = (Integer) tmp.getValue();

                    /*
                     * Save a simplifed view of the propery set for the current
                     * version.
                     */
                    
                    this.metadata = tps.asMap();

                    log.info("id="+id+", current version="+version);

                } else {
                    
                    /*
                     * No current version.
                     */
                    
                    this.version = -1;

                    this.metadata = null;
                    
                    log.warn("id="+id+" : no current version");

                }
    
            } else {
                
                /*
                 * Nothing on record for that file identifier.
                 */
                
                this.version = -1;
                
                this.metadata = null;
                
                log.warn("id="+id+" : no record of any version(s)");

            }
            
            if (DEBUG && metadata != null) {

                Iterator<Map.Entry<String,Object>> itr = metadata.entrySet().iterator();
                
                while(itr.hasNext()) {
                    
                    Map.Entry<String, Object> entry = itr.next();
                    
                    log.debug("id=" + id + ", version=" + getVersion() + ", ["
                            + entry.getKey() + "]=[" + entry.getValue() + "]");
                    
                }

            }

        }
        
        /**
         * Read the metadata for the current version of the file from the
         * repository.
         * 
         * @param id
         *            The file identifier.
         */
        public RepositoryDocumentImpl(BigdataRepository repo,String id)
        {
            
            this(repo, id, repo.getMetadataIndex().read(repo.getKeyBuilder(),
                    metadataSchema, id, Long.MAX_VALUE, null/* filter */));
            
        }

        /**
         * Assert that a version of the file existed when this view was
         * constructed.
         * 
         * @throws IllegalStateException
         *             unless a version of the file existed at the time that
         *             this view was constructed.
         */
        final protected void assertExists() {

            if (version == -1) {

                throw new IllegalStateException("No current version: id="+id);
                
            }
            
        }
        
        final public boolean exists() {
            
            return version != -1;
            
        }
        
        final public int getVersion() {

            assertExists();

            return (Integer)metadata.get(MetadataSchema.VERSION);

        }

        /**
         * Note: This is obtained from the earliest available timestamp of the
         * {@link MetadataSchema#ID} property.
         */
        final public long getEarliestVersionCreateTime() {
            
            assertExists();
            
            Iterator<ITPV> itr = tps.iterator();
            
            while(itr.hasNext()) {
                
                ITPV tpv = itr.next();
                
                if(tpv.getName().equals(MetadataSchema.ID)) {
                    
                    return tpv.getTimestamp();
                    
                }
                
            }
            
            throw new AssertionError();
            
        }

        final public long getVersionCreateTime() {

            assertExists();
            
            /*
             * The timestamp for the most recent value of the VERSION property.
             */
            
            final long createTime = tps.get(MetadataSchema.VERSION)
                    .getTimestamp();
            
            return createTime;
            
        }

        final public long getMetadataUpdateTime() {
            
            assertExists();
            
            /*
             * The timestamp for the most recent value of the ID property.
             */
            
            final long metadataUpdateTime = tps.get(MetadataSchema.ID)
                    .getTimestamp();
            
            return metadataUpdateTime;

        }

        /**
         * Return an array containing all non-eradicated values of the
         * {@link MetadataSchema#VERSION} property for this file as of the time
         * that this view was constructed.
         * 
         * @see BigdataRepository#getAllVersionInfo(String)
         */
        final public ITPV[] getAllVersionInfo() {
            
            return repo.getAllVersionInfo(id);
            
        }
        
        final public InputStream getInputStream() {

            assertExists();
            
            return repo.inputStream(id,getVersion());
            
        }
        
        final public Reader getReader() throws UnsupportedEncodingException {

            assertExists();

            return repo.reader(id, getVersion(), getContentEncoding());

        }

        final public String getContentEncoding() {

            assertExists();
            
            return (String)metadata.get(MetadataSchema.CONTENT_ENCODING);
            
        }

        final public String getContentType() {
         
            assertExists();

            return (String)metadata.get(MetadataSchema.CONTENT_TYPE);
            
        }

        final public String getId() {

            return id;
            
        }
        
        final public Object getProperty(String name) {
            
            return metadata.get(name);
            
        }
        
        final public Map<String,Object> asMap() {
            
            assertExists();

            return Collections.unmodifiableMap( metadata );
            
        }

    }

    /**
     * Update the metadata for the current file version.
     * 
     * @param id
     *            The file identifier.
     * 
     * @param metadata
     *            The properties to be written. A <code>null</code> value for
     *            a property will cause the corresponding property to be
     *            deleted. Properties not present in this map will NOT be
     *            modified.
     * 
     * @return The complete metadata for the current file version.
     */
    public Map<String,Object> updateMetadata(String id, Map<String,Object> metadata) {

        // copy since the map might be unmodifyable.
        metadata = new HashMap<String,Object>(metadata);
        
        // set the id - this is required for the primary key.
        metadata.put(MetadataSchema.ID, id);

        // remove the version identifier if any - we do not want this modified!
        metadata.remove(MetadataSchema.VERSION);
        
        return getMetadataIndex().write(getKeyBuilder(), metadataSchema,
                metadata, AUTO_TIMESTAMP, null/* filter */).asMap();
        
    }
    
    /**
     * Create a new file version using the supplied file metadata.
     * <p>
     * Note: This is essentially a delete + create operation. Since the combined
     * operation is NOT atomic it is possible that conflicts can arise when more
     * than one client attempts to update a file concurrently.
     * 
     * @param doc
     *            The file metadata.
     */
    public int update(Document doc) {
        
        Map<String,Object> metadata = doc.asMap();
        
        final String id = (String) metadata.get(MetadataSchema.ID); 
        
        // delete the existing file version (if any).
        delete( id );
        
        // create a new file version using that metadata.
        return create( doc );
        
    }

    public long delete(String id) {

        final RepositoryDocumentImpl doc = (RepositoryDocumentImpl) read(id);
        
        if (!doc.exists()) {
            
            // no current version.

            log.warn("No current version: id=" + id);

            return 0L;

        }

        final int version = doc.getVersion();

        /*
         * Delete blocks from the file version.
         * 
         * Note: This is efficient in that it handles the delete on the data
         * service for each index partition. However, if the data spans more
         * than one index partition then the requests to delete the data on each
         * index partition are issued in sequence. A range-delete procedure
         * could be even more efficient since it can be parallelized when the
         * operation spans more than one index partition.
         */

        long blockCount = 0;

        final IKeyBuilder keyBuilder = getKeyBuilder();

        // the key for {file,version}
        final byte[] fromKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .getKey();

        // the key for {file,successor(version)}
        final byte[] toKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version + 1)
                .getKey();

        final ITupleIterator itr = getDataIndex().rangeIterator(fromKey, toKey,
                0/* capacity */, IRangeQuery.REMOVEALL, null/* filter */);

        while (itr.hasNext()) {

            itr.next();

            blockCount++;

        }

        log.info("Deleted " + blockCount + " blocks : id=" + id + ", version="
                + version);
        
        /*
         * Mark the file version as deleted.
         * 
         * Note: This only deletes the "version" property - the other properties
         * are not changed.
         */
        {
            
            final Map<String, Object> metadata = new HashMap<String, Object>();

            // primary key.
            metadata.put(MetadataSchema.ID, id);

            // delete marker.
            metadata.put(MetadataSchema.VERSION, null);

            getMetadataIndex().write(getKeyBuilder(), metadataSchema, metadata,
                    AUTO_TIMESTAMP, null/* filter */);
            
        }

        /*
         * There was a current version for the file. We have written a delete
         * marker and also deleted any blocks for that file version.
         */
        
        return blockCount;
        
    }

    /**
     * Return an array describing all non-eradicated versions of a file.
     * <p>
     * This method returns all known version identifiers together with their
     * timestamps, thereby making it possible to read either the metadata or the
     * data for historical file versions - as long as the metadata and/or data
     * has not yet been eradicated.
     * <p>
     * The file metadata and data blocks for historical version(s) of a file
     * remain available until they are eradicated from their respective indices
     * by a compacting merge in which the history policies no longer perserve
     * those data.
     * <p>
     * In order to read the historical file metadata you need to know the
     * timestamp associated with the version identifer which you wish to read.
     * This should be timestamp when that version was <em>deleted</em> MINUS
     * ONE in order to read the last valid metadata for the file version that
     * file version was deleted.
     * <p>
     * Likewise, in order to read the historical version data you need to know
     * the version identifer which you wish to read as well as the timestamp.
     * Again, this should be timestamp when that version was <em>deleted</em>
     * MINUS ONE in order to read the last committed state for the file version.
     * <p>
     * Historical file version metadata is eradicated atomically since the
     * entire logical row will be hosted on the same index partition. Either the
     * file version metadata is available or it is now.
     * <p>
     * Historical file version data is eradicated one index partition at a time.
     * If the file version spans more than one index partition then it may be
     * possible to read some blocks from the file but not others.
     * <p>
     * Historical file version metadata and data will remain available until
     * their governing history policy is no longer satisified. Therefore, when
     * in doubt, you can consult the history policy in force for the file to
     * determine whether or not its data may have been eradicated.
     * 
     * @param id
     *            The file identifier.
     * 
     * @return An array containing (timestamp,version) tuples. Tuples where the
     *         {@link ITPV#getValue()} returns <code>null</code> give the
     *         timestamp at which a file version was <em>deleted</em>. Tuples
     *         where the {@link ITPV#getValue()} returns non-<code>null</code>
     *         give the timestamp at which a file version was <em>created</em>.
     * 
     * @see #readMetadata(String, long), to read the file version metadata based
     *      on a timestamp.
     * 
     * @see #inputStream(String, int, long), to read the file data as of a
     *      specific timestamp.
     * 
     * @todo expose history policy for a file (from its zone metadata, which is
     *       replicated onto the index partition metadata). Make sure that the
     *       zone metadata is consistent for the file version metadata and file
     *       version data. This means looking up the {@link IndexMetadata} for
     *       the index partition in which the file data is stored.
     */
    public ITPV[] getAllVersionInfo(String id) {
        
        /*
         * Query for all metadata for the file.
         */
        ITPS tps = readMetadata(id,Long.MAX_VALUE);

        Vector<ITPV> vec = new Vector<ITPV>();

        /*
         * Filter for only the version propertys, skipping "delete" entries.
         */
        Iterator<? extends ITPV> itr = tps.iterator();
        
        while(itr.hasNext()) {
            
            ITPV tpv = itr.next();
            
            if(!tpv.getName().equals(MetadataSchema.VERSION)) {
                
                // Not a version field.
                
                continue;
                
            }

            vec.add(tpv);

        }

        return vec.toArray(new ITPV[vec.size()]);

    }
    
    /**
     * @todo write tests.
     */
    @SuppressWarnings("unchecked")
    public Iterator<? extends DocumentHeader> getDocumentHeaders(String fromId,
            String toId) {

        return new Striterator(getMetadataIndex().rangeQuery(getKeyBuilder(),
                metadataSchema, fromId, toId, 0/* capacity */,
                Long.MAX_VALUE/* timestamp */, null/* filter */))
                .addFilter(new Resolver() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    protected Object resolve(Object arg0) {
                        
                        ITPS tps = (ITPS) arg0;
                        
                        String id = (String) tps.get(MetadataSchema.ID).getValue();
                        
                        return new RepositoryDocumentImpl(
                                BigdataRepository.this, id, tps);
                        
                    }

                });
        
    }

    /**
     * Efficient delete of file metadata and file data for all files and file
     * versions spanned by the specified file identifiers.
     * 
     * @todo run this in two threads?
     */
    public long deleteAll(String fromId, String toId) {
        
        IKeyBuilder keyBuilder = getKeyBuilder();

        // the key for {fromId}
        final byte[] fromKey = keyBuilder.reset().appendText(fromId,
                true/* unicode */, false/* successor */).getKey();

        // the key for {successor(toId)}
        final byte[] toKey = keyBuilder.reset().appendText(toId,
                true/* unicode */, true/* successor */).getKey();


        // delete file metadata
        long ndeleted = 0;
        {

            /*
             * Delete file version metadata.
             */
            getMetadataIndex().getIndex().submit(fromKey, toKey,
                    new FileVersionDeleteProc(fromId, toId), null/* handler */);
            
        }
        
        // delete file blocks.
        {

            final ITupleIterator itr = getDataIndex()
                    .rangeIterator(fromKey, toKey, 0/* capacity */,
                            IRangeQuery.REMOVEALL, null/* filter */);

            long blockCount = 0;

            while (itr.hasNext()) {

                itr.next();

                blockCount++;

            }
            
        }

        return ndeleted;
        
    }

    /**
     * A procedure that performs a key range scan, marking all non-deleted
     * versions within the key range as deleted (by storing a null property
     * value for the {@link MetadataSchema#VERSION}).
     * 
     * @todo Make sure that overflow handling for the {@link SparseRowStore}
     *       causes the deleted file versions to be left behind eventually.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class FileVersionDeleteProc implements IIndexProcedure {

        /**
         * 
         */
        private static final long serialVersionUID = -31946508577453575L;

        private String fromId;
        private String toId;
        
        public FileVersionDeleteProc(String fromId, String toId) {
        
            this.fromId = fromId;
            
            this.toId = toId;
            
        }
        
        public Object apply(IIndex ndx) {

            throw new UnsupportedOperationException();
            
        }
        
    }
    
    /**
     * FIXME Integrate with {@link FullTextIndex} to providing indexing and
     * search of file versions. Deleted file versions should be removed from the
     * text index. There should be explicit metadata on the file version in
     * order for it to be indexed. The text indexer will require content type
     * and encoding information in order to handle indexing. Low-level output
     * stream, writer, block write and block append operations will not trigger
     * the indexer since it depends on the metadata index to know whether or not
     * a file version should be indexed. However you could explicitly submit a
     * file version for indexing.
     * <p>
     * Perhaps the best way to handle this is to queue document metadata up for
     * a distributed full text indexing service. The service accepts metadata
     * for documents from the queue and decides whether or not the document
     * should be indexed based on its metadata and how the document should be
     * processed if it is to be indexed. Those business rules would be
     * registered with the full text indexing service. (Alternatively, they can
     * be configured with the {@link BigdataRepository} and applied locally as
     * the blocks of the file are written into the repository. That's certainly
     * easier right off the bat.)
     * 
     * @todo crawl or query job obtains a set of URLs, writing them onto a file.
     *       <p>
     *       m/r job downloads documents based on set of URLs, writing all
     *       documents into a single file version. text-based downloads can be
     *       record compressed and decompressed after the record is read. binary
     *       downloads will be truncated at 64M and might be skipped all
     *       together if the exceed the block size (get images, but not wildely
     *       large files).
     *       <p>
     *       m/r job extracts a simplified html format from the source image,
     *       writing the result onto another file. this job will optionally
     *       split documents into "pages" by breaking where necessary at
     *       paragraph boundaries.
     *       <p>
     *       m/r job builds text index from simplified html format.
     *       <p>
     *       m/r job runs extractors on simplified html format, producing
     *       rdf/xml which is written onto another file. The rdf/xml for each
     *       harvested document is written as its own logical record, perhaps
     *       one record per block.
     *       <p>
     *       concurrent batch load of rdf/xml into scale-out knowledge base. the
     *       input is a single file comprised of blocks, each of which is an
     *       rdf/xml file.
     */
    public Iterator<String> search(String query) {

        throw new UnsupportedOperationException();
        
    }

    /*
     * file data operations (read, atomic append).
     */
    
    /**
     * Atomic append of a single block to a file version.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AtomicBlockAppendProc implements ISimpleIndexProcedure,
            Externalizable {

        private static final long serialVersionUID = 1441331704737671258L;

        protected static transient Logger log = Logger
                .getLogger(AtomicBlockAppendProc.class);

        /**
         * True iff the {@link #log} level is INFO or less.
         */
        final public static transient boolean INFO = log.getEffectiveLevel()
                .toInt() <= Level.INFO.toInt();

        /**
         * True iff the {@link #log} level is DEBUG or less.
         */
        final public static transient boolean DEBUG = log.getEffectiveLevel()
                .toInt() <= Level.DEBUG.toInt();

        private String id;
        private int version;
        private int off;
        private int len;
        private byte[] b;
        
        /**
         * 
         * @param id
         *            The file identifier.
         * @param version
         *            The file version.
         * @param b
         *            The buffer containing the data to be written.
         * @param off
         *            The offset in the buffer of the first byte to be written.
         * @param len
         *            The #of bytes to be written.
         */
        public AtomicBlockAppendProc(BigdataRepository repo, String id, int version, byte[] b, int off, int len) {

            assert id != null && id.length() > 0;
            assert version >= 0;
            assert b != null;
            assert off >= 0 : "off="+off;
            assert len >= 0 && off + len <= b.length;
            assert len <= repo.BLOCK_SIZE : "len="+len+" exceeds blockSize="+repo.BLOCK_SIZE;

            this.id = id;
            this.version = version;
            this.off = off;
            this.len = len;
            this.b = b;

        }
        
        /**
         * This procedure runs on the unisolated index. The block identifier is
         * computed as a one up long integer for that file version using locally
         * available state. The raw data for the block is written directly onto
         * the {@link Journal} and an index entry is added for the file,
         * version, and block whose value is the address of the block's data on
         * the {@link Journal}.
         * <p>
         * Note: The caller MUST have correctly identified the data service on
         * which the tail of the file exists (or on which the head of the file
         * will be written).
         * <p>
         * The block identifier is computed by reading and decoding the key for
         * the last block written for this file version (if any). Special cases
         * exist when the file version spans more than one index partition, when
         * the block would be the first block (in key order) for the index
         * partition, and when the block would be the last block (in key order)
         * for the index partition.
         * 
         * @return <code>true</code> iff the block was overwritten.
         */
        public Object apply(IIndex ndx) {

            // tunnel through to the backing journal.
            final AbstractJournal journal = (AbstractJournal)((AbstractBTree)ndx).getStore();
            
            // obtain the thread-local key builder for that journal.
            final IKeyBuilder keyBuilder = journal.getKeyBuilder();
            
            /*
             * The next block identifier to be assigned.
             */
            final long block;
            {

                /*
                 * Find the key for the last block written for this file
                 * version. We do this by forming a probe key from the file,
                 * version, and the maximum allowed block identifier. This is
                 * guarenteed to be after any existing block for that file and
                 * version.
                 * 
                 * @todo This implies that the leftSeparator for the index
                 * partition MUST NOT split the blocks for a file unless there
                 * is at least one block in the index partition. In practice
                 * this guarentee is easy to maintain. By default we choose to
                 * split an index partition on a file boundary. If that would
                 * result in an uneven split (or an empty split in the case of
                 * very large files) then we choose a split point that lies
                 * within the file's data - leaving at least one block for the
                 * file (probably many) in both partitions created by the split.
                 */
                
                final byte[] toKey = keyBuilder.reset().appendText(id,
                        true/* unicode */, false/* successor */).append(
                        version).append(Long.MAX_VALUE).getKey();

                // @todo promote this interface onto IIndex?
                // @todo verify iface implemented for index partition view.
                final ILinearList tmp = (ILinearList) ndx;
                
                /*
                 * Index of the first key after this file version.
                 * 
                 * Note: This will always be an insertion point (a negative
                 * value) since the toKey only encodes the successor of the file
                 * identifier.
                 * 
                 * We convert the insertion point to an index.
                 * 
                 * If the index is zero (0) then there are no blocks for this
                 * file and the file will be the first file in the index order
                 * on this index partition (there may or may not be other files
                 * already on the index partition).
                 * 
                 * Else fetch the key at that index. If that key encodes the
                 * same id as this file then we are appending to a file with
                 * existing block(s) and we decode the block identifier from the
                 * key. Otherwise this will be the first block written for that
                 * file.
                 */
                int toIndex = tmp.indexOf(toKey);

                assert toIndex < 0 : "Expecting insertion point: id=" + id
                        + ", version=" + version + ", toIndex=" + toIndex;

                log.debug("insertionPoint="+toIndex);
                
                toIndex = -(toIndex+1); // convert to an index.

                // #of entries in the index.
                final int entryCount = ((AbstractBTree)ndx).getEntryCount();
                
                log.debug("toIndex="+toIndex+", entryCount="+entryCount);

                if (toIndex == 0) {

                    /*
                     * Insertion point is before all other entries in the index.
                     * 
                     * Note: In this case we need to examine the leftSeparator
                     * key for the index partition. If that key is for the same
                     * file version then we use the successor of the block
                     * identifier found in that key.
                     * 
                     * Note: when it is not for the same file version it MAY be
                     * that the leftSeparator does not include the block
                     * identifier - the block identifier is only required in the
                     * leftSeparator when the a file version spans both the
                     * prior index partition and this index partition.
                     */
                    
                    log.debug("Insertion point is before all entries in the index partition: id="
                                    + id + ", version=" + version);
                    
                    final byte[] leftSeparator = ((BTree) ndx)
                            .getIndexMetadata().getPartitionMetadata()
                            .getLeftSeparatorKey();

                    block = getNextBlockFromPriorKey(keyBuilder, leftSeparator);
                    
                } else {
                    
                    if (toIndex == entryCount) {

                        /*
                         * Insertion point is after all entries in the index.
                         * 
                         * Note: In this case we consider the prior key in the
                         * index partition. If that key is for the same file
                         * version then we use the successor of the block
                         * identifier found in that key.
                         */

                        log.debug("Insertion point is after all entries in the index partition: id="
                                        + id + ", version=" + version);

                    } else {

                        /*
                         * Insertion point is at the toKey.
                         * 
                         * Note: Since the probe key is beyond the last block
                         * for the file version we adjust the toIndex so that we
                         * consider the prior key.
                         */

                        log.debug("Insertion point is at the toKey: id=" + id
                                + ", version=" + version);

                    }

                    /*
                     * Adjust to consider the key before the insertion point.
                     */

                    toIndex--;
                    
                    /*
                     * Look at the key at the computed index. If it is a key for
                     * this file version then we use the successor of the given
                     * block identifier. Otherwise we are writing a new file
                     * version and the block identifier will be zero (0).
                     */
                    
                    log.debug("adjusted toIndex="+toIndex+", entryCount="+entryCount);
                    
                    // the key at that index.
                    final byte[] key = tmp.keyAt(toIndex);

                    assert key != null : "Expecting entry: id=" + id
                            + ", version=" + version + ", toIndex=" + toIndex;

                    block = getNextBlockFromPriorKey(keyBuilder, key);
                    
                }

                log.info("Will write " + len + " bytes on id=" + id
                        + ", version=" + version + ", block#=" + block);
                
            }

            {

                /*
                 * write the block on the journal obtaining the address at which
                 * it was written - use 0L for the address of an empty block.
                 */
                final long addr = len == 0 ? 0L : journal.write(ByteBuffer
                        .wrap(b, off, len));

                // form the key for the index entry for this block.
                final byte[] key = keyBuilder.reset().appendText(id,
                        true/* unicode */, false/* successor */).append(
                        version).append(block).getKey();

                // record the address of the block in the index.
                {

                    final DataOutputBuffer out = new DataOutputBuffer(
                            Bytes.SIZEOF_LONG);

                    // encode the value for the entry.
                    out.reset().putLong(addr);

                    final byte[] val = out.toByteArray();

                    // insert the entry into the index.
                    ndx.insert(key, val);

                }

                log.info("Wrote " + len + " bytes : id=" + id + ", version="
                        + version + ", block#=" + block + " @ addr"
                        + journal.toString(addr));

            }

            // the block identifier.
            return block;

        }

        /**
         * Decode the block identifier in the key and return the block
         * identifier plus one, which is the block identifier to be used for the
         * atomic append operation. If the key does NOT encode the same file +
         * version then no blocks exist for that file version and the method
         * returns zero (0L) as the block identifer to be used.
         * 
         * @param keyBuilder
         *            The key builder.
         * @param key
         *            The key - either from the index partition or in some cases
         *            from the leftSeparator of the index partition metadata.
         *            <p>
         *            Note that the leftSeparator MAY be an empty byte[] (e.g.,
         *            for the 1st index partition in the key order) and MIGHT
         *            NOT include the block identifier (the block identifier is
         *            only included when it is necessary to split a file across
         *            index partitions). When the block identifier is omitted
         *            from the key and the key encodes the same file and version
         *            we therefore use zero (0L) as the next block identifier
         *            since we will be appending the first block to the file
         *            version.
         * 
         * @return The block identifier that will be used by the atomic append
         *         operation.
         */
        protected long getNextBlockFromPriorKey(IKeyBuilder keyBuilder,
                byte[] key) {

            // encode just the file id and the version.
            final byte[] prefix = keyBuilder.reset().appendText(id,
                    true/* unicode */, false/* successor */).append(version)
                    .getKey();

            if (DEBUG)
                log.debug("Comparing\nkey   :" + Arrays.toString(key)
                        + "\nprefix:" + Arrays.toString(prefix));

            /*
             * Test the encoded file id and version against the encoded file id
             * and version in the recovered key. If they compare equals (for the
             * length of the key that we just built) then they encode the same
             * file id and version.
             * 
             * (I.e., if true, then the key is from a block entry for this
             * version of this file).
             */

            if (key.length >= prefix.length) {

                final int cmp = BytesUtil.compareBytesWithLenAndOffset(0,
                        prefix.length, prefix, 0, prefix.length, key);

                log.debug("Comparing " + prefix.length + " byte prefix with "
                        + key.length + " byte key: cmp=" + cmp);

                if (cmp == 0) {

                    /*
                     * The key at the computed toIndex is the same file version.
                     */
                    if (prefix.length + Bytes.SIZEOF_LONG == key.length) {
                        
                        /*
                         * The given key includes a block identifier so we
                         * extract it.
                         * 
                         * Note: When the given key is a leftSeparator for an
                         * index partition AND the file version is not split
                         * across the index partition then the block identifer
                         * MAY be omitted from the leftSeparator. In this case
                         * the block identifier will be zero since there are no
                         * blocks yet for that file version.
                         */

                        // last block identifier assigned for this file + 1.
                        final long block = KeyBuilder.decodeLong(key,
                                key.length - Bytes.SIZEOF_LONG) + 1;

                        if (block > MAX_BLOCK) {

                            throw new RuntimeException(
                                    "File version has maximum #of blocks: id="
                                            + id + ", version=" + version);

                        }

                        log.info("Appending to existing file version: id=" + id
                                + ", version=" + version + ", block=" + block);

                        return block;

                    } else {
                        
                        /*
                         * This case arises when the leftSeparator encodes the
                         * file version but does not include a block identifier.
                         */
                        
                        log.info("Key is for same file version but does not contain block identifier.");
                        
                    }
                    
                } else {
                    
                    /*
                     * Since the key does not compare as equal for the full
                     * length of the prefix it can not encode the same file
                     * version.
                     */
                    
                    log.debug("Key does not compare as equal for length of prefix.");
                    
                }

            } else {
                
                /*
                 * Since the key is shorter than the prefix it can not be for
                 * the same file version.
                 */
                
                log.debug("Key is shorter than prefix.");
                
            }

            /*
             * The key at computed toIndex is a different file version so we are
             * starting a new file version at block := 0.
             */

            log.info("Appending to new file version: id=" + id + ", version="
                    + version + ", block=" + 0L);

            return 0L;

        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {

            id = in.readUTF();

            version = in.readInt();

            off = 0; // Note: offset always zero when de-serialized.

            len = in.readInt();

            b = new byte[len];

            in.readFully(b);

        }

        public void writeExternal(ObjectOutput out) throws IOException {

            out.writeUTF(id);

            out.writeInt(version);

            /*
             * Note: offset not written when serialized and always zero when
             * de-serialized.
             */
            
            out.writeInt(len); /* length */
            
            out.write(b, off, len); /* data */
            
        }
        
    }

    /**
     * Atomic write of a single block for a file version.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AtomicBlockWriteProc implements ISimpleIndexProcedure,
            Externalizable {

        private static final long serialVersionUID = 4982851251684333327L;

        protected static transient Logger log = Logger
                .getLogger(AtomicBlockWriteProc.class);

        /**
         * True iff the {@link #log} level is INFO or less.
         */
        final public static transient boolean INFO = log.getEffectiveLevel()
                .toInt() <= Level.INFO.toInt();

        /**
         * True iff the {@link #log} level is DEBUG or less.
         */
        final public static transient boolean DEBUG = log.getEffectiveLevel()
                .toInt() <= Level.DEBUG.toInt();

        private String id;
        private int version;
        private long block;
        private int off;
        private int len;
        private byte[] b;
        
        /**
         * 
         * @param id
         *            The file identifier.
         * @param version
         *            The file version.
         * @param block
         *            The block identifier.
         * @param b
         *            The buffer containing the data to be written.
         * @param off
         *            The offset in the buffer of the first byte to be written.
         * @param len
         *            The #of bytes to be written.
         */
        public AtomicBlockWriteProc(BigdataRepository repo,String id, int version, long block, byte[] b, int off, int len) {

            assert id != null && id.length() > 0;
            assert version >= 0;
            assert block >= 0 && block <= MAX_BLOCK;
            assert b != null;
            assert off >= 0 : "off="+off;
            assert len >= 0 && off + len <= b.length;
            assert len <= repo.BLOCK_SIZE : "len="+len+" exceeds blockSize="+repo.BLOCK_SIZE;

            this.id = id;
            this.version = version;
            this.block = block;
            this.off = off;
            this.len = len;
            this.b = b;

        }
        
        /**
         * This procedure runs on the unisolated index. The raw data is written
         * directly onto the {@link Journal} and the index is added/updated
         * using the given file, version and block and the address of the
         * block's data on the {@link Journal}.
         * 
         * @return A {@link Boolean} whose value is <code>true</code> iff the
         *         block was overwritten.
         */
        public Object apply(IIndex ndx) {

            // tunnel through to the backing journal.
            final AbstractJournal journal = (AbstractJournal)((AbstractBTree)ndx).getStore();
            
            // obtain the thread-local key builder for that journal.
            final IKeyBuilder keyBuilder = journal.getKeyBuilder();

            /*
             * Write the block on the journal, obtaining the address at which it
             * was written - use 0L as the address for an empty block.
             */
            final long addr = len == 0 ? 0L : journal.write(ByteBuffer.wrap(b,
                    off, len));

            // form the key for the index entry for this block.
            final byte[] key = keyBuilder.reset().appendText(id,
                    true/* unicode */, false/* successor */).append(version)
                    .append(block).getKey();

            // record the address of the block in the index.
            final boolean overwrite;
            {

                final DataOutputBuffer out = new DataOutputBuffer(
                        Bytes.SIZEOF_LONG);

                // encode the value for the entry.
                out.reset().putLong(addr);

                final byte[] val = out.toByteArray();

                // insert the entry into the index.
                overwrite = ndx.insert(key, val) != null;

            }

            log.info("Wrote " + len + " bytes : id=" + id + ", version="
                    + version + ", block#=" + block + " @ addr"
                    + journal.toString(addr) + ", overwrite=" + overwrite);

            return Boolean.valueOf(overwrite);

        }
        
        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {

            id = in.readUTF();

            version = in.readInt();

            block = in.readLong();

            off = 0; // Note: offset always zero when de-serialized.

            len = in.readInt();

            b = new byte[len];

            in.readFully(b);

        }

        public void writeExternal(ObjectOutput out) throws IOException {

            out.writeUTF(id);

            out.writeInt(version);

            out.writeLong(block);

            /*
             * Note: offset not written when serialized and always zero when
             * de-serialized.
             */
            
            out.writeInt(len); /* length */
            
            out.write(b, off, len); /* data */
            
        }
        
    }

    /**
     * Returns an iterator that visits all block identifiers for the file
     * version in sequence.
     * <p>
     * Note: This may be used to efficiently distribute blocks among a
     * population of clients, e.g., in a map/reduce paradigm.
     */
    public Iterator<Long> blocks(String id,int version) {
        
        final IKeyBuilder keyBuilder = getKeyBuilder();

        final byte[] fromKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .getKey();

        final byte[] toKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version + 1)
                .getKey();

        // just the keys.
        final int flags = IRangeQuery.KEYS;
        
        // visits the keys for the file version in block order.
        final ITupleIterator itr = getDataIndex().rangeIterator(fromKey, toKey,
                0/* capacity */, flags, null/* filter */);

        // resolve keys to block identifiers.
        return new BlockIdentifierIterator( id, version, itr );
        
    }
    
    /**
     * Extracts the block identifier from the key.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class BlockIdentifierIterator implements Iterator<Long> {
        
        final private String id;
        final private int version;
        final private ITupleIterator src;

        public String getId() {
            
            return id;
            
        }
        
        public int getVersion() {
            
            return version;
            
        }
        
        public BlockIdentifierIterator(String id, int version, ITupleIterator src) {
        
            if (id == null)
                throw new IllegalArgumentException();
            if (src == null)
                throw new IllegalArgumentException();
            
            this.id = id;
            this.version = version;
            this.src = src;
            
        }

        public boolean hasNext() {

            return src.hasNext();
            
        }

        public Long next() {

            ITuple tuple = src.next();
            
            byte[] key = tuple.getKey();
            
            long block = KeyBuilder.decodeLong(key, key.length
                    - Bytes.SIZEOF_LONG);

            return block;
            
        }

        /**
         * Removes the last visited block for the file version.
         */
        public void remove() {

            src.remove();
            
        }
        
    }
    
    /**
     * Copies blocks from one file version to another. The data in each block of
     * the source file version is copied into a new block that is appended to
     * the target file version. Empty blocks are copied. Partial blocks are NOT
     * combined. The block identifiers are NOT preserved since atomic append is
     * used to add blocks to the target file version.
     * 
     * @param fromId
     * @param fromVersion
     * @param toId
     * @param toVersion
     * 
     * @return The #of blocks copied.
     * 
     * @todo This could be made more efficient by sending the copy operation to
     *       each index partition in turn. that would avoid having to copy the
     *       data first to the client and thence to the target index partition.
     *       However, that would involve the data service in RPCs which might
     *       have high latency.
     */
    public long copyBlocks(String fromId, int fromVersion, String toId,
            int toVersion) {

        final Iterator<Long> src = blocks(fromId,fromVersion);
        
        long nblocks = 0L;
        
        while(src.hasNext()) {
        
            final long blockId = src.next();

            // read block
            final byte[] block = readBlock(fromId, fromVersion, blockId);
            
            // write block.
            appendBlock(toId, toVersion, block, 0, block.length);
            
            nblocks++;
            
        }
        
        return nblocks;
        
    }
    
    /**
     * Atomic write of a block for a file version.
     * <p>
     * Note: You can write any valid block identifier at any time. If the block
     * exists then its data will be replaced.
     * <p>
     * Note: Writing blocks out of sequence can create "holes". Those holes may
     * be filled by later writing the "missing" blocks.
     * {@link #copyBlocks(String, int, String, int)} will renumber the blocks
     * and produce a dense sequence of blocks.
     * <p>
     * Note: Atomic append will always write the successor of the largest block
     * identifier already written on the file version. If you write block
     * {@link #MAX_BLOCK} then it will no longer be possible to append blocks to
     * that file version, but you can still write blocks using
     * {@link #writeBlock(String, int, long, byte[], int, int)}.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The file version.
     * @param block
     *            The block identifier in [0:{@link #MAX_BLOCK}].
     * @param b
     *            The buffer containing the bytes to be written. When the buffer
     *            contains more than {@link #BLOCK_SIZE} bytes it will be broken
     *            up into multiple blocks.
     * @param off
     *            The offset of the 1st byte to be written.
     * @param len
     *            The #of bytes to be written.
     * 
     * @return <code>true</code> iff the block was overwritten (ie., if the
     *         block already exists, which case its contents were replaced).
     * 
     * @throws IllegalArgumentException
     *             if <i>id</id> is <code>null</code> or an empty string.
     * @throws IllegalArgumentException
     *             if <i>version</id> is negative.
     * @throws IllegalArgumentException
     *             if <i>block</id> is negative.
     * @throws IllegalArgumentException
     *             if <i>b</id> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>off</id> is negative or greater than the length of the
     *             byte[].
     * @throws IllegalArgumentException
     *             if <i>len</id> is negative or <i>off+len</i> is greater
     *             than the length of the byte[].
     * @throws IllegalArgumentException
     *             if <i>len</i> is greater than {@link #BLOCK_SIZE}.
     * 
     * @todo return the data for the old block instead in the case of an
     *       overwrite?
     */
    public boolean writeBlock(String id, int version, long block, byte[] b, int off, int len) {

        if (id == null || id.length() == 0)
            throw new IllegalArgumentException();
        if (version < 0)
            throw new IllegalArgumentException();
        if (block < 0L) {
            /*
             * Note: restriction implies 63-bit block identifier (no
             * negative#s).
             */
            throw new IllegalArgumentException();
        }
        if (block > MAX_BLOCK) {
            throw new IllegalArgumentException();
        }
        if (b == null)
            throw new IllegalArgumentException();
        if (off < 0 || off > b.length)
            throw new IllegalArgumentException("off="+off+", b.length="+b.length);
        if (len < 0 || off + len > b.length)
            throw new IllegalArgumentException("off="+off+", len="+len+", b.length="+b.length);
        if(len>BLOCK_SIZE) {
            throw new IllegalArgumentException();
        }

        // construct the atomic write operation.
        final IIndexProcedure proc = new AtomicBlockWriteProc(this, id, version,
                block, b, off, len);

        // the key for the {file,version,block}
        final byte[] key = getKeyBuilder().reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .append(block).getKey();

        return (Boolean) getDataIndex().submit(key, proc);

//        if (getDataIndex() instanceof ClientIndexView) {
//            
//            /*
//             * Remote index - figure out which index partition will get the
//             * write.
//             */
//
//            return (Boolean) ((ClientIndexView) getDataIndex()).submit(key,
//                    proc);
//
//        } else {
//
//            /*
//             * Run on a local index.
//             */
//
//            return ((Boolean) proc.apply(getDataIndex())).booleanValue();
//
//        }

    }

    /**
     * Atomic delete of the first block of the file version.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * 
     * @return The block identifier of the deleted block -or- <code>-1L</code>
     *         if nothing was deleted.
     */
    public long deleteHead(String id, int version) {

        log.info("id="+id+", version="+version);

        IKeyBuilder keyBuilder = getKeyBuilder();

        // the key for {file,version}
        final byte[] fromKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .getKey();

        // the key for {file,successor(version)}
        final byte[] toKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(
                version + 1).getKey();

        /*
         * The REMOVALL flag together with a limit of ONE (1) is used to obtain
         * an atomic delete of the first block for this file version.
         */

        final ITupleIterator itr = getDataIndex()
                .rangeIterator(fromKey, toKey,
                1, // Note: limit is ONE block!
                IRangeQuery.KEYS|IRangeQuery.REMOVEALL, null/* filter */);
        
        if (!itr.hasNext()) {

            log.warn("Nothing to delete: id=" + id + ", version=" + version);

            return -1L;

        }
        
        /*
         * Consume the iterator but note that the block was already deleted if
         * this was a remote request.
         */
        
        final long block = new BlockIdentifierIterator(id, version, itr).next();
            
        log.info("id="+id+", version="+version+" : deleted block="+block);

        return block;
        
    }
    
    /**
     * Atomic delete of a block for a file version.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * @param block
     *            The block identifier -or- <code>-1L</code> to read the first
     *            block in the file version regardless of its block identifier.
     * 
     * @return <code>true</code> iff the block was deleted.
     */
    public boolean deleteBlock(String id, int version, long block) {
        
        if (id == null || id.length() == 0)
            throw new IllegalArgumentException();
        if (version < 0)
            throw new IllegalArgumentException();
        if (block < 0L) {
            /*
             * Note: restriction implies 63-bit block identifier (no
             * negative#s).
             */
            throw new IllegalArgumentException();
        }
        if (block > MAX_BLOCK) {
            throw new IllegalArgumentException();
        }

        final byte[] key = getKeyBuilder().reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .append(block).getKey();
        
        /*
         * Note: The return value is just the serialized address of that block
         * on the journal (8 bytes).
         */
        
        final boolean deleted = getDataIndex().remove(key) != null;
        
        return deleted;
        
    }
    
    /**
     * Atomic read of the first block of the file version.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * 
     * @return The contents of the block -or- <code>null</code> iff there are
     *         no blocks for that file version. Note that an empty block will
     *         return an empty byte[] rather than <code>null</code>.
     */
    public byte[] readHead(String id, int version) {
        
        /*
         * Setup range scan than will span all blocks for the file version. We
         * are only interested in the first block, but this is how we get at its
         * data using an atomic read.
         */
        final byte[] fromKey = getKeyBuilder().reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .append(0L).getKey();

        final byte[] toKey = getKeyBuilder().reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .append(Long.MAX_VALUE).getKey();

        /*
         * Resolve the requested block : keys and data.
         */
        final ITupleIterator itr = getDataIndex()
                .rangeIterator(fromKey, toKey, 1/* capacity */,
                        IRangeQuery.KEYS | IRangeQuery.VALS, null/* filter */);

        if (!itr.hasNext()) {

            log.info("id=" + id + ", version=" + version + " : no blocks");

            return null;

        }

        return readBlock(id, version, itr.next());
        
    }
    
    /**
     * Atomic read of a block for a file version.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * @param block
     *            The block identifier.
     * 
     * @return The contents of the block -or- <code>null</code> iff the block
     *         does not exist. Note that an empty block will return an empty
     *         byte[] rather than <code>null</code>.
     * 
     * @todo offer a variant that returns an {@link InputStream}?
     */
    public byte[] readBlock(String id, int version, long block) {
        
        if (id == null)
            throw new IllegalArgumentException();
        
        /*
         * Setup range scan than will span exactly the specified block.
         * 
         * Note: This uses a range scan because a lookup will return the address
         * of the block rather than its data!
         */
        final byte[] fromKey = getKeyBuilder().reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .append(block).getKey();

        final byte[] toKey = getKeyBuilder().reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .append(block + 1).getKey();

        /*
         * Resolve the requested block : keys and data.
         */
        final ITupleIterator itr = getDataIndex()
                .rangeIterator(fromKey, toKey, 1/* capacity */,
                        IRangeQuery.KEYS | IRangeQuery.VALS, null/* filter */);

        if (!itr.hasNext()) {

            log.info("id=" + id + ", version=" + version + ", block=" + block
                    + " : does not exist");

            return null;

        }

        return readBlock(id, version, itr.next());
        
    }

    /**
     * Helper to read a block from an {@link ITuple}.
     * 
     * @param id
     * @param version
     * @param tuple
     * @return
     */
    private byte[] readBlock(String id, int version, ITuple tuple) {
        
        final byte[] key = tuple.getKey();
        
        // decode the block identifier from the key.
//        block = KeyBuilder.decodeLong(tuple.getKeyBuffer().array(),
//                tuple.getKeyBuffer().pos() - Bytes.SIZEOF_LONG);
        long block = KeyBuilder.decodeLong(key, key.length - Bytes.SIZEOF_LONG);

        final long addr;
        try {

            DataInput in = tuple.getValueStream();
        
            addr = in.readLong();
            
        } catch (IOException e) {
            
            throw new RuntimeException(e);
            
        }
        
        if (addr == 0L) {

            /*
             * Note: empty blocks are allowed and are recorded with 0L as
             * their address.
             */

            log.info("id=" + id + ", version=" + version + ", block=" + block
                    + " : empty block.");

            return new byte[]{};

        }
        
        /*
         * Read the block from the backing store.
         */
        final IBlock tmp = tuple.readBlock(addr);

        final int len = tmp.length();
        
        log.info("id=" + id + ", version=" + version + ", block=" + block
                + " : " + len + " bytes");

        // @todo reuse buffers, but must return {byte[],off,len} tuple.
        final byte[] data = new byte[len];
        
        try {

            final int nread = tmp.inputStream().read(data, 0, len);

            if (nread != len) {

                throw new RuntimeException("Expecting to read " + len
                        + " bytes but read " + nread + " bytes");

            }
            
        } catch (IOException e) {
            
            throw new RuntimeException(e);
            
        }
        
        return data;

    }

    /**
     * Atomic append of a block to a file version.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The file version.
     * @param b
     *            The buffer containing the data to be written..
     * @param off
     *            The offset of the 1st byte to be written.
     * @param len
     *            The #of bytes to be written in [0:{@link #BLOCK_SIZE}].
     * 
     * @return The block identifer for the written block.
     * 
     * @throws IllegalArgumentException
     *             if <i>id</id> is <code>null</code> or an empty string.
     * @throws IllegalArgumentException
     *             if <i>version</id> is negative.
     * @throws IllegalArgumentException
     *             if <i>b</id> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>off</id> is negative or greater than the length of the
     *             byte[].
     * @throws IllegalArgumentException
     *             if <i>len</id> is negative or <i>off+len</i> is greater
     *             than the length of the byte[].
     * @throws IllegalArgumentException
     *             if <i>len</i> is greater than {@link #BLOCK_SIZE}.
     */
    public long appendBlock(String id, int version, byte[] b, int off, int len) {
        
        if (id == null || id.length() == 0)
            throw new IllegalArgumentException();
        if (version < 0)
            throw new IllegalArgumentException();
        if (b == null)
            throw new IllegalArgumentException();
        if (off < 0 || off > b.length)
            throw new IllegalArgumentException("off="+off+", b.length="+b.length);
        if (len < 0 || off + len > b.length)
            throw new IllegalArgumentException("off="+off+", len="+len+", b.length="+b.length);
        if (len > BLOCK_SIZE) {
            throw new IllegalArgumentException();
        }

        // construct the atomic append operation.
        final IIndexProcedure proc = new AtomicBlockAppendProc(this, id, version, b,
                off, len);

        // the last possible key for this file
        final byte[] key = getKeyBuilder().reset().appendText(id,
                true/* unicode */, true/* successor */).append(version)
                .append(-1L).getKey();

        /*
         * Figure out which index partition will absorb writes on the end of the
         * file. We do this by finding the index partition that would contain
         * the successor of the id and then considering its leftSeparator. If
         * the leftSeparator is greater than the id then the id does not enter
         * this index partition and we use the prior index partition. Otherwise
         * the id enters this partition and we use it.
         * 
         * Note: File versions allow us to avoid painful edge cases when a file
         * has been deleted that spans more than one index partition. Since we
         * never attempt to write on the deleted file version we are not faced
         * with the problem of locating the largest index partition that
         * actually has data for that file. When a large file has been deleted
         * there can be EMPTY index partitions (containing only deleted entries)
         * until the next compacting merge.
         */
        return (Long) getDataIndex().submit(key, proc);

// if (getDataIndex() instanceof ClientIndexView) {
//
// return (Long) ((ClientIndexView) getDataIndex()).submit(key, proc);
//            
//        } else {
//
//            /*
//             * Run on a local index.
//             */
//            
//            return (Long) proc.apply( getDataIndex() );
//            
//        }
        
    }

    /**
     * Return the maximum #of blocks in the file version. The return value
     * includes any deleted but not yet eradicated blocks for the specified file
     * version, so it represents an upper bound on the #of blocks that could be
     * read for that file version.
     * <p>
     * Note: the block count only decreases when a compacting merge eradicates
     * deleted blocks from an index partition. It will increase any time there
     * is a write on a block for the file version for which neither a delete nor
     * an undeleted entry exists. The only way to count the #of non-deleted
     * blocks for a file version is to traverse the {@link #blocks(String, int)}
     * iterator.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The file version identifier.
     * 
     * @return The #of blocks in that file.
     */
    public long getBlockCount(String id, int version) {
     
        final byte[] fromKey = getKeyBuilder().reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .getKey();

        final byte[] toKey = getKeyBuilder().reset().appendText(id,
                true/* unicode */, false/* successor */).append(version + 1)
                .getKey();

        final long nblocks = getDataIndex().rangeCount(fromKey, toKey);

        log.info("id=" + id + ", version=" + version + ", nblocks=" + nblocks);

        return nblocks;
        
    }

    /**
     * Return a {@link Writer} that will <em>append</em> character data on the
     * file version. Characters written on the {@link Writer} will be converted
     * to bytes using the specified encoding. Bytes will be buffered until the
     * block is full and then written on the file version using an atomic
     * append. An {@link Writer#flush()} will force a non-empty partial block to
     * be written immediately.
     * <p>
     * Note: Map/Reduce processing of a file version MAY be facilitated greatly
     * by ensuring that "records" never cross a block boundary - this means that
     * file versions can be split into blocks and blocks distributed to clients
     * without any regard for the record structure within those blocks. The
     * caller can prevent records from crossing block boundaries by the simple
     * expediency of invoking {@link Writer#flush()} to force the atomic append
     * of a (partial but non-empty) block to the file.
     * <p>
     * Since the characters are being converted to bytes, the caller MUST make
     * {@link Writer#flush()} decisions with an awareness of the expansion rate
     * of the specified encoding. For simplicity, it is easy to specify
     * <code>UTF-16</code> in which case you can simply count two bytes
     * written for each character written.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * @param encoding
     *            The character set encoding.
     * 
     * @return The writer on which to write the character data.
     * 
     * @throws UnsupportedEncodingException
     */
    public Writer writer(String id, int version, String encoding)
            throws UnsupportedEncodingException {
        
        log.info("id="+id+", version="+version+", encoding="+encoding);

        return new OutputStreamWriter(outputStream(id, version), encoding);
        
    }
    
    /**
     * Read character data from a file version.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * @param encoding
     *            The character set encoding.
     *            
     * @return The reader from which you can read the character data.
     * 
     * @throws UnsupportedEncodingException
     */
    public Reader reader(String id, int version, String encoding) throws UnsupportedEncodingException {

        log.info("id="+id+", version="+version+", encoding="+encoding);
        
        if (encoding == null) {

            throw new IllegalStateException();
            
        }
        
        return new InputStreamReader(inputStream(id, version), encoding);

    }

    /**
     * Read data from a file version.
     * <p>
     * Note: The input stream will remain coherent for the file version as of
     * the time that the view on the file version is formed. Additional atomic
     * appends MAY be read, but that is NOT guarenteed. If the file is deleted
     * and its data is expunged by a compacting merge during the read then the
     * read MAY be truncated.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * 
     * @return An input stream from which the caller may read the data in the
     *         file -or- <code>null</code> if there is no data for that file
     *         version, including no deleted blocks pending garbage collection.
     *         An empty input stream MAY be returned since empty blocks are
     *         allowed. An empty stream will also be returned after a file
     *         version is deleted until the deleted blocks are eradicated from
     *         the file data index.
     */
    public FileVersionInputStream inputStream(String id,int version) {

        return inputStream(id, version, ITx.UNISOLATED);
        
    }

    /**
     * Read data from a file version.
     * <p>
     * Some points about consistency and transaction identifiers.
     * <ol>
     * 
     * <li> When using an {@link ITx#UNISOLATED} read addition atomic writes and
     * atomic appends issued after the input stream view was formed MAY be read,
     * but that is NOT guarenteed - it depends on the buffering of the range
     * iterator used to read blocks for the file version. Likewise, if the file
     * is deleted and its data is expunged by a compacting merge during the read
     * then the read MAY be truncated. </li>
     * 
     * <li> It is possible to re-create historical states of a file version
     * corresponding to a <em>commit point</em> for the
     * {@link #getDataIndex() data index} provided that the relevant data has
     * not been eradicated by a compacting merge. It is not possible to recover
     * all states - merely committed states - since unisolated writes may be
     * grouped together by group commit and therefore have the same commit
     * point. </li>
     * 
     * <li> It is possible to issue transactional read requests, but you must
     * first open a transaction with an {@link ITransactionManagerService}. In general
     * the use of full transactions is discouraged as the
     * {@link BigdataRepository} is designed for high throughput and high
     * concurrency with weaker isolation levels suitable for scale-out
     * processing techniques including map/reduce.</li>
     * 
     * </ol>
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * @param tx
     *            The transaction identifier. This is generally either
     *            {@link ITx#UNISOLATED} to use an unisolated read -or-
     *            <code>- timestamp</code> to use a historical read for the
     *            most recent consistent state of the file data not later than
     *            <i>timestamp</i>.
     * 
     * @return An input stream from which the caller may read the data in the
     *         file -or- <code>null</code> if there is no data for that file
     *         version, including no deleted blocks pending garbage collection.
     *         An empty input stream MAY be returned since empty blocks are
     *         allowed. An empty stream will also be returned after a file
     *         version is deleted until the deleted blocks are eradicated from
     *         the file data index.
     */
    public FileVersionInputStream inputStream(String id, int version, long tx) {

        log.info("id=" + id + ", version=" + version + ", tx=" + tx);

        /*
         * Range count the file and version on the federation - this is the
         * number of blocks of data for that file and version as of the start of
         * this read operation. If the result is zero then there are no index
         * partitions which span that file and version and we return null.
         * 
         * Note: This step is skipped for historical and transactional reads
         * since getBlockCount() does not accept the transaction identifier.
         */

        if (tx == ITx.UNISOLATED && getBlockCount(id, version) == 0L) {

            log.info("No data: id=" + id + ", version=" + version);

            return null;

        }
        
        /*
         * Return an input stream that will progress through a range scan of the
         * blocks for that file and version.
         */
        final byte[] fromKey = getKeyBuilder().reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .getKey();

        final byte[] toKey = getKeyBuilder().reset().appendText(id,
                true/* unicode */, false/* successor */).append(version + 1)
                .getKey();

        /*
         * The capacity is essentially the #of block addresses to transfer at a
         * time, not the #of blocks. I've set a moderately low limit here since
         * the blocks themselves need to be transferred as well, so there is
         * little point in buffering too many block addresses.
         * 
         * The addresses associated with a block identifier are updated when the
         * block is re-written, so if you buffer a lot of block addresses here
         * then updates to the blocks for the buffered identifiers will not be
         * visible to the client.
         * 
         * Finally, for very large files you may find that the block addresses
         * grow stale (the resource on which they were written may be moved or
         * deleted following a compacting merge), forcing a re-start of the read
         * from the last visited block identifier.
         * 
         * @todo handle automatic restart of the read from the next block
         * identifier if we learn that the resource on which a block was written
         * has been deleted.
         */
        final int capacity = 1000;
        
        // both keys and values.
        final int flags = IRangeQuery.KEYS | IRangeQuery.VALS;
        
        final ITupleIterator itr;
        
        final IIndex dataIndex;
        
        if (tx == ITx.UNISOLATED) {

            dataIndex = getDataIndex();

        } else {

            /*
             * Obtain the index view for that historical timestamp or isolated
             * by the specified transaction.
             */

            dataIndex = fed.getIndex(DATA_NAME,tx);
            
        }

        itr = dataIndex
                .rangeIterator(fromKey, toKey, capacity, flags, null/* filter */);

        return new FileVersionInputStream(id, version, itr);
        
    }

    /**
     * Return an output stream that will <em>append</em> on the file version.
     * Bytes written on the output stream will be buffered until they are full
     * blocks and then written on the file version using an atomic append. An
     * {@link OutputStream#flush()} will force a non-empty partial block to be
     * written immediately.
     * <p>
     * Note: Map/Reduce processing of a file version MAY be facilitated greatly
     * by ensuring that "records" never cross a block boundary - this means that
     * files can be split into blocks and blocks distributed to clients without
     * any regard for the record structure within those blocks. The caller can
     * prevent records from crossing block boundaries by the simple expediency
     * of invoking {@link OutputStream#flush()} to force the atomic append of a
     * (partial but non-empty) block to the file.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * 
     * @return The output stream.
     */
    public OutputStream outputStream(String id, int version) {

        log.info("id="+id+", version="+version);

        return new FileVersionOutputStream(this, id, version);

    }
    
    /**
     * Copies data from the input stream to the file version. The data is
     * buffered into blocks. Each block is written on the file version using an
     * atomic append. Writing an empty stream will cause an empty block to be
     * appended (this ensures that read back will read an empty stream).
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * @param is
     *            The input stream (closed iff it is fully consumed).
     * 
     * @return The #of bytes copied.
     */
    public long copyStream(String id, int version, InputStream is) {
        
        final FileVersionOutputStream os = (FileVersionOutputStream) outputStream(
                id, version);

        final long ncopied;
        
        try {

            ncopied = os.copyStream( is );
       
            if (ncopied == 0) {
                
                // force an empty block to be written.
                appendBlock(id, version, new byte[]{}, 0, 0);
                
            }
            
            os.close();
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
        return ncopied;

    }

    /**
     * Class buffers up to a block of data at a time and flushes blocks using an
     * atomic append operation on the identifier file version.
     * 
     * @todo this would benefit from asynchronous write-behind of the last block
     *       so that caller's do not wait for the RPC that writes the block onto
     *       the data index. use a blocking queue of buffers to be written so
     *       that the caller can not get far ahead of the database. a queue
     *       capacity of 1 or 2 should be sufficient.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class FileVersionOutputStream extends OutputStream {

        protected final BigdataRepository repo;
        protected final String id;
        protected final int version;
        
        /**
         * The file identifier.
         */
        public String getId() {
            
            return id;
            
        }

        /**
         * The file version identifer.
         */
        public int getVersion() {

            return version;
            
        }

        /**
         * The buffer in which the current block is being accumulated.
         */
        private final byte[] buffer;

        /**
         * The index of the next byte in {@link #buffer} on which a byte would be
         * written.
         */
        private int len = 0;

        /**
         * #of bytes written onto this output stream.
         */
        private long nwritten;
        
        /**
         * #of bytes written onto this output stream.
         * 
         * @todo handle overflow of long - leave counter at {@link Long#MAX_VALUE}.
         */
        public long getByteCount() {
            
            return nwritten;
            
        }

        /**
         * #of blocks written onto the file version.
         */
        private long nblocks;
        
        /**
         * #of blocks written onto the file version.
         */
        public long getBlockCount() {
           
            return nblocks;
            
        }
        
        /**
         * Create an output stream that will atomically append blocks of data to
         * the specified file version.
         * 
         * @param id
         *            The file identifier.
         * @param version
         *            The version identifier.
         */
        public FileVersionOutputStream(BigdataRepository repo, String id, int version) {
            
            if (repo == null)
                throw new IllegalArgumentException();
            if (id == null)
                throw new IllegalArgumentException();
            
            this.repo = repo;
            
            this.id = id;
            
            this.version = version;

            this.buffer = new byte[repo.BLOCK_SIZE];
            
        }

        /**
         * Buffers the byte. If the buffer would overflow then it is flushed.
         * 
         * @throws IOException
         */
        public void write(int b) throws IOException {

            if (len == buffer.length) {

                // buffer would overflow.
                
                flush();
                
            }
            
            buffer[len++] = (byte) (b & 0xff);
            
            nwritten++;
            
        }

        /**
         * If there is data data accumulated in the buffer then it is written on
         * the file version using an atomic append (empty buffers are NOT
         * flushed).
         * 
         * @throws IOException
         */
        public void flush() throws IOException {
            
            if (len > 0) {

                log.info("Flushing buffer: id="+id+", version="+version+", len="+len);
                
                repo.appendBlock(id, version, buffer, 0, len);

                len = 0;
                
                nblocks++;
                
            }
            
        }
        
        /**
         * Flushes the buffer.
         * 
         * @throws IOException
         */
        public void close() throws IOException {
           
            flush();
            
        }

        /**
         * Consumes the input stream, writing blocks onto the file version. The
         * output stream is NOT flushed.
         * 
         * @param is
         *            The input stream (closed iff it is fully consumed).
         * 
         * @return The #of bytes copied from the input stream.
         * 
         * @throws IOException
         */
        public long copyStream(InputStream is) throws IOException {

            long ncopied = 0L;

            while (true) {

                if (this.len == buffer.length) {

                    // flush if the buffer would overflow.
                    
                    flush();
                    
                }
                
                // next byte to write in the buffer.
                final int off = this.len;

                // #of bytes remaining in the buffer.
                final int remainder = this.buffer.length - off;

                // read into the buffer.
                final int nread = is.read(buffer, off, remainder);

                if (nread == -1) {

                    // the input stream is exhausted.
                    
                    log.info("Copied " + ncopied + " bytes: id=" + id
                            + ", version=" + version);

                    try {

                        is.close();
                        
                    } catch (IOException ex) {
                        
                        log.warn("Problem closing input stream: id=" + id
                                + ", version=" + version, ex);
                        
                    }

                    return ncopied;

                }

                // update the index of the next byte to write in the buffer.
                this.len = off + nread;

                // update #of bytes copied.
                ncopied += nread;

                // update #of bytes written on this output stream.
                nwritten += nread;

            }

        }
        
    }
    
    /**
     * Reads from blocks visited by a range scan for a file and version.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class FileVersionInputStream extends InputStream {

        protected final String id;
        protected final int version;
        private final ITupleIterator src;

        /**
         * The current block# whose data are being read.
         */
        private long block;
        
        /**
         * A buffer holding the current block's data. This is initially filled
         * from the first block by the ctor. When no more data is available it
         * is set to <code>null</code> to indicate that the input stream has
         * been exhausted.
         * 
         * @todo reuse buffers sized out to the block size.
         */
        private byte[] b;
        
        /**
         * The next byte to be returned from the current block's data.
         */
        private int off;
        
        /**
         * The #of bytes remaining in the current block's data.
         */
        private int len;
        
        /**
         * The file identifier.
         */
        public String getId() {
            
            return id;
            
        }

        /**
         * The file version identifer.
         */
        public int getVersion() {

            return version;
            
        }
        
        /**
         * The current block identifier.
         */
        public long getBlock() {

            return block;
            
        }
        
        public FileVersionInputStream(String id, int version, ITupleIterator src) {
            
            this.id = id;
            
            this.version = version;
            
            this.src = src;
            
            // read the first block of data.
            nextBlock();
            
        }
        
        /**
         * Reads the next block of data from the iterator and sets it on the
         * internal buffer. If the iterator is exhausted then the internal
         * buffer is set to <code>null</code>.
         * 
         * @return true iff another block of data was read.
         */
        private boolean nextBlock() {
            
            assert b == null || off == len;
            
            if (!src.hasNext()) {

                log.info("No more blocks: id="+id+", version="+version);
                
                b = null;
                
                off = 0;
                
                len = 0;
                
                return false;
                
            }
            
            final ITuple tuple = src.next();
            
            /*
             * decode the block address.
             */
            final long addr;
            try {

                DataInput in = tuple.getValueStream();
            
                addr = in.readLong();
                
            } catch (IOException e) {
                
                throw new RuntimeException(e);
                
            }
            
            if (addr == 0L) {

                /*
                 * Note: empty blocks are allowed and are recorded with 0L as
                 * their address.
                 */

                b = new byte[] {};
                
                off = 0;
                
                len = 0;
                
                log.info("Read zero bytes: id="+id+", version="+version+", block="+block);
                
            } else {
             
                byte[] key = tuple.getKey();
                
                // decode the block identifier from the key.
//                block = KeyBuilder.decodeLong(tuple.getKeyBuffer().array(),
//                        tuple.getKeyBuffer().pos() - Bytes.SIZEOF_LONG);
                block = KeyBuilder.decodeLong(key, key.length - Bytes.SIZEOF_LONG);
                
                final IBlock tmp = tuple.readBlock(addr);
                
                final int nbytes = tmp.length();
                
                // @todo reuse buffer!
                b = new byte[nbytes];
                
                off = 0;
                
                len = nbytes;
                
                try {

                    final int nread = tmp.inputStream().read(b,off,len);
                    
                    if (nread != len) {

                        throw new RuntimeException("Expecting " + len
                                + " bytes but read " + nread);
                        
                    }

                } catch (IOException e) {

                    throw new RuntimeException(e);
                    
                }
                
                log.info("Read "+b.length+" bytes: id="+id+", version="+version+", block="+block);
                
            }
            
            return true;
            
        }
        
        public int read() throws IOException {

            if (b == null) {

                // nothing left to read.

                return -1;

            }
            
            if(off == len) {
                
                if (!nextBlock()) {
                    
                    // no more blocks so nothing left to read.
                    
                    return -1;
                    
                }
                
            }
            
            // the next byte.
            int v = (0xff & b[off++]);
            
            return v;
            
        }
        
        /**
         * Overriden for greater efficiency.
         */
        public int read(byte[] b,int off, int len) throws IOException {

            if (b == null) {

                // nothing left to read.

                return -1;

            }
            
            if(this.off == this.len) {
                
                if (!nextBlock()) {
                    
                    // no more blocks so nothing left to read.
                    
                    return -1;
                    
                }
                
            }
            
            /*
             * Copy everything in our internal buffer up to the #of bytes
             * remaining in the caller's buffer.
             */
            
            final int n = Math.min(this.len, len); 
            
            System.arraycopy(this.b, this.off, b, off, n);

            this.off += n;
            
            return n;
            
        }
        
    }

    /**
     * Copies blocks onto the target store during overflow handling. Blocks that
     * are no longer referenced by the file data index will be left behind on
     * the journal and eventually discarded with the journal.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class BlobOverflowHandler implements IOverflowHandler {

        /**
         * 
         */
        private static final long serialVersionUID = -8180664203349900189L;

        /**
         * De-serialization constructor.
         */
        public BlobOverflowHandler() {
            
        }

        DataOutputBuffer buf;
        
        public void close() {
            
            buf = null;
            
        }
        
        public byte[] handle(ITuple tuple, IRawStore target) {

            if(buf==null) {
                
                buf = new DataOutputBuffer();
                
            }
            
            final long addr;
            try {

                DataInput in = tuple.getValueStream();
            
                addr = in.readLong();
                
            } catch (IOException e) {
                
                throw new RuntimeException(e);
                
            }
            
            if (addr == 0L) {

                /*
                 * Note: empty blocks are allowed and are recorded with 0L as
                 * their address.
                 */
                
                return KeyBuilder.asSortKey(0L);

            }

            // read block from underlying source store.
            final IBlock block = tuple.readBlock(addr);
            
            // #of bytes in the block.
            final int len = block.length();

            // make sure buffer has sufficient capacity.
            buf.ensureCapacity(len);
            
            // prepare buffer for write.
            buf.reset();
            
            final InputStream bin = block.inputStream();

//            // the address on which the block will be written.
//            final long addr2 = block2.getAddress();
            final long addr2;
            try {

//                // #of bytes read so far.
//                long n = 0;
//                
//                while (len - n > 0) {

                    // read source into buffer.
                    final int nread = bin.read(buf.array(),0,len);

                    if (nread != len) {

                        throw new RuntimeException(
                                "Premature end of block: expected="+len+", actual="+nread);

                    }

                    // write on the target store.
                    addr2 = target.write(buf.wrap());
                    
//                    // write buffer onto sink.
//                    bout.write(buf, 0, nread);

//                    n += nread;
//
//                }

//                bout.flush();

            } catch(IOException ex) {
                
                log.warn(
                        "Problem copying block: addr=" + addr + ", len=" + len,
                        ex);
                
                throw new RuntimeException(ex);
                
            } finally {

//                try {
//                    bout.close();
//                } catch (IOException ex) {
//                    log.warn(ex);
//                }

                try {
                    bin.close();
                } catch (IOException ex) {
                    log.warn(ex);
                }

            }

            // the address of the block on the target store.
            return KeyBuilder.asSortKey(addr2);
            
        }

    }

}
