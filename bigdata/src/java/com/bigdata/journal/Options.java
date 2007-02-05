package com.bigdata.journal;

import java.nio.channels.FileChannel;
import java.util.Properties;

import com.bigdata.rawstore.Bytes;

/**
 * Options for the {@link Journal}. Options are specified as property
 * values to the {@link Journal#Journal(Properties)} constructor.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Options {

    /**
     * <code>file</code> - The name of the file. If the file not found and
     * {@link #CREATE} is true, then a new journal will be created.
     */
    public static final String FILE = "file";

    /**
     * <code>bufferMode</code> - One of "transient", "direct", "mapped",
     * or "disk". See {@link BufferMode} for more information about each
     * mode.
     * 
     * @see BufferMode#Transient
     * @see BufferMode#Direct
     * @see BufferMode#Mapped
     * @see BufferMode#Disk
     */
    public static final String BUFFER_MODE = "bufferMode";

    /**
     * <code>useDirectBuffers</code> - A boolean property whose value controls
     * whether a direct (native) or heap-based {@link ByteBuffer} will be
     * allocated by the selected {@link BufferMode}. Note that this only
     * applies to fully buffered modes, e.g., {@link BufferMode#Transient} or
     * {@link BufferMode#Direct}. This parameter has no effect for the
     * memory-mapped or disk-only buffer modes.
     */
    public static final String USE_DIRECT_BUFFERS = "useDirectBuffers";
    
    /**
     * <code>initialExtent</code> - The initial extent of the journal
     * (bytes). The initial file size is computed by subtracting off the
     * space required by the root blocks and dividing by the slot size.
     */
    public static final String INITIAL_EXTENT = "initialExtent";
    
    /**
     * <code>segment</code> - The unique int32 segment identifier (required
     * unless this is a {@link BufferMode#Transient} journal). Segment
     * identifiers are assigned by a bigdata federation. When using the journal
     * as part of an embedded database you may safely assign an arbitrary
     * segment identifier, e.g., zero(0).
     */
    public static final String SEGMENT = "segment";
    
    /**
     * <code>create</code> - An optional boolean property (default is
     * <code>true</code>). When true and the named file is not found, a new
     * journal will be created.
     * 
     * @todo Write tests for this feature.
     */
    public static final String CREATE = "create";
        
    /**
     * <code>readOnly</code> - When true, the journal must pre-exist and
     * will be read-only (optional, default is <code>false</code>).
     * 
     * @todo Write tests for this feature.
     */
    public static final String READ_ONLY = "readOnly";
    
    /**
     * <code>forceWrites</code> - A trinary property {no, force,
     * forceMetadata} that determines whether the journal will be opened in a
     * mode that writes through synchronously to stable storage (default
     * <code>No</code>). This option does NOT effect the stability of the
     * data on disk but may be used to tweak the file system buffering.
     * 
     * @see #FORCE_ON_COMMIT, which controls the stability of the data on disk.
     * @see ForceEnum
     */
    public static final String FORCE_WRITES = "forceWrites";

    /**
     * <code>forceOnCommit</code> - A trinary property {no, force,
     * forceMetadata} that determines whether the journal will be forced to
     * stable storage on a commit (default <code>forceMetadata</code>).
     * <dl>
     * <dt>no</dt>
     * <dd>This option is useful when the journal is replicated so that we can
     * always failover to another server having the same data. Unless the file
     * is replicated or transient, this mode can lead to lost data if there is a
     * hardware or software failure.</dd>
     * <dt>force</dt>
     * <dd>Force the journal contents, but not the file metadata, to stable
     * storage. The precise semantics of this option are dependent on the OS and
     * hardware platform (some hardware platforms may guarentee that the file
     * metadata is stable using a battery powered disk cache). If there is a
     * crash, the information lost can range from the last modified timestamp on
     * the file to the file length (which can make it impossible to re-open the
     * journal until the file length is corrected with a file system repair
     * utility), to the file allocation structure in the file system (which is
     * always a serious loss, but is normally repairable with a file system
     * repair utility). </dd>
     * <dt>forceMetadata</dt>
     * <dd>Force the journal contents and the file metadata to stable storage.
     * This option is the most secure. </dd>
     * </dl>
     * Based on some reading online, it appears that not forcing metadata can
     * mean anything from having the lastModified timestamp on the file be
     * incorrect to having the wrong file length in the directory structure, to
     * having the file allocation nodes not be coherent. This appears to be OS
     * and hardware specific (some hardware buffers the power for the disk
     * system so that such writes always complete).
     * 
     * @see IBufferStrategy#force(boolean)
     * @see FileChannel#force(boolean)
     * @see ForceEnum
     */
    public static final String FORCE_ON_COMMIT = "forceOnCommit";
    
//    /**
//     * <code>conflictResolver</code> - The name of a class that implements
//     * the {@link IConflictResolver} interface (optional). When specified,
//     * the class MUST define a public constructor with the signature
//     * <code><i>class</i>( Journal journal )</code>. There is NO
//     * default. Resolution of write-write conflicts is enabled iff a
//     * conflict resolution class is declared with this parameter. If a value
//     * is not provided, the a write-write conflict will result in the
//     * rollback of a transaction.
//     */
//    public static final String CONFLICT_RESOLVER = "conflictResolver";
    
    /**
     * <code>deleteOnClose</code> - This optional boolean option causes
     * the journal file to be deleted when the journal is closed (default
     * <em>false</em>). This option is used by the test suites to keep
     * down the disk burden of the tests and MUST NOT be used with live
     * data.
     */
    public final static String DELETE_ON_CLOSE = "deleteOnClose";
    
    /**
     * The default for {@link #USE_DIRECT_BUFFERS}.
     */
    public final static boolean DEFAULT_USE_DIRECT_BUFFERS = false;
    
    /**
     * The default initial extent for a new journal.
     */
    public final static long DEFAULT_INITIAL_EXTENT = 10 * Bytes.megabyte;
    
    /**
     * The default for the {@link #CREATE} option.
     */
    public final static boolean DEFAULT_CREATE = true;

    /**
     * The default for the {@link #READ_ONLY} option.
     */
    public final static boolean DEFAULT_READ_ONLY = false;
    
    /**
     * The default for the {@link #FORCE_WRITES} option (writes are not forced).
     */
    public final static ForceEnum DEFAULT_FORCE_WRITES = ForceEnum.No;
    
    /**
     * The default for the {@link #FORCE_ON_COMMIT} option (file data and
     * metadata are forced).
     */
    public final static ForceEnum DEFAULT_FORCE_ON_COMMIT = ForceEnum.ForceMetadata;
    
    /**
     * The default for the {@link #DELETE_ON_CLOSE} option.
     */
    public final static boolean DEFAULT_DELETE_ON_CLOSE = false;

}
