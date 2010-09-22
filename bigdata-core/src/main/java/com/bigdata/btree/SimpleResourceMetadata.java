/*
 * Created by IntelliJ IDEA.
 * User: gossard
 * Date: Sep 22, 2010
 * Time: 2:40:37 PM
 */
package com.bigdata.btree;

import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.SimpleMemoryRawStore;

import java.util.UUID;

/**
 * Dumb metadata object, used by a btree to return metadata about rawstores.
 * This class was previously an inner-class in {@link com.bigdata.rawstore.SimpleMemoryRawStore SimpleMemoryRawStore},
 * but was moved into the btree package to remove a rawstore dependency on the mdi package.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class SimpleResourceMetadata implements IResourceMetadata {

    /**
     *
     */
    private static final long serialVersionUID = -8333003625527191826L;

    private final UUID uuid;
    
    public SimpleResourceMetadata(UUID uuid) {
        if (uuid == null)
            throw new NullPointerException("uuid cannot be null");
        this.uuid = uuid;
    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }

    //from java.lang.Object
    public boolean equals(Object obj){
        if (obj instanceof SimpleResourceMetadata){
            SimpleResourceMetadata other = (SimpleResourceMetadata)obj;
            return uuid.equals(other.uuid);
        } else
            return false;
    }

    //from com.bigdata.mdi.IResourceMetadata, *NOT* java.lang.Object
    public boolean equals(IResourceMetadata o) {

        return this.equals((Object)o);
    }

    public long getCreateTime() {

        // does not support commit
        return 0L;

    }

    public long getCommitTime() {

        // does not support commit
        return 0L;

    }

    public String getFile() {

        // no backing file.
        return null;

    }

    public UUID getUUID() {

        return uuid;

    }

    public boolean isIndexSegment() {

        // not index segment.
        return false;

    }

    public boolean isJournal() {

        // not journal.
        return false;

    }

//        public long size() {
//
//            // #of bytes not available.
//            return 0L;
//
//        }

}