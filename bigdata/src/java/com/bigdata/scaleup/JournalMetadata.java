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
package com.bigdata.scaleup;

import java.util.UUID;

import com.bigdata.journal.Journal;

/**
 * Metadata required to locate a {@link Journal} resource.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo make this persistence capable by modifying the value serializer to
 * use the {@link IResourceMetadata} interface.
 */
public class JournalMetadata implements IResourceMetadata {

    protected final String filename;
    protected final long nbytes;
    protected final ResourceState state;
    protected final UUID uuid;

    public final boolean isIndexSegment() {
        
        return false;
        
    }
    
    public final boolean isJournal() {
        
        return true;
        
    }
    
    public final String getFile() {
        
        return filename;
        
    }

    /**
     * Note: this value is typically zero (0L) since we can not accurately
     * estimate the #of bytes on the journal dedicated to a given partition of a
     * named index. The value is originally set to zero (0L) by the
     * {@link JournalMetadata#JournalMetadata(Journal, ResourceState)}
     * constructor.
     */
    public final long size() {
        
        return nbytes;
        
    }

    public final ResourceState state() {
        
        return state;
        
    }

    public final UUID getUUID() {
        
        return uuid;
        
    }
    
    public JournalMetadata(Journal journal, ResourceState state) {
        
        if(journal.getFile()==null) {
            
            throw new IllegalArgumentException("Journal is not persistent.");
            
        }
        
        this.filename = journal.getFile().toString();

        /*
         * Note: 0L since we can not easily estimate the #of bytes on the
         * journal that are dedicated to an index partition.
         */
        this.nbytes = 0L;
        
        this.state = state;
        
        this.uuid = journal.getRootBlockView().getUUID();
        
    }
    
    public JournalMetadata(String file, long nbytes, ResourceState state, UUID uuid) {
        
        if(file == null) throw new IllegalArgumentException();

        if(state == null) throw new IllegalArgumentException();
        
        if(uuid == null) throw new IllegalArgumentException();
        
        this.filename = file;
        
        this.nbytes = nbytes;
        
        this.state = state;
        
        this.uuid = uuid;
        
    }
    
}
