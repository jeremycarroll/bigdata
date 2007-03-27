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
 * Created on Mar 17, 2007
 */

package com.bigdata.service;

import java.net.InetSocketAddress;
import java.util.Properties;

import com.bigdata.journal.Journal;
import com.bigdata.scaleup.MasterJournal;
import com.bigdata.scaleup.MetadataIndex;

/**
 * Implementation of a metadata service for a named scale-out index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Tag each index with a UUID. The UUID needs to appear in the index
 * metadata record for each journal and index segment. When it is an named
 * (scale-out) index, the UUID of the scale-out index must be used for each
 * B+Tree metadata record having data for that index. This allows us to map
 * backwards from the data structures to the metadata index. Document this in
 * the UML model. (I still need to get the correct index UUID to each BTree
 * constuctor since they are all using a Random UUID right now.)
 * 
 * @todo Provide a means to reconstruct the metadata index from the journal and
 *       index segment data files. We tag each journal and index segment with a
 *       UUID. Each index is also tagged with a UUID, and that UUID is written
 *       into the metadata record for the index on each journal and index
 *       segment. Based on those UUIDs we are able to work backwards from the
 *       data on disk and identify the indices to which they belong. That
 *       information in combination with the timestamps in the metadata records
 *       and the first/last keys in the index partition is sufficient to
 *       regenerate the metadata indices.
 * 
 * @todo A temporal/immortable database can be realized if we never delete old
 *       journals since they contain the historical committed states of the
 *       database. The use of index segments would still provide fast read
 *       performance on recent data, while a suitable twist on the metadata
 *       index would allow access to those historical states. (E.g., you have to
 *       be able to access the historical state of the metadata index that
 *       corresponds to the commit time of interest for the database.)
 */
public class MetadataService implements IMetadataService, IServiceShutdown {

    /**
     * The name of the journal on which the metadata index is stored.
     * 
     * @todo support two-tier metadata index and reconcile with
     *       {@link MetadataIndex} and {@link MasterJournal}.
     */
    protected Journal journal;
    
    public MetadataService(Properties properties) {
        
        /*
         * @todo setup/resolve the journal and the metadata index on
         * the journal.
         */
        
        journal = new Journal(properties);

    }
    
    public InetSocketAddress getDataService(String name,byte[] key) {
        // TODO Auto-generated method stub
        return null;
    }

    public int getEntryCount(String name) {
        // TODO Auto-generated method stub
        return 0;
    }

    public int rangeCount(String name,byte[] fromKey,byte[] toKey) {
        // TODO Auto-generated method stub
        return 0;
    }

    public void shutdown() {
        // TODO Auto-generated method stub
        
    }

    public void shutdownNow() {
        // TODO Auto-generated method stub
        
    }
    
}
