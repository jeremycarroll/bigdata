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
 */
public class MetadataService implements IMetadataService, IServiceShutdown {

    /**
     * The name of the journal on which the metadata index is stored.
     * 
     * @todo support two-tier metadata index and reconcile with
     *       {@link MetadataIndex} and {@link MasterJournal}.
     */
    protected final Journal journal;
    
    public MetadataService(Properties properties) {
        
        /*
         * @todo setup/resolve the journal and the metadata index on
         * the journal.
         */
        
        throw new UnsupportedOperationException();
        
    }
    
    public static void main(String[] args) {
        
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
