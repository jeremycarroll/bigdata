/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Mar 17, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.rmi.Remote;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;

/**
 * A metadata service for a named index.
 * <p>
 * The metadata service maintains locator information for the data service
 * instances responsible for each partition in the named index. Partitions are
 * automatically split when they overflow (~200M) and joined when they underflow
 * (~50M).
 * <p>
 * Note: methods on this interface MUST throw {@link IOException} in order to be
 * compatible with RMI.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IMetadataService extends ShardLocator, IDataService, Remote {
        
}
