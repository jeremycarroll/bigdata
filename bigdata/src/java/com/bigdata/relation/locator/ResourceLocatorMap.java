/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 30, 2008
 */

package com.bigdata.relation.locator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.bigdata.relation.IRelation;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IIndexManager;

/**
 * A mapping between {@link String}s and {@link IResourceLocator}s.
 * This can be used to locate local, temporary or virtual relations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated this is only useful if explicitly enumerating the resource locators.
 */
public class ResourceLocatorMap<T extends ILocatableResource> implements IResourceLocator<T> {

    /**
     * 
     */
    private final Map<String, IResourceLocator<T>> resourceLocators = new ConcurrentHashMap<String, IResourceLocator<T>>();

    /**
     * De-serialization ctor.
     */
    public ResourceLocatorMap() {
        
    }
        
    /**
     * Add a mapping from an {@link String} to the
     * {@link IResourceLocator} for the identified {@link IRelation}.
     * 
     * @param relationName
     *            The relation name.
     * @param relationLocator
     *            The locator.
     */
    public void add(String relationName, IResourceLocator<T> relationLocator) {
        
        if (relationName == null)
            throw new IllegalArgumentException();

        if (relationLocator == null)
            throw new IllegalArgumentException();
        
        resourceLocators.put(relationName, relationLocator);
        
    }

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    public T locate(String relationName, long timestamp) {
    public T locate(IIndexManager indexManager,
                    IConcurrencyManager concurrencyManager,
                    IBigdataDiscoveryManagement discoveryManager,
                    String relationName,
                    long timestamp)
    {
//BTM - PRE_CLIENT_SERVICE - END

        if (relationName == null)
            throw new IllegalArgumentException();
        
        IResourceLocator<T> relationLocator = resourceLocators.get(relationName);
        
        if (relationLocator == null) {

            throw new IllegalArgumentException("Unknown relation: "+relationName);

        }

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE        return relationLocator.locate(relationName, timestamp);
        return relationLocator.locate(indexManager,
                                      concurrencyManager,
                                      discoveryManager,
                                      relationName,
                                      timestamp);
//BTM - PRE_CLIENT_SERVICE - END

    }

}
