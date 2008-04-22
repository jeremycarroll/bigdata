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
 * Created on Apr 1, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.httpd.CounterSetHTTPD;
import com.bigdata.journal.ITimestampService;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.resources.ResourceManager.Options;
import com.bigdata.util.InnerCause;
import com.bigdata.util.httpd.AbstractHTTPD;

/**
 * Integration provides a view of a local {@link DataService} as if it were a
 * federation.
 * 
 * @see LocalDataServiceClient
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LocalDataServiceFederation extends AbstractFederation {

    private TimestampService timestampService;
    private LoadBalancerService loadBalancerService;
    private DataService dataService;
    private AbstractHTTPD httpd;
    
    /**
     * 
     */
    public LocalDataServiceFederation(LocalDataServiceClient client) {
        
        super(client);

        final Properties properties = client.getProperties();
        
        timestampService = new EmbeddedTimestampService(UUID.randomUUID(),
                properties);
        
        loadBalancerService = new EmbeddedLoadBalancerService(
                UUID.randomUUID(), properties);
        
        /*
         * Note: The embedded data service does not support scale-out indices.
         * Use an embedded or distributed federation for that.
         * 
         * @todo the UUID of the data service might be best persisted with the
         * data service in case anything comes to rely on it, but as far as I
         * can tell nothing does or should.
         */

        // Disable overflow.
        properties.setProperty(Options.OVERFLOW_ENABLED,"false");
        
        dataService = new EmbeddedDataService(UUID.randomUUID(), properties) {

            @Override
            public IDataService getDataService(UUID serviceUUID) {
                
                return LocalDataServiceFederation.this.getDataService(serviceUUID);
                
            }

            @Override
            public IMetadataService getMetadataService() {

                throw new UnsupportedOperationException();
                
            }

            @Override
            public ILoadBalancerService getLoadBalancerService() {

                return loadBalancerService;
                
            }
            
            public ITimestampService getTimestampService() {
                
                return timestampService;
                
            }
            
        };

        /*
         * HTTPD service reporting out statistics. This will be shutdown with
         * the federation.
         */
        {
            
            final int port = Integer.parseInt(properties.getProperty(
                    LocalDataServiceClient.Options.HTTPD_PORT,
                    LocalDataServiceClient.Options.DEFAULT_HTTPD_PORT));

            log.info(LocalDataServiceClient.Options.HTTPD_PORT + "=" + port);

            if (port < 0)
                throw new RuntimeException(
                        LocalDataServiceClient.Options.HTTPD_PORT
                                + " may not be negative");
            
            AbstractHTTPD httpd = null;
            if (port != 0) {
                try {
                    httpd = new CounterSetHTTPD(port,(CounterSet)dataService.getCounters());
                } catch (IOException e) {
                    log.error("Could not start httpd on port=" + port, e);
                }
            }
            this.httpd = httpd;
            
        }
        
    }

    /**
     * Extended for type-safe return.
     */
    public LocalDataServiceClient getClient() {

        return (LocalDataServiceClient) super.getClient();
        
    }
    
    /**
     * Returns an array containing the {@link UUID} of the local {@link IDataService}.
     */
    public UUID[] getDataServiceUUIDs(int maxCount) {
        
        assertOpen();
        
        try {
        
            return new UUID[]{dataService.getServiceUUID()};
            
        } catch (IOException ex) {
            
            // Note: Should never be thrown since this is a local method call.
            throw new RuntimeException(ex);
            
        }
        
    }
    
    /**
     * Returns the local {@link IDataService}.
     */
    public IDataService getAnyDataService() {

        assertOpen();
        
        return dataService;
        
    }

    /**
     * Return the local {@link IDataService}.
     */
    public IDataService getDataService() {
        
        assertOpen();
        
        return dataService;
        
    }

    /**
     * Return the {@link UUID} that identifies the local {@link IDataService}.
     */
    public UUID getDataServiceUUID() {

        assertOpen();
        
        try {
            
            return dataService.getServiceUUID();
            
        } catch (IOException ex) {
            
            // Note: Should never be thrown since this is a local method call.
            throw new RuntimeException(ex);
            
        }

    }
    
    /**
     * Registers an index that does not support scale-out.
     */
    public void registerIndex(IndexMetadata metadata) {
        
        assertOpen();
        
        try {

            registerIndex(metadata,getDataServiceUUID());
            
        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }
        
    }

    /**
     * Registers an index that does not support scale-out.
     */
    public UUID registerIndex(IndexMetadata metadata, UUID dataServiceUUID) {
        
        assertOpen();

        try {

            dataService.registerIndex(metadata.getName(), metadata);
            
        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }
        
        return metadata.getIndexUUID();
    }

    /**
     * Since key-range partitioned indices are not supported this method will
     * log a warning and register the index on the local {@link IDataService} as
     * an unpartitioned index.
     */
    public UUID registerIndex(IndexMetadata metadata, byte[][] separatorKeys,
            UUID[] dataServiceUUIDs) {

        log.warn("key-range partitioned indices not supported: "+metadata.getName());
        
        registerIndex(metadata);
        
        return metadata.getIndexUUID();
        
    }

    public void dropIndex(String name) {

        assertOpen();

        try {
            
            dataService.dropIndex(name);
            
            dropIndexFromCache(name);
            
        } catch (Exception e) {

            throw new RuntimeException(e);
            
        }
        
    }

    synchronized public IIndex getIndex(String name, long timestamp) {

        assertOpen();

        final NT nt = new NT(name, timestamp);

        // check the cache.
        IClientIndex ndx = indexCache.get(nt);

        if (ndx == null) {

            try {

                // test for existence.
                dataService.getIndexMetadata(name, timestamp);

            } catch (Exception ex) {

                if (InnerCause.isInnerCause(ex, NoSuchIndexException.class)) {

                    return null;

                }

                throw new RuntimeException(ex);
                
            }
            
            // exists, so create view.
            ndx = new DataServiceIndex( name, timestamp, dataService );
            
            // put view in the cache.
            indexCache.put(nt,ndx,false/*dirty*/);

        }
        
        // return view.
        return ndx;
        
    }

    /**
     * @throws UnsupportedOperationException
     *             always since the metadata index is not used.
     */
    public IMetadataService getMetadataService() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * @throws UnsupportedOperationException
     *             always
     */
    public IMetadataIndex getMetadataIndex(String name, long timestamp) {

        throw new UnsupportedOperationException();
        
    }

    public ILoadBalancerService getLoadBalancerService() {

        assertOpen();

        return loadBalancerService;
        
    }
    
    public ITimestampService getTimestampService() {

        assertOpen();

        return timestampService;
        
    }

    /**
     * Returns the embedded data service IFF the given serviceUUID is
     * the UUID for the embedded data service and <code>null</code>
     * otherwise.
     */
    public IDataService getDataService(UUID serviceUUID) {

        assertOpen();
        
        if (serviceUUID == null)
            throw new IllegalArgumentException();
        
        if (getDataServiceUUID().equals(serviceUUID)) {
            
            return dataService;
            
        }

        return null; 
        
    }

    /**
     * Return <code>false</code>.
     */
    public boolean isScaleOut() {
        
        return false;
        
    }
    
    /**
     * Extended to shutdown the embedded {@link DataService}.
     */
    synchronized public void shutdown() {
        
        super.shutdown();
        
        dataService.shutdown();

        dataService = null;
        
        loadBalancerService.shutdown();

        loadBalancerService = null;
        
        timestampService.shutdown();

        timestampService = null;
        
        if (httpd != null)
            httpd.shutdown();
        
    }
    
    /**
     * Extended to shutdown the embedded {@link DataService}.
     */
    synchronized public void shutdownNow() {

        super.shutdownNow();
        
        dataService.shutdownNow();

        dataService = null;
        
        loadBalancerService.shutdownNow();

        loadBalancerService = null;

        timestampService.shutdownNow();

        timestampService = null;
        
        if (httpd != null)
            httpd.shutdownNow();

    }
    
    public void destroy() {

        try {
            
            dataService.destroy();
            
        } catch (IOException e) {

            throw new RuntimeException(e);
            
        }
        
        dataService = null;
        
        loadBalancerService.shutdownNow();
        
        loadBalancerService = null;
     
        timestampService.shutdownNow();
        
        timestampService = null;
        
        if (httpd != null)
            httpd.shutdownNow();
     
    }
    
}
