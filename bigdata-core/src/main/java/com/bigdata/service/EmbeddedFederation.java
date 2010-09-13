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
 * Created on Jul 25, 2007
 */

package com.bigdata.service;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ResourceLockService;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.service.EmbeddedClient.Options;

//BTM
import com.bigdata.journal.TransactionService;
import com.bigdata.loadbalancer.EmbeddedLoadBalancer;
import com.bigdata.metadata.EmbeddedShardLocator;
import com.bigdata.transaction.EmbeddedTransactionService;
import net.jini.lookup.ServiceDiscoveryManager;

/**
 * An implementation that uses an embedded database rather than a distributed
 * database. An embedded federation runs entirely in process, but uses the same
 * {@link DataService} and shard locator service implementations as a
 * distributed federation. All services reference the {@link EmbeddedFederation}
 * and use the same thread pool for most operations. However, the
 * {@link EmbeddedDataServiceImpl} has its own {@link WriteExecutorService}.
 * Unlike a distributed federation, an embedded federation starts and stops with
 * the client. An embedded federation may be used to assess or remove the
 * overhead of network operations, to simplify testing of client code, or to
 * deploy a scale-up (vs scale-out) solution.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Put the services into directories named by the service class, e.g.,
 *       MetadataService, just like scale-out.
 * 
 * @todo The EDS/LDS should use their own options in their own namespace to
 *       specify the data directory for the federation (they could just use a
 *       jini configuration). Ditto for the "transient" or "createTempFile"
 *       properties. Everything is namespaced now and the overridden semantics
 *       of com.bigdata.journal.Options.CREATE_TEMP_FILE and
 *       StoreManager#DATA_DIR are just getting us into trouble. Look at all
 *       uses of these options in the unit tests and decouple them from the
 *       journal's options.
 */
public class EmbeddedFederation<T> extends AbstractScaleOutFederation<T> {

    /**
     * Text of the warning message used when a file or directory could not be
     * deleted during {@link #destroy()}.
     */
    private static final String ERR_COULD_NOT_DELETE = "Could not delete: ";

    /**
     * The name of the file used to mark an MDS vs DS service.
     */
    static private final String MDS = ".mds";

    /**
     * The #of data service instances.
     */
    final int ndataServices;
    
    /**
     * True if the federation is not backed by disk.
     */
    private final boolean isTransient;
    
    /**
     * The directory in which the data files will reside. Each directory
     * is named for the service {@link UUID} - restart depends on this.
     */
    private final File dataDir;
    
    /**
     * The directory in which the data files will reside. Each directory
     * is named for the service {@link UUID} - restart depends on this.
     */
    public final File getDataDir() {
        
        return dataDir;
        
    }
    
    /**
     * The (in process) {@link AbstractTransactionService}.
     */
//BTM    private final AbstractTransactionService abstractTransactionService;
private final TransactionService transactionService;
private final EmbeddedTxnServiceImplRemote remoteTxnSrvc;
private final EmbeddedTxnServiceImpl txnSrvc;
    
    /** The (in process) {@link IResourceLockService} */
    private final ResourceLockService resourceLockManager;
    
    /**
     * The (in process) load balancer service.
     */
//BTM    private final LoadBalancerService loadBalancerService;
private final LoadBalancer loadBalancerService;
private final EmbeddedLoadBalancerImplRemote remoteLbs;
private final EmbeddedLoadBalancerImpl lbs;
    
    /**
     * The (in process) shard locator servcie.
     * <p>
     * Note: Not final because not initialized in the constructor.
     */
//BTM    private MetadataService metadataService;
private ShardLocator metadataService;
private EmbeddedMetadataService remoteMds;
private EmbeddedShardLocatorImpl mds;

    /**
     * The (in process) {@link DataService}s.
     * <p>
     * Note: Not final because not initialized in the constructor.
     */
    private DataService[] dataService;
    
    /**
     * Map providing lookup of the (in process) {@link DataService}s by service
     * UUID.
     */
//BTM    private Map<UUID,DataService> dataServiceByUUID = new HashMap<UUID,DataService>();
private Map<UUID,IDataService> dataServiceByUUID = new HashMap<UUID,IDataService>();

    /**
     * Return true if the federation is not backed by disk.
     */
    public boolean isTransient() {
    
        return isTransient;
        
    }
    
    public EmbeddedClient<T> getClient() {
        
        return (EmbeddedClient<T>) super.getClient();
        
    }

    /**
     * The (in process) transaction service.
     */
//BTM    final public ITransactionService getTransactionService() {
final public TransactionService getTransactionService() {

        // Note: return null if service not available/discovered.
        
//BTM        return abstractTransactionService;
return transactionService;
        
    }
    
    /**
     * The (in process) {@link IResourceLockService}.
     */
    final public IResourceLockService getResourceLockService() {
        
        return resourceLockManager;
        
    }
    
    /**
     * Returns the (in process) load balancer service.
     */
//BTM    final public ILoadBalancerService getLoadBalancerService() {
final public LoadBalancer getLoadBalancerService() {

        // Note: return null if service not available/discovered.

        return loadBalancerService;
        
    }
    
    /**
     * The (in process) shard locator service.
     */
//BTM    final public IMetadataService getMetadataService() {
final public ShardLocator getMetadataService() {

        // Note: return null if service not available/discovered.

        return metadataService;
        
    }

    /**
     * Return the (in process) data service given its service UUID.
     * 
     * @param serviceUUID
     * 
     * @return The {@link DataService} for that UUID or <code>null</code> if
     *         there is no data service instance with that service UUID.
     */
    final public IDataService getDataService(final UUID serviceUUID) {

        // Note: return null if service not available/discovered.

        return dataServiceByUUID.get(serviceUUID);
        
    }
    
    /**
     * The #of configured data services in the embedded federation.
     */
    final public int getDataServiceCount() {
        
        return ndataServices;
        
    }
    
    /**
     * There are {@link #getDataServiceCount()} data services defined in the
     * federation. This returns the data service with that index.
     * 
     * @param index
     *            The index.
     * 
     * @return The data service at that index.
     */
    final public DataService getDataService(final int index) {
        
        assertOpen();

        return dataService[index];
        
    }
    
    final public UUID[] getDataServiceUUIDs(final int maxCount) {

        assertOpen();

        if (maxCount < 0)
            throw new IllegalArgumentException();
        
        final int n = maxCount == 0 ? ndataServices : Math.min(maxCount,
                ndataServices);
        
        final UUID[] uuids = new UUID[ n ];
        
        for(int i=0; i<n; i++) {
            
            uuids[i] = getDataService( i ).getServiceUUID();
            
        }
        
        return uuids;
        
    }

    final public IDataService getAnyDataService() {
        
        return getDataService(0);
        
    }

//    /**
//     * There are no preconditions for a service start.
//     */
//    @Override
//    protected boolean awaitPreconditions(long timeout, TimeUnit unit)
//            throws InterruptedException {
//
//        return true;
//
//    }
    
    /**
     * Start or restart an embedded bigdata federation.
     * 
     * @param client
     *            The client.
     */
    protected EmbeddedFederation(final EmbeddedClient<T> client) {
        
        super(client);
        
        final Properties properties = client.getProperties();
        
        // true iff the federation is diskless.
        isTransient = BufferMode.Transient.toString().equals(
                properties.getProperty(Options.BUFFER_MODE));
        
        if (log.isInfoEnabled())
            log.info("federation is "+(isTransient?"not ":"")+"persistent");
        
        // true if temp files are being requested.
        final boolean createTempFile = Boolean.parseBoolean(properties
                .getProperty(Options.CREATE_TEMP_FILE,
                        ""+Options.DEFAULT_CREATE_TEMP_FILE));

boolean serviceImplRemote = ( (null != properties.getProperty(EmbeddedClient.Options.SERVICE_IMPL_REMOTE)) ? true : false);
System.out.println("\n*** serviceImplRemote = "+serviceImplRemote);
        /*
         * The directory in which the data files will reside.
         */
        if (isTransient) {

            // No data directory.
            dataDir = null;

        } else {

            if (createTempFile) {

                // files will be created in a temporary directory.
                final File tmpDir = new File(properties.getProperty(
                        Options.TMP_DIR, System.getProperty("java.io.tmpdir")));
                
                try {

                    // create temp file.
                    dataDir = File.createTempFile("bigdata", ".fed", tmpDir);
                    
                    // delete temp file.
                    dataDir.delete();
                    
                    // re-create as directory.
                    dataDir.mkdir();
                    
                } catch (IOException e) {

                    throw new RuntimeException(e);
                    
                }

                // unset this property so that it does not propagate to the journal ctor.
                properties.setProperty(Options.CREATE_TEMP_FILE, "false");
                
            } else {

                String val = properties.getProperty(Options.DATA_DIR);
                
                if (val == null) {

                    throw new RuntimeException("Required property: "
                            + Options.DATA_DIR);
                    
                }
                
                dataDir = new File(val);
                
            }

            if (log.isInfoEnabled())
                log.info(Options.DATA_DIR + "=" + dataDir);

            if (!dataDir.exists()) {

                if (!dataDir.mkdirs()) {

                    throw new RuntimeException("Could not create directory: "
                            + dataDir.getAbsolutePath());

                }

            }

            if (!dataDir.isDirectory()) {

                throw new RuntimeException(
                        "Regular file exists with that name: "
                                + dataDir.getAbsolutePath());

            }
            
        }

final String hostname = AbstractStatisticsCollector.fullyQualifiedHostName;
        
        /*
         * Start the transaction service.
         */
        {

//BTM*** ------------------------------------------------------------------------------
            final Properties p = new Properties(properties);
            
            if (isTransient) {

                // disable snapshots
//BTM                p.setProperty(
//BTM                                EmbeddedTransactionServiceImpl.Options.SHAPSHOT_INTERVAL,
//BTM                                "0");
p.setProperty(EmbeddedTxnServiceImplRemote.Options.SNAPSHOT_INTERVAL, "0");
p.setProperty(EmbeddedTxnServiceImpl.Options.SNAPSHOT_INTERVAL, "0");
                
            } else {
                
                // specify the data directory for the txService.
//BTM                p.setProperty(EmbeddedTransactionServiceImpl.Options.DATA_DIR,
//BTM                        new File(dataDir, "txService").toString());
p.setProperty(EmbeddedTxnServiceImplRemote.Options.DATA_DIR, new File(dataDir, "txService").toString());
            }

//BTM - Note: in EmbeddedTransactionService, if snapshotInterval is 0, then it's considered transient, and dataDir is set to null,
//BTM -       but the dataDir system property is always set here because EmbeddedTransactionService expects it to be non-null in
//BTM -       the constructor.
p.setProperty(EmbeddedTxnServiceImpl.Options.DATA_DIR, new File(dataDir, "txService").toString());                
p.setProperty(EmbeddedTxnServiceImpl.Options.THREAD_POOL_SIZE, EmbeddedTxnServiceImpl.Options.DEFAULT_THREAD_POOL_SIZE);
            
            if(serviceImplRemote) {
//BTM            abstractTransactionService = new EmbeddedTransactionServiceImpl(
//BTM                    UUID.randomUUID(), p).start();
                transactionService = new EmbeddedTxnServiceImplRemote(UUID.randomUUID(), p).start();
                remoteTxnSrvc = (EmbeddedTxnServiceImplRemote)transactionService;
                txnSrvc = null;
            } else {
                transactionService =
                    new EmbeddedTxnServiceImpl
                            (UUID.randomUUID(),
                             hostname,
                             null,//SDM - replace with real SDM after conversion to smart proxy?
                             dataServiceByUUID,
                             p);

                remoteTxnSrvc = null;
                txnSrvc = (EmbeddedTxnServiceImpl)transactionService;
            }
System.out.println("*** serviceImplRemote = "+serviceImplRemote+" >>> remoteTxnSrvc = "+remoteTxnSrvc);
System.out.println("*** serviceImplRemote = "+serviceImplRemote+" >>> txnSrvc       = "+txnSrvc);
//BTM*** ------------------------------------------------------------------------------
        }

        /*
         * Start the lock manager.
         */
        resourceLockManager = new ResourceLockService();
        
//BTM*** For now, move the creation of the loadBalancerService to a
//BTM*** point below AFTER the creation of the dataServices, so that the
//BTM*** dataServiceByUUID map, when passed into the EmbeddedLoadBalancer,
//BTM*** is populated with those created dataServices, and the
//BTM*** EmbeddedLoadBalancer can then "discover" those DataServices.
//BTM*** But once the DataService is converted to a smart proxy model
//BTM*** and the shard.ServiceImpl/EmbeddedDataService instantiated below
//BTM*** registers with the lookup service, the dataServiceByUUID map
//BTM*** can be removed and a non-null SDM can be used by the
//BTM*** loadBalancerService to actually discover that DataService.

//BTM        {
//BTM
//BTM            final Properties p = new Properties(properties);
//BTM            
//BTM            if (isTransient) {
//BTM
//BTM //BTM                p.setProperty(LoadBalancerService.Options.TRANSIENT, "true");
//BTM p.setProperty(EmbeddedLoadBalancerServiceImpl.Options.TRANSIENT, "true");
//BTM
//BTM            } else {
//BTM                
//BTM                // specify the data directory for the load balancer.
//BTM
//BTM                p.setProperty(EmbeddedLoadBalancerServiceImpl.Options.LOG_DIR,
//BTM                        new File(dataDir, "lbs").toString());
//BTM                
//BTM            }
//BTM
            /*
             * Start the load balancer.
             */
//BTM            try {
//BTM         
//BTM                loadBalancerService = new EmbeddedLoadBalancerServiceImpl(UUID
//BTM                        .randomUUID(), p).start();
//BTM            
//BTM            } catch (Throwable t) {
//BTM            
//BTM                log.error(t, t);
//BTM                
//BTM                throw new RuntimeException(t);
//BTM                
//BTM            }
//BTM
//BTM        }


//BTM
UUID mdsServiceUUID = null;
Properties mdsProperties = null;
File mdsServiceDir = null;
        /*
         * The directory in which the data files will reside.
         */
        if (isTransient) {

            /*
             * Always do first time startup since there is no persistent state.
             */
//BTM            ndataServices = createFederation(properties, isTransient);
ndataServices = createFederation(properties, isTransient, serviceImplRemote);//BTM - create metadata only if remote

        } else {

            /*
             * Persistent (re-)start.
             * 
             * Look for pre-existing (meta)data services. Each service stores
             * its state in a subdirectory named by the serviceUUID. We
             * recognize these directories by the ability to parse the directory
             * name as a UUID (the embedded services do not store a service.id
             * file - that is just the jini services). In addition a ".mds" file
             * is created in the service directory that corresponds to the
             * metadata service.
             */

            final File[] serviceDirs = dataDir.listFiles(new FileFilter() {

                public boolean accept(File pathname) {
                    
                    if(!pathname.isDirectory()) {
                        
                        if(log.isInfoEnabled())
                            log.info("Ignoring normal file: "+pathname);
                        
                        return false;
                        
                    }
                    
                    final String name = pathname.getName();
                    
                    try {
                        
                        UUID.fromString(name);

                        if (log.isInfoEnabled())
                            log.info("Found service directory: " + pathname);

                        return true;

                    } catch (IllegalArgumentException ex) {

                        if (log.isInfoEnabled())
                            log.info("Ignoring directory: " + pathname);

                        return false;

                    }

                }
                
            });
            
            if (serviceDirs.length == 0) {
                
                /*
                 * First time startup.
                 */
//BTM                ndataServices = createFederation(properties,isTransient);
ndataServices = createFederation(properties, isTransient, serviceImplRemote);//BTM - create metadata only if remote

            } else {

                /*
                 * Reload services from disk.
                 */

                // expected #of data services.
                dataService = new DataService[serviceDirs.length - 1];

                int ndataServices = 0;
                int nmetadataServices = 0;

                for(File serviceDir : serviceDirs ) {
                    
                    final UUID serviceUUID = UUID.fromString(serviceDir.getName());

                    final Properties p = new Properties(properties);

                    /*
                     * Note: Use DATA_DIR if the metadata service is using a
                     * ResourceManager and FILE if it is using a simple Journal.
                     */
                    p.setProperty(MetadataService.Options.DATA_DIR, serviceDir.toString());
//                    p.setProperty(Options.FILE, new File(serviceDir,"journal"+Options.JNL).toString());


                    if(new File(serviceDir,MDS).exists()) {
                        
                        /*`
                         * metadata service.
                         */

if(serviceImplRemote) {//BTM - BEGIN --------------------------------------------------------------
                        metadataService = new EmbeddedMetadataService(this, serviceUUID, p).start();
//BTM
remoteMds = (EmbeddedMetadataService)metadataService;
mds = null;
                        
                        nmetadataServices++;
                        
                        if (nmetadataServices > 1) {

                            throw new RuntimeException(
                                    "Not expecting more than one metadata service");
                            
                        }
} else {// !serviceImplRemote - BTM
    mdsServiceUUID = serviceUUID;
    mdsProperties  = p;
    mdsServiceDir  = serviceDir;
}//srviceImplRemote: BTM - END -------------------------------------------------------------------
                        
                    } else {
                        
                        /*
                         * data service.
                         */
                        
                        final DataService dataService = new EmbeddedDataServiceImpl(
                                serviceUUID, p).start();

                        if (ndataServices == this.dataService.length) {

                            throw new RuntimeException(
                                    "Too many data services?");
                            
                        }

                        this.dataService[ndataServices++] = dataService;
                     
                        dataServiceByUUID.put(serviceUUID, dataService);
                        
                    }
                    
                }

                assert ndataServices == this.dataService.length;
                
                this.ndataServices = ndataServices;

            }

        }

//BTM - BEGIN LoadBalancer BLOCK ------------------------------------------------------------
        {
//BTM            final String hostname = AbstractStatisticsCollector.fullyQualifiedHostName;

            final Properties p = new Properties(properties);
            
            if (isTransient) {

                p.setProperty(LoadBalancerService.Options.TRANSIENT, "true");
                p.setProperty(EmbeddedLoadBalancer.Options.TRANSIENT, "true");

                p.setProperty(EmbeddedLoadBalancerImpl.Options.LOG_DIR,
                              new File
                    (EmbeddedLoadBalancerImpl.Options.DEFAULT_LOG_DIR).toString());
            } else {
                // specify the data directory for the load balancer.
                p.setProperty(EmbeddedLoadBalancerImplRemote.Options.LOG_DIR,
                        new File(dataDir, "lbs").toString());
                p.setProperty(EmbeddedLoadBalancerImpl.Options.LOG_DIR,
                        new File(dataDir, "lbs").toString());
            }

            if(serviceImplRemote) {
                try {
                    loadBalancerService = 
                        new EmbeddedLoadBalancerImplRemote
                                (UUID.randomUUID(), p).start();
                } catch (Throwable t) {
                    log.error(t, t);
                    throw new RuntimeException(t);
                }
                remoteLbs = (EmbeddedLoadBalancerImplRemote)loadBalancerService;
                lbs = null;
            } else {
                loadBalancerService = 
                    new EmbeddedLoadBalancerImpl
                            (UUID.randomUUID(),
                             hostname,
                             null,//SDM - replace with real SDM after conversion to smart proxy?
//BTM*** EmbeddedDataService.this,
//BTM*** remove after EmbeddedDataService is converted to smart proxy
                             dataServiceByUUID,
                             p);
                remoteLbs = null;
                lbs = (EmbeddedLoadBalancerImpl)loadBalancerService;
            }
System.out.println("*** serviceImplRemote = "+serviceImplRemote+" >>> remoteLbs = "+remoteLbs);
System.out.println("*** serviceImplRemote = "+serviceImplRemote+" >>> lbs       = "+lbs);


//BTM*** ------------------------------------------------------------------------------
//BTM - BEGIN - start metadata (shard locator) service after the dataServiceByUUID map has been populated and the LBS has been created
if(!serviceImplRemote) {//BTM - BEGIN --------------------------------------------------------------
    if(mdsServiceUUID == null) {
        mdsServiceUUID = UUID.randomUUID();
    }
    if(mdsProperties == null) {
        mdsProperties = new Properties(properties);
    }
    if(mdsServiceDir == null) {
        if (!isTransient) {
            mdsServiceDir = new File(dataDir, mdsServiceUUID.toString());
            mdsServiceDir.mkdirs();
            try {
                new RandomAccessFile(new File(mdsServiceDir, MDS), "rw").close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            mdsProperties.setProperty(EmbeddedShardLocatorImpl.Options.DATA_DIR, mdsServiceDir.toString());
        }
    }
    mdsProperties.setProperty(EmbeddedShardLocatorImpl.Options.THREAD_POOL_SIZE, EmbeddedShardLocatorImpl.Options.DEFAULT_THREAD_POOL_SIZE);
    metadataService = new EmbeddedShardLocatorImpl
                            (mdsServiceUUID,
                             hostname,
                             null,//SDM - replace with real SDM after conversion to smart proxy?
                             transactionService,
                             loadBalancerService,
                             dataServiceByUUID,
                             mdsProperties);
    remoteMds = null;
    mds = (EmbeddedShardLocatorImpl)metadataService;
}//!srviceImplRemote: BTM - END -------------------------------------------------------------------

//BTM - END   - start metadata (shard locator) service after the dataServiceByUUID map has been populated
//BTM*** ------------------------------------------------------------------------------


            /*
             * Have the data services join the load balancer.
             */
            for (IDataService ds : this.dataService) {
                try {
                    if(remoteLbs != null) {
                        remoteLbs.join(ds.getServiceUUID(), 
                                       ds.getServiceIface(),
                                       hostname);
                    } else {
                        lbs.join(ds.getServiceUUID(),
                                 ds.getServiceIface(), 
                                 ds.getServiceName(),
                                 hostname);
                    }
                } catch (IOException e) {
                    // Should never be thrown for an embedded service.
                    log.warn(e.getMessage(), e);
                }
            }

            /*
             * Other service joins.
             */
            if(remoteLbs != null) {
                remoteLbs.join
                    (remoteTxnSrvc.getServiceUUID(),
                     remoteTxnSrvc.getServiceIface(), 
                     hostname);
                remoteLbs.join(remoteLbs.getServiceUUID(),
                               remoteLbs.getServiceIface(), 
                               hostname);
                remoteLbs.join(remoteMds.getServiceUUID(),
                               remoteMds.getServiceIface(), 
                               hostname);
            } else {//smart proxy
                lbs.join
                    (txnSrvc.getServiceUUID(),
                     txnSrvc.getServiceIface(), 
                     (txnSrvc.getServiceUUID()).toString(),
                     hostname);
                lbs.join(lbs.getServiceUUID(),
                         lbs.getServiceIface(), 
                         (lbs.getServiceUUID()).toString(),
                         hostname);
                lbs.join(mds.getServiceUUID(),
                         mds.getServiceIface(), 
                         (mds.getServiceUUID()).toString(),
                         hostname);
            }

        }
//BTM - END LoadBalancer BLOCK ------------------------------------------------------------

    }

    /**
     * Create a new federation.
     * 
     * @param properties
     * 
     * @return The #of created data services.
     * 
     * FIXME The embedded federation setup is not correct when transient buffers
     * are requested. We wind up creating the resource manager files in the
     * current working directory when we really want the resource manager to
     * startup with transient journals (and disallow overflow since you can not
     * re-open a store or even write an index segment).
     */
    private int createFederation(final Properties properties,
                                 final boolean isTransient,
//BTM
                                 final boolean serviceImplRemote)
    {

        final int ndataServices;
        
        /*
         * The #of data services (used iff this is a 1st time start).
         */
        {

            final String val = properties.getProperty(Options.NDATA_SERVICES,
                    Options.DEFAULT_NDATA_SERVICES);

            ndataServices = Integer.parseInt(val);

            if (ndataServices <= 0) {

                throw new IllegalArgumentException(Options.NDATA_SERVICES + "="
                        + val);

            }
        
        }

        /*
         * Start the metadata service.
         */
if(serviceImplRemote) {//BTM - BEGIN --------------------------------------------------------------
        {

            final Properties p = new Properties(properties);
            
            final UUID serviceUUID = UUID.randomUUID();

            if (!isTransient) {

                final File serviceDir = new File(dataDir, serviceUUID.toString());

                serviceDir.mkdirs();

                /*
                 * Create ".mds" file to mark this as the metadata service
                 * directory.
                 */
                try {

                    new RandomAccessFile(new File(serviceDir, MDS), "rw")
                            .close();

                } catch (IOException e) {

                    throw new RuntimeException(e);

                }

                p.setProperty(MetadataService.Options.DATA_DIR, serviceDir.toString());

            }
            
            metadataService = new EmbeddedMetadataService(this, serviceUUID, p)
                    .start();
//BTM
remoteMds = (EmbeddedMetadataService)metadataService;
mds = null;
            
        }
}//srviceImplRemote: BTM - END -------------------------------------------------------------------
        
        /*
         * Start the data services.
         */
        {

            dataService = new DataService[ndataServices];

            for (int i = 0; i < ndataServices; i++) {

                final Properties p = new Properties(properties);

                final UUID serviceUUID = UUID.randomUUID();

                if (!isTransient) {

                    final File serviceDir = new File(dataDir, serviceUUID
                            .toString());

                    serviceDir.mkdirs();

                    p.setProperty(DataService.Options.DATA_DIR, serviceDir
                            .toString());

                }

                dataService[i] = new EmbeddedDataServiceImpl(serviceUUID, p)
                        .start();

                dataServiceByUUID.put(serviceUUID, dataService[i]);

            }

        }

//BTM - BEGIN - start metadata (shard locator) service after the dataServiceByUUID map has been populated
/* *********************************************************************************************************************
//BTM - DON'T START META_DATA SERVICE HERE? Do it outside of this method?
        {// BEGIN - Start the shard locator (metadata) service AFTER the data services so the dataServiceByUUID is populated
            final Properties p = new Properties(properties);
            final UUID serviceUUID = UUID.randomUUID();

            if (!isTransient) {
                final File serviceDir = new File(dataDir, serviceUUID.toString());
                serviceDir.mkdirs();
                // Create ".mds" file to mark this as the metadata service directory.
                try {
                    new RandomAccessFile(new File(serviceDir, MDS), "rw").close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                p.setProperty(MetadataService.Options.DATA_DIR, serviceDir.toString());
                p.setProperty(EmbeddedShardLocatorImpl.Options.DATA_DIR, serviceDir.toString());
            }
p.setProperty(EmbeddedShardLocatorImpl.Options.THREAD_POOL_SIZE, EmbeddedShardLocatorImpl.Options.DEFAULT_THREAD_POOL_SIZE);

            if(serviceImplRemote) {
                metadataService = new EmbeddedMetadataService(this, serviceUUID, p).start();
                remoteMds = (EmbeddedMetadataService)metadataService;
                mds = null;
            } else {
                metadataService = new EmbeddedShardLocatorImpl
                            (serviceUUID,
                             hostname,
                             null,//SDM - replace with real SDM after conversion to smart proxy?
                             embeddedTxnService,
                             embeddedLbs,
                             dataServiceByUUID,
                             p);
                remoteMds = null;
                mds = (EmbeddedShardLocatorImpl)metadataService;
            }
        }// END - Start the shard locator (metadata) service AFTER the data services so the dataServiceByUUID is populated
//BTM - END   - start metadata (shard locator) service after the dataServiceByUUID map has been populated
******************************************************************************************************************** */

        return ndataServices;

    }
    
    /**
     * Concrete implementation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class EmbeddedDataServiceImpl extends AbstractEmbeddedDataService {

        /**
         * @param serviceUUID
         * @param properties
         */
        public EmbeddedDataServiceImpl(UUID serviceUUID, Properties properties) {
       
            super(serviceUUID, properties);
            
        }

        @Override
        public EmbeddedFederation<T> getFederation() {

            return EmbeddedFederation.this;

        }

    }
    

//BTM*** ------------------------------------------------------------------------------
    protected class EmbeddedShardLocatorImpl extends EmbeddedShardLocator {
        public EmbeddedShardLocatorImpl(UUID serviceUUID, 
                                        String hostname,
                                        ServiceDiscoveryManager sdm,
                                        TransactionService embeddedTxnService,
                                        LoadBalancer embeddedLbs,
                                        Map<UUID, IDataService> embeddedDataServiceMap,
                                        Properties properties)
        {
            super(serviceUUID,
                  hostname,
                  sdm,
                  embeddedTxnService,
                  embeddedLbs,
                  embeddedDataServiceMap,
                  Integer.parseInt(properties.getProperty(EmbeddedShardLocatorImpl.Options.THREAD_POOL_SIZE)),
                  properties);
        }
    }
//BTM*** ------------------------------------------------------------------------------


//BTM*** ------------------------------------------------------------------------------
    protected class EmbeddedLoadBalancerImplRemote extends AbstractEmbeddedLoadBalancerService {
        
        /**
         * @param serviceUUID
         * @param properties
         */
        public EmbeddedLoadBalancerImplRemote(UUID serviceUUID, Properties properties) {
            super(serviceUUID, properties);
        }

        @Override
        public EmbeddedFederation<T> getFederation() {
            return EmbeddedFederation.this;
        }
    }

    protected class EmbeddedLoadBalancerImpl extends EmbeddedLoadBalancer {
        
        public EmbeddedLoadBalancerImpl(UUID serviceUUID, 
                                        String hostname,
                                        ServiceDiscoveryManager sdm,
                                        Map<UUID, IDataService> dataServiceMap,//BTM - remove once EmbeddedDataService converted?
                                        Properties properties)
        {
            super(serviceUUID, hostname,
                  sdm, dataServiceMap,//BTM*** - remove after DataService smart proxy?
                  properties.getProperty(EmbeddedLoadBalancerImpl.Options.LOG_DIR),
                  properties);
        }
    }
//BTM*** ------------------------------------------------------------------------------
    
//BTM*** ------------------------------------------------------------------------------
//BTM    protected class EmbeddedTransactionServiceImpl extends AbstractEmbeddedTransactionService {
protected class EmbeddedTxnServiceImplRemote extends AbstractEmbeddedTransactionService {

        /**
         * @param serviceUUID
         * @param properties
         */
//BTM        public EmbeddedTransactionServiceImpl(UUID serviceUUID, Properties properties) {
public EmbeddedTxnServiceImplRemote(UUID serviceUUID, Properties properties) {
           
            super(serviceUUID, properties);
            
        }

        @Override
        public EmbeddedFederation<T> getFederation() {

            return EmbeddedFederation.this;

        }
        
//        @Override
//        protected void setReleaseTime(final long releaseTime) {
//            
//            for (DataService ds : dataService) {
//
//                ds.setReleaseTime(releaseTime);
//                
//            }
//            
//        }

    }
    
    protected class EmbeddedTxnServiceImpl extends EmbeddedTransactionService {
        public EmbeddedTxnServiceImpl(UUID serviceUUID, 
                                      String hostname,
                                      ServiceDiscoveryManager sdm,
                                      Map<UUID, IDataService> dataServiceMap,//BTM - change to ShardService?
                                      Properties properties)
        {
            super(serviceUUID,
                  hostname,
                  sdm,
                  dataServiceMap,
                  properties.getProperty(EmbeddedTxnServiceImpl.Options.DATA_DIR),
                  Integer.parseInt(properties.getProperty(EmbeddedTxnServiceImpl.Options.THREAD_POOL_SIZE)),
                  properties);
        }
    }
//BTM*** ------------------------------------------------------------------------------
    
    /**
     * Normal shutdown of the services in the federation.
     */
    synchronized public void shutdown() {
        
        if (log.isInfoEnabled())
            log.info("begin");

        super.shutdown();
        
        if (transactionService != null) {

//BTM            transactionService.shutdown();
if(remoteTxnSrvc != null) {
    remoteTxnSrvc.shutdown();
} else {
    txnSrvc.shutdown();
}

        }

        for (int i = 0; i < dataService.length; i++) {

            if (dataService[i] != null) {

                dataService[i].shutdown();

            }
            
        }

        if (metadataService != null) {

//BTM            metadataService.shutdown();
if(remoteMds != null) {
    remoteMds.shutdown();
} else {
    mds.shutdown();
}
            
        }
        
        if (loadBalancerService != null) {

            if(remoteLbs != null) {
                remoteLbs.shutdown();
            } else {
                lbs.shutdown();
            }

//            loadBalancerService = null;
            
        }
        
//        // Note: don't clear ref until all down since nextTimestamp() still active.
//        abstractTransactionService = null;

        if (log.isInfoEnabled())
            log.info("done");

    }

    /**
     * Immediate shutdown of the services in the embedded federation.
     */
    synchronized public void shutdownNow() {

        if (log.isInfoEnabled())
            log.info("begin");

        super.shutdownNow();

        if (transactionService != null) {

//BTM            transactionService.shutdownNow();
if(remoteTxnSrvc != null) {
    remoteTxnSrvc.shutdownNow();
} else {
    txnSrvc.shutdownNow();
}

        }

        for (int i = 0; i < dataService.length; i++) {

            if (dataService[i] != null) {

                dataService[i].shutdownNow();

            }

        }

//BTM        metadataService.shutdownNow();
if(remoteMds != null) {
    remoteMds.shutdownNow();
} else {
    mds.shutdownNow();
}

        if (loadBalancerService != null) {

            if(remoteLbs != null) {
                remoteLbs.shutdownNow();
            } else {
                lbs.shutdownNow();
            }

//            loadBalancerService = null;
            
        }
        
//        // Note: don't clear ref until all down since nextTimestamp() still active.
//        abstractTransactionService = null;

        if (log.isInfoEnabled())
            log.info("done");

    }

    public void destroy() {

        super.destroy();

//BTM        transactionService.destroy();
if(remoteTxnSrvc != null) {
    remoteTxnSrvc.destroy();
} else {
    txnSrvc.destroy();
}
        
        for (int i = 0; i < dataService.length; i++) {

            if (dataService[i] != null) {

                dataService[i].destroy();
                
            }
 
        }

        if (metadataService != null) {

            // the file flagging this as the MDS rather than a DS.
//BTM            final File tmp = new File(metadataService.getResourceManager().getDataDir(), EmbeddedFederation.MDS);
final File tmp;
if(remoteMds != null) {
    tmp = new File(remoteMds.getResourceManager().getDataDir(), EmbeddedFederation.MDS);
} else {
    tmp = new File(mds.getResourceManager().getDataDir(), EmbeddedFederation.MDS);
}

            if(!tmp.delete()) {

                log.warn(ERR_COULD_NOT_DELETE + tmp);

            }

//BTM            metadataService.destroy();
if(remoteMds != null) {
    remoteMds.destroy();
} else {
    mds.destroy();
}

        }
        if(remoteLbs != null) {
            remoteLbs.destroy();
        } else {
            lbs.destroy();
        }

        if (!isTransient && !dataDir.delete()) {

            log.warn(ERR_COULD_NOT_DELETE + dataDir);
            
        }
        
//        // Note: don't clear ref until all down since nextTimestamp() still active.
//        abstractTransactionService = null;
        
    }
    
    /**
     * Return <code>false</code>.
     */
    final public boolean isDistributed() {
        
        return false;
        
    }
    
    final public boolean isStable() {

        return !isTransient;

    }
    
    /**
     * This scans the {@link DataService}s and reports the most recent value.
     */
    public long getLastCommitTime() {

        assertOpen();
        
        long maxValue = 0;

        // check each of the data services.
        for(int i=0; i<dataService.length; i++) {

            final long commitTime = dataService[i].getResourceManager()
                    .getLiveJournal().getRootBlockView().getLastCommitTime();

            if (commitTime > maxValue) {

                maxValue = commitTime;
                
            }
            
        }
        
        // and also check the metadata service
        {

//BTM            final long commitTime = metadataService.getResourceManager().getLiveJournal().getRootBlockView().getLastCommitTime();
final long commitTime;
if(remoteMds != null) {
    commitTime = remoteMds.getResourceManager().getLiveJournal().getRootBlockView().getLastCommitTime();
} else {
    commitTime = mds.getResourceManager().getLiveJournal().getRootBlockView().getLastCommitTime();
}

            if (commitTime > maxValue) {

                maxValue = commitTime;

            }

        }
        
        return maxValue;
        
    }

    public IDataService getDataServiceByName(final String name) {

        for (IDataService ds : dataService) {

            final String serviceName;
            try {

                serviceName = ds.getServiceName();
                
            } catch (IOException e) {
                
                // note: will not be thrown (local service, no RMI).
                throw new RuntimeException(e);
                
            }
            
            if (name.equals(serviceName)) {

                return ds;

            }

        }
        
        // no match.
        return null;
        
    }

}
