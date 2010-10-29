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
import com.bigdata.shard.EmbeddedShardService;
import com.bigdata.transaction.EmbeddedTransactionService;
import net.jini.lookup.ServiceDiscoveryManager;

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation that uses an embedded database rather than a distributed
 * database. An embedded federation runs entirely in process, but uses the same
 * {@link ShardService} and shard locator service implementations as a
 * distributed federation. All services reference the {@link EmbeddedFederation}
 * and use the same thread pool for most operations. However, the
 * the implementatin of the embedded shard service has its own
 * {@link WriteExecutorService}. Unlike a distributed federation, an embedded
 * federation starts and stops with the client. An embedded federation may be
 * used to assess or remove the overhead of network operations, to simplify
 * testing of client code, or to deploy a scale-up (vs scale-out) solution.
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
private LoadBalancer loadBalancerService = null;
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
     * The (in process) {@link ShardService}s.
     * <p>
     * Note: Not final because not initialized in the constructor.
     */
//BTM    private DataService[] dataService;
private ShardService[] dataService;
private List<EmbeddedDataServiceImpl> remoteDsList = new ArrayList<EmbeddedDataServiceImpl>();
private List<EmbeddedShardServiceImpl> dsList = new ArrayList<EmbeddedShardServiceImpl>();
    
    /**
     * Map providing lookup of the (in process) {@link ShardService}s
     * by service UUID.
     */
//BTM    private Map<UUID,DataService> dataServiceByUUID = new HashMap<UUID,DataService>();
private Map<UUID, ShardService> dataServiceByUUID = new HashMap<UUID, ShardService>();

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
     * @return The {@link ShardService} for that UUID or <code>null</code>
     *         if there is no data service instance with that service UUID.
     */
//BTM    final public IDataService getDataService(final UUID serviceUUID) {
final public ShardService getDataService(final UUID serviceUUID) {

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
     * There are {@link #getDataServiceCount()} data services defined
     * in the federation. This returns the data service with that
     * index.
     * 
     * @param index
     *            The index.
     * 
     * @return The data service at that index.
     */
//BTm    final public DataService getDataService(final int index) {
final public ShardService getDataService(final int index) {
        
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
            
//BTM            uuids[i] = getDataService( i ).getServiceUUID();
ShardService shardService = getDataService(i);
if(shardService instanceof IService) {
    try {
        uuids[i] = ((IService)shardService).getServiceUUID();
    } catch(IOException e) {
        log.warn("failed to retrieve service UUID for shardService["+i+"]", e);
    }
} else {
    uuids[i] = ((Service)shardService).getServiceUUID();
}            
        }
        
        return uuids;
        
    }

//BTM    final public IDataService getAnyDataService() {
final public ShardService getAnyDataService() {
        
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
        {//BEGIN transaction service -------------------------------------

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
                             null,
                             dataServiceByUUID,//not needed ???
                             p);

                remoteTxnSrvc = null;
                txnSrvc = (EmbeddedTxnServiceImpl)transactionService;
            }
System.out.println("*** serviceImplRemote = "+serviceImplRemote+" >>> remoteTxnSrvc = "+remoteTxnSrvc);
System.out.println("*** serviceImplRemote = "+serviceImplRemote+" >>> txnSrvc       = "+txnSrvc);

        }//END transaction service ---------------------------------------

        /*
         * Start the lock manager.
         */
        resourceLockManager = new ResourceLockService();


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
//BTM            try {//Start the load balancer
//BTM         
//BTM                loadBalancerService = new EmbeddedLoadBalancerServiceImpl(UUID.randomUUID(), p).start();
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

        {//BEGIN load balancer (remote and smart proxy) -----------------------
//BTM            final String hostname = AbstractStatisticsCollector.fullyQualifiedHostName;

            final Properties p = new Properties(properties);

            if (isTransient) {

                p.setProperty(LoadBalancerService.Options.TRANSIENT, "true");
                p.setProperty(EmbeddedLoadBalancer.Options.TRANSIENT, "true");
                p.setProperty
                    (EmbeddedLoadBalancerImpl.Options.LOG_DIR,
                new File
                (EmbeddedLoadBalancerImpl.Options.DEFAULT_LOG_DIR).toString());

            } else {//specify the data directory for the load balancer
               
                p.setProperty
                    (EmbeddedLoadBalancerImplRemote.Options.LOG_DIR,
                     new File(dataDir, "lbs").toString());
                p.setProperty
                    (EmbeddedLoadBalancerImpl.Options.LOG_DIR,
                     new File(dataDir, "lbs").toString());
            }

            if(serviceImplRemote) {//remote load balancer ---------------------
                try {
                    loadBalancerService = 
                        new EmbeddedLoadBalancerImplRemote
                                (UUID.randomUUID(), p).start();
                } catch (Throwable t) {
                    log.error(t, t);
                    throw new RuntimeException(t);
                }
                remoteLbs = 
                    (EmbeddedLoadBalancerImplRemote)loadBalancerService;
                lbs = null;

            } else {//smart proxy load balancer -------------------------------

                loadBalancerService = 
                    new EmbeddedLoadBalancerImpl
                            (UUID.randomUUID(),
                             hostname,
                             null,//SDM ==> using embedded federation
                             dataServiceByUUID,//must be set after ds started
                             p);
                remoteLbs = null;
                lbs = (EmbeddedLoadBalancerImpl)loadBalancerService;
            }
System.out.println("*** serviceImplRemote = "+serviceImplRemote+" >>> remoteLbs = "+remoteLbs);
System.out.println("*** serviceImplRemote = "+serviceImplRemote+" >>> lbs       = "+lbs);

        }//END load balancer (remote and smart proxy) -------------------------


        //BEGIN shard service (remote & smart proxy) and remote metadata ------

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
            //Note: createFederation creates only the remote metadata service,
            //      the shard locator service is created after the shard
            //      (data) services are created (either smart proxy or remote)
            ndataServices = 
                createFederation(properties, isTransient,
                                 hostname, serviceImplRemote);
        } else {//!isTransient

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
                //Note: createFederation creates only the remote metadata
                //      service, the shard locator service is created
                //      after the shard (data) services are created
                //      (either smart proxy or remote)
                ndataServices = 
                    createFederation(properties, isTransient,
                                     hostname, serviceImplRemote);

            } else {//serviceDirs.length != 0 ==> restart
                /*
                 * Reload services from disk.
                 */

                // expected #of data services.
//BTM              dataService = new DataService[serviceDirs.length - 1];
                dataService = new ShardService[serviceDirs.length - 1];

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
                        
                        /*
                         * metadata service.
                         */
//BTM
                        if(serviceImplRemote) {// remote metadata -------------
                            metadataService = 
                                new EmbeddedMetadataService
                                        (this, serviceUUID, p).start();
                            remoteMds = 
                                (EmbeddedMetadataService)metadataService;
                            mds = null;
                        
                            nmetadataServices++;
                        
                            if (nmetadataServices > 1) {
                                throw new RuntimeException(
                                    "Not expecting more than one metadata "
                                    +"service");
                            }
                        } else {// smart proxy shard locator (started later)
                            mdsServiceUUID = serviceUUID;
                            mdsProperties  = p;
                            mdsServiceDir  = serviceDir;
                        }//srviceImplRemote -----------------------------------
                        
                    } else {//MDS service dir does NOT exist ==> shard services
 
                        /*
                         * data service.
                         */
                        
                        if(serviceImplRemote) {// remote data service ---------

//BTM                        final DataService dataService = new EmbeddedDataServiceImpl(serviceUUID, p).start();

                            p.setProperty(DataService.Options.DATA_DIR,
                                          serviceDir.toString());//added by BTM
                            ShardService shardService = 
                                             new EmbeddedDataServiceImpl
                                                 (serviceUUID, p).start();
                            EmbeddedDataServiceImpl remoteDs = 
                                (EmbeddedDataServiceImpl)shardService;
                            EmbeddedShardServiceImpl ds = null;

                            if (ndataServices == this.dataService.length) {
                                throw new RuntimeException(
                                        "Too many data services?");
                            }

//BTM                       this.dataService[ndataServices++] = dataService;
//BTM                       dataServiceByUUID.put(serviceUUID, dataService);

                            this.dataService[ndataServices++] = shardService;
                            this.remoteDsList.add(remoteDs);
                            this.dsList.add(ds);
                            dataServiceByUUID.put(serviceUUID, shardService);

                        } else {// !srviceImplRemote: smart proxy shard -------

                            p.setProperty
                                (EmbeddedShardServiceImpl.Options.DATA_DIR,
                                 serviceDir.toString());
                            p.setProperty
                   (EmbeddedShardServiceImpl.Options.THREAD_POOL_SIZE,
                    EmbeddedShardServiceImpl.Options.DEFAULT_THREAD_POOL_SIZE);
                            ShardService shardService = 
                                new EmbeddedShardServiceImpl
                                    (serviceUUID,
                                     hostname,
                                     null,//SDM ==> testing with embedded fed
                                     transactionService,
                                     loadBalancerService,
                                     p);
                            EmbeddedDataServiceImpl remoteDs = null;
                            EmbeddedShardServiceImpl ds = 
                                (EmbeddedShardServiceImpl)shardService;

                            dataService[ndataServices++] = shardService;
                            this.remoteDsList.add(remoteDs);
                            this.dsList.add(ds);
                            dataServiceByUUID.put(serviceUUID, shardService);

                        }//serviceImplRemote ----------------------------------

                    }//endif(MDS serviceDir exists or not)
                    
                }//end loop(serviceDirs)

                assert ndataServices == this.dataService.length;
                this.ndataServices = ndataServices;

            }//endif(serviceDirs.length == or != 0) ==> 1st start or restart

        }//endif(isTransient)
        //END shard service (remote & smart proxy) and remote metadata --------



/////////////////////////////////////////////////////////////////////////////
//BTM*** ----------------------------------------------------------------------
//BTM - BEGIN - start metadata (shard locator) service after the
//BTM -         dataServiceByUUID map has been populated and the
//BTM -         LBS has been created

        //BEGIN - smart proxy shard locator service ---------------------------
        if(!serviceImplRemote) {

            //Note: if mdsServiceUUID, mdsProperties, mdsServiceDir == null,
            //      then this means that it's the 1st time starting the shard
            //      locator, else the shard locator is being re-started

            if(mdsServiceUUID == null) {
                mdsServiceUUID = UUID.randomUUID();
            }

            if(mdsProperties == null) {
                mdsProperties = new Properties(properties);
            }

            if(mdsServiceDir == null) {
                if (!isTransient) {
                    mdsServiceDir = 
                        new File(dataDir, mdsServiceUUID.toString());
                    mdsServiceDir.mkdirs();
                    try {
                        new RandomAccessFile
                                (new File(mdsServiceDir, MDS), "rw").close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    mdsProperties.setProperty
                        (EmbeddedShardLocatorImpl.Options.DATA_DIR,
                         mdsServiceDir.toString());
                }
            }

            mdsProperties.setProperty
                (EmbeddedShardLocatorImpl.Options.THREAD_POOL_SIZE,
                 EmbeddedShardLocatorImpl.Options.DEFAULT_THREAD_POOL_SIZE);

            metadataService =
                new EmbeddedShardLocatorImpl
                            (mdsServiceUUID,
                             hostname,
                             null,//SDM ==> testing with embedded federation
                             transactionService,
                             loadBalancerService,
                             dataServiceByUUID,
                             mdsProperties);
            remoteMds = null;
            mds = (EmbeddedShardLocatorImpl)metadataService;
        }//endif(!srviceImplRemote)
        //END - smart proxy shard locator service -----------------------------
//////////////////////////////////////////////////////////////////////////

        // IBigdataDiscoveryManagement of EmbeddedShardService depends on the
        // EmbeddedShardLocator that is created AFTER EmbeddedShardService
        // was created; therefore, set it on the EmbeddedShardService
        for(EmbeddedShardServiceImpl ds : this.dsList) {
            if(ds != null) {
                ds.setEmbeddedMds(metadataService);
                ds.setEmbeddedDsMap(dataServiceByUUID);
            }
        }

        {//BEGIN - service join with load balancer ----------------------------

//BTM            for (IDataService ds : this.dataService) {
//BTM
//BTM - BEGIN CHANGE DATA_SERVICE TO SHARD_SERVICE

            //shard (data) service joins

            if(remoteLbs != null) {// ==> use remote form
                for (EmbeddedDataServiceImpl remoteDs : remoteDsList) {
                    remoteLbs.join(remoteDs.getServiceUUID(),
                                   remoteDs.getServiceIface(),
                                   hostname);
                }
            } else {//remoteLbs == null ==> use smart proxy form
                for (EmbeddedShardServiceImpl ds : dsList) {
                    lbs.join(ds);
                }
            }
//BTM for (ShardService ds : this.dataService) {
//BTM                try {
//BTM                    if(remoteLbs != null) {
//BTM                        remoteLbs.join(ds.getServiceUUID(),
//BTM                                       ds.getServiceIface(),
//BTM                                       hostname);
//BTM                    } else {
//BTM                        lbs.join(ds.getServiceUUID(),
//BTM                                 ds.getServiceIface(),
//BTM                                 ds.getServiceName(),
//BTM                                 hostname);
//BTM                    }
//BTM                } catch (IOException e) {
//BTM                    // Should never be thrown for an embedded service.
//BTM                    log.warn(e.getMessage(), e);
//BTM                }
//BTM            }
//BTM - END CHANGE DATA_SERVICE TO SHARD_SERVICE

            //other service joins

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
                lbs.join(txnSrvc);
                lbs.join(lbs);
                lbs.join(mds);
            }

        }//END - service join with load balancer ------------------------------
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
                                 final String  hostname,
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
}//serviceImplRemote: BTM - END -------------------------------------------------------------------

        
        /*
         * Start the data services.
         */
        {
//BTM            dataService = new DataService[ndataServices];
dataService = new ShardService[ndataServices];

            for (int i = 0; i < ndataServices; i++) {

                final Properties p = new Properties(properties);

                final UUID serviceUUID = UUID.randomUUID();

                if (!isTransient) {
                    final File serviceDir = new File(dataDir, serviceUUID.toString());
                    serviceDir.mkdirs();
                    p.setProperty(DataService.Options.DATA_DIR, serviceDir.toString());
p.setProperty(EmbeddedShardServiceImpl.Options.DATA_DIR, serviceDir.toString());
                }

if(serviceImplRemote) {//BTM - BEGIN --------------------------------------------------------------
//BTM                dataService[i] = new EmbeddedDataServiceImpl(serviceUUID, p)
//BTM                        .start();
//BTM
//BTM                dataServiceByUUID.put(serviceUUID, dataService[i]);

    ShardService shardService = new EmbeddedDataServiceImpl(serviceUUID, p).start();
    EmbeddedDataServiceImpl remoteDs = (EmbeddedDataServiceImpl)shardService;
    EmbeddedShardServiceImpl ds = null;

    dataService[i] = shardService;
    this.remoteDsList.add(remoteDs);
    this.dsList.add(ds);
    dataServiceByUUID.put(serviceUUID, shardService);

} else {// !serviceImplRemote: BTM - START SMART PROXY SHARD_SERVICE

    p.setProperty(EmbeddedShardServiceImpl.Options.THREAD_POOL_SIZE, EmbeddedShardServiceImpl.Options.DEFAULT_THREAD_POOL_SIZE);
System.out.println("\nEmbeddedFederation CREATE_FEDERATION: embeddedMds = "+metadataService+"\n");
    ShardService shardService = new EmbeddedShardServiceImpl
                            (serviceUUID,
                             hostname,
                             null,//SDM ==> testing with embedded federation
                             transactionService,
                             loadBalancerService,
                             p);
    EmbeddedDataServiceImpl remoteDs = null;
    EmbeddedShardServiceImpl ds = (EmbeddedShardServiceImpl)shardService;

    dataService[i] = shardService;
    this.remoteDsList.add(remoteDs);
    this.dsList.add(ds);
    dataServiceByUUID.put(serviceUUID, shardService);

}//srviceImplRemote: BTM - END -------------------------------------------------------------------

            }//end loop(i)

        }//end start shard services


//BTM - Note that although the purely remote impl of the metadata service does
//BTM   not require that the data service be started before it, the smart proxy
//BTM   impl of the shard locator does require that the data service be started
//BTM   prior to starting the shard locator. This is so that the dataServiceByUUID
//BTM   map can be populated with references to those data services. The purely
//BTM   impl doesn't need that map since it is passed the federation which the
//BTM   metadata service can query for the data services that were started.
//BTM   Because of this requirement, this method was originally changed below
//BTM   to start both the metadata service and the shard locator only after the
//BTM   data services have been started above. But after doing some testing and
//BTM   a little analysis, it was determined that because the shard locator service
//BTM   is no longer implemented to also be a data service (as the metadata
//BTM   service is), the shard locator service doesn't have to be started
//BTM   in this method; that is, the code that starts the shard locator 
//BTM   service can be executed in this class's constructor (which invokes
//BTM   this method when it's a first-time start as opposed to a restart).
//BTM   Once this was realized, the code below was commented out and the
//BTM   code above that starts the metadata service for the first time was
//BTM   un-commented so that the original logic and behavior that bigdata
//BTM   had placed in this method regarding the metadata service would be
//BTM   maintained. The code that starts the shard locator for the first
//BTM   was then moved out of this method into the constructor. The code
//BTM   below that is commented out, is currently left in place for reference,
//BTM   and will eventually be removed during the 2nd phase -- the cleanup
//BTM   phase.
/* BEGIN COMMENTED OUT *******************************************************************************************************
//BTM - DON'T START META_DATA SERVICE HERE? Do it outside of this method?????
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
END COMMENTED OUT ************************************************************************************************************ */

        return ndataServices;

    }
    
    /**
     * Concrete implementation.
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
    protected class EmbeddedShardServiceImpl extends EmbeddedShardService {
        public EmbeddedShardServiceImpl(UUID serviceUUID, 
                                        String hostname,
                                        ServiceDiscoveryManager sdm,
                                        TransactionService embeddedTxnService,
                                        LoadBalancer embeddedLoadBalancer,
                                        Properties properties)
        {
            super(serviceUUID,
                  hostname,
                  sdm,
                  embeddedTxnService,
                  embeddedLoadBalancer,
                  null,//zookeeperAccessor,
                  null,//zookeeperAcl,
                  null,//zookeeperRoot,
                  Integer.parseInt(properties.getProperty(EmbeddedShardServiceImpl.Options.THREAD_POOL_SIZE)),
                  com.bigdata.shard.Constants.DEFAULT_INDEX_CACHE_SIZE,
                  com.bigdata.shard.Constants.DEFAULT_INDEX_CACHE_TIMEOUT,
                  com.bigdata.service.MetadataIndexCachePolicy.CacheAll,
                  com.bigdata.shard.Constants.DEFAULT_RESOURCE_LOCATOR_CACHE_SIZE,
                  com.bigdata.shard.Constants.DEFAULT_RESOURCE_LOCATOR_CACHE_TIMEOUT,
                  com.bigdata.shard.Constants.DEFAULT_RANGE_QUERY_CAPACITY,
                  com.bigdata.shard.Constants.DEFAULT_BATCH_API_ONLY,
                  com.bigdata.shard.Constants.DEFAULT_TASK_TIMEOUT,
                  com.bigdata.shard.Constants.DEFAULT_MAX_PARALLEL_TASKS_PER_REQUEST,
                  com.bigdata.shard.Constants.DEFAULT_MAX_STALE_LOCATOR_RETRIES,
                  com.bigdata.shard.Constants.DEFAULT_COLLECT_QUEUE_STATISTICS,//true
                  com.bigdata.shard.Constants.DEFAULT_COLLECT_PLATFORM_STATISTICS,//false
                  com.bigdata.shard.Constants.DEFAULT_LOAD_BALANCER_REPORTING_PERIOD,
                  com.bigdata.shard.Constants.DEFAULT_HTTPD_PORT,
                  properties);
        }
    }
//BTM*** ------------------------------------------------------------------------------

//BTM*** ------------------------------------------------------------------------------
    protected class EmbeddedShardLocatorImpl extends EmbeddedShardLocator {
        public EmbeddedShardLocatorImpl(UUID serviceUUID, 
                                        String hostname,
                                        ServiceDiscoveryManager sdm,
                                        TransactionService embeddedTxnService,
                                        LoadBalancer embeddedLbs,
                                        Map<UUID, ShardService> embeddedDataServiceMap,
                                        Properties properties)
        {
            super(serviceUUID,
                  hostname,
                  sdm,
                  embeddedTxnService,
                  embeddedLbs,
                  embeddedDataServiceMap,
                  null,//zookeeperAccessor,
                  null,//zookeeperAcl,
                  null,//zookeeperRoot,
                  Integer.parseInt(properties.getProperty(EmbeddedShardLocatorImpl.Options.THREAD_POOL_SIZE)),
                  com.bigdata.metadata.Constants.DEFAULT_INDEX_CACHE_SIZE,
                  com.bigdata.metadata.Constants.DEFAULT_INDEX_CACHE_TIMEOUT,
                  com.bigdata.service.MetadataIndexCachePolicy.CacheAll,
                  com.bigdata.metadata.Constants.DEFAULT_RESOURCE_LOCATOR_CACHE_SIZE,
                  com.bigdata.metadata.Constants.DEFAULT_RESOURCE_LOCATOR_CACHE_TIMEOUT,
                  com.bigdata.shard.Constants.DEFAULT_RANGE_QUERY_CAPACITY,
                  com.bigdata.shard.Constants.DEFAULT_BATCH_API_ONLY,
                  com.bigdata.shard.Constants.DEFAULT_TASK_TIMEOUT,
                  com.bigdata.shard.Constants.DEFAULT_MAX_PARALLEL_TASKS_PER_REQUEST,
                  com.bigdata.shard.Constants.DEFAULT_MAX_STALE_LOCATOR_RETRIES,
                  com.bigdata.shard.Constants.DEFAULT_COLLECT_QUEUE_STATISTICS,//true
                  com.bigdata.shard.Constants.DEFAULT_COLLECT_PLATFORM_STATISTICS,//false
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
                                        Map<UUID, ShardService> dataServiceMap,
                                        Properties properties)
        {
            super(serviceUUID, hostname,
                  sdm, 
                  dataServiceMap,
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
                                      Map<UUID, ShardService> dataServiceMap,
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

//BTM        for (int i = 0; i < dataService.length; i++) {
//BTM            if (dataService[i] != null) {
//BTM                dataService[i].shutdown();
//BTM            }
//BTM        }
for(EmbeddedDataServiceImpl remoteDs : remoteDsList) {
    if(remoteDs != null) remoteDs.shutdown();
}

for(EmbeddedShardServiceImpl ds : dsList) {
    if(ds != null) ds.shutdown();
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

//BTM        for (int i = 0; i < dataService.length; i++) {
//BTM            if (dataService[i] != null) {
//BTM                dataService[i].shutdownNow();
//BTM            }
//BTM        }
for(EmbeddedDataServiceImpl remoteDs : remoteDsList) {
    if(remoteDs != null) remoteDs.shutdownNow();
}

for(EmbeddedShardServiceImpl ds : dsList) {
    if(ds != null) ds.shutdownNow();
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
        
//BTM        for (int i = 0; i < dataService.length; i++) {
//BTM            if (dataService[i] != null) {
//BTM                dataService[i].destroy();
//BTM            }
//BTM        }
for(EmbeddedDataServiceImpl remoteDs : remoteDsList) {
    if(remoteDs != null) remoteDs.destroy();
}

for(EmbeddedShardServiceImpl ds : dsList) {
    if(ds != null) ds.destroy();
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
     * This scans the {@link ShardService}s and reports the most recent value.
     */
    public long getLastCommitTime() {

        assertOpen();
        
        long maxValue = 0;

        // check each of the data services.
//BTM        for(int i=0; i<dataService.length; i++) {
//BTM            final long commitTime = dataService[i].getResourceManager()
//BTM                    .getLiveJournal().getRootBlockView().getLastCommitTime();
//BTM
//BTM            if (commitTime > maxValue) {
//BTM                maxValue = commitTime;
//BTM            }
//BTM        }
for(EmbeddedDataServiceImpl remoteDs : remoteDsList) {
    if(remoteDs != null) {
        long commitTime = remoteDs.getResourceManager().getLiveJournal().getRootBlockView().getLastCommitTime();
        if (commitTime > maxValue) {
            maxValue = commitTime;
        }
    }
}
for(EmbeddedShardServiceImpl ds : dsList) {
    if(ds != null) {
        long commitTime = ds.getResourceManager().getLiveJournal().getRootBlockView().getLastCommitTime();
        if (commitTime > maxValue) {
            maxValue = commitTime;
        }
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

//BTM    public IDataService getDataServiceByName(final String name) {
public ShardService getDataServiceByName(final String name) {

//BTM        for (IDataService ds : dataService) {
for (ShardService ds : dataService) {

            final String serviceName;
            try {
if(ds instanceof IService) {
//BTM                serviceName = ds.getServiceName();
    serviceName = ((IService)ds).getServiceName();
} else {
    serviceName = ((Service)ds).getServiceName();

}
                
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
