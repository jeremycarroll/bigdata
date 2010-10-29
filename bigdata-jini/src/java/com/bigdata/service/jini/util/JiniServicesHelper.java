package com.bigdata.service.jini.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.core.lookup.ServiceID;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import com.bigdata.jini.start.IServiceListener;
import com.bigdata.jini.start.config.ZookeeperClientConfig;
import com.bigdata.jini.start.config.ZookeeperServerConfiguration;
import com.bigdata.jini.start.process.ProcessHelper;
import com.bigdata.jini.start.process.ZookeeperProcessHelper;
import com.bigdata.jini.util.ConfigMath;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.resources.ResourceFileFilter;
//BTM import com.bigdata.service.IDataService;
import com.bigdata.service.jini.AbstractServer;
import com.bigdata.service.jini.ClientServer;
import com.bigdata.service.jini.DataServer;
import com.bigdata.service.jini.FakeLifeCycle;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.LoadBalancerServer;
import com.bigdata.service.jini.MetadataServer;
import com.bigdata.service.jini.TransactionServer;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.zookeeper.ZooHelper;

//BTM
import com.bigdata.service.CallableExecutor;
import com.bigdata.service.ShardService;
import com.bigdata.service.Service;
import com.bigdata.shard.EmbeddedShardService;
import java.util.UUID;

/**
 * A helper class that starts all the necessary services for a Jini federation.
 * This is used when testing, but NOT for benchmarking performance. For
 * benchmarking you MUST connect to an existing federation, ideally one deployed
 * over a cluster of machines!
 * <p>
 * Note: You MUST specify a sufficiently lax security policy, for example:
 * 
 * <pre>
 * -Djava.security.policy=policy.all
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JiniServicesHelper extends JiniCoreServicesHelper {

    protected final static Logger log = Logger
            .getLogger(JiniServicesHelper.class);
//BTM
private boolean serviceImplRemote;

//BTM    public MetadataServer metadataServer0;
public MetadataServer mdsRemote0;
public ShardLocatorTask mds0;

//BTM    public DataServer dataServer1;
//BTM    public DataServer dataServer0;
public DataServer dsRemote1;
public DataServer dsRemote0;
public ShardServiceTask ds1;
public ShardServiceTask ds0;

//BTM    public LoadBalancerServer loadBalancerServer0;
public LoadBalancerServer lbsRemote0;
public LoadBalancerTask lbs0;

//BTM    public TransactionServer transactionServer0;
public TransactionServer txnServiceRemote0;
public TransactionTask txnService0;

//BTM    public ClientServer clientServer0;
public ClientServer csRemote0;
public CallableExecutorTask cs0;

    public JiniClient<?> client;

//    public ZooKeeper zookeeper;
//    
//    public ZookeeperClientConfig zooConfig;

    public JiniFederation<?> getFederation() {
        
        return client.getFederation();
        
    }
    
    /**
     * Return a proxy for #dataServer0.
     */
//BTM    public IDataService getDataService0(){
public ShardService getDataService0(){

//BTM        return client.getFederation().getDataService( JiniUtil.serviceID2UUID(dataServer0.getServiceID()) );
        if(dsRemote0 != null) {
            return client.getFederation().getDataService( JiniUtil.serviceID2UUID(dsRemote0.getServiceID()) );
        } else if( ds0 != null) {
            for(int i=0; i<3; i++) {
                try {
                    ShardService shardService = client.getFederation().getDataService( ((Service)ds0).getServiceUUID() );
                    return shardService;
                } catch(UnsupportedOperationException e) {//service not ready
                    try { Thread.sleep(1000L); } catch (InterruptedException ex) { }
                }
            }
            return client.getFederation().getDataService( ((Service)ds0).getServiceUUID() );
        }
        return null;
    }

    /**
     * Return a proxy for #dataServer1.
     */
//BTM    public IDataService getDataService1(){
public ShardService getDataService1(){

//BTM        return client.getFederation().getDataService( JiniUtil.serviceID2UUID(dataServer1.getServiceID()) );
        if(dsRemote1 != null) {
            return client.getFederation().getDataService( JiniUtil.serviceID2UUID(dsRemote1.getServiceID()) );
        } else if (ds1 != null) {
            for(int i=0; i<3; i++) {
                try {
                    ShardService shardService = client.getFederation().getDataService( ((Service)ds1).getServiceUUID() );
                    return shardService;
                } catch(UnsupportedOperationException e) {//service not ready
                    try { Thread.sleep(1000L); } catch (InterruptedException ex) { }
                }
            }
            return client.getFederation().getDataService( ((Service)ds1).getServiceUUID() );
        }
        return null;
    }

    /**
     * The default configuration file for stand alone testing.
     */
    public static final File CONFIG_STANDALONE = new File(
            "bigdata-jini/src/resources/config/bigdataStandaloneTesting.config");

    /**
     * Return a new file whose contents are the byte-by-byte concatenation of
     * the {@link #CONFIG_STANDALONE} configuration file and a configuration
     * file specified by the caller, so leave some whitespace at the top or
     * bottom of one of the files. The new file is created using the temporary
     * file mechanism and SHOULD be deleted by the caller when they are done
     * with it. This is used to write unit tests which depend on the
     * configuration of components not specified in {@link #CONFIG_STANDALONE}.
     * 
     * @param file
     *            The additional configuration file. This file should only
     *            include the component configuration without any imports.
     * 
     * @return The new configuration file.
     * 
     * @throws IOException
     */
    public static File append(final File file) throws IOException {

        if (file == null)
            throw new IllegalArgumentException();

        if (!file.isFile())
            throw new FileNotFoundException(file.getPath());

        final File tmp = File.createTempFile("bigdata-", ".config");

        InputStream is = null;
        OutputStream os = null;
        try {

            is = new BufferedInputStream(new FileInputStream(CONFIG_STANDALONE));
            os = new BufferedOutputStream(new FileOutputStream(tmp));

            int b;
            while ((b = is.read()) != -1) {

                os.write(b);
                
            }
            
            is.close();

            is = null;
            
            is = new BufferedInputStream(new FileInputStream(file));

            while ((b = is.read()) != -1) {

                os.write(b);
                
            }
            
            is.close();
            
            return tmp;

        } catch(Throwable t) {
            
            tmp.delete();
            
            throw new RuntimeException(t);
            
        } finally {

            if (is != null) {
                is.close();
            }

            if (os != null) {
                os.close();
            }

        }
        
    }
    
    /**
     * New helper instance using {@link #CONFIG_STANDALONE}. Use
     * {@link #start()} and {@link #destroy()} to start and await the services
     * and to tear them down respectively.
     */
    public JiniServicesHelper() {

        this(new String[] { CONFIG_STANDALONE.getPath() });
        
    }

    /**
     * New helper instance. Use {@link #start()} and {@link #destroy()} to start
     * and await the services and to tear them down respectively.
     * <p>
     * Note: Constructing a different configuration file for testing purposes or
     * applying overrides must be done very carefully. It is generally safe to
     * override certain parameters for the various services, either in a copy of
     * the {@link #CONFIG_STANDALONE} file or in jini style overrides. However,
     * in order to start more than one instance of a service you must modify
     * {@link #start()} to explicitly override the
     * {@link AbstractServer.ConfigurationOptions#SERVICE_DIR} for each
     * instance.
     * 
     * @param args
     *            The arguments specify the configuration file (required) and
     *            optional jini overrides.
     */
    public JiniServicesHelper(final String[] args) {
//BTM
//BTM        if (args == null)
//BTM            throw new IllegalArgumentException();
//BTM
//BTM        if (args.length == 0)
//BTM            throw new IllegalArgumentException();
//BTM
//BTM        if (args[0] == null)
//BTM            throw new IllegalArgumentException();
//BTM
//BTM        if (!new File(args[0]).exists()) {
//BTM
//BTM            throw new RuntimeException("Configuration file not found: "
//BTM                    + args[0]);
//BTM            
//BTM        }
//BTM        
//BTM        this.args = args;
this(new String[] { CONFIG_STANDALONE.getPath() }, false);
    }

//BTM
    public JiniServicesHelper(boolean serviceImplRemote) {
        this(new String[] { CONFIG_STANDALONE.getPath() },
             serviceImplRemote);   
    }

    public JiniServicesHelper(final String[] args,
                              boolean serviceImplRemote)
    {
        if (args == null)
            throw new IllegalArgumentException();

        if (args.length == 0)
            throw new IllegalArgumentException();

        if (args[0] == null)
            throw new IllegalArgumentException();

        if (!new File(args[0]).exists()) {

            throw new RuntimeException("Configuration file not found: "
                    + args[0]);
            
        }
        
        this.args = args;
        this.serviceImplRemote = serviceImplRemote;
    }



    private final String[] args;

    /**
     * Thread pool used to run the various services.
     */
    private final ExecutorService threadPool = Executors
            .newCachedThreadPool(new DaemonThreadFactory(getClass().getName()
                    + ".threadPool"));
    
    private final IServiceListener serviceListener = new ServiceListener();

    /**
     * The directory in which all federation state is located.
     */
    private File fedServiceDir;

    /**
     * The zookeeper client port chosen by {@link #start()}.
     */
    private int clientPort;
    
    /**
     * The directory in which zookeeper is running.
     */
    private File zooDataDir;

    /**
     * Starts all services and connects the {@link JiniClient} to the
     * federation.
     * 
     * @throws InterruptedException
     * @throws ConfigurationException
     * @throws RuntimeException
     *             if something goes wrong, typically a configuration error or
     *             you did not set the security policy.
     * 
     * @todo Start/stop jini. This class does not know where jini was installed
     *       and is therefore unable to start it automatically. The bundled
     *       configuration for jini uses variables. An alternative bundled
     *       configuration could be devised without those variables or by
     *       binding them based on the root of the source tree.
     * 
     * @todo not reentrant, but should throw exception if called twice.
     */
    public void start() throws InterruptedException, ConfigurationException {

        try {

System.out.println("\nJiniServicesHelper >>> CALLING innerStart");
            innerStart();
            
        } catch (Throwable t) {
            
            try {
            
                destroy();
            
            } catch (Throwable t2) {
                
                log.error("Shutdown error: " + t2, t2);
                
            }

            // throw the startup error, not any possible shutdown error.
            throw new RuntimeException("Startup error: " + t, t);
            
        }

    }

    /**
     * Invoked by {@link #start()}.
     * 
     * @throws InterruptedException
     * @throws ConfigurationException
     * @throws KeeperException
     */
    private void innerStart() throws InterruptedException,
            ConfigurationException, KeeperException {

        System.setSecurityManager(new SecurityManager());
//BTM
net.jini.discovery.LookupDiscoveryManager ldm = null;
net.jini.lookup.ServiceDiscoveryManager sdm = null;

        /*
         * Pull some fields out of the configuration file that we need to
         * set things up.
         */
        
        // all services will be located in children of this directory.
        final File fedServiceDir;

        // the federation name.
        final String fedname;
        {
          
            final Configuration config = ConfigurationProvider
                    .getInstance(args);

            fedname = (String) config.getEntry("bigdata", "fedname",
                    String.class);

            fedServiceDir = (File) config.getEntry("bigdata", "serviceDir",
                    File.class);

            if (fedServiceDir.getPath().equals(".")) {

                throw new RuntimeException(
                        "Startup directory MUST NOT be the current directory.");

            }

            System.err.println("fedname=" + fedname);

            System.err.println("fedServiceDir=" + fedServiceDir);

            this.fedServiceDir = fedServiceDir;

//BTM
try {
    ldm = new net.jini.discovery.LookupDiscoveryManager(new String[] {fedname}, null, null, config);
    sdm = new net.jini.lookup.ServiceDiscoveryManager(ldm, null, config);
} catch(Throwable t) {
    t.printStackTrace();
}
        }

        /*
         * Setup a zookeeper instance.
         * 
         * Note: [options] contains the overrides that we need to connect to the
         * right zookeeper instance. This is very important! These options need
         * to be used when we start the client or any of the services within
         * this method so they use the right zookeeper instance.
         */
        final String[] options;
        {

            // the zookeeper service directory.
            zooDataDir = new File(fedServiceDir, "zookeeper");
            
            if(zooDataDir.exists()) {
                
                // clear out old zookeeper state first.
                recursiveDelete(zooDataDir);
                
            }
            
            // create.
            zooDataDir.mkdirs();

            try {

                // find ports that are not in use.
                clientPort = getPort(2181/* suggestedPort */);
                final int peerPort = getPort(2888/* suggestedPort */);
                final int leaderPort = getPort(3888/* suggestedPort */);
                final String servers = "1=localhost:" + peerPort + ":"
                        + leaderPort;

                options = new String[] {
                        // overrides the clientPort to be unique.
                        QuorumPeerMain.class.getName()
                                + "."
                                + ZookeeperServerConfiguration.Options.CLIENT_PORT
                                + "=" + clientPort,
                        // overrides servers declaration.
                        QuorumPeerMain.class.getName() + "."
                                + ZookeeperServerConfiguration.Options.SERVERS
                                + "=\"" + servers + "\"",
                        // overrides the dataDir
                        QuorumPeerMain.class.getName() + "."
                                + ZookeeperServerConfiguration.Options.DATA_DIR
                                + "=new java.io.File("
                                + ConfigMath.q(zooDataDir.toString()) + ")"//
                };
                
                System.err.println("options=" + Arrays.toString(options));

                final Configuration config = ConfigurationProvider
                        .getInstance(concat(args, options));

                // start zookeeper (a server instance).
                final int nstarted = ZookeeperProcessHelper.startZookeeper(
                        config, serviceListener);

                if (nstarted != 1) {

                    throw new RuntimeException(
                            "Expected to start one zookeeper instance, not "
                                    + nstarted);
                    
                }

            } catch (Throwable t) {

                // don't leave around the dataDir if the setup fails.
                recursiveDelete(zooDataDir);

                throw new RuntimeException(t);

            }

        }
        
        /*
         * Start the jini lookup service.
         * 
         * FIXME Pass all required parameters by setting -Dfoo, etc. 
         */
//        new Thread(new Runnable() {
//            public void run() {
//                LookupStarter.main(new String[] {});
//            }
//        }).start();

        /*
         * Connect the client and wait until zookeeper is up.
         */
        {

            // new client.
            client = JiniClient.newInstance(concat(args, options));

            // connect the client - this will get discovery running.
            final JiniFederation<?> fed = client.connect();

            if (!fed.getZookeeperAccessor().awaitZookeeperConnected(1000,
                    TimeUnit.MILLISECONDS)) {

                throw new RuntimeException("Zookeeper client not connected.");

            }

//            zookeeper = fed.getZookeeper();

//            zooConfig = fed.getZooConfig();

            fed.createKeyZNodes(fed.getZookeeper());
            
        }

//BTM - BEGIN - IDATA_SERVICE TO SHARD_SERVICE

        //setup configuration overrides for smart proxy impls

        java.util.ArrayList<String> txnOptionsList = new java.util.ArrayList<String>();
        java.util.ArrayList<String> lbsOptionsList = new java.util.ArrayList<String>();
        java.util.ArrayList<String> mdsOptionsList = new java.util.ArrayList<String>();
        java.util.ArrayList<String> ds0OptionsList = new java.util.ArrayList<String>();
        java.util.ArrayList<String> ds1OptionsList = new java.util.ArrayList<String>();
        java.util.ArrayList<String> csOptionsList = new java.util.ArrayList<String>();
        for(int i=0; i<options.length; i++) {
            txnOptionsList.add(options[i]);
            lbsOptionsList.add(options[i]);
            mdsOptionsList.add(options[i]);
            ds0OptionsList.add(options[i]);
            ds1OptionsList.add(options[i]);
            csOptionsList.add(options[i]);
        }
        //joinGroupsOverStr should be of the form:
        //String joinGroupsOverrideStr = 
        //           "com.bigdata.<config-component-name>.groupsToJoin=new String[] "
        //           +"{"
        //           +"\""+fedname+"\""
        //           +"}";
        txnOptionsList.add("com.bigdata.transaction.groupsToJoin=new String[] {"+"\""+fedname+"\""+"}");
        lbsOptionsList.add("com.bigdata.loadbalancer.groupsToJoin=new String[] {"+"\""+fedname+"\""+"}");
        mdsOptionsList.add("com.bigdata.metadata.groupsToJoin=new String[] {"+"\""+fedname+"\""+"}");
        ds0OptionsList.add("com.bigdata.shard.groupsToJoin=new String[] {"+"\""+fedname+"\""+"}");
        ds1OptionsList.add("com.bigdata.shard.groupsToJoin=new String[] {"+"\""+fedname+"\""+"}");
        csOptionsList.add("com.bigdata.executor.groupsToJoin=new String[] {"+"\""+fedname+"\""+"}");

        String txnServiceDir = ConfigMath.q(ConfigMath.getAbsolutePath(new File(fedServiceDir, "txn")));
        String lbsServiceDir = ConfigMath.q(ConfigMath.getAbsolutePath(new File(fedServiceDir, "lbs")));
        String mdsServiceDir = ConfigMath.q(ConfigMath.getAbsolutePath(new File(fedServiceDir, "mds")));
        String csServiceDir = ConfigMath.q(ConfigMath.getAbsolutePath(new File(fedServiceDir, "cs")));

        txnOptionsList.add("com.bigdata.transaction.persistenceDirectory=new String(" + txnServiceDir + ")");
        lbsOptionsList.add("com.bigdata.loadbalancer.persistenceDirectory=new String(" + lbsServiceDir + ")");
        mdsOptionsList.add("com.bigdata.metadata.persistenceDirectory=new String(" + mdsServiceDir + ")");
        csOptionsList.add("com.bigdata.executor.persistenceDirectory=new String(" + csServiceDir + ")");

        String[] txnServiceImplArgs = 
         concat(args, txnOptionsList.toArray(new String[txnOptionsList.size()]) );
        String[] lbsServiceImplArgs = 
         concat(args, lbsOptionsList.toArray(new String[lbsOptionsList.size()]) );
        String[] mdsServiceImplArgs = 
         concat(args, mdsOptionsList.toArray(new String[mdsOptionsList.size()]) );
        String[] csServiceImplArgs = 
         concat(args, csOptionsList.toArray(new String[csOptionsList.size()]) );

//BTM - for debugging
if(serviceImplRemote) {
    System.out.println("\n*** serviceImplRemote = "+serviceImplRemote+" JiniServicesHelper >>> [purely remote]");
} else {
    System.out.println("\n*** serviceImplRemote = "+serviceImplRemote+" JiniServicesHelper >>> [NON-remote]\n");
    for(int i=0; i<txnServiceImplArgs.length; i++) {
        System.out.println("**** BTM - JiniServicesHelper >>> txnServiceImplArgs["+i+"] = "+txnServiceImplArgs[i]);
    }
    System.out.println("***");
    for(int i=0; i<lbsServiceImplArgs.length; i++) {
        System.out.println("**** BTM - JiniServicesHelper >>> lbsServiceImplArgs["+i+"] = "+lbsServiceImplArgs[i]);
    }
    System.out.println("***");
    for(int i=0; i<mdsServiceImplArgs.length; i++) {
        System.out.println("**** BTM - JiniServicesHelper >>> mdsServiceImplArgs["+i+"] = "+mdsServiceImplArgs[i]);
    }
    for(int i=0; i<csServiceImplArgs.length; i++) {
        System.out.println("**** BTM - JiniServicesHelper >>> csServiceImplArgs["+i+"] = "+csServiceImplArgs[i]);
    }
    System.out.println("***");
}

        {//begin ds1

            final File serviceDir = new File(fedServiceDir, "ds1");
            final File dataDir = new File(serviceDir, "data");

System.err.println("\n---------- JiniServicesHelper (ds1): dataDir = "+dataDir+"\n");

if(serviceImplRemote) {
            final String[] overrides = new String[] {
                    /*
                     * Override the service directory.
                     */
                    DataServer.class.getName()
                            + "."
                            + AbstractServer.ConfigurationOptions.SERVICE_DIR
                            + "=new java.io.File("
                            + ConfigMath.q(ConfigMath
                                    .getAbsolutePath(serviceDir)) + ")",
                    /*
                     * Override the data directory. Since we are not shelling
                     * out a new JVM, the current working directory when the
                     * service starts will be the same directory in which this
                     * JVM was started. Therefore we must specify an absolution
                     * directory.
                     */
                    DataServer.class.getName()
                            + ".properties = new com.bigdata.util.NV[] {\n"
                            +
                            //
                            " new NV(" + "DataServer.Options.DATA_DIR" + ", "
                            + ConfigMath.q(ConfigMath.getAbsolutePath(dataDir))
                            + ")\n" +
                            //
                            "}\n" };

            System.err.println("overrides=" + Arrays.toString(overrides));

//BTM            threadPool.execute(dataServer1 = new DataServer(concat(args, concat(overrides, options)), new FakeLifeCycle()));
    threadPool.execute(dsRemote1 = new DataServer(concat(args, concat(overrides, options)), new FakeLifeCycle()));

} else {//smart proxy
        String absServiceDir = ConfigMath.q(ConfigMath.getAbsolutePath(serviceDir));
        String absDataDir = ConfigMath.q(ConfigMath.getAbsolutePath(dataDir));
        String ds1ServiceDir = 
                    EmbeddedShardService.class.getName()
                            + "."
                            + AbstractServer.ConfigurationOptions.SERVICE_DIR
                            + "=new java.io.File(" + absServiceDir + ")";

        String ds1DataDir =
                    EmbeddedShardService.class.getName()
                            + ".properties = new com.bigdata.util.NV[] {\n"
                            +
                            //
                            " new NV(" + "EmbeddedShardService.Options.DATA_DIR" + ", "
                            + absDataDir
                            + ")\n" +
                            //
                            "}\n";

        ds1OptionsList.add("com.bigdata.shard.persistenceDirectory=new String(" + absServiceDir + ")");
        ds1OptionsList.add(ds1ServiceDir);
        ds1OptionsList.add(ds1DataDir);
        String[] ds1ServiceImplArgs = 
         concat(args, ds1OptionsList.toArray(new String[ds1OptionsList.size()]) );

//BTM - for debugging
    for(int i=0; i<ds1ServiceImplArgs.length; i++) {
        System.err.println("**** BTM - JiniServicesHelper >>> ds1 >>> ds1ServiceImplArgs["+i+"] = "+ds1ServiceImplArgs[i]);
    }
System.out.println("\nJiniServicesHelper >>> NEW ShardServiceTask(ds1) - BEGIN");
    ds1 = new ShardServiceTask(ds1ServiceImplArgs);
    threadPool.execute(ds1);
}
        }//end ds1

        {//begin ds0

            final File serviceDir = new File(fedServiceDir, "ds0");
            final File dataDir = new File(serviceDir, "data");

System.err.println("\n---------- JiniServicesHelper (ds0): dataDir = "+dataDir+"\n");

if(serviceImplRemote) {

            final String[] overrides = new String[] {
                    /*
                     * Override the service directory.
                     */
                    DataServer.class.getName()
                            + "."
                            + AbstractServer.ConfigurationOptions.SERVICE_DIR
                            + "=new java.io.File("
                            + ConfigMath.q(ConfigMath
                                    .getAbsolutePath(serviceDir)) + ")",
                    /*
                     * Override the data directory. Since we are not shelling
                     * out a new JVM, the current working directory when the
                     * service starts will be the same directory in which this
                     * JVM was started. Therefore we must specify an absolution
                     * directory.
                     */
                    DataServer.class.getName()
                            + ".properties = new com.bigdata.util.NV[] {\n"
                            +
                            //
                            " new NV(" + "DataServer.Options.DATA_DIR" + ", "
                            + ConfigMath.q(ConfigMath.getAbsolutePath(dataDir))
                            + ")\n" +
                            //
                            "}\n" };

            System.err.println("overrides=" + Arrays.toString(overrides));

//BTM            threadPool.execute(dataServer0 = new DataServer(concat(args, concat(overrides, options)), new FakeLifeCycle()));
    threadPool.execute(dsRemote0 = new DataServer(concat(args, concat(overrides, options)), new FakeLifeCycle()));

} else {//smart proxy
        String absServiceDir = ConfigMath.q(ConfigMath.getAbsolutePath(serviceDir));
        String absDataDir = ConfigMath.q(ConfigMath.getAbsolutePath(dataDir));
        String ds0ServiceDir = 
                    EmbeddedShardService.class.getName()
                            + "."
                            + AbstractServer.ConfigurationOptions.SERVICE_DIR
                            + "=new java.io.File(" + absServiceDir + ")";

        String ds0DataDir =
                    EmbeddedShardService.class.getName()
                            + ".properties = new com.bigdata.util.NV[] {\n"
                            +
                            //
                            " new NV(" + "EmbeddedShardService.Options.DATA_DIR" + ", "
                            + absDataDir
                            + ")\n" +
                            //
                            "}\n";

        ds0OptionsList.add("com.bigdata.shard.persistenceDirectory=new String(" + absServiceDir + ")");
        ds0OptionsList.add(ds0ServiceDir);
        ds0OptionsList.add(ds0DataDir);
        String[] ds0ServiceImplArgs = 
         concat(args, ds0OptionsList.toArray(new String[ds0OptionsList.size()]) );

//BTM - for debugging
    for(int i=0; i<ds0ServiceImplArgs.length; i++) {
        System.err.println("**** BTM - JiniServicesHelper >>> ds0 >>> ds0ServiceImplArgs["+i+"] = "+ds0ServiceImplArgs[i]);
    }
System.out.println("\nJiniServicesHelper >>> NEW ShardServiceTask(ds0) - BEGIN");
    ds0 = new ShardServiceTask(ds0ServiceImplArgs);
    threadPool.execute(ds0);
}
        }//end ds0
//BTM - END - IDATA_SERVICE TO SHARD_SERVICE

//BTM        threadPool.execute(clientServer0 = new ClientServer(concat(
//BTM                args, options), new FakeLifeCycle()));
//BTM
//BTM        threadPool.execute(transactionServer0 = new TransactionServer(concat(
//BTM                args, options), new FakeLifeCycle()));
//BTM
//BTM        threadPool.execute(metadataServer0 = new MetadataServer(concat(args,
//BTM                options), new FakeLifeCycle()));
//BTM
//BTM        threadPool.execute(loadBalancerServer0 = new LoadBalancerServer(concat(
//BTM                args, options), new FakeLifeCycle()));
//BTM
//BTM -----------------------------------------------------------------------
if(serviceImplRemote) {
//BTM System.out.println("\n*** serviceImplRemote = "+serviceImplRemote+" JiniServicesHelper >>> [purely remote]");

        threadPool.execute(csRemote0 = new ClientServer(concat(
                args, options), new FakeLifeCycle()));

        threadPool.execute(txnServiceRemote0 = new TransactionServer(concat(
                args, options), new FakeLifeCycle()));

        threadPool.execute(mdsRemote0 = new MetadataServer(concat(args,
                options), new FakeLifeCycle()));

        threadPool.execute(lbsRemote0 = new LoadBalancerServer(concat(
                args, options), new FakeLifeCycle()));
} else {
//BTM - BEGIN - FOR IDATA_SERVICE TO SHARD_SERVICE
//BTM System.out.println("\n*** serviceImplRemote = "+serviceImplRemote+" JiniServicesHelper >>> [NON-remote]");
//BTM    java.util.ArrayList<String> optionsList = new java.util.ArrayList<String>();
//BTM    for(int i=0; i<options.length; i++) {
//BTM        optionsList.add(options[i]);
//BTM    }
//BTM    String joinGroupsOverrideStr = 
//BTM                   "com.bigdata.loadbalancer.groupsToJoin=new String[] "
//BTM                   +"{"
//BTM                   +"\""+fedname+"\""
//BTM                   +"}";
//BTM    optionsList.add(joinGroupsOverrideStr);
//BTM - END - FOR IDATA_SERVICE TO SHARD_SERVICE

    cs0 = new CallableExecutorTask(csServiceImplArgs);
    threadPool.execute(cs0);

    txnService0 = new TransactionTask(txnServiceImplArgs);
    threadPool.execute(txnService0);

    mds0 = new ShardLocatorTask(mdsServiceImplArgs);
    threadPool.execute(mds0);

    lbs0 = new LoadBalancerTask(lbsServiceImplArgs);
    threadPool.execute(lbs0);
}
//BTM -----------------------------------------------------------------------

        // Wait until all the services are up.
//BTM        getServiceID(clientServer0);
//BTM        getServiceID(transactionServer0);
//BTM        getServiceID(metadataServer0);
//BTM        getServiceID(dataServer0);
//BTM        getServiceID(dataServer1);
//BTM        getServiceID(loadBalancerServer0);
//BTM
//BTM -----------------------------------------------------------------------

if(csRemote0 != null) getServiceID(csRemote0);
if(cs0 != null) getServiceID(com.bigdata.service.CallableExecutor.class, sdm);

if(txnServiceRemote0 != null) getServiceID(txnServiceRemote0);
if(txnService0 != null) getServiceID(com.bigdata.service.ShardLocator.class, sdm);

if(mdsRemote0 != null) getServiceID(mdsRemote0);
if(mds0 != null) getServiceID(com.bigdata.service.ShardLocator.class, sdm);

if(dsRemote0 != null) getServiceID(dsRemote0);
if(ds0 != null) getServiceID(com.bigdata.service.ShardService.class, sdm);

if(dsRemote1 != null) getServiceID(dsRemote1);
if(ds1 != null) getServiceID(com.bigdata.service.ShardService.class, sdm);

if(lbsRemote0 != null) getServiceID(lbsRemote0);
if(lbs0 != null) getServiceID(com.bigdata.service.LoadBalancer.class, sdm);

if(sdm != null) sdm.terminate();
if(ldm != null) ldm.terminate();
//BTM -----------------------------------------------------------------------

    }

    /**
     * Shuts down and <em>destroys</em> the services in the federation. The
     * shutdown is abrupt. You can expect to see messages about interrupted IO
     * such as
     * 
     * <pre>
     * java.rmi.MarshalException: error marshalling arguments; nested exception is: 
     *     java.io.IOException: request I/O interrupted
     *     at net.jini.jeri.BasicInvocationHandler.invokeRemoteMethodOnce(BasicInvocationHandler.java:785)
     *     at net.jini.jeri.BasicInvocationHandler.invokeRemoteMethod(BasicInvocationHandler.java:659)
     *     at net.jini.jeri.BasicInvocationHandler.invoke(BasicInvocationHandler.java:528)
     *     at $Proxy5.notify(Ljava.lang.String;Ljava.util.UUID;Ljava.lang.String;[B)V(Unknown Source)
     * </pre>
     * 
     * These messages can be safely ignored IF they occur during this method.
     */
    public void destroy() {

        ZooKeeper zookeeper = null;
        
        ZookeeperClientConfig zooConfig = null;
        
        if (client != null && client.isConnected()) {

            zooConfig = client.getFederation().getZooConfig();
            
            zookeeper = client.getFederation().getZookeeper();
            
            client.disconnect(true/* immediateShutdown */);

            client = null;

        }

//BTM        if (metadataServer0 != null) {
//BTM
//BTM            metadataServer0.destroy();
//BTM
//BTM            metadataServer0 = null;
//BTM
//BTM        }
if (mdsRemote0 != null) {
    mdsRemote0.destroy();
    mdsRemote0 = null;
}
//BTM        if (dataServer0 != null) {
//BTM
//BTM            dataServer0.destroy();
//BTM
//BTM            dataServer0 = null;
//BTM
//BTM        }
//BTM
//BTM        if (dataServer1 != null) {
//BTM
//BTM            dataServer1.destroy();
//BTM
//BTM            dataServer1 = null;
//BTM
//BTM        }
if (dsRemote0 != null) {
    dsRemote0.destroy();
    dsRemote0 = null;
}
if (dsRemote1 != null) {
    dsRemote1.destroy();
    dsRemote1 = null;
}
if (ds0 != null) {
    ds0.destroy();
    ds0 = null;
}
if (ds1 != null) {
    ds1.destroy();
    ds1 = null;
}
//BTM - destroy the shard locator after the data services are destroyed; which will avoid InterruptedException from WORMStrategy.releaseCache
//BTM - that seems to be thrown in the data service only if the shard locator is no longer available. Note that this issue 
//BTM - does not appear to affect the remote implementation of the metadata service
if (mds0 != null) {
    mds0.destroy();
    mds0 = null;
}

//BTM        if (clientServer0 != null) {
//BTM
//BTM            clientServer0.destroy();
//BTM
//BTM            clientServer0 = null;
//BTM
//BTM        }
if (csRemote0 != null) {
    csRemote0.destroy();
    csRemote0 = null;
}
if (cs0 != null) {
    cs0.destroy();
    cs0 = null;
}
        
//BTM        if (loadBalancerServer0 != null) {
//BTM
//BTM            loadBalancerServer0.destroy();
//BTM
//BTM            loadBalancerServer0 = null;
//BTM
//BTM        }
if (lbsRemote0 != null) {
    lbsRemote0.destroy();
    lbsRemote0 = null;
}
if (lbs0 != null) {
    lbs0.destroy();
    lbs0 = null;
}

//BTM        if (transactionServer0 != null) {
//BTM
//BTM            transactionServer0.destroy();
//BTM
//BTM            transactionServer0 = null;
//BTM
//BTM        }
if (txnServiceRemote0 != null) {
    txnServiceRemote0.destroy();
    txnServiceRemote0 = null;
}
if (txnService0 != null) {
    txnService0.destroy();
    txnService0 = null;
}

        if (zookeeper != null && zooConfig != null) {

            try {

                // clear out everything in zookeeper for this federation.
                zookeeper.delete(zooConfig.zroot, -1/* version */);
                
            } catch (Exception e) {
                
                // ignore.
                log.warn("zroot=" + zooConfig.zroot + " : "
                        + e.getLocalizedMessage(), e);
                
            }
            
        }
        
        try {
         
            ZooHelper.kill(clientPort);
            
        } catch (Throwable t) {
            log.error("Could not kill zookeeper: clientPort=" + clientPort
                    + " : " + t, t);
        }

        if (zooDataDir != null && zooDataDir.exists()) {
            /*
             * Wait a bit and then try and delete the zookeeper directory.
             */
            
//BTM - BEGIN
            // When using the smart proxy implementation of a given
            // service, that service's ServiceImpl class is instantiated
            // in a service-specific task (ex. ShardLocatorTask). When
            // this method shuts down the service by calling destroy on
            // the task (above), the task's destroy method ultimately
            // calls Thread.currentThread().interrupt(); which allows
            // the task to exit. Thus, for the smart proxy case, the
            // interrupt status is set to true when this method reaches
            // this point; which means that the call to Thread.sleep()
            // below will encounter an InterruptedException, which
            // causes a RuntimeException to be thrown. Although the
            // original author's intent for throwing a RuntimeException
            // under such circumstances is unclear (as is the need for
            // a 250 millisecond wait before deleting the zookeeper
            // directory), in order to maintain the original logic (at
            // least for the time being), the interrupt status is set
            // to false if it is determined to be currently set to true.
if( (Thread.currentThread()).isInterrupted() ) {
    Thread.interrupted();// clears the interrupt status
}
//BTM - END
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            recursiveDelete(zooDataDir);
        }
        
//        // Stop the lookup service.
//        new Thread(new Runnable() {
//            public void run() {
//                LookupStarter.main(new String[] { "-stop" });
//            }
//        }).start();
        
        if (fedServiceDir != null && fedServiceDir.exists()) {

            fedServiceDir.delete();
            
        }
        
        threadPool.shutdownNow();
        
    }

    /**
     * Return the {@link ServiceID} of a server that we started ourselves.
     * The method waits until the {@link ServiceID} becomes available on
     * {@link AbstractServer#getServiceID()}.
     * 
     * @throws RuntimeException
     *                If the {@link ServiceID} can not be found after a
     *                timeout.
     * 
     * @throws RuntimeException
     *                if the thread is interrupted while it is waiting to
     *                retry.
     */
    static private ServiceID getServiceID(final AbstractServer server) {

        ServiceID serviceID = null;

        for (int i = 0; i < 20 && serviceID == null; i++) {

            /*
             * Note: This can be null since the serviceID is not assigned
             * synchronously by the registrar.
             */

            serviceID = server.getServiceID();

            if (serviceID == null) {

                /*
                 * We wait a bit and retry until we have it or timeout.
                 */

                try {

                    Thread.sleep(200);

                } catch (InterruptedException e) {

                    throw new RuntimeException("Interrupted: " + e, e);

                }

            }

        }

        if (serviceID == null)
            throw new RuntimeException("Server did not start? "+server);

        return serviceID;

    }


    /**
     * Return an open port on current machine. Try the suggested port first. If
     * suggestedPort is zero, just select a random port
     */
    protected static int getPort(final int suggestedPort) throws IOException {
        
        ServerSocket openSocket;
        
        try {
        
            openSocket = new ServerSocket(suggestedPort);
            
        } catch (BindException ex) {
            
            // the port is busy, so look for a random open port
            openSocket = new ServerSocket(0);
        
        }

        final int port = openSocket.getLocalPort();
        
        openSocket.close();

        return port;
        
    }

    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself.
     * <p>
     * Note: Files that are not recognized will be logged by the
     * {@link ResourceFileFilter}.
     * 
     * @param f
     *            A file or directory.
     */
    private void recursiveDelete(final File f) {

        if (f.isDirectory()) {

            final File[] children = f.listFiles();

            if (children == null) {

                // The directory does not exist.
                return;
                
            }
            
            for (int i = 0; i < children.length; i++) {

                recursiveDelete(children[i]);

            }

        }

        if(log.isInfoEnabled())
            log.info("Removing: " + f);

        if (f.exists() && !f.delete()) {

            log.warn("Could not remove: " + f);

        }

    }

//BTM -----------------------------------------------------------------------
    private ServiceID getServiceID(Class classType,
                                   net.jini.lookup.ServiceDiscoveryManager sdm)
    {
        if(sdm == null) {
            throw new RuntimeException
                               ("JiniServicesHelper.getServiceID: null sdm");
        }
        net.jini.core.lookup.ServiceTemplate tmpl = 
        new net.jini.core.lookup.ServiceTemplate
                              (null, new Class[] {classType}, null);
        net.jini.core.lookup.ServiceItem item = null;
        try {
            item = sdm.lookup(tmpl, null, (20L*200L) );
        } catch(Throwable t) { 
            t.printStackTrace();
        }

        if ( (item == null) || (item.service == null) ) {
            throw new RuntimeException("JiniServicesHelper.getServiceID: "
                                       +"server did not start? "
                                       +"["+classType+"]");
        }
        return item.serviceID;
    }

    // Convenience class that allows one to instantiate and run the
    // callable executor service's ServiceImpl class as a task in a
    // thread pool.
    private class CallableExecutorTask implements Runnable {
        private String[] args;
        private com.bigdata.executor.ServiceImpl callableExecutor;
        CallableExecutorTask(String[] args) {
            this.args = args;
        }
        public void run() {
            try {
                this.callableExecutor =
                    new com.bigdata.executor.ServiceImpl
                                                 (args,new FakeLifeCycle());
            } catch(Throwable t) {
                t.printStackTrace();
                return;
            }
            while( !(Thread.currentThread()).isInterrupted() ) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) { /*exit while loop*/ }
            }
        }
        public void destroy() {
            try {
                this.callableExecutor.destroy();
            } catch(Throwable t) { /* swallow */ }
            Thread.currentThread().interrupt();
        }
    }

    // Convenience class that allows one to instantiate and run the
    // transaction service's ServiceImpl class as a task in a thread pool.
    private class TransactionTask implements Runnable {
        private String[] args;
        private com.bigdata.transaction.ServiceImpl txnService;
        TransactionTask(String[] args) {
            this.args = args;
        }
        public void run() {
            try {
                this.txnService =
                    new com.bigdata.transaction.ServiceImpl
                                                 (args,new FakeLifeCycle());
            } catch(Throwable t) {
                t.printStackTrace();
                return;
            }
            while( !(Thread.currentThread()).isInterrupted() ) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) { /*exit while loop*/ }
            }
        }
        public void destroy() {
            try {
                this.txnService.destroy();
            } catch(Throwable t) { /* swallow */ }
            Thread.currentThread().interrupt();
        }
    }

    // Convenience class that allows one to instantiate and run the
    // shard service's ServiceImpl class as a task in a thread pool.
    private class ShardServiceTask implements Runnable, Service {
        private String[] args;
        private com.bigdata.shard.ServiceImpl shardService;
        private boolean ready = false;

        ShardServiceTask(String[] args) {
            this.args = args;
        }
        public void run() {
            try {
                this.shardService =
                    new com.bigdata.shard.ServiceImpl
                            (args,new FakeLifeCycle());
                ready = true;
            } catch(Throwable t) {
                t.printStackTrace();
                return;
            }
            while( !(Thread.currentThread()).isInterrupted() ) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) { /*exit while loop*/ }
            }
        }
        public void destroy() {
            try {
                this.shardService.destroy();
            } catch(Throwable t) { /* swallow */ }
            Thread.currentThread().interrupt();
        }
        public UUID getServiceUUID() {
            if(!ready) {
                throw new UnsupportedOperationException
                              ("service not ready");
            }
            return (this.shardService).getServiceUUID();
        }
        public Class getServiceIface() {
            throw new UnsupportedOperationException();
        }
        public String getServiceName() {
            throw new UnsupportedOperationException();
        }
        public String getHostname() {
            throw new UnsupportedOperationException();
        }
    }

    // Convenience class that allows one to instantiate and run the
    // shard locator's ServiceImpl class as a task in a thread pool.
    private class ShardLocatorTask implements Runnable {
        private String[] args;
        private com.bigdata.metadata.ServiceImpl shardLocator;
        ShardLocatorTask(String[] args) {
            this.args = args;
        }
        public void run() {
            try {
                this.shardLocator =
                    new com.bigdata.metadata.ServiceImpl
                                                 (args,new FakeLifeCycle());
            } catch(Throwable t) {
                t.printStackTrace();
                return;
            }
            while( !(Thread.currentThread()).isInterrupted() ) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) { /*exit while loop*/ }
            }
        }
        public void destroy() {
            try {
                this.shardLocator.destroy();
            } catch(Throwable t) { /* swallow */ }
            Thread.currentThread().interrupt();
        }
    }

    // Convenience class that allows one to instantiate and run the
    // load balancer's ServiceImpl class as a task in a thread pool.
    private class LoadBalancerTask implements Runnable {
        private String[] args;
        private com.bigdata.loadbalancer.ServiceImpl loadBalancer;
        LoadBalancerTask(String[] args) {
            this.args = args;
        }
        public void run() {
            try {
                this.loadBalancer =
                    new com.bigdata.loadbalancer.ServiceImpl
                                                 (args,new FakeLifeCycle());
            } catch(Throwable t) {
                t.printStackTrace();
                return;
            }
            while( !(Thread.currentThread()).isInterrupted() ) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) { /*exit while loop*/ }
            }
        }
        public void destroy() {
            try {
                this.loadBalancer.destroy();
            } catch(Throwable t) { /* swallow */ }
            Thread.currentThread().interrupt();
        }
    }
//BTM -----------------------------------------------------------------------

    /**
     * Mock implementation used by some unit tests.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static private class ServiceListener implements IServiceListener {

        public Queue<ProcessHelper> running = new ConcurrentLinkedQueue<ProcessHelper>();

        public void add(ProcessHelper service) {

            if (log.isInfoEnabled())
                log.info("adding: " + service);

            running.add(service);

        }

        public void remove(ProcessHelper service) {

            if (log.isInfoEnabled())
                log.info("removing: " + service);

            running.remove(service);

        }

    }
    
}
