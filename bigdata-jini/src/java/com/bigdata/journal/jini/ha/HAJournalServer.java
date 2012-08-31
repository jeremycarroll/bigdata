package com.bigdata.journal.jini.ha;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.rmi.RemoteException;
import java.rmi.server.ServerNotActiveException;
import java.util.Properties;
import java.util.UUID;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.core.lookup.ServiceID;
import net.jini.export.ServerContext;
import net.jini.io.context.ClientHost;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAGlueDelegate;
import com.bigdata.ha.QuorumService;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.quorum.Quorum;
import com.bigdata.service.DataService;
import com.bigdata.service.jini.FakeLifeCycle;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.RemoteAdministrable;
import com.bigdata.service.jini.RemoteDestroyAdmin;
import com.sun.jini.start.LifeCycle;

/**
 * An administratable server for an {@link HAJournal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HAJournalServer extends AbstractServer {

    private static final Logger log = Logger.getLogger(HAJournal.class);

    public interface ConfigurationOptions {
        
        String COMPONENT = HAJournalServer.class.getName();
        
        /**
         * The {@link InetSocketAddress} at which the managed {@link HAJournal}
         * exposes its write pipeline interface (required).
         */
        String WRITE_PIPELINE_ADDR = "writePipelineAddr";
        
        /**
         * The ordered {@link UUID} each service in the write replication
         */
        String PIPELINE_UUIDS = "pipelineUUIDs";
        
    }
    
    public HAJournalServer(final String[] args, final LifeCycle lifeCycle) {

        super(args, lifeCycle);

    }

    @Override
    protected HAGlue newService(final Configuration config)
            throws ConfigurationException {

        // The address at which this service exposes its write pipeline.
        final InetSocketAddress writePipelineAddr = (InetSocketAddress) config
                .getEntry(ConfigurationOptions.COMPONENT,
                        ConfigurationOptions.WRITE_PIPELINE_ADDR,
                        InetSocketAddress.class);

        // The write replication pipeline.
        final UUID[] pipelineUUIDs = (UUID[]) config.getEntry(
                ConfigurationOptions.COMPONENT,
                ConfigurationOptions.PIPELINE_UUIDS, UUID[].class);

        /*
         * Configuration properties for this HAJournal.
         */
        final Properties properties = JiniClient.getProperties(
                HAJournal.class.getName(), config);

        // Force the writePipelineAddr into the Properties.
        properties
                .put(HAJournal.Options.WRITE_PIPELINE_ADDR, writePipelineAddr);

        final ServiceID serviceID = getServiceID();

        final UUID serviceUUID = JiniUtil.serviceID2UUID(serviceID);

        final Quorum<HAGlue, QuorumService<HAGlue>> quorum = new StaticQuorum(
                serviceUUID, pipelineUUIDs);

        final HAJournal journal = new HAJournal(properties, quorum);

        final HAGlue service = journal.newHAGlue(serviceUUID);

        final AdministrableHAGlueService administrableService = new AdministrableHAGlueService(
                this, service);

        return administrableService;

    }

    /**
     * Start an {@link HAJournal}.
     * <p>
     * <strong>Jini MUST be running</strong>
     * <p>
     * <strong>You MUST specify a sufficiently lax security policy</strong>,
     * e.g., using <code>-Djava.security.policy=policy.all</code>, where
     * <code>policy.all</code> is the name of a policy file.
     * 
     * @param args
     *            The name of the configuration file.
     */
    public static void main(final String[] args) {

        if (args.length == 0) {

            System.err.println("usage: <config-file> [config-overrides]");

            System.exit(1);

        }

        new HAJournalServer(args, new FakeLifeCycle()).run();
        
        System.exit(0);

    }
    
    /**
     * Adds jini administration interfaces to the basic {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AdministrableHAGlueService extends HAGlueDelegate
            implements RemoteAdministrable, RemoteDestroyAdmin {

        final protected HAJournalServer server;

        public AdministrableHAGlueService(final HAJournalServer server,
                final HAGlue service) {

            super(service);

            this.server = server;
            
        }
        
        public Object getAdmin() throws RemoteException {

            if (log.isInfoEnabled())
                log.info("serviceID=" + server.getServiceID());

            return server.proxy;
            
        }
        
        /**
         * Sets up the {@link MDC} logging context. You should do this on every
         * client facing point of entry and then call
         * {@link #clearLoggingContext()} in a <code>finally</code> clause. You
         * can extend this method to add additional context.
         * <p>
         * This implementation adds the following parameters to the {@link MDC}.
         * <dl>
         * <dt>serviceName</dt>
         * <dd>The serviceName is typically a configuration property for the
         * service. This datum can be injected into log messages using
         * <em>%X{serviceName}</em> in your log4j pattern layout.</dd>
         * <dt>serviceUUID</dt>
         * <dd>The serviceUUID is, in general, assigned asynchronously by the
         * service registrar. Once the serviceUUID becomes available it will be
         * added to the {@link MDC}. This datum can be injected into log
         * messages using <em>%X{serviceUUID}</em> in your log4j pattern layout.
         * </dd>
         * <dt>hostname</dt>
         * <dd>The hostname statically determined. This datum can be injected
         * into log messages using <em>%X{hostname}</em> in your log4j pattern
         * layout.</dd>
         * <dt>clientname
         * <dt>
         * <dd>The hostname or IP address of the client making the request.</dd>
         * </dl>
         * Note: {@link InetAddress#getHostName()} is used. This method makes a
         * one-time best effort attempt to resolve the host name from the
         * {@link InetAddress}.
         */
        protected void setupLoggingContext() {

            try {

                // Note: This _is_ a local method call.
                final ServiceID serviceUUID = server.getServiceID();

                // Will be null until assigned by the service registrar.

                if (serviceUUID != null) {

                    MDC.put("serviceUUID", serviceUUID);

                }

                MDC.put("serviceName", server.getServiceName());

                MDC.put("hostname", server.getHostName());

                try {

                    final InetAddress clientAddr = ((ClientHost) ServerContext
                            .getServerContextElement(ClientHost.class))
                            .getClientHost();

                    MDC.put("clientname", clientAddr.getHostName());

                } catch (ServerNotActiveException e) {

                    /*
                     * This exception gets thrown if the client has made a
                     * direct (vs RMI) call so we just ignore it.
                     */

                }

            } catch (Throwable t) {

                /*
                 * Ignore.
                 */

            }

        }

        /**
         * Clear the logging context.
         */
        protected void clearLoggingContext() {
            
            MDC.remove("serviceName");

            MDC.remove("serviceUUID");

            MDC.remove("hostname");
            
            MDC.remove("clientname");

        }

        /*
         * DestroyAdmin
         */

        @Override
        synchronized public void destroy() {

            if (!server.isShuttingDown()) {

                /*
                 * Run thread which will destroy the service (asynchronous).
                 * 
                 * Note: By running this is a thread, we avoid closing the
                 * service end point during the method call.
                 */

                server.runDestroy();

//            } else if (isOpen()) {
//
//                /*
//                 * The server is already shutting down, so invoke our super
//                 * class behavior to destroy the persistent state.
//                 */
//
//                super.destroy();

            }

        }

        @Override
        synchronized public void shutdown() {

//            // normal service shutdown (blocks).
//            super.shutdown();

            // jini service and server shutdown.
            server.shutdownNow(false/* destroy */);

        }

        @Override
        synchronized public void shutdownNow() {

//            // immediate service shutdown (blocks).
//            super.shutdownNow();

            // jini service and server shutdown.
            server.shutdownNow(false/* destroy */);
            
        }

//        /**
//         * Extends the base behavior to return a {@link Name} of the service
//         * from the {@link Configuration}. If no name was specified in the
//         * {@link Configuration} then the value returned by the base class is
//         * returned instead.
//         */
//        @Override
//        public String getServiceName() {
//
//            String s = server.getServiceName();
//
//            if (s == null)
//                s = super.getServiceName();
//
//            return s;
//
//        }

    }

}
