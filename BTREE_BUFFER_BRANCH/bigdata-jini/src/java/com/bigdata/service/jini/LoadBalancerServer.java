package com.bigdata.service.jini;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.ServerNotActiveException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;

import net.jini.config.Configuration;
import net.jini.export.ServerContext;
import net.jini.io.context.ClientHost;
import net.jini.io.context.ClientSubject;
import net.jini.lookup.entry.Name;

import org.apache.log4j.MDC;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.httpd.CounterSetHTTPD;
import com.bigdata.counters.httpd.CounterSetHTTPDServer;
import com.bigdata.journal.ITx;
import com.bigdata.service.DefaultServiceFederationDelegate;
import com.bigdata.service.IFederationDelegate;
import com.bigdata.service.IService;
import com.bigdata.service.LoadBalancerService;
import com.bigdata.service.jini.util.DumpFederation;
import com.bigdata.service.jini.util.DumpFederation.FormatRecord;
import com.bigdata.service.jini.util.DumpFederation.FormatTabTable;
import com.bigdata.util.httpd.AbstractHTTPD;
import com.bigdata.util.httpd.NanoHTTPD;
import com.bigdata.util.httpd.NanoHTTPD.Response;
import com.sun.jini.start.LifeCycle;
import com.sun.jini.start.ServiceDescriptor;
import com.sun.jini.start.ServiceStarter;

/**
 * The load balancer server.
 * <p>
 * The {@link LoadBalancerServer} starts the {@link LoadBalancerService}. The
 * server and service are configured using a {@link Configuration} file whose
 * name is passed to the {@link LoadBalancerServer#LoadBalancerServer(String[])}
 * constructor or {@link #main(String[])}.
 * <p>
 * 
 * @see src/resources/config for sample configurations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LoadBalancerServer extends AbstractServer {

    /**
     * Options for this server.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends AdministrableLoadBalancer.Options {
        
    }
    
    /**
     * 
     * @param args
     *            Either the command line arguments or the arguments from the
     *            {@link ServiceDescriptor}. Either way they identify the jini
     *            {@link Configuration} (you may specify either a file or URL)
     *            and optional overrides for that {@link Configuration}.
     * @param lifeCycle
     *            The life cycle object. This is used if the server is started
     *            by the jini {@link ServiceStarter}. Otherwise specify a
     *            {@link FakeLifeCycle}.
     */
    public LoadBalancerServer(final String[] args, final LifeCycle lifeCycle) {

        super(args, lifeCycle);
        
        try {

            /*
             * Note: This signal is not supported under Windows.
             */
            new SigHUPHandler("HUP");

        } catch (IllegalArgumentException ex) {

            log.warn("Signal handler not installed: " + ex);
            
        }
    }

    /**
     * Starts a new {@link LoadBalancerServer}. This can be done
     * programmatically by executing
     * 
     * <pre>
     * new LoadBalancerServer(args, new FakeLifeCycle()).run();
     * </pre>
     * 
     * within a {@link Thread}.
     * 
     * @param args
     *            The name of the {@link Configuration} file for the service.
     */
    public static void main(String[] args) {
        
        new LoadBalancerServer(args, new FakeLifeCycle()).run();

        System.exit(0);
//      Runtime.getRuntime().halt(0);

    }

    @Override
    protected LoadBalancerService newService(final Properties properties) {
        
        final LoadBalancerService service = new AdministrableLoadBalancer(this, properties);
        
        /*
         * Setup a delegate that let's us customize some of the federation
         * behaviors on the behalf of the load balancer.
         * 
         * Note: We can't do this with the local or embedded federations since
         * they have only one client per federation and an attempt to set the
         * delegate more than once will cause an exception to be thrown!
         */
        final JiniClient client = getClient();

        if(client.isConnected()) {

            /*
             * Note: We need to set the delegate before the client is connected
             * to the federation. This ensures that the delegate, and hence the
             * load balancer, will see all join/leave events.
             */

            throw new IllegalStateException();
            
        }
        
        client.setDelegate(new LoadBalancerServiceFederationDelegate(service));

        return service;
        
    }
    
    /**
     * SIGHUP Handler.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class SigHUPHandler implements SignalHandler {

        private final SignalHandler oldHandler;

        /**
         * Install handler.
         * 
         * @param signalName
         *            The signal name.
         * 
         * @see http://www-128.ibm.com/developerworks/java/library/i-signalhandling/
         * 
         * @see http://forum.java.sun.com/thread.jspa?threadID=514860&messageID=2451429
         *      for the use of {@link Runtime#addShutdownHook(Thread)}.
         * 
         * @see http://twit88.com/blog/2008/02/06/java-signal-handling/
         */
        @SuppressWarnings("all") // Signal is in the sun namespace
        protected SigHUPHandler(final String signalName) {

            final Signal signal = new Signal(signalName);

            this.oldHandler = Signal.handle(signal, this);
            
            if (log.isInfoEnabled())
                log.info("Installed handler: " + signal + ", oldHandler="
                        + this.oldHandler);

        }

        @SuppressWarnings("all") // Signal is in the sun namespace
        public void handle(final Signal sig) {

            log.warn("Processing signal: " + sig);

            try {
                
                final AdministrableLoadBalancer service = (AdministrableLoadBalancer) impl;

                if (service != null) {

                    service.logCounters();
                    
                }

                /*
                 * This appears willing to halt the server so I am not chaining
                 * back to the previous handler!
                 */
                
//                // Chain back to previous handler, if one exists
//                if (oldHandler != SIG_DFL && oldHandler != SIG_IGN) {
//
//                    oldHandler.handle(sig);
//
//                }

            } catch (Throwable t) {

                log.error("Signal handler failed : " + t, t);

            }

        }

    }

    /**
     * Overrides the {@link IFederationDelegate} leave/join behavior to notify
     * the {@link LoadBalancerService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class LoadBalancerServiceFederationDelegate extends
            DefaultServiceFederationDelegate<LoadBalancerService> {

        /**
         * @param service
         */
        public LoadBalancerServiceFederationDelegate(
                LoadBalancerService service) {

            super(service);

        }
        
        /**
         * Notifies the {@link LoadBalancerService}.
         */
        public void serviceJoin(IService service, UUID serviceUUID) {

            try {

                // Note: This is an RMI request!
                final Class serviceIface = service.getServiceIface();
                
                // Note: This is an RMI request!
                final String hostname = service.getHostname();

                if (log.isInfoEnabled())
                    log.info("serviceJoin: serviceUUID=" + serviceUUID
                            + ", serviceIface=" + serviceIface + ", hostname="
                            + hostname);
                
                // this is a local method call.
                this.service.join(serviceUUID, serviceIface, hostname);

            } catch (IOException ex) {

                log.error(ex.getLocalizedMessage(), ex);
                
            }
            
        }

        /**
         * Notifies the {@link LoadBalancerService}.
         */
        public void serviceLeave(UUID serviceUUID) {

            if (log.isInfoEnabled())
                log.info("serviceUUID=" + serviceUUID);
            
            this.service.leave(serviceUUID);
            
        }

        /**
         * Interface allows for implementation of different handlers for "GET".
         * <p>
         * Note: The implementations MUST be an inner class of a class derived
         * from {@link NanoHTTPD} since the {@link Response} ctor requires an
         * outer {@link NanoHTTPD} instance.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        public interface HTTPGetHandler {

            /**
             * HTTP GET 
             * 
             * @param uri
             *            Percent-decoded URI without parameters, for example
             *            "/index.cgi"
             * @param method
             *            "GET", "POST" etc.
             * @param parms
             *            Parsed, percent decoded parameters from URI and, in
             *            case of POST, data. The keys are the parameter names.
             *            Each value is a {@link Collection} of {@link String}s
             *            containing the bindings for the named parameter. The
             *            order of the URL parameters is preserved.
             * @param header
             *            Header entries, percent decoded
             * 
             * @return HTTP response
             * 
             * @see Response
             */
            public Response doGet(String uri, String method, Properties header,
                    LinkedHashMap<String, Vector<String>> parms)
                    throws Exception;
        
        }
        
        /**
         * Hacked to recognize URL paths other than the root and dispatch to an
         * appropriate handler. Handlers include performance counters (at the
         * root path), dump of the indices in the federation (/indices), and
         * events (/events).
         */
        public AbstractHTTPD newHttpd(final int httpdPort,
                final CounterSet counterSet) throws IOException {
            
            return new CounterSetHTTPD(httpdPort, counterSet, service) {

                /**
                 * Handler provides dump of index partitions for either all
                 * indices or each index namespace identified by a
                 * <code>namespace</code> URL query parameter.
                 * 
                 * @author <a
                 *         href="mailto:thompsonbry@users.sourceforge.net">Bryan
                 *         Thompson</a>
                 * @version $Id$
                 */
                class IndicesHandler implements HTTPGetHandler {

                    public Response doGet(String uri, String method, Properties header,
                            LinkedHashMap<String, Vector<String>> parms)
                            throws Exception {
                        
                        Vector<String> namespaces = parms.get("namespace");

                        Vector<String> timestamps = parms.get("timestamp");

                        // default is all indices.
                        if (namespaces == null) {

                            namespaces = new Vector<String>();

                            namespaces.add("");

                        }

                        // default is the most recently committed state.
                        if (timestamps == null) {

                            timestamps = new Vector<String>();

                            timestamps.add("" + ITx.READ_COMMITTED);

                        }
                        
                        final JiniFederation fed = (JiniFederation) ((LoadBalancerService) service)
                                .getFederation();
                        
                        final StringWriter w = new StringWriter();
                        
                        // @todo conneg for the mime type and the formatter.
                        final FormatRecord formatter = new FormatTabTable(w);

                        formatter.writeHeaders();

                        for (String t : timestamps) {

                            final long timestamp;
                            try {

                                timestamp = Long.valueOf(t);
                                
                            } catch (NumberFormatException ex) {
                                
                                return new Response(NanoHTTPD.HTTP_BADREQUEST,
                                        NanoHTTPD.MIME_TEXT_PLAIN,
                                        "Not a valid timestamp: " + t);
                                
                            }
                            
                            /*
                             * A read-only transaction as of the specified
                             * commit time.
                             */
                            final long tx = fed.getTransactionService().newTx(
                                    timestamp);

                            try {

                                final DumpFederation dumper = new DumpFederation(
                                        fed, tx, formatter);

                                for (String s : namespaces) {

                                    dumper.dumpIndices(s);

                                }

                            } finally {

                                // discard read-only transaction.
                                fed.getTransactionService().abort(tx);

                            }

                        }
                        
                        final Response r = new Response(NanoHTTPD.HTTP_OK,
                                NanoHTTPD.MIME_TEXT_PLAIN, w.toString());

                        /*
                         * Sets the cache behavior.
                         * 
                         * Note: These cache control parameters SHOULD indicate
                         * that the response is valid for 60 seconds, that the
                         * client must revalidate, and that the response is
                         * cachable even if the client was authenticated.
                         */
                        r.addHeader("Cache-Control",
                                "max-age=60, must-revalidate, public");
                        
                        return r;
                        
                    }
                    
                }

                final IndicesHandler indicesHandler = new IndicesHandler();
                
                public Response doGet(String uri, String method, Properties header,
                        LinkedHashMap<String, Vector<String>> parms)
                        throws Exception {

                    if(uri.equals("/indices")) {
                        
                        return indicesHandler.doGet(uri, method, header, parms);
                        
                    } else if(uri.equals("/")) {
                    
                        try {

                            reattachDynamicCounters();

                        } catch (Exception ex) {

                            /*
                             * Typically this is because the live journal has
                             * been concurrently closed during the request.
                             */

                            log.warn("Could not re-attach dynamic counters: "
                                    + ex, ex);

                        }
                        
                    }

                    return super.doGet(uri, method, header, parms);
                    
                }

            };

        }

    }
    
    /**
     * Adds jini administration interfaces to the basic {@link LoadBalancerService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AdministrableLoadBalancer extends LoadBalancerService implements
            RemoteAdministrable, RemoteDestroyAdmin {
        
        protected LoadBalancerServer server;
        
        public AdministrableLoadBalancer(LoadBalancerServer server,Properties properties) {
            
            super(properties);
            
            this.server = server;
            
        }
        
        @Override
        public JiniFederation getFederation() {

            return server.getClient().getFederation();
            
        }

        public Object getAdmin() throws RemoteException {

            if (log.isInfoEnabled())
                log.info(""+getServiceUUID());

            return server.proxy;
            
        }
        
        /**
         * Adds the following parameters to the {@link MDC}
         * <dl>
         * 
         * <dt>clientname
         * <dt>
         * <dd>The hostname or IP address of the client making the request (at
         * {@link #log.isInfoEnabled()} or better)</dd>
         * 
         * </dl>
         */
        protected void setupLoggingContext() {

            super.setupLoggingContext();

            if (log.isInfoEnabled())
                MDC.put("clientname", getClientHostname());

        }

        protected void clearLoggingContext() {

            if (log.isInfoEnabled())
                MDC.remove("clientname");

            super.clearLoggingContext();
            
        }
        
        /*
         * DestroyAdmin
         */

        /**
         * Destroy the service and deletes any files containing resources (<em>application data</em>)
         * that was in use by that service.
         * <p>
         * Note: The {@link LoadBalancerService} writes counters into a
         * configured directly but does not otherwise have configured state.
         * Those counters are NOT destroyed so that they may be used for
         * post-mortem analysis. See {@link CounterSetHTTPDServer}.
         * 
         * @throws RemoteException
         */
        public void destroy() throws RemoteException {

            server.runDestroy();

        }

        synchronized public void shutdown() {
            
            // normal service shutdown.
            super.shutdown();
            
            // jini service and server shutdown.
            server.shutdownNow();
            
        }
        
        synchronized public void shutdownNow() {
            
            // immediate service shutdown.
            super.shutdownNow();
            
            // jini service and server shutdown.
            server.shutdownNow();
            
        }
        
        /**
        * Note: {@link InetAddress#getHostName()} is used. This method makes a
        * one-time best effort attempt to resolve the host name from the
        * {@link InetAddress}.
        * 
        * @todo we could pass the class {@link ClientSubject} to obtain the
        *       authenticated identity of the client (if any) for an incoming
        *       remote call.
         */
        protected String getClientHostname() {

            InetAddress clientAddr;

            try {

                clientAddr = ((ClientHost) ServerContext
                        .getServerContextElement(ClientHost.class))
                        .getClientHost();

            } catch (ServerNotActiveException e) {

                /*
                 * This exception gets thrown if the client has made a direct
                 * (vs RMI) call.
                 */

                try {

                    clientAddr = Inet4Address.getLocalHost();

                } catch (UnknownHostException ex) {

                    return "localhost";

                }

            }

            return clientAddr.getCanonicalHostName();

        }

        /**
         * Extends the base behavior to return a {@link Name} of the service
         * from the {@link Configuration}. If no name was specified in the
         * {@link Configuration} then the value returned by the base class is
         * returned instead.
         */
        public String getServiceName() {

            String s = server.getServiceName();

            if (s == null)
                s = super.getServiceName();

            return s;

        }

        /**
         * Logs the counters on a file created using
         * {@link File#createTempFile(String, String, File)} in the log
         * directory.
         * 
         * @throws IOException
         * 
         * @todo this method is not exposed to RMI (it is not on any
         *       {@link Remote} interface) but it could be.
         */
        public void logCounters() throws IOException {

            final File file = File.createTempFile("counters-hup", ".xml", logDir);

            super.logCounters(file);

        }

    }

}
