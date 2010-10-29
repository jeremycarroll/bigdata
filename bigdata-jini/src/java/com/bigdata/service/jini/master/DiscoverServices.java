package com.bigdata.service.jini.master;

import java.rmi.Remote;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceRegistrar;

//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.lookup.BigdataCachingServiceClient;
import com.bigdata.service.jini.master.TaskMaster.JobState;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.service.IService;
import com.bigdata.service.Service;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceDiscoveryListener;
import java.util.UUID;

/**
 * Class discovers and returns services matching a {@link ServicesTemplate}.
 * A number of instances of this class are submitted in parallel to verify
 * that the pre-conditions for the {@link JobState} are satisified and to
 * return the {@link ServiceItem}s for the callable executor service to
 * which the client tasks will be distributed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DiscoverServices implements Callable<ServiceItem[]> {

    protected static final transient Logger log = Logger.getLogger(DiscoverServices.class);
	
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE     private final JiniFederation fed;
    private final ServiceDiscoveryManager sdm;
//BTM - PRE_CLIENT_SERVICE - END
    
    private final ServicesTemplate servicesTemplate;

    private final long timeout;

    /**
     * 
     * @param fed
     *            The {@link JiniFederation}, which is already handling
     *            {@link ServiceRegistrar} discovery.
     * @param servicesTemplate
     *            The template to be matching, including the minimum #of
     *            matches to be made.
     * @param timeout
     *            The timeout in milliseconds to await the minimum #of
     *            matching services.
     */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    public DiscoverServices(final JiniFederation fed,
//BTM - PRE_CLIENT_SERVICE            final ServicesTemplate servicesTemplate, final long timeout) {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        if (fed == null)
//BTM - PRE_CLIENT_SERVICE            throw new IllegalArgumentException();
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        if (servicesTemplate == null)
//BTM - PRE_CLIENT_SERVICE            throw new IllegalArgumentException();
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        if (timeout <= 0)
//BTM - PRE_CLIENT_SERVICE            throw new IllegalArgumentException();
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        this.fed = fed;
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        this.servicesTemplate = servicesTemplate;
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        this.timeout = timeout;
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE    }
    public DiscoverServices(final ServiceDiscoveryManager sdm,
                            final ServicesTemplate servicesTemplate,
                            final long timeout)
    {
        if (sdm == null) {
            throw new NullPointerException("null sdm");
        }
        if (servicesTemplate == null) {
            throw new NullPointerException("null serviceTemplate");
        }

        if (timeout <= 0) {
            throw new IllegalArgumentException
                          ("non-positive timeout ["+timeout+"]");
        }
        this.sdm = sdm;
        this.servicesTemplate = servicesTemplate;
        this.timeout = timeout;
    }
//BTM - PRE_CLIENT_SERVICE - END

    /**
     * Return all matching services, waiting no longer than the timeout
     * specified to the ctor.
     * 
     * @return All matching services. The caller can use as many as they
     *         desire and is responsible for handling the case when fewer
     *         services were matched than were requested.
     */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    public ServiceItem[] call() throws Exception {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        /*
//BTM - PRE_CLIENT_SERVICE         * Setup cached discovery for services matching the template and
//BTM - PRE_CLIENT_SERVICE         * filter.
//BTM - PRE_CLIENT_SERVICE         * 
//BTM - PRE_CLIENT_SERVICE         * Note: The cache miss timeout is ignored since we will not use
//BTM - PRE_CLIENT_SERVICE         * lookup by ServiceID here.
//BTM - PRE_CLIENT_SERVICE         */
//BTM - PRE_CLIENT_SERVICE        final BigdataCachingServiceClient<Remote> serviceClient = new BigdataCachingServiceClient<Remote>(
//BTM - PRE_CLIENT_SERVICE                fed, Remote.class/* serviceIfaceIsForLoggingOnly */,
//BTM - PRE_CLIENT_SERVICE                servicesTemplate.template, servicesTemplate.filter, 1000/* cacheMissTimeout */) {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        };
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        final long begin = System.currentTimeMillis();
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        int serviceCount;
//BTM - PRE_CLIENT_SERVICE        long elapsed = 0;
//BTM - PRE_CLIENT_SERVICE        int ntries = 0;
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        while ((serviceCount = serviceClient.getServiceCache()
//BTM - PRE_CLIENT_SERVICE				.getServiceCount()) < servicesTemplate.minMatches
//BTM - PRE_CLIENT_SERVICE				&& (elapsed = (System.currentTimeMillis() - begin)) < timeout) {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        	ntries++;
//BTM - PRE_CLIENT_SERVICE        	
//BTM - PRE_CLIENT_SERVICE            if (log.isDebugEnabled())
//BTM - PRE_CLIENT_SERVICE				log.debug("Discovered " + serviceCount + " : ntries=" + ntries
//BTM - PRE_CLIENT_SERVICE						+ ", elapsed=" + elapsed + ", template="
//BTM - PRE_CLIENT_SERVICE						+ servicesTemplate);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE            final long remaining = timeout - elapsed;
//BTM - PRE_CLIENT_SERVICE            
//BTM - PRE_CLIENT_SERVICE            // sleep a bit to await further service discovery.
//BTM - PRE_CLIENT_SERVICE            Thread.sleep(remaining < 100 ? remaining : 100);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        }
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        /*
//BTM - PRE_CLIENT_SERVICE         * Return all discovered services which matched the template.
//BTM - PRE_CLIENT_SERVICE         */
//BTM - PRE_CLIENT_SERVICE        final ServiceItem[] a = serviceClient.getServiceCache()
//BTM - PRE_CLIENT_SERVICE				.getServiceItems(0/* maxCount */, null/* filter */);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        if (log.isInfoEnabled())
//BTM - PRE_CLIENT_SERVICE			log
//BTM - PRE_CLIENT_SERVICE					.info("Discovered " + serviceCount + " : ntries=" + ntries
//BTM - PRE_CLIENT_SERVICE							+ ", elapsed=" + elapsed + ", template="
//BTM - PRE_CLIENT_SERVICE							+ servicesTemplate);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE		return a;
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE	}
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE - NOTE: the intent of the original code above seems to
//BTM - PRE_CLIENT_SERVICE -       be to wait up to the given timeout for the desired
//BTM - PRE_CLIENT_SERVICE -       services matching the given template to be discovered.
//BTM - PRE_CLIENT_SERVICE -       Thus, rather than using a BigdataCachingServiceClient 
//BTM - PRE_CLIENT_SERVICE -       in a wait loop in this class, the new version of the
//BTM - PRE_CLIENT_SERVICE -       call() method below uses the blocking versin of the
//BTM - PRE_CLIENT_SERVICE -       given sdm; which should provide the same behavior.
    public ServiceItem[] call() throws Exception {
        final ServiceItem[] a = 
                  sdm.lookup(servicesTemplate.template,
                             Integer.MAX_VALUE,//minimum acceptable
                             Integer.MAX_VALUE,//return no more than this many
                             servicesTemplate.filter,
                             timeout);//wait no more than this amount of time

        if (log.isInfoEnabled()) {
            log.info("Discovered "+a.length+" service(s) [timeout="+timeout
                     +", template="+servicesTemplate+"]");
        }
        return a;
    }
//BTM - PRE_CLIENT_SERVICE - END

}
