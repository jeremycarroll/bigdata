package com.bigdata.jini.start.config;

import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.LookupCache;

import com.bigdata.jini.lookup.entry.HostnameFilter;
import com.bigdata.jini.lookup.entry.ServiceItemFilterChain;
import com.bigdata.service.IDataService;
import com.bigdata.service.jini.DataServiceFilter;
import com.bigdata.service.jini.JiniFederation;

/**
 * Constraint on the #of {@link IDataService}s on the same host.
 */
public class MaxDataServicesPerHostConstraint extends
        MaxServicesPerHostConstraint {

    /**
     * @param className
     * @param maxServices
     * @param timeout
     */
    public MaxDataServicesPerHostConstraint(int maxServices) {

        super(maxServices);

    }

    /**
     * 
     */
    private static final long serialVersionUID = 4146058645608689955L;

    public boolean allow(final JiniFederation fed) throws Exception {

        if (fed == null) // required
            throw new IllegalArgumentException();

        final LookupCache lookupCache = fed.getDataServicesClient()
                .getLookupCache();

        final ServiceItemFilterChain filter = new ServiceItemFilterChain();

        // only consider data services.
        filter.add(DataServiceFilter.INSTANCE);

        // filters for _this_ host.
        filter.add(new HostnameFilter());

        final ServiceItem[] serviceItems = lookupCache.lookup(filter,
                maxServices);

        final boolean allowed = serviceItems.length < maxServices;

        // if (INFO)
        // log.info // @todo lower logging level.
        MaxServicesPerHostConstraint.log.warn("New instance: allowed="
                + allowed + ", maxServices=" + maxServices + ", #found="
                + serviceItems.length);
        //
        return false;

    }

}
