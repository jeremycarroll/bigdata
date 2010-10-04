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
package com.bigdata.shard;

import com.bigdata.service.ShardService;

/**
 * Constants shared by classes in this service implementation's package.
 */
public interface Constants {

    Class  SERVICE_TYPE = ShardService.class;
    String SERVICE_NAME = SERVICE_TYPE.getSimpleName();

    /* Name of this component; used in config entry retrieval and the logger.*/
    String COMPONENT_NAME = ((Constants.class).getPackage()).getName();
    String F_SEP          = System.getProperty("file.separator");

    int  LOWER_BOUND_THREAD_POOL_SIZE = 1;
    int  UPPER_BOUND_THREAD_POOL_SIZE = 100;
    int  DEFAULT_THREAD_POOL_SIZE     = 30;

    int  DEFAULT_INDEX_CACHE_SIZE     = 20; 
    int  LOWER_BOUND_INDEX_CACHE_SIZE = 1;
    int  UPPER_BOUND_INDEX_CACHE_SIZE = 1000;

    long DEFAULT_INDEX_CACHE_TIMEOUT     = 60L*1000L;//1 minute
    long LOWER_BOUND_INDEX_CACHE_TIMEOUT = 1L*1000L; //1 second
    long UPPER_BOUND_INDEX_CACHE_TIMEOUT = Long.MAX_VALUE;//forever

    int  DEFAULT_RESOURCE_LOCATOR_CACHE_SIZE     = 20; 
    int  LOWER_BOUND_RESOURCE_LOCATOR_CACHE_SIZE = 1;
    int  UPPER_BOUND_RESOURCE_LOCATOR_CACHE_SIZE = 1000;

    long DEFAULT_RESOURCE_LOCATOR_CACHE_TIMEOUT     = 60L*1000L;//1 minute
    long LOWER_BOUND_RESOURCE_LOCATOR_CACHE_TIMEOUT = 1L*1000L; //1 second
    long UPPER_BOUND_RESOURCE_LOCATOR_CACHE_TIMEOUT = Long.MAX_VALUE;//forever

    long DEFAULT_LOAD_BALANCER_REPORTING_PERIOD     = 60L*1000L;//1 minute
    long LOWER_BOUND_LOAD_BALANCER_REPORTING_PERIOD = 10L*1000L; //10 seconds
    long UPPER_BOUND_LOAD_BALANCER_REPORTING_PERIOD = Long.MAX_VALUE;

    int  DEFAULT_HTTPD_PORT     =  0;//random
    int  LOWER_BOUND_HTTPD_PORT = -1;//disable the daemon
    int  UPPER_BOUND_HTTPD_PORT = Integer.MAX_VALUE;

    long EXECUTOR_TERMINATION_TIMEOUT = 1L*60L*1000L;
}
