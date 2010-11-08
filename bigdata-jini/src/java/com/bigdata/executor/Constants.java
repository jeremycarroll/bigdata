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
package com.bigdata.executor;

import com.bigdata.service.IBigdataClient;
import com.bigdata.service.CallableExecutor;

/**
 * Constants shared by classes in this service implementation's package.
 */
public interface Constants {

    Class  SERVICE_TYPE = CallableExecutor.class;
    String SERVICE_NAME = SERVICE_TYPE.getSimpleName();

    /* Name of this component; used in config entry retrieval and the logger.*/
    String COMPONENT_NAME = ((Constants.class).getPackage()).getName();
    String F_SEP          = System.getProperty("file.separator");

    long EXECUTOR_TERMINATION_TIMEOUT = 1L*60L*1000L;

    // Default values from IBigdataClient.Options

    int  LOWER_BOUND_THREAD_POOL_SIZE = 0;
    int  UPPER_BOUND_THREAD_POOL_SIZE = 100;
    int  DEFAULT_THREAD_POOL_SIZE =
         Integer.parseInt
         (IBigdataClient.Options.DEFAULT_CLIENT_THREAD_POOL_SIZE);

    int  LOWER_BOUND_INDEX_CACHE_SIZE = 1;
    int  UPPER_BOUND_INDEX_CACHE_SIZE = 1000;
    int  DEFAULT_INDEX_CACHE_SIZE = 
         Integer.parseInt
         (IBigdataClient.Options.DEFAULT_CLIENT_INDEX_CACHE_CAPACITY);

    long LOWER_BOUND_INDEX_CACHE_TIMEOUT = 1L*1000L; //1 second
    long UPPER_BOUND_INDEX_CACHE_TIMEOUT = Long.MAX_VALUE;//forever
    long DEFAULT_INDEX_CACHE_TIMEOUT =
         Long.parseLong
         (IBigdataClient.Options.DEFAULT_CLIENT_INDEX_CACHE_TIMEOUT);

    int  LOWER_BOUND_RESOURCE_LOCATOR_CACHE_SIZE = 1;
    int  UPPER_BOUND_RESOURCE_LOCATOR_CACHE_SIZE = 1000;
    int  DEFAULT_RESOURCE_LOCATOR_CACHE_SIZE =
         Integer.parseInt
         (IBigdataClient.Options.DEFAULT_CLIENT_LOCATOR_CACHE_CAPACITY);

    long LOWER_BOUND_RESOURCE_LOCATOR_CACHE_TIMEOUT = 1L*1000L; //1 second
    long UPPER_BOUND_RESOURCE_LOCATOR_CACHE_TIMEOUT = Long.MAX_VALUE;//forever
    long DEFAULT_RESOURCE_LOCATOR_CACHE_TIMEOUT =
         Long.parseLong
         (IBigdataClient.Options.DEFAULT_CLIENT_LOCATOR_CACHE_TIMEOUT);

    long LOWER_BOUND_LOAD_BALANCER_REPORTING_PERIOD = 10L*1000L; //10 seconds
    long UPPER_BOUND_LOAD_BALANCER_REPORTING_PERIOD = Long.MAX_VALUE;
    long DEFAULT_LOAD_BALANCER_REPORTING_PERIOD =
         Long.parseLong
         (IBigdataClient.Options.DEFAULT_REPORT_DELAY);

    int  LOWER_BOUND_HTTPD_PORT = -1;//disable the daemon
    int  UPPER_BOUND_HTTPD_PORT = Integer.MAX_VALUE;
    int  DEFAULT_HTTPD_PORT =
         Integer.parseInt
         (IBigdataClient.Options.DEFAULT_HTTPD_PORT);//0 - random

    int  LOWER_BOUND_MAX_STALE_LOCATOR_RETRIES = 0;
    int  UPPER_BOUND_MAX_STALE_LOCATOR_RETRIES = Integer.MAX_VALUE;
    int  DEFAULT_MAX_STALE_LOCATOR_RETRIES =
         Integer.parseInt
         (IBigdataClient.Options.DEFAULT_CLIENT_MAX_STALE_LOCATOR_RETRIES);

    int  LOWER_BOUND_MAX_PARALLEL_TASKS_PER_REQUEST = 1;
    int  UPPER_BOUND_MAX_PARALLEL_TASKS_PER_REQUEST = 1000;
    int  DEFAULT_MAX_PARALLEL_TASKS_PER_REQUEST =
         Integer.parseInt
         (IBigdataClient.Options.DEFAULT_CLIENT_MAX_PARALLEL_TASKS_PER_REQUEST);

    long LOWER_BOUND_TASK_TIMEOUT = 0;
    long UPPER_BOUND_TASK_TIMEOUT = Long.MAX_VALUE;
    long DEFAULT_TASK_TIMEOUT =
         Long.parseLong
         (IBigdataClient.Options.DEFAULT_CLIENT_TASK_TIMEOUT);

    int  LOWER_BOUND_RANGE_QUERY_CAPACITY = 256;
    int  UPPER_BOUND_RANGE_QUERY_CAPACITY = Integer.MAX_VALUE;
    int  DEFAULT_RANGE_QUERY_CAPACITY =
         Integer.parseInt
         (IBigdataClient.Options.DEFAULT_CLIENT_RANGE_QUERY_CAPACITY);

    boolean DEFAULT_BATCH_API_ONLY =
         Boolean.parseBoolean
         (IBigdataClient.Options.DEFAULT_CLIENT_BATCH_API_ONLY);

    boolean DEFAULT_COLLECT_QUEUE_STATISTICS =
         Boolean.parseBoolean
         (IBigdataClient.Options.DEFAULT_COLLECT_QUEUE_STATISTICS);

    boolean DEFAULT_COLLECT_PLATFORM_STATISTICS =
Boolean.FALSE;
//         Boolean.parseBoolean
//         (IBigdataClient.Options.DEFAULT_COLLECT_PLATFORM_STATISTICS);
}
