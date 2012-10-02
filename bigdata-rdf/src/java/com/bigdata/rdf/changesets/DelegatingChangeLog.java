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
package com.bigdata.rdf.changesets;

import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.log4j.Logger;

/**
 * This delegating change log allows change events to be propogated to multiple
 * delegates through a listener pattern.
 * 
 * @author mike
 */
public class DelegatingChangeLog implements IChangeLog {

    private static transient final Logger log = Logger
            .getLogger(DelegatingChangeLog.class);

    private final CopyOnWriteArraySet<IChangeLog> delegates;

    public DelegatingChangeLog() {
        
        this.delegates = new CopyOnWriteArraySet<IChangeLog>();
        
    }

    public void addDelegate(final IChangeLog delegate) {
        
        this.delegates.add(delegate);
        
    }

    public void removeDelegate(final IChangeLog delegate) {

        this.delegates.remove(delegate);
        
    }

    public void changeEvent(final IChangeRecord record) {

        if (log.isInfoEnabled())
            log.info(record);

        for (IChangeLog delegate : delegates) {

            delegate.changeEvent(record);

        }

    }

    public void transactionBegin() {

        if (log.isInfoEnabled())
            log.info("");

        for (IChangeLog delegate : delegates) {

            delegate.transactionBegin();

        }

    }
    
    public void transactionPrepare() {

        if (log.isInfoEnabled())
            log.info("");

        for (IChangeLog delegate : delegates) {

            delegate.transactionPrepare();

        }

    }
    
    public void transactionCommited(final long commitTime) {

        if (log.isInfoEnabled())
            log.info("transaction committed");

        for (IChangeLog delegate : delegates) {

            delegate.transactionCommited(commitTime);

        }

    }

    public void transactionAborted() {

        if (log.isInfoEnabled())
            log.info("transaction aborted");

        for (IChangeLog delegate : delegates) {

            delegate.transactionAborted();

        }

    }

}
