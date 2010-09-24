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
package com.bigdata.journal;

import java.util.Properties;

import com.bigdata.rawstore.IMRMW;

/**
 * <p>
 * An append-only persistence capable data structure supporting atomic commit,
 * scalable named indices, and transactions. Writes are logically appended to
 * the journal to minimize disk head movement.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IJournal extends IMRMW, IAtomicStore, IBTreeManager, IStoreFile {

    /**
     * A copy of the properties used to initialize this journal.
     */
    public Properties getProperties();
    
    /**
     * Shutdown the journal politely. Scheduled operations will run to
     * completion, but no new operations will be scheduled.
     */
    public void shutdown();

    /**
     * Immediate shutdown.
     */
    public void shutdownNow();
    
}
