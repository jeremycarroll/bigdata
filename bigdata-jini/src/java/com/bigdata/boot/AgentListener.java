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
package com.bigdata.boot;

/**
 * Interface implemented by agents of the boot launcher to provide 
 * listener instances that will receive commands from the boot launcher.
 */
public interface AgentListener {

    /**
     * Method called by the boot agent when the boot launcher wishes to
     * shut down the agent's associated launched process. This method
     * is also called when the launcher crashes; to prevent processes
     * from accumulating in the background.
     */
    void beginShutdown();
}
