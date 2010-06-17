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
package com.bigdata.process;

import com.bigdata.util.Format;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// Note: all calls into this class must be synchronized externally
class RestartGroup {

    static private final Logger logger = Logger.getLogger(RestartGroup.class);

    // List of processes which are members of the restart group.
    private final List<ProcessManagement> memberList =
                                          new ArrayList<ProcessManagement>();

    /** Set of processes that need to stop before restart can continue. */
    private final Set<String> stopPendingSet = new HashSet<String>();

    /** Adds a member to the process restart group. */
    public void addMember(ProcessManagement pInfo) {

        if(memberList.contains(pInfo)) {
            throw new IllegalStateException
                          ("process already added to process restart group "
                           +"['"+pInfo.getTag()+"']");
        }
        memberList.add(pInfo);
    }

    /**
     * Triggers a restart of all running processes in the restart group.
     */
    public void triggerRestart() {

        if( stopPendingSet.isEmpty() ) {
            logger.log(Level.INFO, 
                       new Format("restart triggered on {0}", this) );

            for(ProcessManagement pInfo : memberList) {
                pInfo.restart_stop();
            }

            if( stopPendingSet.isEmpty() ) {
                beginRestart();// all the group members are already stopped
            }
        }
    }

    /**
     * Adds the given process tag to the set of processes that need
     * to shut down before the restart group can be started again.
     */
    public void addToStopPendingSet(String pTag) {
        stopPendingSet.add(pTag);
    }

    /**
     * Removes the given process tag from the set of processes that need
     * to shut down.
     */
    public void removeFromStopPendingSet(String pTag) {
        stopPendingSet.remove(pTag);
        logger.log(Level.DEBUG, "process removed from stopPendingSet - "
                   +"remaining processes="+stopPendingSet.toString() );

        if( stopPendingSet.isEmpty() ) {
            
            beginRestart();// restart since last process to stop was removed
        }
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        if( !(obj instanceof RestartGroup) ) return false;

        if( obj == this ) return true;//same object

        RestartGroup obj1 = this;
        RestartGroup obj2 = (RestartGroup)obj;

        List<String> pTagList1 = new ArrayList<String>();
        for( ProcessManagement procMgmt : obj1.memberList ) {
            pTagList1.add(procMgmt.getTag());
        }

        List<String> pTagList2 = new ArrayList<String>();
        for( ProcessManagement procMgmt : obj2.memberList ) {
            pTagList2.add(procMgmt.getTag());
        }

        if( !(pTagList1.containsAll(pTagList2)) ) return false;
        if( !(pTagList2.containsAll(pTagList1)) ) return false;

        if( !((obj1.stopPendingSet).containsAll(obj2.stopPendingSet)) ) {
            return false;
        }
        if( !((obj2.stopPendingSet).containsAll(obj1.stopPendingSet)) ) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RestartGroup(");
        boolean first = true;
        for(ProcessManagement pInfo : memberList) {
            if( !first ) sb.append(',');
            first = false;
            sb.append( pInfo.getTag() );
        }
        sb.append(')');
        return sb.toString();
    }

    private void beginRestart() {
        // Start all the processes again
        for(ProcessManagement pInfo : memberList) {
            pInfo.restart_start();
        }
    }
}
