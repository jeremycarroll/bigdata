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
package com.bigdata.loadbalancer;

import com.bigdata.service.ShutdownAdmin;

import net.jini.admin.JoinAdmin;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;

import java.io.InvalidObjectException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.UUID;

class AdminProxy implements JoinAdmin, ShutdownAdmin, Serializable {

    private static final long serialVersionUID = 1L;

    final PrivateInterface innerProxy;
    final UUID             proxyId;

    public static AdminProxy createProxy(PrivateInterface innerProxy,
                                         UUID             proxyId)
    {
        return new AdminProxy(innerProxy, proxyId);
    }

    /** Private constructor, called only by createProxy. */
    AdminProxy(PrivateInterface innerProxy, UUID proxyId) {
        this.innerProxy = innerProxy;
        this.proxyId    = proxyId;
    }

    // Required by net.jini.admin.JoinAdmin

    public Entry[] getLookupAttributes() throws RemoteException {
        return innerProxy.getLookupAttributes();
    }

    public void addLookupAttributes(Entry[] attrSets) throws RemoteException {
        innerProxy.addLookupAttributes(attrSets);
    }

    public void modifyLookupAttributes(Entry[] attrSetTemplates, 
                                       Entry[] attrSets)
                                                 throws RemoteException
    {
        innerProxy.modifyLookupAttributes(attrSetTemplates, attrSets);
    }

    public String[] getLookupGroups() throws RemoteException {
        return innerProxy.getLookupGroups();
    }

    public void addLookupGroups(String[] groups) throws RemoteException {
        innerProxy.addLookupGroups(groups);
    }

    public void removeLookupGroups(String[] groups) throws RemoteException {
        innerProxy.removeLookupGroups(groups);
    }

    public void setLookupGroups(String[] groups) throws RemoteException {
        innerProxy.setLookupGroups(groups);
    }

    public LookupLocator[] getLookupLocators() throws RemoteException {
        return innerProxy.getLookupLocators();
    }

    public void addLookupLocators(LookupLocator[] locators) 
                                                     throws RemoteException
    {
        innerProxy.addLookupLocators(locators);
    }

    public void removeLookupLocators(LookupLocator[] locators)
                                                     throws RemoteException
    {
        innerProxy.removeLookupLocators(locators);
    }

    public void setLookupLocators(LookupLocator[] locators)
                                                     throws RemoteException
    {
        innerProxy.setLookupLocators(locators);
    }

    // Required by ShutdownAdmin

    public void shutdown() throws IOException {
        innerProxy.shutdown();
    }

    public void shutdownNow() throws IOException {
        innerProxy.shutdownNow();
    }

    public void kill(int status) throws IOException {
        innerProxy.kill(status);
    }

    // Required by com.sun.jini.admin.DestroyAdmin (from ShutdownAdmin)

    public void destroy() throws RemoteException {
        innerProxy.destroy();
    }


    //Methods for good proxy behavior: hashCode, equals, readObject, etc.

    public int hashCode() {
        if(proxyId == null) return 0;
        return proxyId.hashCode();
    }

    /** 
     * Proxy equality is defined as <i>reference equality</i>; that is,
     * two proxies are equal if they reference (are proxies to) the
     * same backend server.
     */
    public boolean equals(Object obj) {
        if(proxyId == null) return false;
        return proxyId.equals(obj);
    }

    private void readObject(ObjectInputStream s) throws IOException,
                                                        ClassNotFoundException
    {
        s.defaultReadObject();

        // Verify fields are valid
        String errStr1 = "com.bigdata.loadbalancer.AdminProxyProxy."
                         +"readObject failure - ";
        String errStr2 = " field is null";

        if(innerProxy == null) {
            throw new InvalidObjectException(errStr1+"innerProxy"+errStr2);
        }

        if(proxyId == null) {
            throw new InvalidObjectException(errStr1+"proxyId"+errStr2);
        }
    }

    private void readObjectNoData() throws InvalidObjectException {
        throw new InvalidObjectException
         ("No data found when attempting to deserialize "
          +"com.bigdata.loadbalancer.AdminProxyProxy "
          +"instance");
    }
}
