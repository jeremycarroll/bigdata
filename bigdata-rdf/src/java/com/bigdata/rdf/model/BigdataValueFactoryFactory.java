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
package com.bigdata.rdf.model;

import java.util.Hashtable;

import com.bigdata.rdf.lexicon.LexiconRelation;

/**
 * @author Matthew Roy ( <a href="mailto:mroy@cambridgesemantics.com">mroy@cambridgesemantics.com</a>)
 * 
 */
public class BigdataValueFactoryFactory {

     private static Hashtable<String, BigdataValueFactory>                      staticFactories = new Hashtable<String, BigdataValueFactory>();

    /**
     * Return the instance associated with the <i>namespace</i>.
     * <p>
     * Note: This canonicalizing mapping for {@link BigdataValueFactoryImpl}s is based on the namespace of the {@link LexiconRelation}. This makes the instances
     * canonical within a JVM instance, which is all that we care about. The actual assignments of term identifiers to {@link BigdataValue}s is performed by the
     * {@link LexiconRelation} itself and is globally consistent for a given lexicon.
     * 
     * @param namespace
     *            The namespace of the {@link LexiconRelation}.
     */
    public static BigdataValueFactory getInstance(final String namespace) {

        if (namespace == null)
            throw new IllegalArgumentException();

        synchronized (staticFactories) {
            BigdataValueFactory a = staticFactories.get(namespace);
            if (a == null) {
                return BigdataValueFactoryImpl.getInstance(namespace);
            }
            return a;

        }

    }

    public static void setInstance(final String namespace,final BigdataValueFactory valueFactory) {
        if (namespace == null||valueFactory==null)
            throw new IllegalArgumentException();

        synchronized (staticFactories) {
            staticFactories.put(namespace,valueFactory);

        }
    }
    
    /**
     * Remove a {@link BigdataValueFactoryImpl} from the canonicalizing mapping.
     * <p>
     * Entries in this canonicalizing mapping for a {@link LexiconRelation} MUST be {@link #remove(String)}ed if the {@link LexiconRelation} is destroyed in
     * case a distinct lexicon is subsequently creating with the same namespace. There is no need to discard an entry during abort processing.
     * 
     * @param namespace
     *            The namespace of the {@link LexiconRelation}.
     */
    public static void remove(final String namespace) {

        if (namespace == null)
            throw new IllegalArgumentException();

        synchronized (staticFactories) {
            if(staticFactories.remove(namespace)==null) {
                BigdataValueFactoryImpl.remove(namespace);
            }

        }

    }
}
