/**

 The Notice below must appear in each file of the Source Code of any
 copy you distribute of the Licensed Product.  Contributors to any
 Modifications may add their own copyright notices to identify their
 own contributions.

 License:

 The contents of this file are subject to the CognitiveWeb Open Source
 License Version 1.1 (the License).  You may not copy or use this file,
 in either source code or executable form, except in compliance with
 the License.  You may obtain a copy of the License from

 http://www.CognitiveWeb.org/legal/license/

 Software distributed under the License is distributed on an AS IS
 basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 the License for the specific language governing rights and limitations
 under the License.

 Copyrights:

 Portions created by or assigned to CognitiveWeb are Copyright
 (c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
 information for CognitiveWeb is available at

 http://www.CognitiveWeb.org

 Portions Copyright (c) 2002-2003 Bryan Thompson.

 Acknowledgements:

 Special thanks to the developers of the Jabber Open Source License 1.0
 (JOSL), from which this License was derived.  This License contains
 terms that differ from JOSL.

 Special thanks to the CognitiveWeb Open Source Contributors for their
 suggestions and support of the Cognitive Web.

 Modifications:

 */
/*
 * Created on Dec 15, 2006
 */

package com.bigdata.objndx;

import java.util.Iterator;

/**
 * Interface for a node or a leaf of a B+-Tree.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IAbstractNode {

    /**
     * Recursive search locates the approprate leaf and inserts the entry under
     * the key. The leaf is split iff necessary. Splitting the leaf can cause
     * splits to cascade up towards the root. If the root is split then the
     * total depth of the tree is inceased by one.
     * 
     * @param key
     *            The external key.
     * @param entry
     *            The value.
     * 
     * @return The previous value or <code>null</code> if the key was not
     *         found.
     */
    public Object insert(Object key, Object entry);

    /**
     * Recursive search locates the entry for the probe key.
     * 
     * @param key
     *            The external key.
     * 
     * @return The entry or <code>null</code> iff there is no entry for
     *         that key.
     */
    public Object lookup(Object key);

    /**
     * Recursive search locates the appropriate leaf and removes and returns
     * the pre-existing value stored under the key (if any).
     * 
     * @param key
     *            The external key.
     *            
     * @return The value or null if there was no entry for that key.
     */
    public Object remove(Object key);

    /**
     * Traversal of index values in key order.
     */
    public KeyValueIterator entryIterator();

    /**
     * Post-order traveral of nodes and leaves in the tree. For any given
     * node, its children are always visited before the node itself (hence
     * the node occurs in the post-order position in the traveral). The
     * iterator is NOT safe for concurrent modification.
     * 
     * @return Iterator visiting {@link IAbstractNode}s.
     */
    public Iterator postOrderIterator();

}
