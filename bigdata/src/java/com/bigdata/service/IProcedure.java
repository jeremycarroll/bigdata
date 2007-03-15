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
 * Created on Mar 15, 2007
 */

package com.bigdata.service;

import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITx;

/**
 * A procedure to be executed on an {@link IDataService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IProcedure {

    /**
     * Run the procedure.
     * <p>
     * Unisolated procedures have "auto-commit" ACID properties for the local
     * {@link IDataService} on which they execute, but DO NOT have distributed
     * ACID properties. In order for a distributed procedure to be ACID, the
     * procedure MUST be fully isolated.
     * 
     * @param tx
     *            The transaction identifier (aka start time) -or- zero (0L) IFF
     *            this is an unisolationed operation.
     * @param store
     *            The store against which writes will be made. If the procedure
     *            is running inside of a transaction, then this will be an
     *            {@link ITx}. If the procedure is running unisolated, then
     *            this will be an {@link IJournal}.
     */
    public void apply(long tx,IIndexStore store);
    
}
