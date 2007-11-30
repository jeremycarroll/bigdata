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
/*
 * Created on Feb 12, 2007
 */

package com.bigdata.btree;

/**
 * Error messages for the B+Tree package.
 * 
 * @todo add error codes; localize.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Errors {

    public static final transient String ERR_NTUPLES_NON_POSITIVE = "ntuples is non-positive";

    public static final transient String ERR_KEYS_NULL = "keys is null";

    public static final transient String ERR_VALS_NULL = "values is null";

    public static final transient String ERR_NOT_ENOUGH_KEYS = "not enough keys";

    public static final transient String ERR_NOT_ENOUGH_VALS = "not enough values";

}
