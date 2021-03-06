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
package com.bigdata.bop.joinGraph;

/**
 * Exception thrown when the join graph does not have any solutions in the data
 * (running the cutoff joins to explore the join graph does not produce any
 * solutions such that we can not complete a path through the join graph without
 * a zero cardinality).
 */
public class NoSolutionsException extends RuntimeException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public NoSolutionsException() {
		super();
	}

	public NoSolutionsException(String message, Throwable cause) {
		super(message, cause);
	}

	public NoSolutionsException(String message) {
		super(message);
	}

	public NoSolutionsException(Throwable cause) {
		super(cause);
	}
	
}