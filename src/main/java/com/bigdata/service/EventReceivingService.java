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
package com.bigdata.service;

import java.io.IOException;

/**
 * Bigdata services that receive event notifications of type
 * <code>com.bigdata.service.Event</code> should implement this
 * interface.
 */
public interface EventReceivingService {

    /**
     * Accepts the event, either updates the existing event with the same
     * {@link UUID} or adds the event to the set of recent events.
     */
    void notifyEvent(Event e) throws IOException;

}
