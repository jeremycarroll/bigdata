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

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;

/**
 * Class that provides the basic transport of messages between the boot
 * launcher and boot agents.
 */
public class MessageTransport {
    public static final int DEFAULT_PORT = 10302;

    private final Socket sock;
    private final InputStream in;
    private final OutputStream out;

    // Socket must be connected already
    public MessageTransport(Socket sock) throws IOException {
        this.sock = sock;
        this.in = new BufferedInputStream(sock.getInputStream());
        this.out = sock.getOutputStream();
    }

    /**
     * Sends a message on the connection. A message consists of a list of
     * strings.
     *
     * This method is synchronized internally and is thread-safe.
     */
    public void send(List<String> message) throws IOException {
        if(message == null) {
            throw new NullPointerException("null message parameter");
        }

        // Build the final message, escaping embedded spaces
        StringBuilder buf = new StringBuilder(100);
        boolean first = true;
        for(String mi : message) {
            if( !first ) {
                buf.append(' ');
            } else {
                first = false;
            }
            buf.append(mi.replaceAll(" ", "##"));
        }
        buf.append('\n');

        synchronized(out) {
            out.write(buf.toString().getBytes());
        }
    }

    /**
     * Receives a message from the connection, blocking until a whole message
     * is received. A list of tokens comprising the message is returned.
     *
     * This method is thread-safe.
     *
     * @throws java.io.EOFException
     * @throws java.io.IOException
     */
    public List<String> recv() throws java.io.IOException {
        final int byteBufSize = 128;
        byte[] byteBuf = new byte[byteBufSize];
        StringBuilder stringBuf = new StringBuilder(200);
        boolean done = false;

        synchronized(in) {
            while(! done) {
                // save the current position in the stream
                in.mark(byteBufSize);

                // read data from the stream
                int len = in.read(byteBuf, 0, byteBufSize);
                if(len <= 0) throw new EOFException();

                // scan the new data for a LF
                for(int i=0; i<len; i++) {
                    if(byteBuf[i] == '\n') {
                        // Found the LF.
                        // Fix the position of the InputStream by rewinding
                        // back to the last mark and skipping only the data
                        // and LF for this line
                        in.reset();
                        in.skip(i + 1);

                        // Adjust the length of the data to append to the
                        // result. It will not include the LF character.
                        len = i;

                        done = true;
                        break;
                    }
                }

                // Append the new data to the result
                stringBuf.append(new String(byteBuf, 0, len));
            }
        }
        return Arrays.asList(stringBuf.toString().split(" "));
    }

    /**
     * Closes this message transport object and the underlying socket.
     */
    public void close() {
        try {
            sock.close();
        } catch(IOException e) { }
    }
}
