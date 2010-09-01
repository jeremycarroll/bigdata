/*

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
 * Created on Jul 11, 2009
 */

package com.bigdata.service.jini.master;

import com.bigdata.util.config.NicUtil;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Creates a web servers for a given directory, making all the files available
 * via http access over a specified port. Only GET requests for existing
 * readable files in the given directory or subdirectories are allowed.
 * This web server is intended to be created by cluster configuration files
 * (where the keywork "static" might be helpful).
 */
public class FileServer {

    private final static List<FileServer> fileServers =
            new LinkedList<FileServer>();

    private HttpServer server;
    private int port;
    private String thisHost;
    private ExecutorService threadPool;

    /**
     * Create a web server for the given directory on the specified port
     * allowing a specified backlog of connections on the socket.
     * The files are made available to all requestors without credentials,
     * so be careful not to expose private data.
     *
     * @param dir the directory to be served.
     * @param port the port on which the server provides its services.
     * @param backlog the socket backlog.
     * @throws IOException if the server cannot be created.
     */
    public FileServer(final File dir, int port, int backlog)
    throws IOException {

        if (dir == null || !dir.isDirectory())
            throw new IllegalArgumentException();
        this.port = port;
        this.thisHost = NicUtil.getDefaultIpv4Address();
        server = HttpServer.create(new InetSocketAddress(port), backlog);
        server.createContext("/", new ScannerHandler(dir));
        threadPool = Executors.newCachedThreadPool();
        server.setExecutor(threadPool);
        server.start();
        synchronized (fileServers) {
            fileServers.add(this);
        }
    }

    /**
     * Create and return a URL for the given URL path. The path will be
     * treated as relative to the root URL for the web server (i.e.
     * included in the URL as is.)
     * @param path path to a file relative to the root of the server.
     * @return URL through which the file can be accessed.
     */
    public URL getURL(String path) {
        try {
            return new URL("http", thisHost, port, path);
        } catch (MalformedURLException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Stop the web server immediately, closing all connections.
     */
    public void stop() {
        server.stop(0);
    }

    /**
     * Stop all web servers created. This is needed to stop web servers
     * which have have been started in configuration files and for which
     * no easily accessible reference exists.
     */
    public static void stopAll() {
        synchronized (fileServers) {
            for (FileServer fileServer : fileServers) {
                fileServer.server.stop(0);
            }
        }
    }

    /**
     * Convenience static method for River configuration files.
     * See {@link FileServer#getURL(String)}.
     * @param server a FileServer object for which a URL will be created
     * using the specified path.
     * @param path path to a file relative to the root of the server.
     * @return URL through which the file can be accessed.
     */
    public static URL getURL(FileServer server, String path) {
        return server.getURL(path);
    }

    private static class ScannerHandler implements HttpHandler {
        private File fileOrDir;
        private ScannerHandler(File fileOrDir) {
            this.fileOrDir = fileOrDir;
        }
        public void handle(HttpExchange t) throws IOException {
            if (t.getRequestMethod().equalsIgnoreCase("GET")) {
                URI uri = t.getRequestURI();
                String path = uri.getPath();
                File target = new File(fileOrDir, path);
                if (target.isFile() && target.canRead()) {
                    t.sendResponseHeaders(200, target.length());
                    OutputStream os = t.getResponseBody();
                    FileInputStream fis = new FileInputStream(target);
                    byte[] buff = new byte[64*1024];
                    while (true) {
                        int n = fis.read(buff);
                        if (n < 0) {
                            break;
                        }
                        os.write(buff, 0, n);
                    }
                    os.close();
                    return;
                }
            }
            byte[] response = "File Not Available".getBytes("utf8");
            t.sendResponseHeaders(404, response.length);
            OutputStream os = t.getResponseBody();
            os.write(response);
            os.close();
        }
    }
}
