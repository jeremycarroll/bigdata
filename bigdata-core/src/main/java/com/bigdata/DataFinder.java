/*
 * Created by IntelliJ IDEA.
 * User: gossard
 * Date: Aug 18, 2010
 * Time: 10:55:06 AM
 */
package com.bigdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public abstract class DataFinder {
    static final Logger log = LoggerFactory.getLogger(DataFinder.class);
    static final DataFinder DEFAULT_SEARCH;
    static {
        log.debug("Building default path list for DataFinder access");


        List<DataFinder> defaultPath = new ArrayList<DataFinder>();

        try{
            File workingDirectory = new File("").getAbsoluteFile();
            log.debug("Adding working directory to default search path : {}", workingDirectory.getPath() );
            defaultPath.add( new FileResourceFinder( workingDirectory )); //This is the working directory the JVM was launched from.
        } catch (SecurityException se){
            log.warn("Could not add working directory to default search path, security manager didn't allow us to determine it's absolute path.");
        }

        try{
            String propVal = System.getProperty("app.home");//Set by ant and maven, this is typically the directory the build script is in.
            if ( propVal != null ) {
                File dir = new File(propVal);
                log.debug("Adding build directory to default search path : {}",dir.getAbsolutePath());
                defaultPath.add( new FileResourceFinder( dir ) );
            } else {
                log.warn("Could not add build directory to default search path, system property 'app.home' not set.");
            }
        } catch (SecurityException se){
            log.warn("Could not add build directory to default search path, security manager didn't allow access to the system property 'app.home'.");
        }
        
        DEFAULT_SEARCH = new SearchDataFinder( defaultPath );
    }

    public abstract boolean exists(String path);
    public abstract String getBestPath(String path);
    public abstract URI getBestURI(String path);
    public abstract InputStream getInputStream(String path);

    public static String bestPath(String path){
        return DEFAULT_SEARCH.getBestPath(path);
    }

    public static URI bestURI(String path){
        return DEFAULT_SEARCH.getBestURI(path);
    }

    public static DataFinder defaultFinder(){
        return DEFAULT_SEARCH;
    }



    public static class SearchDataFinder extends DataFinder {
        List<DataFinder> search;

        public SearchDataFinder(List<? extends DataFinder> finders){
            search = new ArrayList<DataFinder>(finders);
        }        

        public boolean exists(String path){
            for (DataFinder next : search){
                if (next.exists(path))
                    return true;
            }
            return false;
        }

        public String getBestPath(String path){
            for (DataFinder next : search){
                if (next.exists(path))
                    return next.getBestPath(path);
            }
            return path;
        }

        public URI getBestURI(String path) {
            for (DataFinder next : search){
                if (next.exists(path))
                    return next.getBestURI(path);
            }
            return new File(path).toURI();            
        }

        public InputStream getInputStream(String path){
             for (DataFinder next : search){
                if (next.exists(path))
                    return next.getInputStream(path);
            }
            return null;
        }

    }

    public static class FileResourceFinder extends DataFinder {
        File baseDir;

        public FileResourceFinder(File baseDir){
            this.baseDir = baseDir.getAbsoluteFile();
        }

        File fetch(String path){
            File fetch = new File(baseDir,path);
            return fetch;
        }

        public boolean exists(String path){
            return fetch( path ).exists();
        }

        public String getBestPath(String path){
            File fetch = fetch( path );
            if ( fetch.exists() ) {
                String absolutePath = fetch.getAbsolutePath();
                if (!path.equals(absolutePath))
                    log.debug("Replacing requested path {} with {}",path,absolutePath);
                return absolutePath;
            } else
                return path;
        }

        @Override
        public URI getBestURI(String path) {
            URI uri = new File(getBestPath(path)).toURI();
            log.debug("Converting {} to uri {}",path,uri);
            return uri;
        }

        public InputStream getInputStream(String path){
            try {
                File fetch = fetch( path );
                if ( fetch.exists() )
                    return new FileInputStream(fetch);
            } catch (FileNotFoundException fnfe){
                //drop through, return nothing
            }
            return null;
        }

    }


}