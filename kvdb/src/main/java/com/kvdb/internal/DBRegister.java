package com.kvdb.internal;

import com.kvdb.DB;

import java.lang.ref.WeakReference;
import java.util.HashMap;

/* Maintain DB register for paths to create only single DB
 * instance for a particular path.
 */
public class DBRegister {

    /* Mapping path > DB instance*/
    static final HashMap<String, WeakReference<DB>> map = new HashMap<>();

    /* Register db instance mapping to a particular path*/
    public static void register(String path, DB db) {
        if(path == null || db == null) return;
        synchronized(map){
            map.put(path,new WeakReference<DB>(db));
        }
    }
    /* deregister DB instance if it exists for a particular path*/
    public static void deregister(String path) {
        synchronized(map){
            map.remove(path);
        }
    }
    /* Returns DB instance if it exists for a particular path*/
    public static DB get(String path){
        synchronized (map) {
            if(map.containsKey(path)){
                DB db = map.get(path).get();
                if(db == null) map.remove(path);
                return db;
            }
            return null;
        }
    }
}
