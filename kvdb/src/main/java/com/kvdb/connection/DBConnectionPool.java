package com.kvdb.connection;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.kvdb.DB;
import com.kvdb.KVDBException;
import com.kvdb.internal.DBConfig;
import com.kvdb.internal.Log;

/* DBConnection pool containing active connections */
public class DBConnectionPool {
    private static final String TAG = "DBConnectionPool";
    private final Object lock = new Object();
    private int maxPoolSize;
    private boolean isOpen;
    private int activeConnections;
    private DBConfig config;
    private WeakReference<DB> db;

    /*List of connections*/
    private final ArrayList<DBConnection> connections = new ArrayList<DBConnection>();

    /*Single mutex used to share write lock between connections
     * so that only one connection will be writing to database at a time
     * this lock is not acquired during read only transactions
     */
    Semaphore mutex;
    /* Thread local connection as we do not have asynchronus operations for a connection
    *  there is no use of having multiple connections over same thread. We can consider
    *  multiple connections when we implement asychronus tasks in DB connection class
    */
    ThreadLocal<DBConnection> threadLocal = new ThreadLocal<DBConnection>() {
        /*@Override
        protected DBConnection initialValue(){
            try {
                Log.i(TAG,"Initial connection open on thread " + Thread.currentThread());
                return newConnection();
            } catch (KVDBException e) {
                e.printStackTrace();
            }
            return null;
        }*/
    };

    public static DBConnectionPool create(String path, int flags, DB db) {
        if (path == null) {
            throw new IllegalArgumentException("Path provided is null");
        }
        DBConnectionPool pool = new DBConnectionPool(path,flags,db);
        return pool;
    }

    private DBConnectionPool(String path, int flags, DB _db){
        config = new DBConfig(path,flags);
        db = new WeakReference<DB>(_db);
        setMaxPoolSize();
        mutex = new Semaphore(1);

    }
    /* get or set new connection from thread local element*/
    public DBConnection getConnection() throws KVDBException {
        synchronized (lock) {
            DBConnection c = threadLocal.get();
            if (c == null) {
                c = newConnection();
                threadLocal.set(c);
                Log.i(TAG, "Connection open on thread " + Thread.currentThread().getName() + " > " + c.toString());
            }
            if(!c.isOpen()) c.open();
            return c;
        }
    }
    /* get new connection to database through which we can interact with database*/
    private DBConnection newConnection() throws KVDBException {
        synchronized (lock) {
            if(activeConnections > maxPoolSize){
                throw new KVDBException("Maximum pool size reached");
            }
            DBConnection dbc = DBConnection.create(config,this);
            if(dbc.open()) {
                connections.add(dbc);
                activeConnections++;
                isOpen = true;
            } else {
                throw new KVDBException("Unable to open connection");
            }
            return dbc;
        }
    }
    /*release connection after used. It tries closing connection to database at native
     * but may not if connection is active but we will still remove from pool
     * Connection will close after transaction is complete as we set flag
     */
    public void releaseConnection(DBConnection dbc){
        synchronized (lock) {
            if(connections.contains(dbc)){
                closeConnection(dbc);
                connections.remove(dbc);
                threadLocal.set(null);
                activeConnections--;
            }
            if(activeConnections == 0) isOpen = false;
        }
    }
    void setMaxPoolSize(){
        maxPoolSize = 5; //maximum 5 connection per database
    }
    /*If WAL is enabled we can read in parallel to write
     * but only one connection can write to the database
     */
    boolean canProceedWithRead(){
        if(config.wal) {
            return true;
        } else {
            for(DBConnection con : connections) {
                if(con.state.isWriteTransActive) return false;
            }
            return true;
        }
    }
    /* close connection call to connection*/
    void closeConnection(DBConnection connection) {
        try {
            connection.close();
        } catch (Exception ex) {
            Log.e(TAG, "Unable to close connection(it may still be remove from pool): " + connection);
        }
    }
    /*closes all connections made from this pool*/
    void close(){
        synchronized (lock) {
            for(DBConnection c : connections) {
                closeConnection(c);
            }
            connections.clear();
            isOpen = false;
        }
    }
    boolean isOpen(){
        return isOpen;
    }
    /* write lock apis used by connections to gain write lock*/
    boolean acquireWriteLockWait(int timeout){
        try {
            return mutex.tryAcquire(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }
    boolean acquireWriteLockWaitInf(){
        try {
            mutex.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
    boolean acquireWriteLock(){
        return mutex.tryAcquire();
    }
    void releaseWriteLock(){
        mutex.release();
    }
    ArrayList<DBConnection> getActiveConnections(){
        return connections;
    }
    DB getDB(){
        return db.get();
    }
}
