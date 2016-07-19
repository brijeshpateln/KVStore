package com.kvdb.connection;

import com.kvdb.DB;
import com.kvdb.DBQuery;
import com.kvdb.KVDBException;
import com.kvdb.internal.DBConfig;
import com.kvdb.internal.Log;


public class DBConnection extends DBConnectionBase{
    private static final String TAG = "DBConnection";
    /*Reference to DB instance so it wont go away if connection is active */
    DB db;

    /*DB connection config*/
    DBConfig config;

    /*State of DB connection to keep track of read/write transactions*/
    DBConnectionState state;

    /* Connection pool this connection is assiciated with*/
    DBConnectionPool pool;

    /*flag indicating that user requested to close the connection*/
    boolean needClose;

    boolean finalized = false;

    /* static method to create DBConnection instance */
    protected static DBConnection create(DBConfig c, DBConnectionPool _p) {
        return new DBConnection(c, _p);
    }

    private DBConnection(DBConfig c, DBConnectionPool _p) {
        config = c;
        state = new DBConnectionState(this);
        pool = _p;
        db = _p.getDB();
    }

    /* Open database */
    public boolean open() {
        try {
            cPtr = nativeOpen(config.path,config.openFlags);
            Log.i(TAG,"Opened connection to database : " + Long.toHexString(cPtr));
            state.isOpen = true;
            return true;
        } catch (KVDBException e) {
            e.printStackTrace();
        }
        return false;
    }
    public boolean isOpen(){
        return state.isOpen;
    }

    /* Try close connection. It may not close is any transaction is active */
    public void close() {
        //Not good if finalized is true and read write transaction did not complete
        if(!finalized && state.isReadWriteActive()){
            needClose = true;
        } else {
            try {
                Log.i(TAG,"Closing connection to database : " + cPtr);
                nativeClose(cPtr);
                cPtr = 0;
                state.isOpen = false;
            } catch (KVDBException e) {
                e.printStackTrace();
            }
        }
    }
    /*Connection will be removed from pool*/
    public void release(){
        pool.releaseConnection(this);
    }

    /* Begin read only transaction */
    public void beginReadTransaction() throws KVDBException{
        synchronized (this) {
            if(needClose) throw new KVDBException("Connection close already called");
            if(state.isReadWriteActive()) throw new KVDBException("Nested transaction not allowed");
            if(!pool.canProceedWithRead()) throw new KVDBException("Write active and not in WAL");
            execute(DBQuery.BEGIN_TRANSACTION);
            state.setReadTransactionActive(true);
        }
    }

    /* Begin read/write transaction. We need to acquire write lock before proceeding */
    public void beginReadWriteTransaction() throws KVDBException {
        synchronized (this) {
            if(needClose) throw new KVDBException("Connection close already called");
            if(state.isReadWriteActive()) throw new KVDBException("Nested transaction not allowed");
            if(pool.acquireWriteLockWait(3000)){
                /*It might happen that during acquire lock connection was closed*/
                if(needClose){
                    pool.releaseWriteLock();
                    throw new KVDBException("Connection was closed");
                }
                execute(DBQuery.BEGIN_TRANSACTION);
                state.setWriteTransactionActive(true);
            } else {
                throw new KVDBException("Cannot acquire write lock");
            }
        }
    }

    /* End read transaction */
    public void endReadTransaction() throws KVDBException {
        synchronized(this) {
            execute(DBQuery.COMMIT);
            state.setReadTransactionActive(false);
            endTransactionCheck();
        }
    }

    /* End read/write transaction */
    public void endReadWriteTransaction() throws KVDBException {
        synchronized(this) {
            execute(DBQuery.COMMIT);
            state.setWriteTransactionActive(false);
            pool.releaseWriteLock();
            endTransactionCheck();
        }
    }
    /*rollback transaction*/
    public void rollbackTransaction() throws KVDBException {
        synchronized(this) {
            execute(DBQuery.ROLLBACK);
            if (state.isWriteTransActive) {
                state.setWriteTransactionActive(false);
                pool.releaseWriteLock();
            }
            if (state.isReadTransActive) {
                state.setReadTransactionActive(false);
            }
            endTransactionCheck();
        }
    }
    private void endTransactionCheck(){
        if(needClose) {
            close();
        }
    }
    @Override
    protected void finalize() throws Throwable {
        finalized = true;
        if(state.isOpen)
            pool.releaseConnection(this);
        super.finalize();
    }
}
