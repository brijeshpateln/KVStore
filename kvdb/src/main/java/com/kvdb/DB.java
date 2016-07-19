package com.kvdb;

import java.io.File;

import com.kvdb.connection.DBConnection;
import com.kvdb.connection.DBConnectionPool;
import com.kvdb.internal.DBConfig;
import com.kvdb.internal.DBRegister;
import com.kvdb.internal.Log;

public class DB {
    private static final String TAG = "KVDB";
    /*default file name if not specified by user*/
    private static final String DEFAULT_NAME = "kvstorehsa.db";
    /*Load native library*/
    static {
        //risky but not able search .dll in libs folder, need to find way around or add path to java.library.path
        //System.load("E:\\BitBucketRepo\\HelpShift\\KVStore\\kvdb\\src\\main\\jni\\libkvdb-native.dll"); //for debug
        System.loadLibrary("kvdb-native");
    }
    /*Database open flags*/
    public static final int OPEN_READONLY = 0x001;
    public static final int OPEN_READWRITE = 0x010;
    public static final int OPEN_CREATE = 0x100;
    public static final int DEFAULT_FLAGS = OPEN_READWRITE|OPEN_CREATE;

    /*Pool of DBConnections used to interact database*/
    DBConnectionPool pool;

    /*Database configuration consisting of path and flags */
    DBConfig config;

    public static DB open(String folder, String dbName) throws KVDBException {
        return open(folder,dbName,DEFAULT_FLAGS);
    }

    public static DB open(String folder, int flags) throws KVDBException {
        return open(folder,DEFAULT_NAME,flags);
    }

    public static DB open(String folder) throws KVDBException {
        return open(folder,DEFAULT_NAME);
    }

    public static DB open(String folder, String dbName,int flags) throws KVDBException {
        String dbFilePath = folder + File.separator + dbName;
        return openInternal(dbFilePath,flags);
    }

    private static DB openInternal(String path, int flags) throws KVDBException{
        //check if db instance for this path is already registered
        DB db = DBRegister.get(path);
        if(db != null) {
            return db;
        }

        //create DB instance and try open database with provided flags
        db = new DB(path, flags);
        if(db.open()){
            DBRegister.register(path,db);
        } else {
            db = null;
            throw new KVDBException("Unable to open database");
        }
        return db;
    }

    private DB(String path,int flags) {
        config = new DBConfig(path,flags);
        pool = DBConnectionPool.create(path, flags, this);
    }

    /*Used to open or create database */
    private boolean open() throws KVDBException{
        boolean isNewDB = !(new File(config.path).exists());
        boolean isOpenSuccess = false;

        //open a connection to database ( it will be created if not present )
        Log.i(TAG, "Opening database path : " + config.path);
        DBConnection c = pool.getConnection();
        isOpenSuccess = c.isOpen();

        //prepare if it is a new database with table, index, write ahead logging pragma
        if(isOpenSuccess && isNewDB) {
            Log.i(TAG, "Preparing new database..");
            if(config.wal)
                c.execute(DBQuery.PRAGMA_WAL);
            c.execute(DBQuery.CREATE_TABLE);
            c.execute(DBQuery.CREATE_INDEX);
        }
        return isOpenSuccess;
    }

    /*Used to obtain new connection from pool*/
    public DBConnection getConnection() throws KVDBException {
        return pool.getConnection();
    }

    public void close() throws KVDBException {
        //we only close current thread connection right now
        pool.getConnection().release();
    }
}
