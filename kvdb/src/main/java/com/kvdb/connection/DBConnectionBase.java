package com.kvdb.connection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.kvdb.KVDBException;

/*Connection base class containing all the API's supported*/
public class DBConnectionBase {
    /* SQLite database connection pointer referring to native sql context*/
    long cPtr;

    /* lock for tread safe connection calls as it will not be safe to use
     * same sqlite connection in multiple threads
     */
    private final Object lock = new Object();

    /* Execute raw SQL query */
    public void execute(String sql) throws KVDBException {
        synchronized (lock) {
            nativeExecute(cPtr, sql);
        }
    }
    /* insert (key,value) where value is boolean */
    public void putBoolean(String key, boolean value) throws KVDBException {
        synchronized (lock) {
            isValid(key);
            nativePutBoolean(cPtr, key, value);
        }
    }
    /* insert (key,value) where value is short */
    public void putShort(String key, short value) throws KVDBException {
        synchronized (lock) {
            isValid(key);
            nativePutShort(cPtr, key, value);
        }
    }
    /* insert (key,value) where value is an integer */
    public void putInt(String key, int value) throws KVDBException {
        synchronized (lock) {
            isValid(key);
            nativePutInt(cPtr, key, value);
        }
    }
    /* insert (key,value) where value is long */
    public void putLong(String key, long value) throws KVDBException {
        synchronized (lock) {
            isValid(key);
            nativePutLong(cPtr, key, value);
        }
    }
    /* insert (key,value) where value is float */
    public void putFloat(String key, float value) throws KVDBException {
        synchronized (lock) {
            isValid(key);
            nativePutFloat(cPtr, key, value);
        }
    }
    /* insert (key,value) where value is double */
    public void putDouble(String key, double value) throws KVDBException {
        synchronized (lock) {
            isValid(key);
            nativePutDouble(cPtr, key, value);
        }
    }
    /* insert (key,value) where value byte array */
    public void put(String key, byte[] data) throws KVDBException {
        synchronized (lock) {
            isValid(key,data);
            nativePut(cPtr, key, data);
        }
    }
    /* insert (key,value) where value is string */
    public void put(String key, String value) throws KVDBException {
        synchronized (lock) {
            isValid(key,value);
            nativePutString(cPtr, key, value);
        }
    }
    /* insert (key,value) where value is object array */
    public void put(String key, Object[] value) throws KVDBException {
        synchronized (lock) {
            isValid(key,value);
            byte[] blob = objectToByteArray(value);
            nativePut(cPtr, key, blob);
        }
    }
    /* insert (key,value) where value is an object */
    public void put(String key, Object value) throws KVDBException {
        synchronized (lock) {
            isValid(key,value);
            byte[] blob = objectToByteArray(value);
            nativePut(cPtr, key, blob);
        }
    }
    /* insert (key,value) where value is Serializable array */
    public void put(String key, Serializable[] value) throws KVDBException {
        synchronized (lock) {
            isValid(key,value);
            byte[] blob = objectToByteArray(value);
            nativePut(cPtr, key, blob);
        }
    }
    /* insert (key,value) where value is Serializable object */
    public void put(String key, Serializable value) throws KVDBException {
        synchronized (lock) {
            isValid(key,value);
            byte[] blob = objectToByteArray(value);
            nativePut(cPtr, key, blob);
        }
    }
    /*get api's*/
    public String get(String key) throws KVDBException {
        synchronized (lock) {
            isValid(key);
            return nativeGet(cPtr, key);
        }
    }
    public boolean getBoolean(String key) throws KVDBException {
        synchronized (lock) {
            isValid(key);
            return nativeGetBoolean(cPtr, key);
        }
    }
    public short getShort(String key) throws KVDBException {
        synchronized (lock) {
            isValid(key);
            return nativeGetShort(cPtr, key);
        }
    }
    public int getInt(String key) throws KVDBException {
        synchronized (lock) {
            isValid(key);
            return nativeGetInt(cPtr, key);
        }
    }
    public long getLong(String key) throws KVDBException {
        synchronized (lock) {
            isValid(key);
            return nativeGetLong(cPtr, key);
        }
    }
    public float getFloat(String key) throws KVDBException {
        synchronized (lock) {
            isValid(key);
            return nativeGetFloat(cPtr, key);
        }
    }
    public double getDouble(String key) throws KVDBException {
        synchronized (lock) {
            isValid(key);
            return nativeGetDouble(cPtr, key);
        }
    }
    public byte[] getBytes(String key) throws KVDBException{
        synchronized (lock) {
            isValid(key);
            return nativeGetBytes(cPtr, key);
        }
    }
    public <T> T getObject(String key, Class<T> className) throws KVDBException{
        isValid(key, className);
        byte[] data = getBytes(key);
        return byteArrayToObject(data, className);
    }
    public <T extends Serializable> T get(String key, Class<T> className) throws KVDBException{
        isValid(key, className);
        byte[] data = getBytes(key);
        return byteArrayToObject(data, className);
    }

    public <T extends Serializable> T[] getArray(String key, Class<T> className) throws KVDBException {
        isValid(key, className);
        byte[] data = getBytes(key);
        return byteArrayToObjectArray(data, className);
    }
    public <T> T[] getObjectArray(String key, Class<T> className) throws KVDBException{
        isValid(key, className);
        byte[] data = getBytes(key);
        return byteArrayToObjectArray(data, className);
    }
    /*delete value corresponding to key*/
    public void delete(String key) throws KVDBException {
        synchronized (lock) {
            isValid(key);
            nativeDelete(cPtr, key);
        }
    }
    /* check validity of key,value */
    private void isValid(String key, Object value) throws KVDBException{
        if(key == null) throw new KVDBException("Key cannot be null");
        if(value == null) throw new KVDBException("Value cannot be null");
    }
    /* check validity of key */
    private void isValid(String key) throws KVDBException{
        if(key == null) throw new KVDBException("Key cannot be null");
    }
    /*serialise object to byte array*/
    private byte[] objectToByteArray(Object o) throws KVDBException{
        byte[] result = null;
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            ObjectOutputStream output = new ObjectOutputStream(stream);
            output.writeObject(o);
            output.close();
            result = stream.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            throw new KVDBException(e.getMessage());
        }
        return result;
    }
    /*deserialise byte array to object*/
    @SuppressWarnings("unchecked")
    private <T> T byteArrayToObject(byte[] data, Class<T> className) throws KVDBException{
        T result = null;
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream input = null;
        try {
            input = new ObjectInputStream(in);
            result = (T)input.readObject();
            input.close();
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new KVDBException("Error deserializing : " + e.getMessage());
        }
        return result;
    }
    /*deserialise byte array to object array*/
    @SuppressWarnings("unchecked")
    private <T> T[] byteArrayToObjectArray(byte[] data, Class<T> className) throws KVDBException{
        T[] result = null;
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream input = null;
        try {
            input = new ObjectInputStream(in);
            result = (T[])input.readObject();
            input.close();
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new KVDBException("Error deserialising" + e.getMessage());
        }
        return result;
    }
    public long getCount() throws KVDBException {
        synchronized (lock) {
            return nativeCountKeys(cPtr,null);
        }
    }
    public String[][] executeQueryForResult(String query, Object[] bindArgs) throws KVDBException {
        synchronized (lock) {
            return nativeExecuteForResult(cPtr,query);
        }
    }
    public void executeQuery(String query, Object[] bindArgs){
        //TODO: use native api to prepare, bind and execute
    }
    protected static native long nativeOpen(String path, int openFlags) throws KVDBException;
    protected static native void nativeClose(long cptr) throws KVDBException;
    protected static native void nativeExecute(long cptr, String statement) throws KVDBException;
    protected native void nativeDestroy(String dbName) throws KVDBException;
    private native long nativePrepare(long cptr, String sql) throws KVDBException;
    private native void nativeReset(long connectionPtr, long statementPtr);
    private native int nativeHolderCount(long statementPtr);
    private native boolean nativeIsReadOnly(long statementPtr);
    private native int nativeGetColumns(long connectionPtr, long statementPtr);
    private static native void nativePut(long cptr, String key, byte[] value) throws KVDBException;
    private native void nativePutString(long cptr, String key, String value) throws KVDBException;
    private native void nativePutShort(long cptr, String key, short val) throws KVDBException;
    private native void nativePutInt(long cptr, String key, int val) throws KVDBException;
    private native void nativePutBoolean(long cptr, String key, boolean val) throws KVDBException;
    private native void nativePutDouble(long cptr, String key, double val) throws KVDBException;
    private native void nativePutFloat(long cptr, String key, float val) throws KVDBException;
    private native void nativePutLong(long cptr, String key, long val) throws KVDBException;
    private native void nativeDelete(long cptr, String key) throws KVDBException;
    private native byte[] nativeGetBytes(long cptr, String key) throws KVDBException;
    private native String nativeGet(long cptr, String key) throws KVDBException;
    private native short nativeGetShort(long cptr, String key) throws KVDBException;
    private native int nativeGetInt(long cptr, String key) throws KVDBException;
    private native boolean nativeGetBoolean(long cptr, String key) throws KVDBException;
    private native double nativeGetDouble(long cptr, String key) throws KVDBException;
    private native long nativeGetLong(long cptr, String key) throws KVDBException;
    private native float nativeGetFloat(long cptr, String key) throws KVDBException;
    private native boolean nativeExists(long cptr, String key) throws KVDBException;
    private native long nativeCountKeys (long cptr, String prefix) throws KVDBException;
    private native String[][] nativeExecuteForResult(long cptr, String sql) throws KVDBException;
}
