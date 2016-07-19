package com.kvdb;

import com.kvdb.DB;
import com.kvdb.KVDBException;
import com.kvdb.connection.DBConnection;

import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.util.ArrayList;

import static org.junit.Assert.*;

/**
 * To work on unit tests, switch the Test Artifact in the Build Variants view.
 */
public class KVDBUnitTest {
    @Test
    public void testOpenWithFolder() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"));
        db.close();
    }
    @Test
    public void testOpenWithName() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),"test1.db");
        db.close();
    }
    @Test
    public void testOpenWithFlags() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),"test2.db",DB.OPEN_CREATE | DB.OPEN_READWRITE);
        db.close();
    }
    @Test
    public void testOpenWithReadFlag() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),"test2.db",DB.OPEN_READONLY);
        db.close();
    }
    @Test
    public void testConnectionCreate() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),DB.OPEN_CREATE | DB.OPEN_READWRITE);
        DBConnection c = db.getConnection();
        assertFalse(!c.isOpen());
        c.release();
    }
    @Test
    public void testConnectionOpenClose() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),DB.OPEN_CREATE | DB.OPEN_READWRITE);
        DBConnection c = db.getConnection();
        c.close();
        c.open();
        assertFalse(!c.isOpen());
        c.release();
    }
    @Test
    public void testConnectionPutIntCheck() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),DB.OPEN_CREATE | DB.OPEN_READWRITE);
        DBConnection c = db.getConnection();
        assertFalse(!c.isOpen());
        c.putInt("int", 1234);
        assertEquals(1234,c.getInt("int"));
        c.release();
    }
    @Test
    public void testConnectionPutShortCheck() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),DB.OPEN_CREATE | DB.OPEN_READWRITE);
        DBConnection c = db.getConnection();
        assertFalse(!c.isOpen());
        c.putShort("short", (short) 123);
        assertEquals(123,c.getShort("short"));
        c.release();
    }
    @Test
    public void testConnectionPutLongCheck() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),DB.OPEN_CREATE | DB.OPEN_READWRITE);
        DBConnection c = db.getConnection();
        assertFalse(!c.isOpen());
        c.putLong("long", 123456789012345L);
        assertEquals(123456789012345L,c.getLong("long"));
        c.release();
    }
    @Test
    public void testConnectionPutFloatCheck() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),DB.OPEN_CREATE | DB.OPEN_READWRITE);
        DBConnection c = db.getConnection();
        assertFalse(!c.isOpen());
        c.putFloat("float", 5.4f);
        assertEquals(5.4f,c.getFloat("float"),0.01);
        c.release();
    }
    @Test
    public void testConnectionPutDoubleCheck() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),DB.OPEN_CREATE | DB.OPEN_READWRITE);
        DBConnection c = db.getConnection();
        assertFalse(!c.isOpen());
        c.putDouble("double", 123.4);
        assertEquals(123.4,c.getFloat("double"),0.01);
        c.release();
    }
    @Test
    public void testConnectionPutBooleanCheck() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),DB.OPEN_CREATE | DB.OPEN_READWRITE);
        DBConnection c = db.getConnection();
        assertFalse(!c.isOpen());
        c.putBoolean("boolean", true);
        assertEquals(true,c.getBoolean("boolean"));
        c.release();
    }
    @Test
    public void testConnectionPutStringCheck() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),DB.OPEN_CREATE | DB.OPEN_READWRITE);
        DBConnection c = db.getConnection();
        assertFalse(!c.isOpen());
        c.put("mystring","Winter is coming!! Really!!");
        String ret = c.get("mystring");
        assertFalse(!ret.equals("Winter is coming!! Really!!"));
        c.release();
    }
    @Test
    public void testConnectionPutObjectCheck() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),DB.OPEN_CREATE | DB.OPEN_READWRITE);
        DBConnection c = db.getConnection();
        assertFalse(!c.isOpen());
        ArrayList<String> data = new ArrayList<>();
        data.add("KVDB");
        data.add("Store");
        c.put("object", data);
        assertFalse(!c.get("object",data.getClass()).get(0).equals("KVDB"));
        assertFalse(!c.get("object",data.getClass()).get(1).equals("Store"));
        c.release();
    }
    @Test
    public void testConnectionPutBytesCheck() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),DB.OPEN_CREATE | DB.OPEN_READWRITE);
        DBConnection c = db.getConnection();
        assertFalse(!c.isOpen());
        byte[] data = "This is byte data".getBytes();
        c.put("bytes",data);
        byte[] ret = c.getBytes("bytes");
        for(int i = 0; i < ret.length; i++){
            assertFalse(ret[i]!=data[i]);
        }
        c.release();
    }
    @Test
    public void testConnectionPutArrayOfObjectsCheck() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),DB.OPEN_CREATE | DB.OPEN_READWRITE);
        DBConnection c = db.getConnection();
        assertFalse(!c.isOpen());
        String[] data = {"Game", "Of", "Thrones"};
        c.put("array",data);
        String[] ret = c.getArray("array",String.class);
        for(int i = 0; i < ret.length; i++){
            assertFalse(!data[i].equals(data[i]));
        }
        c.release();
    }
    @Test
    public void testConnectionReadWriteTransactionCheck() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),DB.OPEN_CREATE | DB.OPEN_READWRITE);
        DBConnection c = db.getConnection();
        assertFalse(!c.isOpen());
        c.beginReadWriteTransaction();
        for(int i = 0; i < 1000; i++){
            c.put("String " + i, "default");
        }
        c.endReadWriteTransaction();
        c.release();
    }
    @Test
    public void testConnectionReadZTransactionCheck() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),DB.OPEN_CREATE | DB.OPEN_READWRITE);
        DBConnection c = db.getConnection();
        assertFalse(!c.isOpen());
        c.beginReadTransaction();
        for(int i = 0; i < 1000; i++){
            assertFalse(!"default".equals(c.get("String " + i)));
        }
        c.endReadTransaction();
        c.release();
    }
    @Test
    public void testConnectionRollbackCheck() throws KVDBException {
        DB db = DB.open(System.getProperty("user.home"),DB.OPEN_CREATE | DB.OPEN_READWRITE);
        DBConnection c = db.getConnection();
        assertFalse(!c.isOpen());
        c.beginReadWriteTransaction();
        for(int i = 0; i < 1000; i++){
            c.put("Coco " + i, "default");
        }
        c.rollbackTransaction();
        c.release();
    }
    @Test
    public void testConnectionZZParallelCheck() throws KVDBException, InterruptedException {
        DB db = DB.open(System.getProperty("user.home"),DB.OPEN_CREATE | DB.OPEN_READWRITE);
        DBConnection c = db.getConnection();
        assertFalse(!c.isOpen());
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    DB _db = DB.open(System.getProperty("user.home"),DB.OPEN_CREATE | DB.OPEN_READWRITE);
                    DBConnection _c = _db.getConnection();
                    _c.beginReadTransaction();
                    for(int j = 0; j < 1000; j++){
                        assertFalse(!"default".equals(_c.get("String " + j)));
                    }
                    _c.endReadTransaction();
                    _c.release();
                } catch (KVDBException e) {
                        e.printStackTrace();
                }
            }
        });
        t.start();
        c.beginReadWriteTransaction();
        for(int i = 0; i < 1000; i++){
            c.put("Coco " + i, "default");
        }
        c.endReadWriteTransaction();
        t.join();
        c = db.getConnection();
        c.beginReadTransaction();
        for(int i = 0; i < 1000; i++){
            assertFalse(!"default".equals(c.get("Coco " + i)));
        }
        c.endReadTransaction();
        c.release();
        c.release();
    }
    public static void main(String[] args){
        Result result = JUnitCore.runClasses(KVDBUnitTest.class);
        for(Failure f : result.getFailures()){
            System.out.println(f.getDescription());
        }
        System.out.println(result.wasSuccessful());
    }
}