package com.kvdb;

public class KVDBException extends Exception {
    private static final long serialVersionUID = 1L;
    public KVDBException() {
        super();
    }
    public KVDBException(String msg) {
        super(msg);
    }
}
