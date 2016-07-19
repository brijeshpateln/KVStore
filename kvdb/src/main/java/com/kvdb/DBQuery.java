package com.kvdb;

public class DBQuery {
    public static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS kvstore" +
            " (rowid INTEGER PRIMARY KEY AUTOINCREMENT," +
            "  _key TEXT NOT NULL," +
            "  _value BLOB" +
            " );";
    public static final String PRAGMA_WAL = "PRAGMA journal_mode=WAL;";
    public static final String CREATE_INDEX = "CREATE UNIQUE INDEX IF NOT EXISTS " +
            "keyindex ON kvstore (_key)";
    public static final String BEGIN_TRANSACTION = "BEGIN TRANSACTION;";
    public static final String BEIGIN_IMMEDIATE = "BEGIN IMMEDIATE TRANSACTION;";
    public static final String COMMIT = "COMMIT TRANSACTION;";
    public static final String ROLLBACK = "ROLLBACK TRANSACTION;";
}
