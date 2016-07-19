package com.kvdb.internal;

/*Config used by DBConnectionPool to apply across connections */
public final class DBConfig {
    /*Path of the database */
    public String path;
    /*Flags set to open the database*/
    public int openFlags;
    /* Write ahead logging flag */
    public boolean wal = true;
    public DBConfig(String _p, int _f) {
        path = _p;
        openFlags = _f;
    }
}
