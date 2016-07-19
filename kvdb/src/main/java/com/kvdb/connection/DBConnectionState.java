package com.kvdb.connection;

/* Store connection state which can be shared between
 * different threads reading/writing through same DBConnection
 */
public class DBConnectionState {
    boolean isOpen;
    boolean isReadTransActive;
    boolean isWriteTransActive;
    DBConnection connection;

    public DBConnectionState(DBConnection c) {
        connection = c;
    }
    public boolean isReadWriteActive(){
        return isWriteTransActive || isReadTransActive;
    }
    public void setReadTransactionActive(boolean val){
        isReadTransActive = val;
    }
    public void setWriteTransactionActive(boolean val) {
        isWriteTransActive = val;
    }
}
