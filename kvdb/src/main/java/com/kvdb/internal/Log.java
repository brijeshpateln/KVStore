package com.kvdb.internal;

/* TODO: Add options to enable disable certain logs */
public final class Log {
    public static final int VERBOSE = 0;
    public static final int DEBUG = 1;
    public static final int INFO = 2;
    public static final int WARN = 3;
    public static final int ERROR = 4;
    public static final int ASSERT = 5;
   
    private Log() {
    }
    public static void v(String tag, String msg) {
        System.out.println("[V]" + tag + " " + msg);
    }
    public static void d(String tag, String msg) {
        System.out.println("[D]" + tag + " " + msg);
    }
    public static void i(String tag, String msg) {
        System.out.println("[I]" + tag + " " + msg);
    }
    public static void w(String tag, String msg) {
        System.out.println("[W]" + tag + " " + msg);
    }
    public static void e(String tag, String msg) {
        System.out.println("[E]" + tag + " " + msg);
    }
}
