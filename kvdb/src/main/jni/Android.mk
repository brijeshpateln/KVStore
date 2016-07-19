LOCAL_PATH := $(call my-dir)

include $(CLEAR_VARS)
LOCAL_MODULE := kvdb-native
LOCAL_SRC_FILES := \
    ./sqlite/sqlite3.c \
    ./sqlite/shell.c \
    ./com_kvdb_connection_DBConnectionBase.cpp
LOCAL_LDLIBS := -llog
include $(BUILD_SHARED_LIBRARY)