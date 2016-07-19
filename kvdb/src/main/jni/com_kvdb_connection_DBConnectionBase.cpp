#include "com_kvdb_connection_DBConnectionBase.h"
#include "sqlite/sqlite3.h"
#include <string>
#include <vector>
#include <cstdlib>
#include <sstream>

using namespace std;

// as to_string is not available from std
namespace me
{
    template<typename T> std::string to_string( const T& n )
    {
        std::ostringstream stream ;
        stream << n ;
        return stream.str() ;
    }
}

// throw exception of type KVDBException through jni
void throwException(JNIEnv *env, const char* msg) {
    jclass kvdbexception = env->FindClass("com/kvdb/KVDBException");
    if(NULL == kvdbexception) {
        env->Throw(env->ExceptionOccurred());
        return;
    }
    env->ThrowNew(kvdbexception, msg);
}

//reset sql statemtent
int resetStatement(sqlite3_stmt* statement){
    int err = sqlite3_reset(statement);
    if(err == SQLITE_OK) {
        err = sqlite3_clear_bindings(statement);
    }
    return err;
}

/* make jobjectArray from vector elements */
jobjectArray make_row(JNIEnv *env, jsize count, vector<string> elements, jclass clazz)
{
    jobjectArray row = (env)->NewObjectArray(count, clazz, 0);
    for(int i = 0; i < count; i++) {
        (env)->SetObjectArrayElement(row, i, (env)->NewStringUTF(elements.at(i).c_str()));
    }
    return row;
}
/* function to make array of rows based on query results*/
jobjectArray compute_result(JNIEnv *env,sqlite3_stmt* statement){
    vector<jobjectArray> results;
    jclass stringClass = (env)->FindClass("java/lang/String");
    jclass objectClass;
    int cols = sqlite3_column_count(statement);
    int result = 0;
    jobjectArray jvalues = NULL;
    while(true) {
        result = sqlite3_step(statement);
        if(result == SQLITE_ROW) {
            vector<string> values;
            for(int col = 0; col < cols; col++) {
                values.push_back((char*)sqlite3_column_blob(statement, col));
            }
            jvalues = make_row(env,cols,values,stringClass);
            results.push_back(jvalues);
        } else {
           break;
        }
    }
    if(results.size()!=0) {
        jobjectArray rows = (env)->NewObjectArray(results.size(), env->GetObjectClass(jvalues), 0);
        for(int i = 0; i < results.size(); i++) {
            (env)->SetObjectArrayElement(rows, i, (results.at(i)));
        }
        return rows;
    }
    return NULL;
}
static const char* readQuery = "select _value from kvstore where _key=?";
static const char* writeQuery = "insert or replace into kvstore (_key,_value) values (?,?)";

/* class to for each connections*/
struct DBConnection {
    enum {
        OPEN_READONLY    = 0x001,
        OPEN_READWRITE   = 0x010,
        OPEN_CREATE      = 0x100,
    };
    sqlite3* const db;
    const int openFlags;
    const string path;
    string label;
    DBConnection(sqlite3* db, int openFlags, const string& path) :
        db(db), openFlags(openFlags), path(path) { }
};

// reference from android sqliteconnection.cpp
// Called each time a statement begins execution, when tracing is enabled.
static void sqliteTraceCallback(void *data, const char *sql) {
    DBConnection* connection = static_cast<DBConnection*>(data);
    printf( "statement start %s: \"%s\"\n",
            connection->label.c_str(), sql);
}
// reference from android sqliteconnection.cpp
// Called each time a statement finishes execution, when profiling is enabled.
static void sqliteProfileCallback(void *data, const char *sql, sqlite3_uint64 tm) {
    DBConnection* connection = static_cast<DBConnection*>(data);
    printf( "statement executed %s: \"%s\" took %0.3f ms\n",
            connection->label.c_str(), sql, tm * 0.000001f);
}

// get value in form of byte array
int getValue(JNIEnv *env,DBConnection* connection, jstring jkey, jbyteArray* result){
    sqlite3_stmt* statement;
    const char* key =  env->GetStringUTFChars(jkey, NULL);
    int res = sqlite3_prepare_v2(connection->db,readQuery,-1,&statement,0);
    if(res == SQLITE_OK) {
        res = sqlite3_bind_text(statement,1,key,-1,SQLITE_TRANSIENT);
        if (res == SQLITE_OK) {
            res = sqlite3_step(statement);
            if(res == SQLITE_ROW){
                jbyte* data = (jbyte*)sqlite3_column_blob(statement, 0);
                int size = sqlite3_column_bytes(statement, 0);
                *result = env->NewByteArray(size * sizeof(jbyte));
                env->SetByteArrayRegion(*result, 0, size, data);
            }
        }
    }
    env->ReleaseStringUTFChars(jkey, key);
    sqlite3_finalize(statement);
    return res;
}
int getValueString(JNIEnv *env,DBConnection* connection, jstring jkey, char** result){
    sqlite3_stmt* statement;
    const char * key =  env->GetStringUTFChars(jkey, NULL);
    int res = sqlite3_prepare_v2(connection->db,readQuery,-1,&statement,0);
    if(res == SQLITE_OK) {
        res = sqlite3_bind_text(statement,1,key,-1,SQLITE_TRANSIENT);
        if (res == SQLITE_OK) {
            res = sqlite3_step(statement);
            if(res == SQLITE_ROW){
                const char* str = (const char*)sqlite3_column_blob(statement, 0);
                int size = sqlite3_column_bytes(statement, 0);
                *result = (char*)malloc(size*sizeof(char));
                memcpy(*result,str,size);
            }
        }
    }
    env->ReleaseStringUTFChars(jkey, key);
    sqlite3_finalize(statement);
    return res;
}

// put function used to put (key,value). used by put apis
int putValue(DBConnection* connection, const char* key, const char* value, int length){
    sqlite3_stmt* statement;
    int res = sqlite3_prepare_v2(connection->db,writeQuery,-1,&statement,NULL);
    if(res == SQLITE_OK) {
        res = sqlite3_bind_text(statement,1,key,-1,SQLITE_TRANSIENT);
        if(res == SQLITE_OK) {
            res = sqlite3_bind_blob(statement,2,value,length*sizeof(char),SQLITE_TRANSIENT);
            if(res == SQLITE_OK) {
                res = sqlite3_step(statement);
            }
        }
    }
    sqlite3_finalize(statement);
    return res;
}
/* Create/Open sql connection to database */
JNIEXPORT jlong JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeOpen
(JNIEnv * env , jclass clazz, jstring pathString, jint flags){
    int sqliteFlags;
    if(flags & DBConnection::OPEN_CREATE) {
        sqliteFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
    } else if(flags & DBConnection::OPEN_READONLY) {
        sqliteFlags = SQLITE_OPEN_READONLY;
    } else {
        sqliteFlags = SQLITE_OPEN_READWRITE;
    }
    const char* pathChars = env->GetStringUTFChars(pathString, NULL);
    string path(pathChars);
    env->ReleaseStringUTFChars(pathString, pathChars);

    sqlite3* db;
    int err = sqlite3_open_v2(path.c_str(), &db, sqliteFlags, NULL);
    if(err != SQLITE_OK) {
        throwException(env,"Could not open database");
        return 0;
    }

    if((sqliteFlags & SQLITE_OPEN_READWRITE) && sqlite3_db_readonly(db, NULL)) {
        throwException(env, "Could not open the database in read/write mode.");
        sqlite3_close(db);
        return 0;
    }

    err = sqlite3_busy_timeout(db, 2500);
    if(err != SQLITE_OK) {
        throwException(env, "Could not set busy timeout");
        sqlite3_close(db);
        return 0;
    }

    DBConnection* connection = new DBConnection(db, flags, path);
    connection->label = string(path);
    printf("Open Connection %p\n",connection);
    return reinterpret_cast<jlong>(connection);
}

JNIEXPORT void JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeClose
  (JNIEnv* env, jclass clazz, jlong connectionPtr) {
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    if(connection) {
        printf("Closing connection %p\n", connection->db);
        int err = sqlite3_close(connection->db);
        if (err != SQLITE_OK) {
            printf("sqlite3_close failed: %d\n", err);
            throwException(env, "Unable to close DB");
            return;
        }
        delete connection;
    }
}

JNIEXPORT void JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeExecute
    (JNIEnv* env, jobject clazz, jlong connectionPtr, jstring sql){
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    sqlite3_stmt* statement;
    jsize sqlLength = env->GetStringLength(sql);
    const jchar* sqlstmt = env->GetStringCritical(sql, NULL);
    int err = sqlite3_prepare16_v2(connection->db,sqlstmt, sqlLength * sizeof(jchar), &statement, NULL);
    env->ReleaseStringCritical(sql, sqlstmt);
    err = sqlite3_step(statement);
    sqlite3_finalize(statement);
    if(err != SQLITE_DONE&& err!=SQLITE_ROW) {
        throwException(env, "Execute not done");
    }

}

JNIEXPORT void JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeDestroy
(JNIEnv* env, jobject obzz, jstring sqlString){

}

JNIEXPORT jlong JNICALL Java_com_kvdb_connection_DBConnectionBase_nativePrepare
    (JNIEnv* env, jobject obzz, jlong connectionPtr, jstring sqlString) {
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    jsize sqlLength = env->GetStringLength(sqlString);
    const jchar* sql = env->GetStringCritical(sqlString, NULL);
    sqlite3_stmt* statement;
    int err = sqlite3_prepare16_v2(connection->db, sql, sqlLength * sizeof(jchar), &statement, NULL);
    env->ReleaseStringCritical(sqlString, sql);
    if(err != SQLITE_OK) {
        throwException(env, "problem in preparing statement");
        return 0;
    }
    return reinterpret_cast<jlong>(statement);
}
JNIEXPORT void JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeReset
    (JNIEnv * env, jobject obzz, jlong statementPtr, jlong){
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);
    int err = resetStatement(statement);
    if(err != SQLITE_OK) {
        throwException(env, NULL);
    }
}
JNIEXPORT jint JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeHolderCount
  (JNIEnv* env, jobject obzz, jlong statementPtr) {
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);
    return sqlite3_bind_parameter_count(statement);
}

JNIEXPORT jboolean JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeIsReadOnly
    (JNIEnv* env, jobject obzz, jlong statementPtr){
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);
    return sqlite3_stmt_readonly(statement) != 0;
}
JNIEXPORT jint JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeGetColumns
    (JNIEnv* env, jobject obzz, jlong statementPtr) {
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);
    return sqlite3_column_count(statement);
}

JNIEXPORT void JNICALL Java_com_kvdb_connection_DBConnectionBase_nativePut
    (JNIEnv * env, jobject obzz, jlong connectionPtr, jstring keyString, jbyteArray jvalue){
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    const char * key =  env->GetStringUTFChars(keyString, NULL);
    jsize valueLength = env->GetArrayLength(jvalue);
    char* value = static_cast<char*>(env->GetPrimitiveArrayCritical(jvalue, NULL));
    int result = putValue(connection,key,value,valueLength);
    env->ReleasePrimitiveArrayCritical(jvalue, value, JNI_ABORT);
    if(result != SQLITE_DONE) {
        throwException(env,"Error inserting");
    }
}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativePutString
 * Signature: (JLjava/lang/String;Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_com_kvdb_connection_DBConnectionBase_nativePutString
    (JNIEnv* env, jobject obzz, jlong connectionPtr, jstring keyString, jstring valueString){
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    const char * key =  env->GetStringUTFChars(keyString, NULL);
    jsize valueLength = env->GetStringLength(valueString);
    const char * value = env->GetStringUTFChars(valueString, NULL);
    int result = putValue(connection,key,value,valueLength);
    env->ReleaseStringUTFChars(keyString, key);
    env->ReleaseStringUTFChars(valueString, value);
    if(result != SQLITE_DONE) {
        throwException(env,"Error inserting string");
    }
}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativePutShort
 * Signature: (JLjava/lang/String;S)V
 */
JNIEXPORT void JNICALL Java_com_kvdb_connection_DBConnectionBase_nativePutShort
    (JNIEnv* env, jobject obzz, jlong connectionPtr, jstring jkey, jshort jvalue){
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    const char * key =  env->GetStringUTFChars(jkey, NULL);
    string s = me::to_string(jvalue);
    char const *value = s.c_str();
    int result = putValue(connection,key,value,s.length());
    env->ReleaseStringUTFChars(jkey, key);
    if(result != SQLITE_DONE) {
        throwException(env,"Error inserting short");
    }
}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativePutInt
 * Signature: (JLjava/lang/String;I)V
 */
JNIEXPORT void JNICALL Java_com_kvdb_connection_DBConnectionBase_nativePutInt
    (JNIEnv* env, jobject obzz, jlong connectionPtr, jstring keyString, jint jvalue) {
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    const char * key =  env->GetStringUTFChars(keyString, NULL);
    string s = me::to_string(jvalue);
    char const *value = s.c_str();
    int result = putValue(connection,key,value,s.length());
    env->ReleaseStringUTFChars(keyString, key);
    if(result != SQLITE_DONE) {
        throwException(env,"Error inserting integer");
    }
}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativePutBoolean
 * Signature: (JLjava/lang/String;Z)V
 */
JNIEXPORT void JNICALL Java_com_kvdb_connection_DBConnectionBase_nativePutBoolean
    (JNIEnv* env, jobject obzz, jlong connectionPtr, jstring jkey, jboolean jvalue){
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    const char * key =  env->GetStringUTFChars(jkey, NULL);
    string s = me::to_string(jvalue);
    char const *value = s.c_str();
    int result = putValue(connection,key,value,s.length());
    env->ReleaseStringUTFChars(jkey, key);
    if(result != SQLITE_DONE) {
        throwException(env,"Error inserting boolean");
    }
}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativePutDouble
 * Signature: (JLjava/lang/String;D)V
 */
JNIEXPORT void JNICALL Java_com_kvdb_connection_DBConnectionBase_nativePutDouble
    (JNIEnv* env, jobject obzz, jlong connectionPtr, jstring jkey, jdouble jvalue){
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    const char * key =  env->GetStringUTFChars(jkey, NULL);
    string s = me::to_string(jvalue);
    char const *value = s.c_str();
    int result = putValue(connection,key,value,s.length());
    env->ReleaseStringUTFChars(jkey, key);
    if(result != SQLITE_DONE) {
        throwException(env,"Error inserting double");
    }
}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativePutFloat
 * Signature: (JLjava/lang/String;F)V
 */
JNIEXPORT void JNICALL Java_com_kvdb_connection_DBConnectionBase_nativePutFloat
    (JNIEnv* env, jobject obzz, jlong connectionPtr, jstring jkey, jfloat jvalue){
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    const char * key =  env->GetStringUTFChars(jkey, NULL);
    string s = me::to_string(jvalue);
    char const *value = s.c_str();
    int result = putValue(connection,key,value,s.length());
    env->ReleaseStringUTFChars(jkey, key);
    if(result != SQLITE_DONE) {
        throwException(env,"Error inserting float");
    }
}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativePutLong
 * Signature: (JLjava/lang/String;J)V
 */
JNIEXPORT void JNICALL Java_com_kvdb_connection_DBConnectionBase_nativePutLong
    (JNIEnv* env, jobject obzz, jlong connectionPtr, jstring jkey, jlong jvalue){
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    const char * key =  env->GetStringUTFChars(jkey, NULL);
    string s = me::to_string(jvalue);
    char const *value = s.c_str();
    int result = putValue(connection,key,value,s.length());
    env->ReleaseStringUTFChars(jkey, key);
    if(result != SQLITE_DONE) {
        throwException(env,"Error inserting long");
    }
}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativeDelete
 * Signature: (JLjava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeDelete
    (JNIEnv* env, jobject obzz,jlong connectionPtr,  jstring jkey){
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    sqlite3_stmt* statement;
    int result;
    const char * key =  env->GetStringUTFChars(jkey, NULL);
    const char* query = "delete from kvstore where _key=?";
    int res = sqlite3_prepare_v2(connection->db,query,-1,&statement,0);
    if (res == SQLITE_OK) {
        res = sqlite3_bind_text(statement,1,key,-1,SQLITE_TRANSIENT);
        if (res == SQLITE_OK) {
            res = sqlite3_step(statement);
            if(res != SQLITE_ROW && res != SQLITE_DONE){
                throwException(env,"No such key");
            }
        }
    }
    env->ReleaseStringUTFChars(jkey, key);
    sqlite3_finalize(statement);
}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativeGetBytes
 * Signature: (JLjava/lang/String;)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeGetBytes
    (JNIEnv* env, jobject obzz, jlong connectionPtr, jstring keyString) {
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    jbyteArray array;
    int res = getValue(env,connection,keyString,&array);
    return array;
}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativeGet
 * Signature: (JLjava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeGet
    (JNIEnv* env, jobject obzz, jlong connectionPtr, jstring jkey) {
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    jstring result = NULL;
    sqlite3_stmt* statement;
    const char * key =  env->GetStringUTFChars(jkey, NULL);
    int res = sqlite3_prepare_v2(connection->db,readQuery,-1,&statement,0);
    if(res == SQLITE_OK) {
        res = sqlite3_bind_text(statement,1,key,-1,SQLITE_TRANSIENT);
        if (res == SQLITE_OK) {
            res = sqlite3_step(statement);
            if(res == SQLITE_ROW){
                const char* str = (const char*)sqlite3_column_blob(statement, 0);
                int size = sqlite3_column_bytes(statement, 0);
                result = env->NewStringUTF(str);
            }
        }
    }
    env->ReleaseStringUTFChars(jkey, key);
    sqlite3_finalize(statement);
    return result;
}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativeGetShort
 * Signature: (JLjava/lang/String;)S
 */
JNIEXPORT jshort JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeGetShort
    (JNIEnv* env, jobject obzz, jlong connectionPtr, jstring jkey) {
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    jshort result = 0;
    char* data = NULL;
    int res = getValueString(env,connection,jkey,&data);
    if(res == SQLITE_ROW) {
        result = atoi(data);
    }
    free(data);
    return result;
}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativeGetInt
 * Signature: (JLjava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeGetInt
    (JNIEnv* env, jobject obzz, jlong connectionPtr, jstring jkey) {
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    jint result = 0;
    char* data = NULL;
    int res = getValueString(env,connection,jkey,&data);
    if(res == SQLITE_ROW) {
        result = atoi(data);
    }
    free(data);
    return result;
}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativeGetBoolean
 * Signature: (JLjava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeGetBoolean
    (JNIEnv* env, jobject obzz, jlong connectionPtr, jstring jkey) {
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    jboolean result = 0;
    char* data = NULL;
    int res = getValueString(env,connection,jkey,&data);
    if(res == SQLITE_ROW) {
        result = data[0];
    }
    free(data);
    return result;
}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativeGetDouble
 * Signature: (JLjava/lang/String;)D
 */
JNIEXPORT jdouble JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeGetDouble
    (JNIEnv* env, jobject obzz, jlong connectionPtr, jstring jkey) {
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    jdouble result = 0;
    char* data = NULL;
    int res = getValueString(env,connection,jkey,&data);
    if(res == SQLITE_ROW) {
        result = atof(data);
    }
    free(data);
    return result;
}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativeGetLong
 * Signature: (JLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeGetLong
    (JNIEnv* env, jobject obzz, jlong connectionPtr, jstring jkey) {
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    jlong result = 0;
    char* data = NULL;
    int res = getValueString(env,connection,jkey,&data);
    if(res == SQLITE_ROW) {
        result = atoll(data);
    }
    free(data);
    return result;
}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativeGetFloat
 * Signature: (JLjava/lang/String;)F
 */
JNIEXPORT jfloat JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeGetFloat
    (JNIEnv* env, jobject obzz, jlong connectionPtr, jstring jkey) {
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    jfloat result = 0;
    char* data = NULL;
    int res = getValueString(env,connection,jkey,&data);
    if(res == SQLITE_ROW) {
        result = atof(data);
    }
    free(data);
    return result;
}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativeExists
 * Signature: (JLjava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeExists
(JNIEnv* env, jobject obzz, jlong connectionPtr, jstring jkey){

}

/*
 * Class:     com_kvdb_connection_DBConnectionBase
 * Method:    nativeCountKeys
 * Signature: (JLjava/lang/String;)I
 */
JNIEXPORT jlong JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeCountKeys
(JNIEnv* env, jobject obzz, jlong connectionPtr, jstring jkey){
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    const char* query = "select count(*) from kvstore;";
    sqlite3_stmt* statement;
    jlong data = 0;
    int res = sqlite3_prepare_v2(connection->db,query,-1,&statement,0);
    if(res == SQLITE_OK) {
        res = sqlite3_step(statement);
        if(res == SQLITE_ROW){
            data = sqlite3_column_int(statement, 0);
        }
    }
    sqlite3_reset(statement);
    sqlite3_finalize(statement);
    return data;
}

JNIEXPORT jobjectArray JNICALL Java_com_kvdb_connection_DBConnectionBase_nativeExecuteForResult
(JNIEnv* env, jobject obzz, jlong connectionPtr, jstring sql){
    DBConnection* connection = reinterpret_cast<DBConnection*>(connectionPtr);
    jsize sqlLength = env->GetStringLength(sql);
    const jchar* sqlstmt = env->GetStringCritical(sql, NULL);
    sqlite3_stmt* statement;
    int err = sqlite3_prepare16_v2(connection->db, sqlstmt, sqlLength*sizeof(jchar), &statement, NULL);
    env->ReleaseStringCritical(sql, sqlstmt);
    if(err != SQLITE_OK) {
        return NULL;
    }
    jobjectArray ret = compute_result(env,statement);
    err = sqlite3_reset(statement);
    if(err == SQLITE_OK) {
        err = sqlite3_clear_bindings(statement);
    }
    if(err != SQLITE_OK) {
        throwException(env, NULL);
    }
    sqlite3_finalize(statement);
    return (ret);
}