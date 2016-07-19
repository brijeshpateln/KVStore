# Define a variable for classpath
CLASS_PATH = ../

# Define a virtual path for .class in the bin directory
vpath %.class $(CLASS_PATH)

all : libkvdb-native.dll

# $@ matches the target, $< matches the first dependancy
libkvdb-native.dll : sqlite.o connection.o
	g++ -shared -Wl,--add-stdcall-alias -o $@ sqlite.o connection.o

kvdb-native : sqlite.o connection.o
	g++ -o kvdb-native sqlite.o connection.o

# $@ matches the target, $< matches the first dependancy
connection.o : com_kvstorehsa_connection_DBConnectionBase.cpp
	g++ -I"C:\Program Files\Java\jdk1.8.0_25\include" -I"C:\Program Files\Java\jdk1.8.0_25\include\win32"  -c $< -o $@

sqlite.o : sqlite/sqlite3.c
	gcc -c ./sqlite/sqlite3.c -o sqlite.o

com_kvstorehsa_connection_DBConnectionBase.h:DBConnectionBase.class
	javah -classpath $(CLASS_PATH) $*

clean :
	rm com_kvstorehsa_connection_DBConnectionBase.h kvdb-native.o libkvdb-native.dll