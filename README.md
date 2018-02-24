# Tachyon
A multipart file downloader (aka. download accelerator)

## Building and Running the code:
Make sure you're in the project directory<br>

Windows:<br>
```
mkdir \target\classes
javac -cp ".;./jars/*" ./src/*.java -d ./target/classes/
java -cp ".;./target/classes;./jars/*" Tachyon -url \<file url\> -o \<path to output file\> -c \<max number of connections\>
```

Linux:<br>
```
mkdir -p \target\classes
javac -cp ".:./jars/*" ./src/*.java -d ./target/classes/
java -cp ".:./target/classes:./jars/*" Tachyon -url \<file url\> -o \<path to output file\> -c \<max number of connections\>
```

#### or you can run run.sh (for linux) or run.bat (windows) which should try to download the sample file provided using 4 connections and save it in the same directory

