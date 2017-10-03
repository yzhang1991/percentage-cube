jarName="pctcube.jar"

clean() {
    rm  src/pctcube/*.class \
        src/pctcube/experiments/*.class \
        src/pctcube/database/*.class \
        src/pctcube/database/query/*.class \
        src/pctcube/utils/*.class 2>/dev/null
    rm $jarName 2>/dev/null
}

jars() {
    javac src/pctcube/*.java \
          src/pctcube/experiments/*.java \
          src/pctcube/database/*.java \
          src/pctcube/database/query/*.java \
          src/pctcube/utils/*.java
    jar cf $jarName -C src pctcube
}

jarsifneeded() {
    if [ ! -e $jarName ]; then
        jars;
    fi
}

start() {
    jarsifneeded
    java -classpath $jarName:./third-party/* pctcube.experiments.jPctCubeExpt1
}

if [ $# -eq 0 ]; then
    start
    exit
fi

for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done
