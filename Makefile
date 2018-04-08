SOURCE=/Users/shabbirhussain/Data/Alefiya/data/small/
OUTPUT=/Users/shabbirhussain/Data/Alefiya/data.csv
GROUP_SIZE=2

SPARK_BIN_PATH=
SCALA_BIN_PATH=

# ------------------------------------
# Do not edit! Local config variables.
# ------------------------------------
JAR_NAME=target/artifacts/task.jar
LIB_PATH=target/dependency
RUNTIME_JARS=commons-csv-1.5.jar,json-20180130.jar

COMMA=,
FULL_RUNTIME_JARS=${LIB_PATH}/$(subst ${COMMA},${COMMA}${LIB_PATH}/,${RUNTIME_JARS})

all: setup build run

build_run: build run

build:
	mkdir -p "target/artifacts"
	mkdir -p "target/classes/main/resources/"
	${SCALA_BIN_PATH}scalac -cp "./${LIB_PATH}/*" \
		-d target/classes \
		src/main/scala/org/neu/test/*.scala
	cp -r src/main/resources/* target/classes
	jar cfm ${JAR_NAME} \
		src/main/scala/META-INF/MANIFEST.MF \
		-C target/classes/ .

run:
	${SPARK_BIN_PATH}spark-submit \
	 	--master local --driver-memory 7g \
	 	--jars "${FULL_RUNTIME_JARS}" \
    	--class org.neu.test.Main "${JAR_NAME}" "${SOURCE}" "${OUTPUT}" "${GROUP_SIZE}"

ss:
	${SPARK_BIN_PATH}/spark-shell --driver-memory 7G --executor-memory 7G --executor-cores 3 \
	--jars=${FULL_RUNTIME_JARS} \
	--conf spark.checkpoint.compress=true

setup: clean
	mvn install dependency:copy-dependencies

clean:
	-rm -rf target/*