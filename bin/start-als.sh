#!/bin/bash
#检查环境变量

if [ -z "${ALS_HOME}" ]; then
  export ALS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
CLASS="com.webank.bdp.ml.deploy.LaunchService"
export AlgorithmServer_HOME=$ALS_HOME
export AlgorithmServer_HOME_PID=$AlgorithmServer_HOME/bin/als.pid
if [[ -f "${AlgorithmServer_HOME_PID}" ]]; then
    pid=$(cat ${AlgorithmServer_HOME_PID})
    if kill -0 ${pid} >/dev/null 2>&1; then
      echo "AlgorithmServer is already running."
      return 0;
    fi
fi
export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5050

DEPENDENCIES_JAR=$AlgorithmServer_HOME/lib/AlgorithmServer-0.0.1-SNAPSHOT.jar

spark-submit --class  $CLASS --master yarn $  $DEPENDENCIES_JAR &
als_server_pid=$!

if [[ -z "${als_server_pid}" ]]; then
    echo "AlgorithmServer start failed!"
    exit 1
else
    echo "AlgorithmServer start succeeded!"
    echo $als_server_pid > $AlgorithmServer_HOME_PID
    sleep 1
fi