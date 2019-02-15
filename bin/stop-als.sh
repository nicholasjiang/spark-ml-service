#!/bin/bash
#检查环境变量
if [ -z "${ALS_HOME}" ]; then
  export ALS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
export AlgorithmServer_HOME=$ALS_HOME
export AlgorithmServer_HOME_PID=$AlgorithmServer_HOME/bin/als.pid
function alsserver_to_die() {
	local pid
	local count
	pid=$1
	timeOut=$2
	timeoutTime = $(date "+%S")
	let "timeoutTime +=$timeOut"
	currTime=$(date"+%s")
	forceKill=1

    while [[ $currentTime -lt $timeoutTime ]]; do
        $(kill ${pid} > /dev/null 2> /dev/null)
        if kill -0 ${pid} > /dev/null 2>&1; then
             sleep 3
        else
             forceKill=0
             break
        fi
        currentTime=$(date "+%s")
     done

    if [[ forceKill -ne 0 ]]; then
      $(kill -9 ${pid} > /dev/null 2> /dev/null)
    fi
}

if [[ ! -f "${AlgorithmServer_HOME_PID}" ]]; then
    echo "AlgorithmServer is not running"
else
    pid=$(cat ${AlgorithmServer_HOME_PID})
    if [[ -z "${pid}" ]]; then
      echo "AlgorithmServer is not running"
    else
      alsserver_to_die $pid 40
      $(rm -f ${D_P_E_PID})
      echo "AlgorithmServer is stopped."
    fi
fi