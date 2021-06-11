#!/bin/sh
bin=`dirname "$0"`
basedir=`cd "$bin"/.. >/dev/null; pwd`

CONF_DIR=${basedir}/conf
LOG_DIR=${basedir}/logs

CLASSPATH=${CONF_DIR}:`find ${basedir}/lib -name "*jar"|xargs -i echo {}:|xargs echo|sed 's/ //g'`

JAVA_OPTS="-Dtimely.log.dir=${LOG_DIR} -Dtimely.log.file=timely.log -Dtimely.root.logger=INFO,RFA -Dtimely.security.logger=INFO,RFAS"

exec nohup java -Dtimely_home=${basedir} ${JAVA_OPTS}  -cp $CLASSPATH com.graph.flow.Main 1>${LOG_DIR}/timely.log.out 2>&1 &