#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

CURRENT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
ARCTIC_HOME="$( cd "$CURRENT_DIR/../" ; pwd -P )"

LIB_PATH=$ARCTIC_HOME/lib
LOG_DIR=$ARCTIC_HOME/logs
export CLASSPATH=$ARCTIC_HOME/conf/optimize:$LIB_PATH/:$(find $LIB_PATH/ -type f -name "*.jar" | paste -sd':' -)

while getopts "m:a" arg
do
  case $arg in
       m)
          memory=$OPTARG
          ;;
  esac
done

if [[ -d $JAVA_HOME ]]; then
    JAVA_RUN=$JAVA_HOME/bin/java
else
    JAVA_RUN=java
fi

$JAVA_RUN -Dlog.home=${LOG_DIR} -Dlog.subdir=localOptimizer-${6} -Xmx${memory}m com.netease.arctic.optimizer.local.LocalOptimizer $@ >/dev/null