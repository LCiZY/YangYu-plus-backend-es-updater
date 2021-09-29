#!/bin/bash
 ps -ef | grep 'yangyu.canal-1.0-SNAPSHOT-jar-with-dependencies.jar' | grep -v grep | awk '{print $2}'| xargs kill
