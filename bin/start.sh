#!/bin/sh 
currentpath=$(cd `dirname $0`; pwd)
module_dir=$currentpath
lib_dir=$module_dir
module_console=$module_dir/monitor.log

libs=`ls $lib_dir/*.jar`
for lib in $libs
do
   lib_file=$lib_file:$lib
done
lib_file=${lib_file#:}
runableclass=com.syniverse.TableCompareTool
classes=.:$lib_file

cd $module_dir
java_options="java -Xms512m -Xmx3072m -XX:MaxPermSize=128m -XX:+UseParallelGC -XX:ParallelGCThreads=16 -XX:MaxDirectMemorySize=128m"
$java_options -classpath $classes -Xrs $runableclass
