To execute, the following commands were followed:

1) hadoop fs -put input.txt /user/shriya/input
(put the input.txt file in hdfs)

2)javac -cp "$HADOOP_HOME/share/hadoop/common/hadoop-common-3.2.1.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.2.1.jar" Apriori.java
(compile the Apriori.java file)

3)jar cf Apriori.jar Apriori*.class
(creare a jar file)

4)hadoop jar Apriori.jar Apriori /user/shriya/input /user/shriya/output
(give input path and output path)

5)hdfs dfs -ls /user/shriya/output
(check the output folder)

6)hdfs dfs -cat /user/shriya/output/part-r-00000
(print the output)