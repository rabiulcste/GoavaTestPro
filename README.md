Solution Sketch
-------------------
- First, left outer join of companies.csv with accounts.csv which produce output of companies property with separate accounts property.
- Second, combining same accounts property into a json array with each company object.
- Third, Generating Multiple outputs of each json object.

Joining (reducesidejoin) accounts properties with companies -> ReduceJoin.java  
Related accounts data in an array property inside company object -> CompAccounts.java  
Multiple output generation -> MultipleOutputsJson.java  


Compile and Run
---------------------
#### JOINING PART
**#Copy input from local directory**  
- *bin/hdfs dfs -put /home/rabiul/Desktop/GoavaTestPro/input /user/hduser/input*
- *bin/hdfs dfs -put /home/rabiul/Desktop/GoavaTestPro/json-20180813.jar /user/hduser // for run time use of json.org.* *
**#Compile**  
- */home/rabiul/Desktop/GoavaTestPro$ javac -classpath $HADOOP_HOME/share/hadoop/common/hadoop-common-2.9.0.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.9.0.jar:$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar:/home/rabiul/Desktop/json-20180813.jar -d /home/rabiul/Desktop/GoavaTestPro ReduceJoin.java*
**#Convert into jar file**   
- */home/rabiul/Desktop/GoavaTestPro$ jar -cvf reducejoinj.jar -C /home/rabiul/Desktop/GoavaTestPro/reducejoinc .*
**#Run JAR file**  
- */usr/local/hadoop$ bin/hadoop jar /home/rabiul/Desktop/GoavaTestPro/reducejoinj.jar ReduceJoin input/accounts.csv input/companies.csv initoutput*

#### COMBINATION PART
**#Compile**  
- */home/rabiul/Desktop/GoavaTestPro$ javac -classpath $HADOOP_HOME/share/hadoop/common/hadoop-common-2.9.0.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.9.0.jar:$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar:/home/rabiul/Desktop/json-20180813.jar -d /home/rabiul/Desktop/GoavaTestPro CompAccounts.java*

**#Convert into jar file**   
- */home/rabiul/Desktop/GoavaTestPro$ jar -cvf compaccountsj.jar -C /home/rabiul/Desktop/GoavaTestPro/compaccountsc .*
**#Run JAR file**  
- */usr/local/hadoop$ bin/hadoop jar /home/rabiul/Desktop/GoavaTestPro/compaccountsj.jar CompAccounts initoutput finoutput*

#### Multiple Output Part
**#Compile**  
- */home/rabiul/Desktop/GoavaTestPro$ javac -classpath $HADOOP_HOME/share/hadoop/common/hadoop-common-2.9.0.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.9.0.jar:$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar:/home/rabiul/Desktop/json-20180813.jar -d /home/rabiul/Desktop/GoavaTestPro MultipleOutputsJson.java*
**#Convert into jar file**   
- */home/rabiul/Desktop/GoavaTestPro$ jar -cvf multipleoutputj.jar -C /home/rabiul/Desktop/GoavaTestPro/multipleoutputc .*
**#Run JAR file**  
- */usr/local/hadoop$ bin/hadoop jar /home/rabiul/Desktop/GoavaTestPro/multipleoutputj.jar MultipleOutputsJson finoutput multioutput*

# Comments
**_This is a partial solve of the problem which fulfills -_**
1. Related accounts data represented as an array property inside company object.
2. Property names for the json files converted to lowercase.
3. If any value for the property does not exist in the csv file the property key does not exist in json file.
4. In the output file ORG_NUMBER property replaced with “orgno”.
5. “ORG_NUMBER” property removed from objects in “accounts” array property of the company object.
6. Multiple output generation as of <ORG_NUMBER>.

**_Failed to solve -_**
1. Failed to maintain identation of regular json object. 
2. Orders of the columns in csv files got shuffled in final output .txt file.
