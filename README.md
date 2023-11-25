spark-in-Java

in windows environment, download winutils.exe
https://github.com/steveloughran/winutils
create folder C:\hadoop\bin

For org.apache.spark.SparkException: Job aborted: Task not serializable: java.io.NotSerializableException, we need to change:
myRdd.foreach(...)  to myRdd.collection().forEach(), the reason: data is patitioned, and objects are serialized between different partition. 
