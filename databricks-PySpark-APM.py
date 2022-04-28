import sys
import json
import socket
from ddtrace import tracer
from operator import add
from pyspark.sql import SparkSession
from pyspark import TaskContext


def task_info(*_):
    ctx = TaskContext()
    return ["Stage: {0}, Partition: {1}, Host: {2}, Attempt: {3}, Task Attempt: {4}".format 
    (ctx.stageId(), ctx.partitionId(), socket.gethostname(), ctx.attemptNumber(), ctx.taskAttemptId())]

    
    
    
@tracer.wrap(service="hive", resource="connect")
def onedf():
  with tracer.trace("connect") as my_span:
    data = [("Java", "20000"), ("werwerwerwe", "100000"), ("TExt", "3000")]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF()
    df.explain("extended")
    df.printSchema()
    df.show()
    my_span.set_tag("hive", "connecting")
  twodf()

#@tracer.wrap("word_count", service="hive", resource="disconnect")
def twodf():
  with tracer.trace("disconnect") as my_span:
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3001"), ("C++", "3001"), ("Cobol", "3001")]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF()
    # df.printSchema()
    my_span.set_tag("hive", "disconnect")
    print(TaskContext().taskAttemptId())
    threedf()

def threedf():
  with tracer.trace("disconnect") as my_span:
    data = [("Jaasdfsadfava", "20000"), ("Python", "100000"), ("Scala", "3001"), ("C++", "3001"), ("Cobol", "3001")]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF()
    # df.printSchema()
    my_span.set_tag("hive", "disconnect")
    print(TaskContext().taskAttemptId())
    
    
if __name__ == "__main__":
  spark = SparkSession.builder.appName("PythonWordCount").getOrCreate() 
  
  dd_meta = {"spark.app.id": spark.conf.get("spark.app.id"),
             "spark.app.name": spark.conf.get("spark.app.name"),
             "spark.driver.host": spark.conf.get("spark.driver.host"), 
             "spark.driver.port": spark.conf.get("spark.driver.port"),
             "spark.ui.web.url": sc.uiWebUrl,
             "spark.databricks.clusterUsageTags.azureSubscriptionId": spark.conf.get("spark.databricks.clusterUsageTags.azureSubscriptionId"),
             "spark.databricks.clusterUsageTags.clusterId":spark.conf.get("spark.databricks.clusterUsageTags.clusterId"),
             "spark.databricks.clusterUsageTags.clusterName": spark.conf.get("spark.databricks.clusterUsageTags.clusterName"),
             "spark.databricks.clusterUsageTags.clusterNodeType": spark.conf.get("spark.databricks.clusterUsageTags.clusterNodeType"),
             "spark.databricks.clusterUsageTags.driverContainerId": spark.conf.get("spark.databricks.clusterUsageTags.driverContainerId"),
             "spark.databricks.clusterUsageTags.effectiveSparkVersion": spark.conf.get("spark.databricks.clusterUsageTags.effectiveSparkVersion"),
             "spark.databricks.clusterUsageTags.managedResourceGroup": spark.conf.get("spark.databricks.clusterUsageTags.managedResourceGroup"),
             "spark.databricks.sparkContextId": spark.conf.get("spark.databricks.sparkContextId"),
             "spark.executor.id": spark.conf.get("spark.executor.id"),
             "spark.executor.memory": spark.conf.get("spark.executor.memory"),
             "spark.master": spark.conf.get("spark.master"),
             "dbutils.notebook.entry_point.username": dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get(),
             "dbutils.notebook.entry_point.sessionid": dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('sessionId')
            }
  tracer.set_tags(dd_meta)
  
  onedf()
  task_info()
