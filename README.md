# datadog-databricks

* Assign the Spark Master UI port in an environment variable `DB_DRIVER_PORT` before running the Databricks notebook `init_script`
* Goal:
  * Install the Datadog agent on all the nodes
  * Test a way in which we can get infra metrics
  * Visualize nodes in DD Hosts Map
* TODO:
  * Test a way to get worker metrics via JMX  
