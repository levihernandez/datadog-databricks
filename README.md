# datadog-databricks

* Assign the Spark Master UI port in an environment variable `DB_DRIVER_PORT` before running the Databricks notebook `init_script`
* Goal:
  * Install the Datadog agent on all the nodes
  * Test a way in which we can get infra metrics
  * Visualize nodes in DD Hosts Map

* To collect Spark logs and better organize your data via tags, copy the contents of [`databricks-datadog_allNodes.md`]() as a Python notebook cell.
 * The script can be used for Global Init configs.
 * The script can be used for Cluster only configs (copy/paste the first DD values to the Advanced Options > Environment Variables config.
