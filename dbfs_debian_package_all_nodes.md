## Install Datadog via Local DBFS Datadog debian Package

```
%python
# [Customer, Databricks admin role]  Assign the name of the dbfs directory where the Datadog init script will be stored
initdir = "datadog"

dbutils.fs.put("dbfs:/"+initdir+"/datadog-debian-install.sh","""
#!/bin/bash
cat <<EOF >> /tmp/start_datadog.sh

#!/bin/bash

## For Global Init Script, hard code the following variables and enable them
## For cluster based configs, add the same variables to the cluster Advanced Options > Environment Variables
# DD_SITE=<dd-site>
# DD_ENV=prod
# DD_API_KEY=<dd-api-key>
# DD_ENVIRONMENT=business-prod # Optional
# CUSTOMER_TAGS="team:data-science,department:compliance" # Optional
  
hostip=\$(hostname -I | xargs)
echo "Running script in Databricks cluster as: " $(whoami)

if [[ \${DB_IS_DRIVER} = "TRUE" ]]; then
  echo "This host is the driver: ${DB_IS_DRIVER}"

  echo "Installing Datadog agent in the driver (master node) ..."
  # CONFIGURE HOST TAGS FOR THE DRIVER
  DD_TAGS="\"environment:databricks_\${DD_ENV}\", \"databricks_cluster_id:\${DB_CLUSTER_ID}\", \"databricks_cluster_name:\${DB_CLUSTER_NAME}\", \"spark_node:driver\",\"${CUSTOMER_TAGS}\""
  echo "DD custom tags: \${DD_TAGS}"
  
  # INSTALL THE LATEST DATADOG AGENT 7 ON HOST DRIVER
  echo "Waiting for driver-env.sh to be created in order to install the DD deb package... "
  while [ -z \$gotparams ]; do
    if [ -e "/tmp/driver-env.sh" ]; then
      DB_DRIVER_PORT=\$(grep -i "CONF_UI_PORT" /tmp/driver-env.sh | cut -d'=' -f2)
      gotparams=TRUE
      echo "file created, installing Datadog agent deb package"
      dpkg -i /dbfs/FileStore/datadog-agent_\${DD_AGENT_VERSION}-1_amd64.deb
    fi
    sleep 2
  done
  
  echo "Creating the driver Datadog main configuration file: /etc/datadog-agent/datadog.yaml"
  echo "api_key: \${DD_API_KEY}
site: \${DD_SITE}
hostname: \${hostip}
tags: [\${DD_TAGS}]
env: \${DD_ENV} 
process_config:
    enabled: 'true'
logs_enabled: true" > /etc/datadog-agent/datadog.yaml 
  
  echo "Creating the driver Datadog Spark configuration file: /etc/datadog-agent/conf.d/spark.yaml"
  # Spark Streaming is enabled
  echo "init_config:
instances:
    - spark_url: http://\${DB_DRIVER_IP}:\${DB_DRIVER_PORT}
      spark_cluster_mode: spark_driver_mode
      cluster_name: \${hostip}
      streaming_metrics: true
logs:
    - type: file
      path: /databricks/driver/logs/*
      exclude_paths:
        - /databricks/driver/logs/*.gz
        - /databricks/driver/logs/metrics.json
      source: spark
      service: driver-log
      log_processing_rules:
        - type: multi_line
          name: new_log_start_with_date
          pattern: \d{2,4}[\-\/]\d{2,4}[\-\/]\d{2,4}.*"  > /etc/datadog-agent/conf.d/spark.yaml
 
else
  hostip=\$(hostname -I | xargs)
  # CONFIGURE HOST TAGS FOR WORKERS
  DD_TAGS="\"environment:databricks_\${DD_ENV}\", \"databricks_cluster_id:\${DB_CLUSTER_ID}\", \"databricks_cluster_name:\${DB_CLUSTER_NAME}\", \"spark_node:worker\",\"\${CUSTOMER_TAGS}\""
  echo "DD custom tags: \${DD_TAGS}"
  
  # INSTALL THE LATEST DATADOG AGENT 7 ON HOST WORKER NODES
  # Install Datadog with the Debian package
  dpkg -i /dbfs/FileStore/datadog-agent_\${DD_AGENT_VERSION}-1_amd64.deb
  
  nodeip=\$(hostname -I | xargs)
  echo "Creating the worker Datadog main configuration file: /etc/datadog-agent/datadog.yaml"
  echo "api_key: \${DD_API_KEY}
site: \${DD_SITE}
hostname: \${nodeip}
tags: \[\${DD_TAGS}\]
process_config:
    enabled: 'true'
env: \${DD_ENV} " > /etc/datadog-agent/datadog.yaml

fi

  # RESTARTING THE AGENT
  service datadog-agent restart

EOF

# CLEANING UP
chmod a+x /tmp/start_datadog.sh
/tmp/start_datadog.sh >> /tmp/datadog_start.log 2>&1 & disown
""", True)
```
