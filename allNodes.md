* If you have enabled the Datadog variables at the cluster level, run the following in the notebook to get the DD values and cluster info.

```
%sh 
hostip=$(hostname -I | xargs)
echo "IP: ${hostip}"
echo """
DD_SITE=${DD_SITE}
DD_ENV=${DD_ENV}
DD_API_KEY=${DD_API_KEY}
DD_ENVIRONMENT=${DD_ENVIRONMENT}
CUSTOMER_TAGS=${CUSTOMER_TAGS}
host_ip: ${hostip}
databricks_cluster_id: ${DB_CLUSTER_ID}
databricks_cluster_name: ${DB_CLUSTER_NAME}
user: """ $(whoami)
```

* Below is the init_script, modified to include Databricks, Spark logs, node metrics, live process, and help you customize tags for a better filtering of data.

```
%python
# [Customer, Databricks admin role]  Assign the name of the dbfs directory where the Datadog init script will be stored
initdir = "datadog"

dbutils.fs.put("dbfs:/"+initdir+"/datadog-install-driver-workers.sh","""
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
  
  hostip=$(hostname -I | xargs)
  echo "host_ip: ${hostip}, databricks_cluster_id: ${DB_CLUSTER_ID}, databricks_cluster_name: ${DB_CLUSTER_NAME}, user: "$(whoami)

if [[ \${DB_IS_DRIVER} = "TRUE" ]]; then

  # Downloading the Python script for Spark API collection as logs, file will show up after cluster restart

  echo "Installing Datadog agent in the driver (master node) ..."
  # CONFIGURE HOST TAGS FOR DRIVER
  DD_TAGS="environment:databricks-\${DD_ENV}","databricks_cluster_id:\${DB_CLUSTER_ID}","databricks_cluster_name:\${DB_CLUSTER_NAME}","spark_node:driver","${CUSTOMER_TAGS}"

  # INSTALL THE LATEST DATADOG AGENT 7 ON DRIVER AND WORKER NODES
  DD_AGENT_MAJOR_VERSION=7 DD_API_KEY=\${DD_API_KEY} DD_SITE=\${DD_SITE} DD_HOST_TAGS=\${DD_TAGS} bash -c "\$(curl -L https://s3.amazonaws.com/dd-agent/scripts/install_script.sh)"
  
  # WAIT FOR DATADOG AGENT TO BE INSTALLED
  while [ -z \$datadoginstalled ]; do
    if [ -e "/etc/datadog-agent/datadog.yaml" ]; then
      datadoginstalled=TRUE
    fi
    sleep 2
  done
  echo "Datadog Agent is installed"

  # ENABLE: live process and logs collection
  echo "api_key: \${DD_API_KEY}
site: \${DD_SITE}
hostname: \${hostip}
tags: [\${DD_TAGS}]
env: \${DD_ENV} 
process_config:
    enabled: 'true'
logs_enabled: true" > /etc/datadog-agent/datadog.yaml 

  while [ -z \$gotparams ]; do
    if [ -e "/tmp/driver-env.sh" ]; then
      DB_DRIVER_PORT=\$(grep -i "CONF_UI_PORT" /tmp/driver-env.sh | cut -d'=' -f2)
      gotparams=TRUE
    fi
    sleep 2
  done

  # CREATE CUSTOM DD YAML CONFIG: Create custom Spark API log collector via UDP 
  
  
  # WRITING CONFIG FILE FOR SPARK INTEGRATION WITH STRUCTURED STREAMING METRICS ENABLED
  # MODIFY TO INCLUDE OTHER OPTIONS IN spark.d/conf.yaml.example
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
      source: databricks
      service: driver-log
      log_processing_rules:
        - type: multi_line
          name: new_log_start_with_date
          pattern: \d{2,4}[\-\/]\d{2,4}[\-\/]\d{2,4}.*" > /etc/datadog-agent/conf.d/spark.yaml
else

  hostip=$(hostname -I | xargs)
  echo "host_ip: ${hostip}, databricks_cluster_id: ${DB_CLUSTER_ID}, databricks_cluster_name: ${DB_CLUSTER_NAME}, user: "$(whoami)
  
  # CONFIGURE: Host tags for Worker nodes
  DD_TAGS="environment:databricks-\${DD_ENV}","databricks_cluster_id:\${DB_CLUSTER_ID}","databricks_cluster_name:\${DB_CLUSTER_NAME}","spark_node:worker","${CUSTOMER_TAGS}"

  # INSTALL: Datadog Agent 7 on Worker nodes
  DD_AGENT_MAJOR_VERSION=7 DD_API_KEY=\$DD_API_KEY DD_SITE=\${DD_SITE} DD_HOST_TAGS=\$DD_TAGS bash -c "\$(curl -L https://s3.amazonaws.com/dd-agent/scripts/install_script.sh)"
  
  # ENABLE: live process collection
  echo "api_key: \${DD_API_KEY}
site: \${DD_SITE}
hostname: \${hostip}
tags: [\${DD_TAGS}]
env: \${DD_ENV} 
process_config:
    enabled: 'true'" > /etc/datadog-agent/datadog.yaml 
fi

  # RESTARTING AGENT
  sudo service datadog-agent restart
EOF

# CLEANING UP
chmod a+x /tmp/start_datadog.sh
/tmp/start_datadog.sh >> /tmp/datadog_start.log 2>&1 & disown
""", True)
```
