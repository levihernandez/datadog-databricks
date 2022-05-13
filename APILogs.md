### Push via UDP to Datadog

```python
# [Customer, Databricks admin role]  Assign the name of the dbfs directory where the Datadog init script will be stored
initdir = "datadog"

dbutils.fs.put("dbfs:/"+initdir+"/__init__.py","""
""",True)

dbutils.fs.put("dbfs:/"+initdir+"/SparkApiLogsToDatadog.py","""
import requests
import json
import socket
import os
from time import gmtime, strftime, time

def dd_tag_parser():
  \"\"\"Parsing CUSTOMER_TAGS environment variable to python dict\"\"\"
  customer_tags = os.getenv('CUSTOMER_TAGS')
  array_tags = customer_tags.split(",")
  dict_tags = {}
  for a in array_tags:
    k, v = a.partition(":")[::2]
    dict_tags[k]=v
  return dict_tags

def get_sparkinfo():
  \"\"\"Collect Spark server information from the Databricks driver-env.sh script\"\"\"
  driverenv = {}
  with open("/tmp/driver-env.sh") as myfile:
      for line in myfile:
          name, var = line.partition("=")[::2]
          driverenv[name.strip()] = var.rstrip('\\n')
  driverenv.pop("CONF_DRIVER_JAVA_OPTS")
  driverenv.pop("CONF_EXECUTOR_JAVA_OPTS")
  return driverenv

def get_sysinfo():
    \"\"\"
    Collect basic system information to embedd in the JSON log

    :return: sysinfo: System information JSON payload
    \"\"\"
    epoch_dt = time()
    # IP, Port can be obtained through several ways
    # sparkurl = spark.sparkContext.uiWebUrl  # get it directly from Databricks python notebooks
    # port = sparkurl.split(":")[-1]  # get it directly from Databricks python notebooks
    # ipparse = sparkurl.split(":")[1]  # get it directly from Databricks python notebooks
    # ip = ipparse.split("/")[-1]
    # ip = socket.gethostbyname(socket.gethostname()) # get it directly from Databricks python notebooks (returns localhost IP)
    # Get IP, Port via background script with function get_sparkinfo()
    spark_info = get_sparkinfo()
    ip = spark_info["CONF_PUBLIC_DNS"]
    # port = spark_info["CONF_UI_PORT"]  # get via background python script
    fqdn = socket.getfqdn()
    hostname = socket.gethostname()
    dd_api_key = os.getenv('DD_API_KEY')  # Get DD API Key for log reference
    dd_api_four_mask = dd_api_key[-4:].rjust(len(dd_api_key), '*')  # Mask DD API Key
    checkDate = strftime("%Y-%m-%dT%H:%M:%S.000GMT", gmtime())
    sysinfo = {"hostname": hostname, "epoch": epoch_dt,
               "datetime": checkDate, "ip": ip, "fqdn": fqdn, "dd_api_key": dd_api_four_mask,
               "collector": "python-script"}
    return sysinfo


def get_spark_api(ip, spark_port, query, endpoint):
    \"\"\"Poll the Apache Spark API for each endpoint, Spark docs: https://spark.apache.org/docs/latest/monitoring.html#rest-api
    :param  ip: Spark History or Spark driver IP address
    :param  spark_port: Spark History or Spark driver port number
    :param  query: Spark url param query, see rest-api Spark docs
    :param  endpoint: Spark API url endpoint, see rest-api Spark docs
    :return: data: Returns Spark API endpoint JSON payload
    \"\"\"
    appapi = f"http://{ip}:{spark_port}/api/v1/{endpoint}"  # Concatenate API + Endpoint string
    response = requests.get(appapi, query)  # execute API call
    rsp = {"http.code": response.status_code, "api.endpoint": endpoint, "api.query": query, "spark.ip": ip,
           "spark.port": spark_port}
    if response.status_code == 200:
        rsp["status"] = "success"
        print(rsp)
        data = response.json()
    else:
        rsp["status"] = "failed"
        print(rsp)
        data = {}
    return data


def send_log(ip, udp_port, payload):
    \"\"\"Post JSON log to the Datadog agent on UDP Port.
    Prerequisite: must create the custom Datadog UDP listener config
    before using the current script. conf.yaml type must match the port in this script, see suggested location.

    /etc/datadog-agent/conf.d/spark_api.d/conf.yaml::

        logs:
          - type: udp
            port: 10518
            service: "spark-api"
            source: "rest-logs"

    :param  ip: The host where Spark and Datadog agent run
    :param  udp_port: The Datadog log intake UDP port, typically 10518
    :param  payload: Custom JSON log for each API, JSON array, to be sent to Datadog
    :return: none
    \"\"\"

    json_data = json.dumps(payload, sort_keys=False)
    data = json_data + "\\r\\n"
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(1.0)
        s.sendto(data.encode(), (ip, udp_port))
        s.close()


def collect_logs():
    \"\"\"Process API calls to Apache Spark REST port and forward via UDP to the DD Agent.
    CUSTOMER must MODIFY the following parameters::

        udp_port: The Datadog UDP listener port
        spark_port: The Spark (History or Master) port
        query: Choose a time based query, default processes all Spark (History or Master) logs available
        ddtags: Customize your own tags for better drill down capabilities in Datadog
    \"\"\"
    json_data = {}
    udp_port = 10518
    \"\"\"TODO: Create a function to auto capture the Spark Master port for Databricks\"\"\"
    # sparkurl = spark.sparkContext.uiWebUrl
    # spark_port = sparkurl.split(":")[-1]  # 18080
    spark_info = get_sparkinfo()
    # ip = spark_info["CONF_PUBLIC_DNS"]
    spark_port = spark_info["CONF_UI_PORT"]
    \"\"\"CUSTOMER_TAGS must be in this syntax "team:data-science","department:compliance" \"\"\"
    ddtags = dd_tag_parser() # {"project": "observability", "purpose": "security", "team": "compliance"}  # User custom tags
    sysinfo = get_sysinfo()
    \"\"\"TODO: Create a function to auto capture the IP for Databricks\"\"\"
    ip = sysinfo["ip"]
    print(ddtags)
    json_data.update(ddtags)
    json_data.update(sysinfo)  # Merge system info metadata to the log payload

    \"\"\"TODO: Create a look back at x minutes of data in the Spark API endpoint\"\"\"
    # For time based calls, modify by enabling query = {"minDate": checkDate} and checkDate variable above
    # checkDate = strftime("%Y-%m-%dT%H:%M:%S.000GMT", gmtime())  # current date time calls
    # query = {"minDate": checkDate}
    query = {}  # gets all apps process in a historical manner
    endpoint = "applications"
    level_root = get_spark_api(ip, spark_port, query, endpoint)  # Get inventory of apps

    # Loop through app ids if applications exist
    if level_root:
        query = {}
        endpoint = "version"
        spark_version = get_spark_api(ip, spark_port, query, endpoint)
        json_data["spark.version"] = spark_version["spark"]
        
        \"\"\"Enable the API endpoints to collect data and send it to Datadog\"\"\"
        lev2 = {"spark.job": "jobs", "spark.stage": "stages", "spark.executor": "executors",
                "spark.allexecutor": "allexecutors", "spark.sql": "sql", "spark.environment": "environment"}
        #lev2 = {"spark.sql": "sql"}
        for n, app in enumerate(level_root):
            n += 1
            id = app["id"]
            json_data["spark.app.id"] = id

            for l in lev2:
                key_name = l
                key_value = lev2[l]
                query = {}
                endpoint = f"applications/{id}/{n}/{key_value}"
                api = get_spark_api(ip, spark_port, query, endpoint)
                if len(api) > 0:
                    for j in api:
                        \"\"\"Customize Spark API JSON status key\"\"\"
                        status = "info"
                        if "status" in j:
                            if "COMPLETE" in j["status"]:
                                status = "ok"
                            elif "SUCCESS" in j["status"]:
                                status = "ok"
                            elif "SUCCEEDED" in j["status"]:
                                status = "ok"
                            elif "FAILED" in j["status"]:
                                status = "error"
                            elif "SKIPPED" in j["status"]:
                                status = "warning"
                            elif "ACTIVE" in j["status"]:
                                status = "warning"
                        else:
                            status = "info"
                        payload = {"message": f"Spark API Log: category={key_value}, app_id={id}",
                                   "spark.category": key_value, "status": status}

                        if key_value == "environment":
                            if "runtime" in j:
                                payload["spark.runtime"] = api[j]
                                payload.update(json_data)
                                send_log(ip, udp_port, payload)
                            else:
                                print("skipping")
                        if key_value == "stages":
                            stageid = j["stageId"]
                            attemptid = j["attemptId"]
                            query = {}
                            endpoint = f"applications/{id}/{n}/{key_value}/{stageid}/{attemptid}/taskList"
                            api = get_spark_api(ip, spark_port, query, endpoint)
                            if len(api) > 0:
                                for a in api:
                                    task = {"spark.category": "tasks", "spark.stageId": stageid,
                                            "spark.attemptId": attemptid,
                                            "message": f"Spark API Log: category=tasks, stageId={stageid}, attemptId={attemptid}",
                                            "spark.tasks": a}
                                    task.update(json_data)
                                    send_log(ip, udp_port, task)

                        payload[key_name] = j
                        payload.update(json_data)
                        send_log(ip, udp_port, payload)


if __name__ == "__main__":
    collect_logs()
""",True)
```

### Execute init script in notebook

```bash
%sh 
# Collects all available data in the SQL endpoint
python /dbfs/datadog/SparkApiLogsToDatadog.py
```
