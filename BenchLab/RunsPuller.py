# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Runs Puller #
# MAGIC 
# MAGIC Runs puller will query the actual runs table for any runs that have not completed yet that have a run id in the runs config table
# MAGIC 
# MAGIC Then it will loop through the runs that have do not have completed run data, and query the api for the runs get
# MAGIC 
# MAGIC Finally it will merge those results into the actual runs table

# COMMAND ----------

spark.sql("create database if not exists BenchLab;")
spark.sql("use BenchLab; ")
benchbase = "BenchLab"

# COMMAND ----------



runsToGet = list(spark.sql(f"""
select
  c.runId
from
  benchlab.runsconfig c
  left join {benchbase}.runsactual a on c.runId = a.run_id
where
  a.run_id is null
""").toPandas()['runId'])


# COMMAND ----------

# MAGIC %python
# MAGIC import requests
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql.functions import *
# MAGIC import json
# MAGIC 
# MAGIC #DOMAIN = 'https://e2-demo-west.cloud.databricks.com'
# MAGIC API_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
# MAGIC TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW persistedRunsGet as SELECT * FROM runsActual WHERE 1=2

# COMMAND ----------

if not bronze_df.filter(col('state.life_cycle_state') == 'TERMINATED').take(1):
  print("yes")
else: 
  print("no")

# COMMAND ----------

display(bronze_df.filter(col('state.life_cycle_state') == 'TERMINATED'))

# COMMAND ----------



# COMMAND ----------

for run in runsToGet:
  create_job_endpoint = f'/api/2.0/jobs/runs/get?run_id={run}'
  response = requests.get(API_URL + create_job_endpoint, headers={'Authorization': 'Bearer ' + TOKEN})
  json_data = json.dumps(response.json())
  rdd_results = sc.parallelize([json_data])
  bronze_df = spark.read.json(rdd_results)
  bronze_df = bronze_df.filter(col('state.life_cycle_state') == 'TERMINATED')
  bronze_df.createOrReplaceTempView("RunsGet")
  if bronze_df.filter(col('state.life_cycle_state') == 'TERMINATED').take(1):
    spark.sql("""
    CREATE OR REPLACE TEMP VIEW persistedRunsGet as 

    SELECT * FROM persistedRunsGet UNION

    select
      run_id,
      run_type,
      job_id,
      setup_duration,
      execution_duration,
      start_time,
      state.life_cycle_state,
      state.result_state,
      state.state_message,
      cluster_instance.cluster_id,
      cluster_instance.spark_context_id,
      number_in_job,
      cluster_spec.new_cluster.enable_elastic_disk,
      cluster_spec.new_cluster.node_type_id,
      cluster_spec.new_cluster.spark_version,
      cluster_spec.new_cluster.num_workers,
      cluster_spec.new_cluster.aws_attributes.availability,
      cluster_spec.new_cluster.aws_attributes.zone_id,
      creator_user_name,
      task.notebook_task.notebook_path,
      run_name,
      run_page_url
    from
      RunsGet

    """)

sql = """
INSERT INTO runsActual 
SELECT * FROM persistedRunsGet
"""
spark.sql(sql)
spark.sql("optimize runsActual")

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from persistedRunsGet

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from runsActual

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC  select
# MAGIC     run_id,
# MAGIC     run_type,
# MAGIC     job_id,
# MAGIC     setup_duration,
# MAGIC     execution_duration,
# MAGIC     start_time,
# MAGIC     state.life_cycle_state,
# MAGIC     state.result_state,
# MAGIC     state.state_message,
# MAGIC     cluster_instance.cluster_id,
# MAGIC     cluster_instance.spark_context_id,
# MAGIC     number_in_job,
# MAGIC     cluster_spec.new_cluster.enable_elastic_disk,
# MAGIC     cluster_spec.new_cluster.node_type_id,
# MAGIC     cluster_spec.new_cluster.spark_version,
# MAGIC     cluster_spec.new_cluster.num_workers,
# MAGIC     cluster_spec.new_cluster.aws_attributes.availability,
# MAGIC     cluster_spec.new_cluster.aws_attributes.zone_id,
# MAGIC     creator_user_name,
# MAGIC     task.notebook_task.notebook_path,
# MAGIC     run_name,
# MAGIC     run_page_url
# MAGIC   from
# MAGIC     RunsGet

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from RunsGet

# COMMAND ----------


