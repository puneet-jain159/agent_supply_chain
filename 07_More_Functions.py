# Databricks notebook source
# Create widgets for catalog and database names
dbutils.widgets.text("catalog_name", "supply_chain", "Catalog Name")
dbutils.widgets.text("db_name", "supply_chain_db", "Database Name")

# COMMAND ----------


# Get values from widgets
catalog_name = dbutils.widgets.get("catalog_name")
db_name = dbutils.widgets.get("db_name")


# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false $catalogName=$catalog_name $dbName=$db_name
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION _genie_query(databricks_host STRING, 
# MAGIC                   databricks_token STRING,
# MAGIC                   space_id STRING,
# MAGIC                   question STRING,
# MAGIC                   contextual_history STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'This is a agent that you can converse with to get answers to questions. Try to provide simple questions and provide history if you had prior conversations.'
# MAGIC AS
# MAGIC $$
# MAGIC     import json
# MAGIC     import os
# MAGIC     import time
# MAGIC     from dataclasses import dataclass
# MAGIC     from datetime import datetime
# MAGIC     from typing import Optional
# MAGIC     
# MAGIC     import pandas as pd
# MAGIC     import requests
# MAGIC     
# MAGIC     
# MAGIC     @dataclass
# MAGIC     class GenieResult:
# MAGIC         space_id: str
# MAGIC         conversation_id: str
# MAGIC         question: str
# MAGIC         content: Optional[str]
# MAGIC         sql_query: Optional[str] = None
# MAGIC         sql_query_description: Optional[str] = None
# MAGIC         sql_query_result: Optional[pd.DataFrame] = None
# MAGIC         error: Optional[str] = None
# MAGIC     
# MAGIC         def to_json_results(self):
# MAGIC             result = {
# MAGIC                 "space_id": self.space_id,
# MAGIC                 "conversation_id": self.conversation_id,
# MAGIC                 "question": self.question,
# MAGIC                 "content": self.content,
# MAGIC                 "sql_query": self.sql_query,
# MAGIC                 "sql_query_description": self.sql_query_description,
# MAGIC                 "sql_query_result": self.sql_query_result.to_dict(
# MAGIC                     orient="records") if self.sql_query_result is not None else None,
# MAGIC                 "error": self.error,
# MAGIC             }
# MAGIC             jsonified_results = json.dumps(result)
# MAGIC             return f"Genie Results are: {jsonified_results}"
# MAGIC     
# MAGIC         def to_string_results(self):
# MAGIC             results_string = self.sql_query_result.to_dict(orient="records") if self.sql_query_result is not None else None
# MAGIC             return ("Genie Results are: \n"
# MAGIC                     f"Space ID: {self.space_id}\n"
# MAGIC                     f"Conversation ID: {self.conversation_id}\n"
# MAGIC                     f"Question That Was Asked: {self.question}\n"
# MAGIC                     f"Content: {self.content}\n"
# MAGIC                     f"SQL Query: {self.sql_query}\n"
# MAGIC                     f"SQL Query Description: {self.sql_query_description}\n"
# MAGIC                     f"SQL Query Result: {results_string}\n"
# MAGIC                     f"Error: {self.error}")
# MAGIC     
# MAGIC     class GenieClient:
# MAGIC     
# MAGIC         def __init__(self, *,
# MAGIC                      host: Optional[str] = None,
# MAGIC                      token: Optional[str] = None,
# MAGIC                      api_prefix: str = "/api/2.0/genie/spaces"):
# MAGIC             self.host = host or os.environ.get("DATABRICKS_HOST")
# MAGIC             self.token = token or os.environ.get("DATABRICKS_TOKEN")
# MAGIC             assert self.host is not None, "DATABRICKS_HOST is not set"
# MAGIC             assert self.token is not None, "DATABRICKS_TOKEN is not set"
# MAGIC             self._workspace_client = requests.Session()
# MAGIC             self._workspace_client.headers.update({"Authorization": f"Bearer {self.token}"})
# MAGIC             self._workspace_client.headers.update({"Content-Type": "application/json"})
# MAGIC             self.api_prefix = api_prefix
# MAGIC             self.max_retries = 300
# MAGIC             self.retry_delay = 1
# MAGIC             self.new_line = "\r\n"
# MAGIC     
# MAGIC         def _make_url(self, path):
# MAGIC             return f"{self.host.rstrip('/')}/{path.lstrip('/')}"
# MAGIC     
# MAGIC         def start(self, space_id: str, start_suffix: str = "") -> str:
# MAGIC             path = self._make_url(f"{self.api_prefix}/{space_id}/start-conversation")
# MAGIC             resp = self._workspace_client.post(
# MAGIC                 url=path,
# MAGIC                 headers={"Content-Type": "application/json"},
# MAGIC                 json={"content": "starting conversation" if not start_suffix else f"starting conversation {start_suffix}"},
# MAGIC             )
# MAGIC             resp = resp.json()
# MAGIC             print(resp)
# MAGIC             return resp["conversation_id"]
# MAGIC     
# MAGIC         def ask(self, space_id: str, conversation_id: str, message: str) -> GenieResult:
# MAGIC             path = self._make_url(f"{self.api_prefix}/{space_id}/conversations/{conversation_id}/messages")
# MAGIC             # TODO: cleanup into a separate state machine
# MAGIC             resp_raw = self._workspace_client.post(
# MAGIC                 url=path,
# MAGIC                 headers={"Content-Type": "application/json"},
# MAGIC                 json={"content": message},
# MAGIC             )
# MAGIC             resp = resp_raw.json()
# MAGIC             message_id = resp.get("message_id", resp.get("id"))
# MAGIC             if message_id is None:
# MAGIC                 print(resp, resp_raw.url, resp_raw.status_code, resp_raw.headers)
# MAGIC                 return GenieResult(content=None, error="Failed to get message_id")
# MAGIC     
# MAGIC             attempt = 0
# MAGIC             query = None
# MAGIC             query_description = None
# MAGIC             content = None
# MAGIC     
# MAGIC             while attempt < self.max_retries:
# MAGIC                 resp_raw = self._workspace_client.get(
# MAGIC                     self._make_url(f"{self.api_prefix}/{space_id}/conversations/{conversation_id}/messages/{message_id}"),
# MAGIC                     headers={"Content-Type": "application/json"},
# MAGIC                 )
# MAGIC                 resp = resp_raw.json()
# MAGIC                 status = resp["status"]
# MAGIC                 if status == "COMPLETED":
# MAGIC                     try:
# MAGIC     
# MAGIC                         query = resp["attachments"][0]["query"]["query"]
# MAGIC                         query_description = resp["attachments"][0]["query"].get("description", None)
# MAGIC                         content = resp["attachments"][0].get("text", {}).get("content", None)
# MAGIC                     except Exception as e:
# MAGIC                         return GenieResult(
# MAGIC                             space_id=space_id,
# MAGIC                             conversation_id=conversation_id,
# MAGIC                             question=message,
# MAGIC                             content=resp["attachments"][0].get("text", {}).get("content", None)
# MAGIC                         )
# MAGIC                     break
# MAGIC     
# MAGIC                 elif status == "EXECUTING_QUERY":
# MAGIC                     self._workspace_client.get(
# MAGIC                         self._make_url(
# MAGIC                             f"{self.api_prefix}/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result"),
# MAGIC                         headers={"Content-Type": "application/json"},
# MAGIC                     )
# MAGIC                 elif status in ["FAILED", "CANCELED"]:
# MAGIC                     return GenieResult(
# MAGIC                         space_id=space_id,
# MAGIC                         conversation_id=conversation_id,
# MAGIC                         question=message,
# MAGIC                         content=None,
# MAGIC                         error=f"Query failed with status {status}"
# MAGIC                     )
# MAGIC                 elif status != "COMPLETED" and attempt < self.max_retries - 1:
# MAGIC                     time.sleep(self.retry_delay)
# MAGIC                 else:
# MAGIC                     return GenieResult(
# MAGIC                         space_id=space_id,
# MAGIC                         conversation_id=conversation_id,
# MAGIC                         question=message,
# MAGIC                         content=None,
# MAGIC                         error=f"Query failed or still running after {self.max_retries * self.retry_delay} seconds"
# MAGIC                     )
# MAGIC                 attempt += 1
# MAGIC             resp = self._workspace_client.get(
# MAGIC                 self._make_url(
# MAGIC                     f"{self.api_prefix}/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result"),
# MAGIC                 headers={"Content-Type": "application/json"},
# MAGIC             )
# MAGIC             resp = resp.json()
# MAGIC             columns = resp["statement_response"]["manifest"]["schema"]["columns"]
# MAGIC             header = [str(col["name"]) for col in columns]
# MAGIC             rows = []
# MAGIC             output = resp["statement_response"]["result"]
# MAGIC             if not output:
# MAGIC                 return GenieResult(
# MAGIC                     space_id=space_id,
# MAGIC                     conversation_id=conversation_id,
# MAGIC                     question=message,
# MAGIC                     content=content,
# MAGIC                     sql_query=query,
# MAGIC                     sql_query_description=query_description,
# MAGIC                     sql_query_result=pd.DataFrame([], columns=header),
# MAGIC                 )
# MAGIC             for item in resp["statement_response"]["result"]["data_typed_array"]:
# MAGIC                 row = []
# MAGIC                 for column, value in zip(columns, item["values"]):
# MAGIC                     type_name = column["type_name"]
# MAGIC                     str_value = value.get("str", None)
# MAGIC                     if str_value is None:
# MAGIC                         row.append(None)
# MAGIC                         continue
# MAGIC                     match type_name:
# MAGIC                         case "INT" | "LONG" | "SHORT" | "BYTE":
# MAGIC                             row.append(int(str_value))
# MAGIC                         case "FLOAT" | "DOUBLE" | "DECIMAL":
# MAGIC                             row.append(float(str_value))
# MAGIC                         case "BOOLEAN":
# MAGIC                             row.append(str_value.lower() == "true")
# MAGIC                         case "DATE":
# MAGIC                             row.append(datetime.strptime(str_value, "%Y-%m-%d").date())
# MAGIC                         case "TIMESTAMP":
# MAGIC                             row.append(datetime.strptime(str_value, "%Y-%m-%d %H:%M:%S"))
# MAGIC                         case "BINARY":
# MAGIC                             row.append(bytes(str_value, "utf-8"))
# MAGIC                         case _:
# MAGIC                             row.append(str_value)
# MAGIC                 rows.append(row)
# MAGIC     
# MAGIC             query_result = pd.DataFrame(rows, columns=header)
# MAGIC             return GenieResult(
# MAGIC                 space_id=space_id,
# MAGIC                 conversation_id=conversation_id,
# MAGIC                 question=message,
# MAGIC                 content=content,
# MAGIC                 sql_query=query,
# MAGIC                 sql_query_description=query_description,
# MAGIC                 sql_query_result=query_result,
# MAGIC             )
# MAGIC     
# MAGIC     
# MAGIC     assert databricks_host is not None, "host is not set"
# MAGIC     assert databricks_token is not None, "token is not set"
# MAGIC     assert space_id is not None, "space_id is not set"
# MAGIC     assert question is not None, "question is not set"
# MAGIC     assert contextual_history is not None, "contextual_history is not set"
# MAGIC     client = GenieClient(host=databricks_host, token=databricks_token)
# MAGIC     conversation_id = client.start(space_id)
# MAGIC     formatted_message = f"""Use the contextual history to answer the question. The history may or may not help you. Use it if you find it relevant.
# MAGIC     
# MAGIC     Contextual History: {contextual_history}
# MAGIC     
# MAGIC     Question to answer: {question}
# MAGIC     """
# MAGIC     
# MAGIC     result = client.ask(space_id, conversation_id, formatted_message)
# MAGIC     
# MAGIC     return result.to_string_results()
# MAGIC
# MAGIC $$;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION ask_genie_pharma_gsc(question STRING COMMENT "The question to ask about the pharma supply chain dataset", contextual_history STRING COMMENT "provide relevant history to be able to answer this question, assume genie doesn\'t keep track of history. Use \'no relevant history\' if there is nothing relevant to answer the question.")
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'This Agent interacts with the Genie space API to provide answers to questions about the pharma supply chain dataset.'  
# MAGIC RETURN SELECT _genie_query(
# MAGIC   "https://dbc-023ece0a-5678.cloud.databricks.com/",
# MAGIC   "<Enter the PAT Token>",
# MAGIC   "<Enter the Genie Rooms>",
# MAGIC   question, -- retrieved from function
# MAGIC   contextual_history -- retrieved from function
# MAGIC );

# COMMAND ----------

# dbutils.secrets.get(scope="lr", key="supply-chain")

# COMMAND ----------

# MAGIC %md
# MAGIC # Instead you can also register a table query tool 
# MAGIC
# MAGIC Documentation: https://docs.databricks.com/en/generative-ai/agent-framework/agent-tool.html#table-query-tool

# COMMAND ----------


spark.sql(f"""CREATE OR REPLACE FUNCTION {catalog_name}.{db_name}.lookup_product_demand(
  product_name STRING COMMENT 'Name of the product whose demand data to look up',
  wholesaler_name STRING COMMENT 'Name of the wholesaler associated with the demand data'
)
RETURNS STRING
COMMENT 'Returns metadata about the historical demand for a particular product and wholesaler, including the most recent demand value and date. This information is useful for analyzing demand trends and forecasting.'
RETURN SELECT CONCAT(
    'Product: ', product, ', ',
    'Wholesaler: ', wholesaler, ', ',
    'Date: ', CAST(date AS STRING), ', ',
    'Demand: ', CAST(demand AS STRING)
  )
  FROM {catalog_name}.{db_name}.product_demand_historical
  WHERE product = product_name AND wholesaler = wholesaler_name
  ORDER BY date DESC
  LIMIT 1""")

# COMMAND ----------

# MAGIC %md
# MAGIC # You can search the Vector Index 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Query the vector index (testing)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT string(collect_set(Content)) 
# MAGIC FROM vector_search(index => "${catalog_name}.${db_name}.email_content_index", query => 'What are the delays with distribution center 5', num_results => 5);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION query_unstructured_emails(question STRING COMMENT "The question to ask about supply chain")
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'emails sent by the distribution centers to wholesalers explaining the delays' 
# MAGIC RETURN SELECT string(collect_set(Content)) from vector_search(index => "${catalog_name}.${db_name}.email_content_index", query => question, num_results => 5);

# COMMAND ----------

# MAGIC %md
# MAGIC # Execute Code

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION execute_code_sandbox(
# MAGIC   raw_code STRING COMMENT 'proper python code to be executed, this will run with the exec command',
# MAGIC   return_variable_name STRING COMMENT 'variable name to return from the code as a string'
# MAGIC   )
# MAGIC   RETURNS STRING
# MAGIC   LANGUAGE PYTHON
# MAGIC   COMMENT 'execute arbitrary python code and pick a variable that you want to be returned; use this when you think you can use python to give precise/accurate answers, for any time you want to use math; make your code compatible with python -c'
# MAGIC   AS $$
# MAGIC     import inspect
# MAGIC     import io
# MAGIC     import sys
# MAGIC     from typing import Optional
# MAGIC
# MAGIC     from pydantic import BaseModel
# MAGIC
# MAGIC
# MAGIC     class SandboxCodeExecutionResults(BaseModel):
# MAGIC         raw_code: str
# MAGIC         return_variable: str
# MAGIC         available_variables: dict[str, str]
# MAGIC         stdout_output: str = ""
# MAGIC         stderr_output: str = ""
# MAGIC         return_variable_as_string: Optional[str] = None
# MAGIC
# MAGIC
# MAGIC     def execute_code(raw_code: str, return_variable: Optional[str] = None) -> SandboxCodeExecutionResults:
# MAGIC         old_stdout = sys.stdout
# MAGIC         old_stderr = sys.stderr
# MAGIC         new_stdout = io.StringIO()
# MAGIC         new_stderr = io.StringIO()
# MAGIC         sys.stdout = new_stdout
# MAGIC         sys.stderr = new_stderr
# MAGIC
# MAGIC         local_globals = {}
# MAGIC         try:
# MAGIC             exec(raw_code, local_globals)
# MAGIC         except Exception as e:
# MAGIC             print(f"Exception: {e}", file=new_stderr)
# MAGIC
# MAGIC         sys.stdout = old_stdout
# MAGIC         sys.stderr = old_stderr
# MAGIC
# MAGIC         stdout_output = new_stdout.getvalue()
# MAGIC         stderr_output = new_stderr.getvalue()
# MAGIC
# MAGIC         available_variables = {str(k): f"Type: {type(v)} Value: {str(v)}" for k, v in local_globals.items()
# MAGIC                             if not k.startswith("__") and not k in ["raw_code", "return_variable"]
# MAGIC                             and not inspect.ismodule(v) and not inspect.isfunction(v) and not inspect.isclass(v)}
# MAGIC
# MAGIC         return SandboxCodeExecutionResults(
# MAGIC             raw_code=raw_code,
# MAGIC             return_variable=return_variable,
# MAGIC             available_variables=available_variables,
# MAGIC             stdout_output=stdout_output,
# MAGIC             stderr_output=stderr_output,
# MAGIC             return_variable_as_string=str(local_globals.get(return_variable)) if return_variable else None,
# MAGIC         )
# MAGIC     
# MAGIC     return execute_code(raw_code, return_variable_name).json()
# MAGIC   $$
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT execute_code_sandbox(
# MAGIC   'numbers = [45, 678, 3567, 457]\naverage = sum(numbers) / len(numbers)',
# MAGIC   'average')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE ${catalog_name}.${db_name}.product_demand_historical
