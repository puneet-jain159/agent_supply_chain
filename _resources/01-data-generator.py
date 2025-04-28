# Databricks notebook source
dbutils.widgets.dropdown('reset_all_data', 'false', ['true', 'false'], 'Reset all data')
dbutils.widgets.text('catalogName',  '####CHANGE THIS####', 'Catalog Name')
dbutils.widgets.text('dbName',  '####CHANGE THIS####' , 'Database Name')

# COMMAND ----------

print("Starting ./_resources/01-data-generator")

# COMMAND ----------

reset_all_data = dbutils.widgets.get('reset_all_data') == 'true'
catalogName = dbutils.widgets.get('catalogName')
dbName = dbutils.widgets.get('dbName')

# COMMAND ----------

print(reset_all_data)
print(catalogName)
print(dbName)

# COMMAND ----------

spark.sql(f"CREATE CATALOG if NOT EXISTS {catalogName} ")
spark.sql(f"USE CATALOG {catalogName}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {dbName} ")
spark.sql(f"USE SCHEMA {dbName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Packages

# COMMAND ----------

import pandas as pd
import numpy as np
import datetime

from dateutil.relativedelta import relativedelta
from dateutil import rrule

import os
import string
import random

import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.window import Window

import statsmodels.api as sm
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulate demand series data

# COMMAND ----------

# MAGIC %md
# MAGIC Parameters

# COMMAND ----------

n=3 # Number of replicates per product category
ts_length_in_weeks = 104 # Length of a time series in weeks
number_of_wholesalers = 30
n_distribution_centers = 5
n_plants = 3 # Number of plants

# COMMAND ----------

# MAGIC %md
# MAGIC Create a Product Table

# COMMAND ----------

ProdCatSchema = StructType([       
    StructField('product_categories', StringType(), True),
    StructField('transport_baseline_cost', FloatType(), True)
])

category_data = [
    ("syringe", 0.72),
    ("tablet press", 0.93),
    ("autoclave", 1.11),
    ("glucometer", 0.71),
    ("vial crimper", 0.97),
    ("scalpel", 1.01),
    ("ampoule", 0.91),
    ("pipette", 0.61),
    ("centrifuge", 0.81),
    ("medical storage box", 1.31)
]

products_categories = spark.createDataFrame(data=category_data, schema = ProdCatSchema)

products_versions = spark.createDataFrame(
  list(range(1,(n+1))),
  StringType()).toDF("product_versions")

product_table = (
  products_categories.
  crossJoin(products_versions).
  select(f.concat_ws('_', f.col("product_categories"), f.col("product_versions")).alias("product"), f.col("transport_baseline_cost"))
                )

display(product_table)

# COMMAND ----------

# MAGIC %md 
# MAGIC Introduce pharma wholesalers (upstream supply chain entities managing inventory for pharmacies) 

# COMMAND ----------

wholesaler_table = spark.createDataFrame(
  list(range(1,(number_of_wholesalers+1))),
  StringType()).toDF("number_of_wholesalers")

wholesaler_table = wholesaler_table.select(f.concat_ws('_',f.lit("Wholesaler"), f.col("number_of_wholesalers")).alias("wholesaler"))

display(wholesaler_table)

# COMMAND ----------

products_in_wholesalers_table = (
  product_table.
  crossJoin(wholesaler_table)
)
display(products_in_wholesalers_table)

# COMMAND ----------

# MAGIC %md 
# MAGIC Generate Date Series

# COMMAND ----------

# End Date: Monday of the current week
end_date = datetime.datetime.now().replace(hour=0, minute=0, second= 0, microsecond=0) 
end_date = end_date + datetime.timedelta(-end_date.weekday()) #Make sure to get the monday before

# Start date: Is a monday, since we will go back integer number of weeks
start_date = end_date + relativedelta(weeks= (- ts_length_in_weeks))

# Make a sequence 
date_range = list(rrule.rrule(rrule.WEEKLY, dtstart=start_date, until=end_date))

#Create a pandas data frame
date_range = pd.DataFrame(date_range, columns =['date'])

display(date_range)

# COMMAND ----------

# MAGIC %md
# MAGIC Simulate parameters for ARMA series

# COMMAND ----------

# Define schema for new columns
arma_schema = StructType(
  [
    StructField("Variance_RN", FloatType(), True),
    StructField("Offset_RN", FloatType(), True),
    StructField("AR_Pars_RN", ArrayType(FloatType()), True),
    StructField("MA_Pars_RN", ArrayType(FloatType()), True)
  ]
)

# Generate random numbers for the ARMA process
np.random.seed(123)
n_ = products_in_wholesalers_table.count()


variance_random_number = list(abs(np.random.normal(10, 2, n_)))
offset_random_number = list(np.maximum(abs(np.random.normal(100, 50, n_)), 30))
ar_length_random_number = np.random.choice(list(range(1,4)), n_)
ar_parameters_random_number = [np.random.uniform(low=0.1, high=0.3, size=x) for x in ar_length_random_number] 
ma_length_random_number = np.random.choice(list(range(1,4)), n_)
ma_parameters_random_number = [np.random.uniform(low=0.1, high=0.3, size=x) for x in ma_length_random_number] 


# Collect in a dataframe
pdf_helper = (pd.DataFrame(variance_random_number, columns =['Variance_RN']). 
              assign(Offset_RN = offset_random_number).
              assign(AR_Pars_RN = ar_parameters_random_number).
              assign(MA_Pars_RN = ma_parameters_random_number) 
             )

spark_df_helper = spark.createDataFrame(pdf_helper, schema=arma_schema)
spark_df_helper = (spark_df_helper.
  withColumn("row_id", f.monotonically_increasing_id()).
  withColumn('row_num', f.row_number().over(Window.orderBy('row_id'))).
  drop(f.col("row_id"))
                  )

products_in_wholesalers_table = (products_in_wholesalers_table.
                            withColumn("row_id", f.monotonically_increasing_id()).
                            withColumn('row_num', f.row_number().over(Window.orderBy('row_id'))).
                            drop(f.col("row_id"))
                           )


products_in_wholesalers_table = products_in_wholesalers_table.join(spark_df_helper, ("row_num")).drop(f.col("row_num"))
display(products_in_wholesalers_table)

# COMMAND ----------

# MAGIC %md 
# MAGIC Generate individual demand series

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType

# Function to generate an ARMA process
def generate_arma(arparams, maparams, var, offset, number_of_points, plot):
    np.random.seed(123)
    ar = np.r_[1, arparams] 
    ma = np.r_[1, maparams] 
    y = sm.tsa.arma_generate_sample(ar, ma, number_of_points, scale=var, burnin=1) + offset
    y = np.round(y).astype(int)
    y = np.absolute(y)
    
    if plot:
        x = np.arange(1, len(y) + 1)
        plt.plot(x, y, color="red")
        plt.show()
        
    return y

# Schema for the output DataFrame
schema = StructType([
    StructField("product", StringType(), True),
    StructField("wholesaler", StringType(), True),
    StructField("date", DateType(), True),
    StructField("demand", FloatType(), True),
    StructField("row_number", FloatType(), True)
])

# Generate a time series using a Pandas UDF
def time_series_generator_pandas_udf(pdf):
    out_df = date_range.assign(
        demand=generate_arma(
            arparams=pdf.AR_Pars_RN.iloc[0], 
            maparams=pdf.MA_Pars_RN.iloc[0], 
            var=pdf.Variance_RN.iloc[0], 
            offset=pdf.Offset_RN.iloc[0], 
            number_of_points=date_range.shape[0], 
            plot=False
        ),
        product=pdf["product"].iloc[0],
        wholesaler=pdf["wholesaler"].iloc[0]
    )
    
    out_df["row_number"] = range(0, len(out_df))
    out_df = out_df[["product", "wholesaler", "date", "demand", "row_number"]]
    return out_df

# Partitioning the data to improve parallelism
# Calculate the number of partitions dynamically
n_tasks = products_in_wholesalers_table.select("product", "wholesaler").distinct().count()

# Repartition the data before applying the UDF
products_in_wholesalers_table = products_in_wholesalers_table.repartition(n_tasks, "product", "wholesaler")

# Apply the Pandas UDF and clean up
demand_df = (
    products_in_wholesalers_table
    .groupby("product", "wholesaler")
    .applyInPandas(time_series_generator_pandas_udf, schema)
    .select("product", "wholesaler", "date", "demand")
)

# Ensure date_range.shape[0] * products_in_wholesalers_table.count() matches demand_df.count()
display(demand_df)


# COMMAND ----------

# Test if demand is in a realistic range
#display(demand_df.groupBy("product", "store").mean("demand"))

# COMMAND ----------

# Select a sepecific time series
# display(demand_df.join(demand_df.sample(False, 1 / demand_df.count(), seed=0).limit(1).select("product", "store"), on=["product", "store"], how="inner"))

# COMMAND ----------

# MAGIC %md
# MAGIC Save as a Delta table

# COMMAND ----------

# Write the data 
demand_df.write.mode("overwrite").saveAsTable(f"{catalogName}.{dbName}.product_demand_historical")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate product to distribution center mapping table

# COMMAND ----------

distribution_centers = (
  spark.createDataFrame(list(range(1, n_distribution_centers + 1)),StringType()).
  toDF("distribution_center_helper").
  withColumn("distribution_center", f.concat_ws('_', f.lit("Distribution_Center"), f.col("distribution_center_helper"))).
  select("distribution_center")
)


display(distribution_centers)

# COMMAND ----------

# We need more distribution centers than wholesalers
assert (distribution_centers.count() <= wholesaler_table.count()) & (distribution_centers.count() > 0)

#Replicate distribution centers such that all distribution centers are used, but the table has the same number of rows than wholesaler_table
divmod_res = divmod(wholesaler_table.count(), distribution_centers.count())

rest_helper = distribution_centers.limit(divmod_res[1])
maximum_integer_divisor = (
  spark.createDataFrame(list(range(1, divmod_res[0] + 1)),StringType()).
  toDF("number_helper").
  crossJoin(distribution_centers).
  select("distribution_center")
)

distribution_centers_replicated = maximum_integer_divisor.unionAll(rest_helper)

assert distribution_centers_replicated.count() == wholesaler_table.count()

# Append distribution_centers_replicated and wholesaler_table column-wise
distribution_centers_replicated = (distribution_centers_replicated.
  withColumn("row_id", f.monotonically_increasing_id()).
  withColumn('row_num', f.row_number().over(Window.orderBy('row_id'))).
  drop(f.col("row_id"))
                  )

wholesaler_table = (wholesaler_table.
                            withColumn("row_id", f.monotonically_increasing_id()).
                            withColumn('row_num', f.row_number().over(Window.orderBy('row_id'))).
                            drop(f.col("row_id"))
                           )


distribution_center_to_wholesaler_mapping_table = wholesaler_table.join(distribution_centers_replicated, ("row_num")).drop(f.col("row_num"))
wholesaler_table = wholesaler_table.drop(f.col("row_num"))
distribution_centers_replicated = distribution_centers_replicated.drop(f.col("row_num"))

display(distribution_center_to_wholesaler_mapping_table)

# COMMAND ----------

# MAGIC %md
# MAGIC Save as a Delta table

# COMMAND ----------

# Write the data 
distribution_center_to_wholesaler_mapping_table.write.mode("overwrite").saveAsTable(f"{catalogName}.{dbName}.distribution_center_to_wholesaler_mapping")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalogName}.{dbName}.distribution_center_to_wholesaler_mapping"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate a transport cost table for each plant to each ditribution center for each product

# COMMAND ----------

baseline_costs = products_in_wholesalers_table.select("product", "transport_baseline_cost" ).distinct()
display(baseline_costs)

# COMMAND ----------

plants_lst = ["plant_" + str(i) for i in  range(1,n_plants+1)]
plants_df = spark.createDataFrame([(p,) for p in plants_lst], ['plant'])
display(plants_df)

# COMMAND ----------

tmp_map_distribution_center_to_wholesaler = spark.read.table(f"{catalogName}.{dbName}.distribution_center_to_wholesaler_mapping")
distribution_center_df = (spark.read.table(f"{catalogName}.{dbName}.product_demand_historical").
                          select("product","wholesaler").
                          join(tmp_map_distribution_center_to_wholesaler, ["wholesaler"],  how="inner").
                          select("product","distribution_center").
                          distinct()
                         )
distribution_center_df = distribution_center_df.join(baseline_costs, ["product"],  how="inner")
display(distribution_center_df)

# COMMAND ----------

plants_to_distribution_centers = plants_df.crossJoin(distribution_center_df)
display(plants_to_distribution_centers)

# COMMAND ----------

# For testing
#pdf = plants_to_distribution_centers.filter( (f.col("plant") == "plant_1") & (f.col("product") == "drilling machine_1")).toPandas()

# COMMAND ----------

def cost_generator(pdf: pd.DataFrame) -> pd.DataFrame:
  pdf_return = pdf.assign(transprot_cost_variation =  np.random.uniform(low=1.1, high=2.0, size=len(pdf)))
  pdf_return["transport_cost"] = pdf_return["transport_baseline_cost"] * pdf_return["transprot_cost_variation"]
  pdf_return = pdf_return[[ "plant", "product", "distribution_center", "transport_cost"]]
  return pdf_return

# COMMAND ----------

cost_schema = StructType(
  [
    StructField('plant', StringType()),
    StructField('product', StringType()),
    StructField('distribution_center', StringType()),
    StructField('transport_cost', FloatType())
  ]
)

# COMMAND ----------

spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")
n_tasks = plants_to_distribution_centers.select("plant", "product").distinct().count()

transport_cost_table = (
  plants_to_distribution_centers
  .repartition(n_tasks, "plant", "product")
  .groupBy("plant", "product")
  .applyInPandas(cost_generator, schema=cost_schema)
)

display(transport_cost_table)

# COMMAND ----------

transport_cost_table = (transport_cost_table.
                        groupBy("plant", "product").
                        pivot("distribution_center").
                        agg(f.first("transport_cost")).orderBy("product", "plant")
                       )
display(transport_cost_table)

# COMMAND ----------

# MAGIC %md
# MAGIC Save as a Delta table

# COMMAND ----------

# Write the data 
transport_cost_table.write.mode("overwrite").saveAsTable(f"{catalogName}.{dbName}.transport_cost")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {dbName}.transport_cost"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate a maximum supply table for each plant and product

# COMMAND ----------

# Create a list with all plants
all_plants = spark.read.table(f"{dbName}.transport_cost").select("plant").distinct().collect()
all_plants = [row[0] for row in all_plants]

# Create a list with fractions: Sum must be larger than one to fullfill the demands
fractions_lst = [round(random.uniform(0.4, 0.8),1) for x in all_plants[1:]]
fractions_lst.append(max( 0.4,  1 - sum(fractions_lst)))

# Combine to a dictionary
plant_supply_in_percentage_of_demand = {all_plants[i]: fractions_lst[i] for i in range(len(all_plants))}

#Get maximum demand in history and sum up the demand of all distribution centers
map_store_to_dc_tmp = spark.read.table(f"{dbName}.distribution_center_to_wholesaler_mapping")
max_demands_per_dc = (spark.read.table(f"{dbName}.product_demand_historical").
                      groupBy("product", "wholesaler").
                      agg(f.max("demand").alias("demand")).
                      join(map_store_to_dc_tmp, ["wholesaler"], how = "inner"). # This join will not produce duplicates, as one store is assigned to exactly one distribution center
                      groupBy("product").
                      agg(f.sum("demand").alias("demand"))   
                      ) 
# Distribute parts of the demands per product to the plants
for item in plant_supply_in_percentage_of_demand.items():
  max_demands_per_dc = max_demands_per_dc.withColumn(item[0], f.ceil(item[1] * f.col("demand")))

# This table must be saved in Delta later  
plant_supply = max_demands_per_dc.select("product", *all_plants).sort("product")
#display(plant_supply)

# COMMAND ----------

display(spark.read.table(f"{dbName}.distribution_center_to_wholesaler_mapping"))

# COMMAND ----------

display(spark.read.table(f"{dbName}.product_demand_historical"))

# COMMAND ----------

# MAGIC %md
# MAGIC Save as a Delta table

# COMMAND ----------

# Write the data 
plant_supply.write.mode("overwrite").saveAsTable(f"{catalogName}.{dbName}.plant_supply")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {dbName}.plant_supply"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Bill of Materials

# COMMAND ----------

%pip install networkx --quiet

# COMMAND ----------

import string
import networkx as nx
import random
import numpy as np
import os

# COMMAND ----------

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def generate_random_strings(n):
  random.seed(123)
  random_mat_numbers = set()
  while True:
    random_mat_numbers.add(id_generator(size=5))
    if len(random_mat_numbers) >= n:
      break
  return(random_mat_numbers)

# COMMAND ----------

def extend_one_step(node_from_):
  res_ = [  ]
  node_list_to_be_extended_ = [  ]
  # second level
  random_split_number = random.randint(2, 4)
  for i in range(random_split_number):
    node_to = random_mat_numbers.pop()
    node_list_to_be_extended_.append(node_to)
    res_.append((node_to, node_from_))
  return res_, node_list_to_be_extended_

# COMMAND ----------

def extend_one_level(node_list_to_be_extended, level, sku):
  
  
  print(f"""In  'extend_one_level': level={level} and sku = {sku}  """)
  
  if level == 1:
    head_node = random_mat_numbers.pop() 
    node_list_to_be_extended_one_level = [ ]
    node_list_to_be_extended_one_level.append(head_node)
    res_one_level = [ (head_node, sku) ]
  else:
    res_one_level = [ ]
    node_list_to_be_extended_one_level = [ ]
    
    if len(node_list_to_be_extended) > 2:
      node_list_to_be_extended_ = node_list_to_be_extended[ : 3 ]
    else:
      node_list_to_be_extended_ = node_list_to_be_extended

    for node in node_list_to_be_extended_:
      res_one_step = [ ]
      node_list_to_be_extended_one_step = [ ]
      
      res_one_step, node_list_to_be_extended_one_step = extend_one_step(node)    
      res_one_level.extend(res_one_step)
      node_list_to_be_extended_one_level.extend(node_list_to_be_extended_one_step)
  
  return res_one_level, node_list_to_be_extended_one_level

# COMMAND ----------

# Generate a set of material numbers
random_mat_numbers = generate_random_strings(1000000)

# COMMAND ----------

# Create a list of all SKU's
demand_df = spark.read.table(f"{catalogName}.{dbName}.product_demand_historical")
all_skus = [row['product'] for row in demand_df.select('product').distinct().collect()]
# display(all_skus)

# COMMAND ----------

# Generaze edges
depth = 3
edge_list = [ ]

for sku in all_skus: 
  new_node_list = [ ]
  for level_ in range(1, (depth + 1)):
    new_edge_list, new_node_list = extend_one_level(new_node_list, level = level_, sku=sku)
    edge_list.extend(new_edge_list)

# COMMAND ----------

# Define the graph 
g=nx.DiGraph()
g.add_edges_from(edge_list)  

# COMMAND ----------

# Assign a quantity for the graph
edge_df = nx.to_pandas_edgelist(g)
edge_df = edge_df.assign(qty = np.where(edge_df.target.str.len() == 10, 1, np.random.randint(1,4, size=edge_df.shape[0])))

# COMMAND ----------

bom = edge_df.rename(columns={"source": "material_in", "target": "material_out"} )
bom

# COMMAND ----------

bom_df = spark.createDataFrame(bom) 

# COMMAND ----------

# Write the data 
bom_df.write \
.mode("overwrite") \
.saveAsTable(f"{catalogName}.{dbName}.bom")

# COMMAND ----------

from pyspark.sql.functions import rand

product_names = spark.read.table(f"{catalogName}.{dbName}.product_demand_historical").select("product").distinct()
list_prices = product_names.withColumn("price", rand()*15 + 5)
list_prices.write.saveAsTable(f"{catalogName}.{dbName}.list_prices")

# COMMAND ----------

print("Ending ./_resources/01-data-generator")

# COMMAND ----------

# MAGIC %md
# MAGIC Set Mlflow experiment 

# COMMAND ----------

