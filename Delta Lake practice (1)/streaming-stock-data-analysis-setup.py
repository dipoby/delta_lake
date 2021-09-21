# Databricks notebook source
# MAGIC %md 
# MAGIC # Streaming Stock Analysis With Delta Lake: Setup
# MAGIC 
# MAGIC ### Setup
# MAGIC To run the Streaming Stock Analysis with Delta Lake notebook, first you will run this notebook to generate the streaming data that will be showcased in the main notebook. When the last cell of this notebook starts inserting rows into the table stockDailyPrices_delta, you can start the main notebook.
# MAGIC 
# MAGIC ### Overview
# MAGIC When analyzing stocks, data comes from various sources at different intervals of time. These sources need to be streamed in, joined together efficiently and then calculations and advanced analytics need to be applied. Sometimes these sources need to be updated based on corrections made to the data. Traditionally, this is all very hard to do.
# MAGIC 
# MAGIC Now with Delta Lake and Structured Streaming, this is all much easier.
# MAGIC 
# MAGIC This demo shows how you can analyze streams of economic data using Delta Lake and Structured Streaming
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/01-steaming-stock-data-using-databricks-delta-3.png" width="1000"/>

# COMMAND ----------

# MAGIC %md ## Download the Source Data 
# MAGIC We will download the generated source data used for streaming stock analysis webinar.

# COMMAND ----------

# MAGIC %sh 
# MAGIC wget -N -P /dbfs/ml/streaming-stock-analysis/parquet/stocksDailyPricesSample/ https://pages.databricks.com/rs/094-YMS-629/images/stocksDailyPricesSample.snappy.parquet
# MAGIC wget -N -P /dbfs/ml/streaming-stock-analysis/parquet/stocksFundamentalsSample/ https://pages.databricks.com/rs/094-YMS-629/images/stocksFundamentalsSample.snappy.parquet

# COMMAND ----------

# MAGIC %md ## Configure Locations
# MAGIC 
# MAGIC Set the source Parquet Data and generated Delta Lake Table file paths.

# COMMAND ----------

# Downloaded source Parquet file
parquetPricePath = "/ml/streaming-stock-analysis/parquet/stocksDailyPricesSample/"
parquetFundPath = "/ml/streaming-stock-analysis/parquet/stocksFundamentalsSample/"

# Location of Delta tables (we will create them below)
deltaPricePath = "/ml/streaming-stock-analysis/delta/stocksDailyPrices"
deltaFundPath = "/ml/streaming-stock-analysis/delta/stocksFundamentals"
deltaPriceFundPath = "/ml/streaming-stock-analysis/delta/stocksDailyPricesWFund"

# COMMAND ----------

# MAGIC %md ## Build the Initial Delta Lake Tables 
# MAGIC 
# MAGIC First, let's create the Fund Delta Lake table using the `dfPriceDelta` DataFrame and then we create the Fundamentals Delta Lake table using the `dfFundDelta` DataFrame.

# COMMAND ----------

# Remove the existing delta price data and create again with no data based on the parquet data
dbutils.fs.rm(deltaPricePath, recurse=True)
dfPriceTable = spark.read.parquet(parquetPricePath).limit(0)
dfPriceTable.write.mode("overwrite").format("delta").save(deltaPricePath)

dfPriceDelta = spark.read.format("delta").load(deltaPricePath)
display(dfPriceDelta)

# COMMAND ----------

# MAGIC %md **Note:** the `dfPriceDelta` Delta Lake table is empty (though with the correct schema) due to the `.limit(0)` statement.

# COMMAND ----------

# Remove the current delta fundamental data and create again based on the parquet data
dbutils.fs.rm(deltaFundPath, recurse=True)
dfFundParquet = spark.read.parquet(parquetFundPath)
dfFundParquet.write.mode("overwrite").format("delta").save(deltaFundPath)

dfFundDelta = spark.read.format("delta").load(deltaFundPath)
display(dfFundDelta)

# COMMAND ----------

# MAGIC %md In this demo, we will join the price and fundamentals data into a PriceFund Delta Lake table.  The following code clears out the table/files if they exist and generates a new empty table.

# COMMAND ----------

# Remove the current delta price and fundamental table and create the delta table based price columns plus eps_basic_net from the fundamental data
dbutils.fs.rm(deltaPriceFundPath, recurse=True)
dfPriceFund = dfPriceTable.join(dfFundParquet.limit(0))
dfPriceFund = dfPriceFund.select(dfPriceTable.ticker, "price_date", "open", "high", "low", "close", "volume", "adj_open", "adj_high", "adj_low", "adj_close", "adj_volume", "eps_basic_net")
dfPriceFund.write.mode("overwrite").format("delta").save(deltaPriceFundPath)

dfPriceFundDelta = spark.read.format("delta").load(deltaPriceFundPath)
display(dfPriceFundDelta)

# COMMAND ----------

# MAGIC %md ## Create Temporary View for our Streaming Price Data
# MAGIC * We will first create the database `stockDeltaStreamng` if it does not already exist
# MAGIC * Then, we will drop the table `stockDailyPrices_delta` if it exists
# MAGIC * Finally, we will create the table `stockDailyPrices_delta` pointing to `deltaPricePath`

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE DATABASE IF NOT EXISTS stockDeltaStreaming;
# MAGIC USE stockDeltaStreaming;
# MAGIC DROP TABLE IF EXISTS stockDailyPrices_delta;

# COMMAND ----------

# Create Delta Lake table
sqlq = "CREATE TABLE stockDailyPrices_delta USING DELTA LOCATION '" + deltaPricePath + "'"
spark.sql(sqlq)

# COMMAND ----------

# MAGIC %md ## Stream the Pricing Data based on Price Date
# MAGIC 
# MAGIC Let's stream the pricing data based on price date by:
# MAGIC * Creating the `stockDailyPrices` DataFrame based on the source Parquet data
# MAGIC * Create `datelist` that contains the distinct `price_date` values 
# MAGIC * Load the `stockDailyPrices_delta` table by loading x number of days at a time (`daysAtOnce`) 

# COMMAND ----------

stockDailyPrices = spark.read.format("parquet").load(parquetPricePath)
stockDailyPrices.createOrReplaceTempView("stockDailyPrices")
display(stockDailyPrices)

# COMMAND ----------

# Generate distinct price_date values into the list `datelist`
df1 = spark.sql("select distinct price_date from stockDailyPrices order by price_date")
datelist = [row.price_date for row in df1.collect()]
len(datelist)

# COMMAND ----------

# Create while loop to loop the distinct set of dates and load the `stockDailyPrices_delta` table
import time
datelist_len = len(datelist) - 1

# Span `daysAtOnce` (e.g. loop through 3-days in a row when daysAtOnce=3)
daysAtOnce = 3

i = 0
while (i < datelist_len):
  # configure `daysAtOnce`-day span
  price_date_min = datelist[i]
  price_date_max = datelist[i+daysAtOnce-1]

  # Generate and execute insert statement
  insert_sql = "insert into stockDailyPrices_delta select f.* from stockDailyPrices f where f.price_date >= '"  + price_date_min.strftime('%Y-%m-%d') + "' and f.price_date <= '" + price_date_max.strftime('%Y-%m-%d') + "'"
  spark.sql(insert_sql)
  
  # Print out to review
  print('stockDailyPrices_delta: inserted new row of data, min: [%s], max: [%s]' % (price_date_min, price_date_max))
  
  # Loop through (and sleep)
  i = i + daysAtOnce
  time.sleep(7)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from stockDailyPrices_delta
