# Big Data Engineering Project on Azure
## Project Overview

In this Big Data Engineering project I leverages Azure, Apache Airflow, Apache Spark, DevOps, and Power BI to ingest data from two sources into an Azure Data Lake. Post-ingestion, data transformation and machine learning processes will be performed, with results stored back in the Data Lake. Finally, Azure Synapse and Power BI will be used to generate and visualize reports based on the processed data.Additionally, Azure Key Vault will securely store sensitive information for Databricks mount notebooks.

![Project](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/47e6a869-379b-4201-aecb-2fa1baa7aba1)
## Data Overview
### Data Background
The dataset originates from StackOverflow, capturing daily posts, post types, and user information. 
The dataset's dates have been updated to reflect current dates, ensuring the posts data corresponds to today's date.
![c60ad6c5-1b2a-4d86-9c12-ea9af55064bd](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/e1cefece-5434-4a3e-a82e-b4094180de07)

#### Dataset Schema
The dataset consists of three tables stored in two locations:

RDS: The Users and PostTypes tables are stored in an AWS RDS Postgres database, updated weekly using SCD type 1 (overwrite old records).
Azure Storage Blob: Daily Posts data files in Parquet format.
![posts_files](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/55b8b42b-f1bf-48b3-9171-48fc21cba414)
## Business Requirements
##### Data Lake 
- Create an Azure Data Lake with a hierarchical namespace.
- Use Azure Data Factory (ADF) for data ingestion and processing.
- Ingest Users and PostTypes tables from AWS RDS Postgres to the Data Lake weekly.
- Ingest daily Posts data from Azure Blob container into the Data Lake.
#### Machine Learning Process 
- Create a Databricks notebook to process and clean data, run a machine learning model using Apache Spark, and store results in the Data Lake.
- This machine learning model is to read the posts' text in the Posts files, classify what topic each post is about, and use Spark to output a file listing all the topics for today, ordered by their frequency.
- Use Azure Key Vault to securely store credentials and other sensitive information for Databricks mount notebooks.
####  Chart and Visualization 
- Create a chart in Azure Synapse based on the machine learning output to display the top 10 topics of the day.
- Use Power BI to visualize the data and provide interactive reports and dashboards.
## Project Infrastructure
#### Azure Components
- Azure Data Lake: Store ingested data, machine learning input/output, and processed data.
- Azure Data Factory: Orchestrate data ingestion, transformation, and machine learning processes.
- Azure Synapse: Analyze data and generate BI reports.
- Apache Airflow: Manage and schedule data workflows.
- Apache Spark: Perform data transformation and run machine learning models.
- Power BI: Create interactive visualizations and dashboards.
- Azure Key Vault: Securely store secrets, keys, and certificates.
#### DevOps Integration
Implement DevOps practices to collaborate in Azure Data Factory (ADF).
## Data Ingestion

- Connect to AWS RDS:
Use ADF and Airflow to connect to 
transfer Users and PostTypes tables to the Data Lake (weekly).
![Screenshot 2024-05-16 145216](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/fb9a3971-b573-4e26-8353-c3755a8bacfe)
- Connect to Azure Blob Storage:
Use ADF to copy daily Posts data files from the Azure Blob container to the Data Lake.
Move copied Posts data files to an Archive folder in the Data Lake and delete them from the destination folder to prepare for new data.
![Screenshot 2024-05-16 150018](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/d691499a-1c6c-41b9-ba3a-181f78688e96)
![Screenshot 2024-05-09 162509](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/b474d1a5-8485-4bea-882e-3a53b4a272b6)

- Use Apache Airflow to manage and monitor this pipeline in ADF.
#### Default Arguments
```bash
  default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "azure_data_factory_conn_id": "azure_data_factory",
    "factory_name": "capstone-projectII-adf",
    "resource_group_name": "capstone-projects",
}
```
#### Python Function to Choose Task
```bash
 def choose_task(execution_date, **kwargs):
    today = execution_date.weekday()
    if today == 0:  # Monday
        return 'run_copyOnceWeek'
    else:
        return 'run_copyPostsEveryday'
}
```
#### Task Failure Alert Function
```bash
def task_failure_alert(context):
    """
    Send custom alert email on task failure.
    """
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    task_instance = context['task_instance']

    subject = f"Airflow alert: {dag_id} - {task_id} failed"
    html_content = (
        f"Task <b>{task_id}</b> failed on <b>{execution_date}</b>.<br>"
        f"Log: <pre>{task_instance.log.read()}</pre>"
    )

    send_email(
        to=['##@mail.com'],  
        subject=subject,
        html_content=html_content
    )

```
#### Define the DAG
```bash
with DAG(
    dag_id="capstone-projectII-adf",
    start_date=datetime(2024, 5, 20),
    schedule_interval=timedelta(days=1),  # Daily interval
    catchup=False,
    default_args=default_args,
    default_view="graph",
) as dag:
```
#### DAG Tasks
```bash
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")
    
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=choose_task,
        provide_context=True,  # Provide execution context
    )
    
    run_copyPostsEveryday = AzureDataFactoryRunPipelineOperator(
        task_id="run_copyPostsEveryday", 
        pipeline_name="copyPostsEveryday"
    )
    
    run_copyOnceWeek = AzureDataFactoryRunPipelineOperator(
        task_id="run_copyOnceWeek", 
        pipeline_name="copyOnceWeek"
    )

    # Set up email alerts on task failure
    dag.on_failure_callback = task_failure_alert

    begin >> branching
    branching >> run_copyOnceWeek >> end
    branching >> run_copyPostsEveryday >> end
```
![Screenshot 2024-05-28 233450](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/a53433c4-2186-4fe5-a525-80c01bd55367)
## Data Transformation

- Perform data transformation in an ADF pipeline.
- Use a Databricks notebook with Apache Spark for data cleaning, transformation, and running the machine learning model.
#### Creating the `Posts` dataframe
```
file_location = "/mnt/Posts/*"

posts = spark.read \
  .parquet(file_location)
```
#### Creating the `posttypes` dataframe
```
# Creating the schema for posttypes table
from pyspark.sql.types import *

PT_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("Type", StringType(), True)
])
# Creating the posttypes dataframe by reading data from the specified text file location

# Specifying the file location
file_location = "/mnt/PostTypes.txt"

# Reading the data from the text file into a DataFrame, specifying options for header, separator, schema, and file location
postType = (spark.read
  .option("header", "true")
  .option("sep", ",")
  .schema(PT_schema)  # Assuming PT_schema is defined elsewhere
  .csv(file_location))

# Displaying the postType DataFrame if required
# display(postType)
```
#### Creating the `users` dataframe
```
# Creating the schema for the users table
from pyspark.sql.types import *

users_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("Age", IntegerType(), True),
    StructField("CreationDate", DateType(), True),
    StructField("DisplayName", StringType(), True),
    StructField("DownVotes", IntegerType(), True),
    StructField("EmailHash", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Reputation", IntegerType(), True),
    StructField("UpVotes", IntegerType(), True),
    StructField("Views", IntegerType(), True),
    StructField("WebsiteUrl", StringType(), True),
    StructField("AccountId", IntegerType(), True)
])

# Creating the users dataframe
file_location = "/mnt/users.csv"

users = (spark.read
  .option("header", "true")
  .option("sep", ",")
  .schema(users_schema)
  .csv(file_location))

display(users)
```
#### Join tables and filter data
```
#we only use Posts and posttypes to train the model. so let's join them iwith the posttype id. 

df = posts.join(postType, posts.PostTypeId == postType.id)
display(df)
```
#### Filter the data
```
# Filter the dataframe to only include questions
df = df.filter(col("Type") == "Question")
display(df)
```
```
# Formatting the 'Body' and `Tag` columns for machine learning training
df = (df.withColumn('Body', regexp_replace(df.Body, r'<.*?>', '')) # Transforming HTML code to strings
      .withColumn("Tags", split(trim(translate(col("Tags"), "<>", " ")), " ")) # Making a list of the tags
)

display(df)
```
```
# Filter the dataframe to only include questions
df = df.filter(col("Type") == "Question")
display(df)
```
#### Create a checkpoint to save the dataframe to file only contain the `Body` and `Tag` we need. 
```
# Selecting the "Body" column and renaming it as "text", and selecting the "Tags" column
df = df.select(col("Body").alias("text"), col("Tags"))

# Producing the tags as individual tags instead of an array
# This is duplicating the posts for each possible tag

# Selecting the "text" column and exploding the array column "Tags" into individual rows, 
# aliasing the exploded column as "tags"
df = df.select("text", explode("Tags").alias("tags"))

# Displaying the DataFrame with individual tags if required
display(df)

# saving the file as a checkpoint (in case the cluster gets terminated)

df.write.parquet("/tmp/project.df.parquet")
```
#### prepare data from machine learning
Text Cleaning Preprocessing:

`pyspark.sql.functions.regexp_replace` is used to process the text

1. Remove URLs such as `http://stackoverflow.com`
2. Remove special characters
3. Substituting multiple spaces with single space
4. Lowercase all text
5. Trim the leading/trailing whitespaces
```
# Preprocessing the data:
# 1. Removing URLs from the 'text' column
# 2. Replacing non-alphabetic characters with spaces
# 3. Replacing multiple spaces with a single space
# 4. Converting text to lowercase
# 5. Trimming leading and trailing spaces
cleaned = df.withColumn('text', regexp_replace('text', r"http\S+", "")) \
            .withColumn('text', regexp_replace('text', r"[^a-zA-z]", " ")) \
            .withColumn('text', regexp_replace('text', r"\s+", " ")) \
            .withColumn('text', lower('text')) \
            .withColumn('text', trim('text'))

# Displaying the cleaned DataFrame if required
# display(cleaned)
```
#### Machine Learning Model Training
Feature Transformer:
1. Tokenizer
```

# Importing Tokenizer from PySpark's ML package
from pyspark.ml.feature import Tokenizer

# Creating a Tokenizer object with input column 'text' and output column 'tokens'
tokenizer = Tokenizer(inputCol="text", outputCol="tokens")

# Transforming the cleaned DataFrame by tokenizing the 'text' column into words
tokenized = tokenizer.transform(cleaned)

# Displaying the tokenized DataFrame if required
display(tokenized)
```
2. Stopword Removal
```
# Importing StopWordsRemover from PySpark's ML package
from pyspark.ml.feature import StopWordsRemover

# Creating a StopWordsRemover object with input column 'tokens' and output column 'filtered'
stopword_remover = StopWordsRemover(inputCol="tokens", outputCol="filtered")

# Transforming the tokenized dataset by removing stop words using the StopWordsRemover
stopword = stopword_remover.transform(tokenized)

# Displaying the dataset after stop words removal if required
display(stopword)
```
3. CountVectorizer (TF - Term Frequency)
```
# Importing HashingTF and IDF from PySpark's ML package
from pyspark.ml.feature import HashingTF, IDF

# Creating an IDF (Inverse Document Frequency) estimator with input column 'cv', output column 'features',
# and minDocFreq set to 5 to remove sparse terms
idf = IDF(inputCol='cv', outputCol="features", minDocFreq=5)

# Fitting the IDF model to the text_cv dataset to compute the IDF weights
idf_model = idf.fit(text_cv)

# Transforming the text_cv dataset using the trained IDF model to produce the TF-IDF weighted vectors
text_idf = idf_model.transform(text_cv)

# Displaying the transformed dataset if required
display(text_idf)
```
4. TF-IDF Vectorization
```
# Importing HashingTF and IDF from PySpark's ML package
from pyspark.ml.feature import HashingTF, IDF

# Creating an IDF (Inverse Document Frequency) estimator with input column 'cv', output column 'features',
# and minDocFreq set to 5 to remove sparse terms
idf = IDF(inputCol='cv', outputCol="features", minDocFreq=5)

# Fitting the IDF model to the text_cv dataset to compute the IDF weights
idf_model = idf.fit(text_cv)

# Transforming the text_cv dataset using the trained IDF model to produce the TF-IDF weighted vectors
text_idf = idf_model.transform(text_cv)

# Displaying the transformed dataset if required
display(text_idf)
```
5. Label Encoding
```
# Importing StringIndexer from PySpark's ML package
from pyspark.ml.feature import StringIndexer

# Creating a StringIndexer object to encode the 'tags' column into numerical labels, with the output column named 'label'
label_encoder = StringIndexer(inputCol="tags", outputCol="label")

# Fitting the StringIndexer model to the text_idf dataset to learn the mapping from tags to numerical labels
le_model = label_encoder.fit(text_idf)

# Transforming the text_idf dataset by applying the trained StringIndexer model to create the 'label' column
final = le_model.transform(text_idf)

# Displaying the final dataset if required
display(final)
```
Model Training
```
# Importing LogisticRegression from PySpark's ML package
from pyspark.ml.classification import LogisticRegression

# Creating a LogisticRegression classifier with a maximum of 100 iterations
lr = LogisticRegression(maxIter=100)

# Fitting the LogisticRegression model to the final dataset
lr_model = lr.fit(final)

# Generating predictions using the trained LogisticRegression model
predictions = lr_model.transform(final)

# Displaying the predictions if required
display(predictions)
```
Model Evalution
```
# Importing MulticlassClassificationEvaluator from PySpark's ML package
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Creating an evaluator object for multiclass classification
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")

# Evaluating the predictions using the evaluator to compute ROC-AUC
roc_auc = evaluator.evaluate(predictions)

# Calculating accuracy by counting the correctly predicted labels
accuracy = predictions.filter(predictions.label == predictions.prediction).count() / float(predictions.count())

# Printing the accuracy score with 4 decimal places
print("Accuracy Score: {0:.4f}".format(accuracy))

# Printing the ROC-AUC score with 4 decimal places
print("ROC-AUC: {0:.4f}".format(roc_auc))
```
Create a Pipeline
```
# Importing all the libraries
from pyspark.sql.functions import split, translate, trim, explode, regexp_replace, col, lower
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Preparing the data
# Step 1: Creating the joined table
df = posts.join(postType, posts.PostTypeId == postType.id)
# Step 2: Selecting only Question posts
df = df.filter(col("Type") == "Question")
# Step 3: Formatting the raw data
df = (df.withColumn('Body', regexp_replace(df.Body, r'<.*?>', ''))
      .withColumn("Tags", split(trim(translate(col("Tags"), "<>", " ")), " "))
)
# Step 4: Selecting the columns
df = df.select(col("Body").alias("text"), col("Tags"))
# Step 5: Getting the tags
df = df.select("text", explode("Tags").alias("tags"))
# Step 6: Clean the text
cleaned = df.withColumn('text', regexp_replace('text', r"http\S+", "")) \
                    .withColumn('text', regexp_replace('text', r"[^a-zA-z]", " ")) \
                    .withColumn('text', regexp_replace('text', r"\s+", " ")) \
                    .withColumn('text', lower('text')) \
                    .withColumn('text', trim('text')) 

# Machine Learning
# Step 1: Train Test Split
train, test = cleaned.randomSplit([0.9, 0.1], seed=20200819)
# Step 2: Initializing the transfomers
tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
stopword_remover = StopWordsRemover(inputCol="tokens", outputCol="filtered")
cv = CountVectorizer(vocabSize=2**16, inputCol="filtered", outputCol='cv')
idf = IDF(inputCol='cv', outputCol="features", minDocFreq=5)
label_encoder = StringIndexer(inputCol = "tags", outputCol = "label")
lr = LogisticRegression(maxIter=100)
# Step 3: Creating the pipeline
pipeline = Pipeline(stages=[tokenizer, stopword_remover, cv, idf, label_encoder, lr])
# Step 4: Fitting and transforming (predicting) using the pipeline
pipeline_model = pipeline.fit(train)
predictions = pipeline_model.transform(test)
```



- Store the machine learning output in the Data Lake for further analysis.
Save the Model file to Azure storage
```
# Saving model object to the /mnt/ directory. .
pipeline_model.save('/mnt//model')

# Save the the String Indexer to decode the encoding. We use it in the Sentiment Analysis.
le_model.save('/mnt//stringindexer')
```
Prepare a UDF (User Defined Function)
```
# User defined function to make predictions using a saved ML model and StringIndexer
def predictions_udf(df, ml_model, stringindexer):
    from pyspark.sql.functions import col, regexp_replace, lower, trim
    from pyspark.ml import PipelineModel

    # Filter out rows with empty body text
    df = df.filter("Body is not null")
    
    # Renaming columns to match the model's input requirements
    df = df.select(col("Body").alias("text"), col("Tags"))
    
    # Preprocessing the feature column
    cleaned = df.withColumn('text', regexp_replace('text', r"http\S+", "")) \
                    .withColumn('text', regexp_replace('text', r"[^a-zA-z]", " ")) \
                    .withColumn('text', regexp_replace('text', r"\s+", " ")) \
                    .withColumn('text', lower('text')) \
                    .withColumn('text', trim('text')) 

    # Loading the saved pipeline model
    model = PipelineModel.load(ml_model)

    # Making predictions
    prediction = model.transform(df)

    # Selecting important columns
    predicted = prediction.select(col('text'), col('Tags'), col('prediction'))

    # Decoding the indexer
    from pyspark.ml.feature import StringIndexerModel, IndexToString

    # Loading the StringIndexer model
    indexer = StringIndexerModel.load(stringindexer)

    # Initializing the IndexToString converter
    i2s = IndexToString(inputCol='prediction', outputCol='decoded', labels=indexer.labels)
    converted = i2s.transform(predicted)

    # Returning the DataFrame with important columns
    return converted
```
Load Posts files and ML model
```
# Reading the posts data from parquet files located in the specified directory
posts = spark.read.parquet("/mnt/deBDProject/landing/posts/*")

# Specifying the path to the directory where the ML model is stored
ml_model = "/mnt/deBDProject/model"

# Specifying the path to the directory where the StringIndexer model is stored
stringindexer = "/mnt/deBDProject/stringindexer"
```
Run model to do `Sentiment Analysis`
```
# change the column name 
topics = result.withColumnRenamed('decoded', 'topic').select('topic')

# Aggregate the topics and calculate the total qty of each topic
topic_qty = topics.groupBy(col("topic")).agg(count('topic').alias('qty')).orderBy(desc('qty'))
topic_qty.show()
```
Save the result file to the `BI` folder
```
#def crt_sgl_file(result_path):
        # Write the result DataFrame to a folder containing several files
        path = "/mnt/deBDProject/BI/ml_result"
        topic_qty.write.option("delimiter", ",").option("header", "true").mode("overwrite").csv(path)

        # List the files in the folder and find the CSV file
        filenames = dbutils.fs.ls(path)
        name = ''
        for filename in filenames:
            if filename.name.endswith('csv'):
                org_name = filename.name

        # Copy the CSV file to the specified result path
        dbutils.fs.cp(path + '/'+ org_name, result_path)

        # Delete the folder after copying the file
        dbutils.fs.rm(path, True)

        # Print a message indicating that the single file has been created
        print('Single file created')
```
- Securely store Databricks mount notebook credentials in Azure Key Vault.
![Screenshot 2024-05-16 145343](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/fc4b63af-a0f3-473c-9f85-723adf924acd)
##  Data Visualization

- Use Azure Synapse to connect to the Data Lake.
- Generate a chart displaying the top 10 topics of the day based on the machine learning model output.
```bash
SELECT
    TOP 5 *
FROM
    OPENROWSET(
        BULK 'https://capstoneprojectstore.dfs.core.windows.net/capstone-project-containers/BI/ml_result.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]
```
![SQL script 1](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/f5d61258-072d-408f-8ee9-4a785f7d6690)
- Utilize Power BI to create interactive reports and dashboards for deeper insights and visualization of the data.
![5924876135106199796_121](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/98fa64a2-28d0-4dd8-8d82-904e58d95543)
