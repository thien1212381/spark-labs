# Spark DataFrame vs Dataset

## Dataframe
 - In dataframes, view of data is organized as columns with column name and types info.
 - In addition, we can say data in dataframe is as same as the table in relational database.

## Dataset
 - In Spark, datasets are an extension of dataframes.
 - Datasets are by default a collection of strongly typed JVM objects, unlike dataframes.

## Features
### Data formats

 - Dataframes
   - Organizes the data in the named column.
   - Basically, dataframes can efficiently process unstructured and structured data.
   - Allows the Spark to manage schema.
  
 - Dataset
   - As similar as dataframes, it also efficiently processes unstructured and structured data.
   - Represents data in the form of a collection of row object or JVM objects of row -> Through encoders

### Data Representation

 - Dataframes
    - In dataframe data is organized into named columns. Basically, it is as same as a table in a relational database.

 - Dataset
   - As we know, it is an extension of dataframe API, which provides the functionality of type-safe, object-oriented programming interface of the RDD API.

### Compile-time type safety
 - Dataframe: There is a case if we try to access the column which is not on the table. Then, dataframe APIs does not support compile-time error
 - Dataset: Datasets offers compile-time type safety.

### Data Sources API
 - Dataframe: It allows data processing in different formats, for example, AVRO, CSV, JSON, and storage system HDFS, HIVE tables, MySQL.
 - Dataset: It also supports data from different sources like dataframe.

### Programming Language Support
 - Dataframe: In 4 languages like Java, Python, Scala, and R dataframes are available.
 - Dataset: Only available in Scala and Java.
  
