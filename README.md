# incubyte_final
Python ETL to seperate records from each country to seperate tables

# dataflow
Source: source.txt file -- Has records with '|' seperated columns <br />
Staging: SQLite database with a table for storing all the values from source flat file
Target: SQLite database with seperate tables for each country records <br/><br/>

# Transformation process
1)Reading the source file and store it as Pandas Dataframe <br />
2)It is then stored in a staging table of database <br />
3)Cleaning the data <br />
4)Creating new table for each country in record <br />
5)Load the records to specific country table <br />

# Batch processing
We are storing all the input from flat file to staging table and after completing it start processing the data of whole group

# Exception handling
Try and Except blocks are used to catch errors during file handling
