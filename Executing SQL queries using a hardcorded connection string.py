 # Import necessary libraries
import pyodbc
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine
import zipfile
from sqlalchemy.engine.url import URL
import logging
from datetime import datetime


DATABASE_CONNECTION_STRING = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=SKYEVIEW\\SQLEXPRESS01;"
    "Database=AdventureWorks2022;"
    "Trusted_Connection=yes;"
)

# Set up logging
log_filename = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
logging.basicConfig(filename=log_filename, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')


# Also set up logging to print to console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)

# Log the start of the script
logging.info("Script started.")


# Define functions for executing SQL queries and uploading data

def execute_sql(query):
    """
    Execute SQL query using a hardcorded connection string.

    Parameters:
        query (str): SQL query to execute.
        

    Returns:
        None
    """
    cursor = None  # Initialize cursor to None
    conn = None 

    try: 
        # Connect to database
        logging.info("Attempting to connect to the database for executing SQL.")

        conn = pyodbc.connect(DATABASE_CONNECTION_STRING)
        
        # Create cursor
        cursor = conn.cursor()
        
        logging.info(f"Executing query: {query}")
        
        # Execute query
        cursor.execute(query)
        
        # Commit changes
        conn.commit()
        
        logging.info("SQL query executed successfully.")

    except Exception as e:
        logging.error(f"Error executing query: {e}")

        print(f"Error executing query: {e}")

    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        
        logging.info("Database connection closed after executing SQL.")

def upload_data(table, dataframe, upload_type):
    """
    Upload data to specified table in the database.

    Parameters:
        table (str): Name of the table to upload data.
        dataframe (DataFrame): Pandas DataFrame containing data to upload.
        upload_type (str): Method of upload ('replace', 'append', etc.).

    Returns:
        None
    """
    try:
        # Create engine

        logging.info("Attempting to connect to the database for uploading data.")

        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={DATABASE_CONNECTION_STRING}")
        logging.info(f"Uploading data to table: {table}") 

        dataframe.to_sql( table, engine, if_exists=upload_type, index=False, schema="dbo", chunksize=10000 )
        
        logging.info(f"Data uploaded successfully to {table}.")

    except Exception as e:
        logging.error(f"Error uploading data: {e}")

        print(f"Error uploading data: {e}")

def retrieve_data(query):
    """
    Retrieve data from specified server and database using SQL query.

    Parameters:
        query (str): SQL query to retrieve data.

    Returns:
        DataFrame: Pandas DataFrame containing retrieved data.
    """
    try:
        # Connect to database
        logging.info("Attempting to connect to the database for retrieving data.")

        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={DATABASE_CONNECTION_STRING}")

        logging.info(f"Retrieving data with query: {query}")
        
        # Read data using SQL query
        df = pd.read_sql(query, engine)

        logging.info("Data retrieved successfully.")
        
    except Exception as e:
        logging.error(f"Error retrieving data: {e}")

        print(f"Error retrieving data: {e}")

        df = pd.DataFrame()  # Return empty DataFrame in case of error
   
    return df

####################################################################################################################

###EXERCISE 1  (EXECUTING SQL COMMANDS)

####################################################################################################################

# Create a new table called CustomerFeedback
create_table_query = """
CREATE TABLE dbo.CustomerFeedback (
  FeedbackID INT PRIMARY KEY,
  CustomerID INT NOT NULL,
  FeedbackDate DATE NOT NULL,
  Comments VARCHAR(300)
);
  """
 # Execute sql query
try:
    execute_sql(create_table_query)
    logging.info("SQL query executed successfully")
    print("SQL query executed successfully")
except Exception as e:
   logging.error(f"Failed to execute SQL query:{e}")
   print(f"Failed to execute SQL query:{e}")


 # Upload sample data into CustomerFeedback Table
 
data_to_upload = pd.DataFrame({
    'FeedbackID':[11,12,13,14,15],
    'CustomerID':[101,102,103,104,105],
    'FeedbackDate': ["2024-06-08", "2024-06-09", "2024-06-10", "2024-06-11", "2024-06-12"],
    'Comments': ['Great product!', 'Nice product!', 'Product is okay!', 'Nice', 'It is okay',]
})
# Execute(upload) sql query
try:
     upload_data('CustomerFeedback',data_to_upload,'replace')
     logging.info("Data uploaded successfully")
     print("Data uploaded successfully")
except Exception as e:
    logging.error(f"Failed to upload data:{e}")
    print(f"Failed to upload data:{e}")

# Update CustomerFeedback_Table
update_query = """
UPDATE dbo.CustomerFeedback
SET FeedbackDate =  '2024-06-09'
WHERE  CustomerID = 101
"""
# Execute(update) sql query
try:
     execute_sql(update_query)
     logging.info("SQL query updated successfully")
     print("SQL query updated successfully")
except Exception as e:
    logging.error(f"Failed to update SQL query:{e}")
    print(f"Failed to update SQL query:{e}")

# Delete a record from CustomerFeedback_Table
delete_query_customer_feedback = """
DELETE FROM dbo.CustomerFeedback
WHERE  CustomerID = 105
"""
# Execute query
try:
    execute_sql(delete_query_customer_feedback)
    logging.info("SQL query deleted succesfully")
    print("SQL query deleted succesfully")
except Exception as e:
      logging.error(f"Failed to delete SQL query:{e}")
      print(f"Failed to delete SQL query:{e}")
  

####################################################################################################################

###EXERCISE 2 (UPLOADING DATA)

####################################################################################################################

 # Unzipping_the_zipped_csv_file
try:

    logging.info("Attempting to unzip file")
    print("Attempting to unzip file")
    with zipfile.ZipFile('C:\\Users\\Onyi\\Downloads\\7817_1.csv 2.zip', 'r') as unzip_ref:
        unzip_ref.extractall('extracted_folder')
    
    logging.info("File unzipped successfully.")
    
except Exception as e:
    logging.error(f"Error unzipping file: {e}")
    print(f"Error unzipping file: {e}")


# Read the CSV file into a DataFrame
df = pd.read_csv('extracted_folder/7817_1.csv')

# Define SQL query to create the ProductReviews table

create_table_query = """
    CREATE TABLE dbo.ProductReviews (
        id VARCHAR(50) PRIMARY KEY,
        asins VARCHAR(50),
        brand VARCHAR(50),
        categories VARCHAR(100),
        colors VARCHAR(50),
        dateAdded DATETIME,
        dateUpdated DATETIME,
        dimension VARCHAR(50),
        ean VARCHAR(50),
        [keys] VARCHAR(100),
        manufacturer VARCHAR(50),
        manufacturerNumber VARCHAR(50),
        name VARCHAR(100),
        prices VARCHAR(500),  
        [reviews.date] DATETIME,
        [reviews.doRecommend] INT,
        [reviews.numHelpful] INT,
        [reviews.rating] INT,
        [reviews.sourceURLs] VARCHAR(MAX),
        [reviews.text] VARCHAR(MAX), 
        [reviews.title] VARCHAR(100),
        [reviews.userCity] VARCHAR(50),
        [reviews.userProvince] VARCHAR(50),
        [reviews.username] VARCHAR(50),
        sizes VARCHAR(50),
        upc VARCHAR(50),
        weight VARCHAR(50)
    )
    """

   # Execute sql query
try:
    execute_sql(create_table_query)
    logging.info("SQL query executed successfully")
    print("SQL query executed successfully")
except Exception as e:
   logging.error(f"Failed to execute SQL query:{e}")
   print(f"Failed to execute SQL query:{e}")


# Upload data into the ProductReviews table
try:
    upload_data('ProductReviews', df, 'replace')
    logging.info("Data inserted successfully.")
except Exception as e:
    logging.error(f"Error inserting data: {e}")
    print(f"Error inserting data: {e}")
  
####################################################################################################################

###EXERCISE 3  (EXTRACTING DATA)

####################################################################################################################
# Data Retrieval from SQL Server Database
retrieval_query = """
SELECT  ProductID, SUM(OrderQty) AS TotalOrder, SUM(LineTotal) AS TotalSales
FROM Sales.SalesOrderDetail
WHERE YEAR(ModifiedDate) = 2014
GROUP BY ProductID
ORDER BY TotalOrder
"""

try:
    # Retrieve Data from Database
    data_frame = retrieve_data(retrieval_query)
    logging.info("Data retrieved successfully.")
    print("Data retrieved successfully.")
    
    #Display the first few rows
    print(df.head())

    # Export DataFrame to CSV
    data_frame.to_csv('LastYearSales.csv', index=False)
    logging.info("Data exported to CSV successfully.")
    print("Data exported to CSV successfully.")
    
except Exception as e:
    logging.error(f"Error retrieving or exporting data: {e}")
    print(f"Error retrieving or exporting data: {e}")
