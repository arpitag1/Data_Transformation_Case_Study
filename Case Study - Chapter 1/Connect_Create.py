import snowflake.connector

ctx=snowflake.connector.connect(
    user="<username>",
    password="<password>",
    account="<snowflake account identifier>",
    role='ACCOUNTADMIN',
    session_parameters={
        'TIMEZONE': 'UTC',
    }
)

cs=ctx.cursor()

try:

    cs.execute("CREATE WAREHOUSE IF NOT EXISTS Project_WH")
    cs.execute("USE WAREHOUSE Project_WH")
    cs.execute("CREATE DATABASE IF NOT EXISTS ProjectDB")
    cs.execute("USE DATABASE ProjectDB")
    cs.execute("CREATE SCHEMA IF NOT EXISTS Projectschema")
    cs.execute("USE SCHEMA Projectschema")


    cs.execute("create or replace table store (RowID   Integer, OrderID    String, OrderDate date, ShipDate date, ShipMode string, CustomerID  string, CustomerName    string,    Segment    string,    Country    string,    City    string,    State    string,    PostalCode    integer,    Region    string,    ProductID    string,    Category    string,  SubCategory string,  ProductName  string,  Sales  float, Quantity  integer, Discount  float, Profit float )" )
    cs.execute("CREATE or replace STAGE int_stage")


    cs.execute("CREATE OR REPLACE FILE FORMAT csv_format TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1  ENCODING = 'iso-8859-1' FIELD_OPTIONALLY_ENCLOSED_BY ='\042'")

    cs.execute(f"create or replace table Dim_Customer ( CustomerID  string, CustomerName  string, loadingDate TIMESTAMP,  is_current BOOLEAN,  PRIMARY KEY (CustomerID))")

    cs.execute( "create or replace table Dim_Geo(Row_WID INT PRIMARY KEY IDENTITY(1,1), PostalCode  integer, Country string,  City  string,  State string,  Region string )")
    cs.execute( "create or replace table Dim_Prod(Row_WID INT PRIMARY KEY IDENTITY(1,1), ProductID string, Category string,  SubCategory string,  ProductName  string)")
    cs.execute(  "create or replace table Fact_Sales(RowID Integer, OrderID  String, OrderDate date, ShipDate date, ShipMode string, customerid  string, Segment string, GEO_WID integer,   PROD_WID  string,  Sales float, Quantity  integer, Discount  float, Profit float )")

    print("Run Successfully:")

finally:
    cs.close()