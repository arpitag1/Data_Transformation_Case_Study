import snowflake.connector
import os

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

    os.system('python Connect_Create.py')

    print("Table created:")

    cs.execute("USE WAREHOUSE Project_WH")
    cs.execute("USE DATABASE ProjectDB")
    cs.execute("USE SCHEMA Projectschema")
    cs.execute(f"PUT file://C:/ProjectWork/1SProject/Data/Source/Store* @int_stage")
    cs.execute(f"copy into store from @int_stage file_format = csv_format pattern='Store.*' ON_ERROR=CONTINUE ;")


    cs.execute(f"INSERT INTO Dim_Customer (CustomerID, CustomerName,loadingDate,is_current) select distinct CustomerID, CustomerName,CURRENT_TIMESTAMP(), 'TRUE' from store")
    cs.execute( f"INSERT INTO Dim_Geo (PostalCode , Country , City ,  State , Region ) select distinct PostalCode, Country ,City , State ,Region  from store")
    cs.execute( f"INSERT INTO Dim_Prod (ProductID , Category , SubCategory , ProductName ) select distinct ProductID , Category , SubCategory , ProductName from store")
    cs.execute( f"INSERT INTO Fact_Sales (RowID , OrderID  , OrderDate , ShipDate , ShipMode , customerid  , Segment , geo_wid ,   prod_wid  ,  Sales , Quantity  , Discount  , Profit ) SELECT  t.RowID,   t.orderid,    t.orderdate,    t.shipdate,    t.shipmode,    c.customerid,    t.segment,    g.row_wid,    p.row_wid,    t.sales,    t.quantity,    t.discount,    t.profit FROM  store t JOIN  dim_customer c ON t.customerid = c.customerid JOIN   dim_geo g ON t.country = g.country AND t.city = g.city AND t.state = g.state AND t.postalcode = g.postalcode AND t.region = g.region JOIN  dim_prod p ON t.productid = p.productid AND t.productname = p.productname")

    print("Loading Row Count from the table:")

    cs.execute(f"select count(*) from Dim_Customer")
    print(cs.fetchall())
    cs.execute(f"select count(*) from Dim_Geo")
    print(cs.fetchall())
    cs.execute(f"select count(*) from Dim_Prod")
    print(cs.fetchall())
    cs.execute(f"select count(*) from Fact_Sales")
    print(cs.fetchall())

    os.system('python check_dir.py')

finally:
    cs.close()