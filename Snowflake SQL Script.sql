-- Define Warehouse and Role
Use Warehouse   COMPUTE_WH;
Use Role        ACCOUNTADMIN;

-- Create Database and Schema
CREATE DATABASE if not exists data_analyse_pei;
USE DATABASE data_analyse_pei;

CREATE Schema if not exists RAW_DATA;
CREATE Schema if not exists TRANFORM_DATA;
USE SCHEMA RAW_DATA;

-- Create Internal stage for input files
CREATE STAGE if not exists Input_stage;

-- Data Insert in Order Table
CREATE or Replace TABLE Order_Raw
(Order_ID    INT,
Item        STRING,
Amount      NUMBER(10,2),
Customer_ID  INT);

SHOW STAGES IN SCHEMA DATA_ANALYSE_PEI.RAW_DATA;
COPY INTO Order_Raw
FROM @INPUT_STAGE/Order.csv
FILE_FORMAT = (TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1);

-- Data Insert in Customer Table
CREATE or Replace TABLE Customer_Raw
(Customer_ID    INT,
First           STRING,
Last            STRING,
AGE             INT,
Country         STRING);

COPY INTO Customer_Raw
FROM @INPUT_STAGE/Customer.csv
FILE_FORMAT = (TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1);

-- Data Insert in Shipping Table
List @INPUT_STAGE/Shipping.json;

CREATE OR REPLACE TABLE Json_Shipping_Raw (data VARIANT);

COPY INTO Json_Shipping_Raw
FROM @INPUT_STAGE/Shipping.json
FILE_FORMAT = (TYPE = 'JSON');

select * from json_shipping_raw;

CREATE OR REPLACE TABLE Shipping_Raw (
  Shipping_ID INT,
  Status STRING,
  Customer_ID INT
);

INSERT INTO Shipping_Raw (Shipping_ID, Status, Customer_ID)
SELECT
  shipment.value:Shipping_ID::INT AS Shipping_ID,
  shipment.value:Status::STRING AS Status,
  shipment.value:Customer_ID::INT AS Customer_ID
FROM JSON_SHIPPING_RAW,
LATERAL FLATTEN(input => data) AS shipment;

select * from Shipping_Raw;

-- Check Accuracy
-- To check no negative order amounts
SELECT  *
FROM    ORDER_RAW
WHERE   AMOUNT < 0;  

-- To check special characters in names
SELECT *
FROM CUSTOMER_RAW
Where FIRST LIKE '%!%' OR
      FIRST LIKE '%@%' OR
      FIRST LIKE '%#%' OR
      FIRST LIKE '%$%' OR
      FIRST LIKE '%&%' OR
      FIRST LIKE '%0%' OR
      LAST LIKE '%!%' OR
      LAST LIKE '%@%' OR
      LAST LIKE '%#%' OR
      LAST LIKE '%$%' OR
      LAST LIKE '%&%' OR
      LAST LIKE '%0%' ;
      
-- Count of duplicate customers in shipping table
SELECT 
    Customer_ID,
    COUNT(*) AS occurrence_count
FROM RAW_DATA.SHIPPING_RAW
GROUP BY Customer_ID
HAVING COUNT(*) > 1;

-- Check Completeness
SELECT  *
FROM    ORDER_RAW
WHERE   CUSTOMER_ID IS NULL;  -- To check No missing customer_id

SELECT *
FROM SHIPPING_RAW
WHERE Customer_ID IS NULL;   -- To check No missing customer_id

-- Count of NULLs per table
SELECT 
    COUNT(*) AS total_orders,
    COUNT(Customer_ID) AS non_null_customer_id,
    COUNT(Order_ID) AS non_null_order_id,
    COUNT(Amount) AS non_null_amount
FROM ORDER_RAW;

SELECT 
    COUNT(*) AS total_customers,
    COUNT(Customer_ID) AS non_null_customer_id,
    COUNT(Age) AS non_null_age,
    COUNT(Country) AS non_null_country,
FROM Customer_RAW;

SELECT 
    COUNT(*) AS total_shipping,
    COUNT(Status) AS non_null_Status,
    COUNT(CUSTOMER_ID) AS non_null_customer_id,
FROM Shipping_RAW;

-- Check Reliability
 -- To check orders with non-existent customers
SELECT o.*
FROM order_raw o
LEFT JOIN customer_raw c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;           

 -- To check shipping with non-existent customers
SELECT s.*
FROM SHIPPING_RAW s
LEFT JOIN CUSTOMER_RAW c ON s.Customer_ID = c.Customer_ID
WHERE c.Customer_ID IS NULL;

---Tranformation Logic
CREATE Schema if not exists TRANFORM_DATA;
USE SCHEMA TRANFORM_DATA;

-- Create and Validate Customer Dimension Table
CREATE OR REPLACE TABLE Dim_Customer AS
SELECT
    Customer_ID,
    INITCAP(
        REPLACE(
            REPLACE(
                REPLACE(First, '!', 'i'),
            '0', 'O'),
        '@', 'a')
    ) AS First_Name,
    
    INITCAP(
        REPLACE(
            REPLACE(
                REPLACE(Last, '!', 'i'),
            '0', 'O'),
        '@', 'a')
    ) AS Last_Name,
    Age,
    Country
FROM RAW_DATA.CUSTOMER_RAW;

-- To check special characters in names
SELECT *
FROM DIM_CUSTOMER
Where FIRST_NAME LIKE '%!%' OR
      FIRST_NAME LIKE '%@%' OR
      FIRST_NAME LIKE '%#%' OR
      FIRST_NAME LIKE '%$%' OR
      FIRST_NAME LIKE '%&%' OR
      FIRST_NAME LIKE '%0%' OR
      LAST_NAME LIKE '%!%' OR
      LAST_NAME LIKE '%@%' OR
      LAST_NAME LIKE '%#%' OR
      LAST_NAME LIKE '%$%' OR
      LAST_NAME LIKE '%&%' OR
      LAST_NAME LIKE '%0%' ;

-- Create and Validate Shipping Dimension Table

CREATE OR REPLACE TABLE DIM_SHIPPING AS
SELECT
    Customer_ID,
    FIRST_VALUE(Shipping_ID) OVER (PARTITION BY Customer_ID ORDER BY Shipping_ID DESC) AS Latest_Shipping_ID,
    FIRST_VALUE(Status) OVER (PARTITION BY Customer_ID ORDER BY Shipping_ID DESC) AS Latest_Shipping_Status
FROM RAW_DATA.SHIPPING_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY Customer_ID ORDER BY Shipping_ID DESC) = 1;

Select * from dim_shipping;

CREATE OR REPLACE TABLE FACT_ORDER AS
SELECT
    o.Order_ID,
    o.Customer_ID,
    o.Item,
    o.Amount,
    COALESCE(s.Latest_Shipping_Status, 'Yet to ship') AS Latest_Shipping_Status
FROM RAW_DATA.ORDER_RAW o
LEFT JOIN DIM_SHIPPING s
    ON o.Customer_ID = s.Customer_ID
WHERE o.Customer_ID IS NOT NULL
  AND o.Amount >= 0
  AND o.Order_ID IS NOT NULL;

Select * from fact_order;
Select * from dim_shipping;
Select * from dim_customer;

--Business Reporting Requirements
-- the total amount spent and the country for the Pending delivery status for each country.

SELECT 
    c.Country,
    SUM(f.Amount) AS Total_Amount_Spent
FROM FACT_ORDER f
JOIN DIM_CUSTOMER c ON f.Customer_ID = c.Customer_ID
WHERE f.Latest_Shipping_Status = 'Pending'
GROUP BY c.Country;

-- the total number of transactions and total amount spent for each customer, along with the product details.
SELECT 
    f.Customer_ID,
    c.First_Name,
    c.Country,
    f.item,
    COUNT(f.Order_ID) AS Total_Transactions,
    SUM(f.Amount) AS Total_Amount
FROM FACT_ORDER f
JOIN DIM_CUSTOMER c ON f.Customer_ID = c.Customer_ID
GROUP BY 
    f.Customer_ID,f.item,c.First_Name, c.Country;

-- the maximum product purchased for each country.
SELECT 
    c.Country,
    f.item,
    COUNT(f.ORDER_ID) AS Total_Quantity
FROM FACT_ORDER f
JOIN DIM_CUSTOMER c ON f.Customer_ID = c.Customer_ID
GROUP BY c.Country, f.ITEM
QUALIFY ROW_NUMBER() OVER (PARTITION BY c.Country ORDER BY COUNT(f.ORDER_ID) DESC) = 1;

-- the most purchased product based on the age category less than 30 and above 30.
SELECT 
    CASE 
        WHEN c.Age < 30 THEN 'Under 30'
        ELSE '30 and above'
    END AS Age_Group,
    f.Item,
    COUNT(f.ORDER_ID) AS Total_Quantity
FROM FACT_ORDER f
JOIN DIM_CUSTOMER c ON f.Customer_ID = c.Customer_ID
GROUP BY Age_Group, f.item
QUALIFY ROW_NUMBER() OVER (PARTITION BY Age_Group ORDER BY COUNT(f.ORDER_ID) DESC) = 1;

-- the country that had minimum transactions and sales amount.
SELECT 
    Country,
    COUNT(f.Order_ID) AS Total_Transactions,
    SUM(f.Amount) AS Total_Sales_Amount
FROM FACT_ORDER f
JOIN DIM_CUSTOMER c ON f.Customer_ID = c.Customer_ID
GROUP BY Country
ORDER BY Total_Transactions ASC, Total_Sales_Amount ASC
LIMIT 1;










