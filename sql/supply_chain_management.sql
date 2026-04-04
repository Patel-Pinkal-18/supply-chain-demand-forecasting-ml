CREATE DATABASE supply_chain_ml;
USE supply_chain_ml;
CREATE TABLE supply_chain_data (
    Product_type VARCHAR(100),
    SKU VARCHAR(50),
    Price DECIMAL(10,2),
    Availability INT,
    Number_of_products_sold INT,
    Revenue_generated DECIMAL(12,2),
    Customer_demographics VARCHAR(100),
    Stock_levels INT,
    Lead_times INT,
    Order_quantities INT,
    Shipping_times INT,
    Shipping_carriers VARCHAR(100),
    Shipping_costs DECIMAL(10,2),
    Supplier_name VARCHAR(100),
    Location VARCHAR(100),
    Lead_time INT,
    Production_volumes INT,
    Manufacturing_lead_time INT,
    Manufacturing_costs DECIMAL(10,2),
    Inspection_results VARCHAR(50),
    Defect_rates DECIMAL(10,2),
    Transportation_modes VARCHAR(100),
    Routes VARCHAR(100),
    Costs DECIMAL(10,2)
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/supply_chain_data.csv'
INTO TABLE supply_chain_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SELECT * FROM supply_chain_data;
SELECT COUNT(*) AS total_rows
FROM supply_chain_data;
SELECT
    SUM(CASE WHEN Product_type IS NULL THEN 1 ELSE 0 END) AS Product_type_nulls,
    SUM(CASE WHEN SKU IS NULL THEN 1 ELSE 0 END) AS SKU_nulls,
    SUM(CASE WHEN Price IS NULL THEN 1 ELSE 0 END) AS Price_nulls,
    SUM(CASE WHEN Availability IS NULL THEN 1 ELSE 0 END) AS Availability_nulls,
    SUM(CASE WHEN Number_of_products_sold IS NULL THEN 1 ELSE 0 END) AS sold_nulls,
    SUM(CASE WHEN Costs IS NULL THEN 1 ELSE 0 END) AS Costs_nulls
FROM supply_chain_data;
CREATE TABLE supply_chain_ml_ready AS
SELECT
    SKU AS ProductID,
    Number_of_products_sold AS HistoricalSales,
    Price,
    Availability,
    Stock_levels,
    Order_quantities,
    Shipping_times,
    Shipping_costs,
    Supplier_name,
    Location,
    Lead_times,
    Lead_time,
    Production_volumes,
    Manufacturing_lead_time,
    Manufacturing_costs,
    Inspection_results,
    Defect_rates,
    Transportation_modes,
    Routes,
    Costs
FROM supply_chain_data;
ALTER TABLE supply_chain_ml_ready
ADD Date DATE;
SET @@SQL_SAFE_UPDATES = 0;
WITH numbered AS (
    SELECT ProductID,
           ROW_NUMBER() OVER (ORDER BY ProductID) AS rn
    FROM supply_chain_ml_ready
)
UPDATE supply_chain_ml_ready t
JOIN numbered n
ON t.ProductID = n.ProductID
SET t.Date = DATE_ADD('2023-01-01', INTERVAL n.rn - 1 DAY)
WHERE t.ProductID IS NOT NULL;
ALTER TABLE supply_chain_ml_ready
ADD Promotion VARCHAR(10);

SET @@SQL_SAFE_UPDATES = 0;

UPDATE supply_chain_ml_ready t
JOIN (
    SELECT 
        AVG(Availability) AS avg_availability,
        AVG(Order_quantities) AS avg_order_quantities
    FROM supply_chain_ml_ready
) a
SET t.Promotion = CASE
    WHEN t.Availability > a.avg_availability
         AND t.Order_quantities > a.avg_order_quantities
    THEN 'Yes'
    ELSE 'No'
END;

ALTER TABLE supply_chain_ml_ready
ADD Weather VARCHAR(20);

SET @@SQL_SAFE_UPDATES = 0;
SET @@SQL_SAFE_UPDATES = 0;

UPDATE supply_chain_ml_ready t
JOIN (
    SELECT AVG(Shipping_times) AS avg_shipping_times
    FROM supply_chain_ml_ready
) a
SET t.Weather = CASE
    WHEN t.Shipping_times > a.avg_shipping_times THEN 'Adverse'
    ELSE 'Normal'
END;

ALTER TABLE supply_chain_ml_ready
ADD EconomicIndicators VARCHAR(30);

UPDATE supply_chain_ml_ready t
JOIN (
    SELECT AVG(Costs) AS avg_costs
    FROM supply_chain_ml_ready
) a
SET t.EconomicIndicators = CASE
    WHEN t.Costs > a.avg_costs THEN 'High_Cost_Environment'
    ELSE 'Stable_Environment'
END;

ALTER TABLE supply_chain_ml_ready
ADD Month_No INT,
ADD Day_Of_Week INT,
ADD Quarter_No INT,
ADD Day_No INT;

UPDATE supply_chain_ml_ready
SET
    Month_No = MONTH(Date),
    Day_Of_Week = DAYOFWEEK(Date),
    Quarter_No = QUARTER(Date),
    Day_No = DAY(Date);
    
ALTER TABLE supply_chain_ml_ready
ADD Inventory_Pressure DECIMAL(12,4),
ADD Cost_per_Unit_Sold DECIMAL(12,4),
ADD Lead_Time_Gap INT;

UPDATE supply_chain_ml_ready
SET
    Inventory_Pressure = Order_quantities * 1.0 / NULLIF(Stock_levels + 1, 0),
    Cost_per_Unit_Sold = Costs * 1.0 / NULLIF(HistoricalSales + 1, 0),
    Lead_Time_Gap = Manufacturing_lead_time - Lead_times;
    
SELECT *
FROM supply_chain_ml_ready
LIMIT 10;

SELECT
    ProductID,
    Date,
    HistoricalSales,
    Price,
    Availability,
    Stock_levels,
    Order_quantities,
    Shipping_times,
    Shipping_costs,
    Supplier_name,
    Location,
    Lead_times,
    Lead_time,
    Production_volumes,
    Manufacturing_lead_time,
    Manufacturing_costs,
    Inspection_results,
    Defect_rates,
    Transportation_modes,
    Routes,
    Costs,
    Promotion,
    Weather,
    EconomicIndicators,
    Month_No,
    Day_Of_Week,
    Quarter_No,
    Day_No,
    Inventory_Pressure,
    Cost_per_Unit_Sold,
    Lead_Time_Gap
FROM supply_chain_ml_ready;

SELECT ProductID, SUM(HistoricalSales) AS total_sales
FROM supply_chain_ml_ready
GROUP BY ProductID
ORDER BY total_sales DESC;

SELECT Supplier_name, AVG(HistoricalSales) AS avg_sales
FROM supply_chain_ml_ready
GROUP BY Supplier_name
ORDER BY avg_sales DESC;

SELECT Transportation_modes, AVG(Shipping_times) AS avg_shipping_time
FROM supply_chain_ml_ready
GROUP BY Transportation_modes
ORDER BY avg_shipping_time;

SELECT Supplier_name, AVG(Defect_rates) AS avg_defect_rate
FROM supply_chain_ml_ready
GROUP BY Supplier_name
ORDER BY avg_defect_rate DESC;

CREATE VIEW vw_supply_chain_ml_dataset AS
SELECT
    ProductID,
    Date,
    HistoricalSales,
    Price,
    Availability,
    Stock_levels,
    Order_quantities,
    Shipping_times,
    Shipping_costs,
    Supplier_name,
    Location,
    Lead_times,
    Lead_time,
    Production_volumes,
    Manufacturing_lead_time,
    Manufacturing_costs,
    Inspection_results,
    Defect_rates,
    Transportation_modes,
    Routes,
    Costs,
    Promotion,
    Weather,
    EconomicIndicators,
    Month_No,
    Day_Of_Week,
    Quarter_No,
    Day_No,
    Inventory_Pressure,
    Cost_per_Unit_Sold,
    Lead_Time_Gap
FROM supply_chain_ml_ready;

SELECT * FROM vw_supply_chain_ml_dataset;