/*
=============
Quality Checks
=============
Script Purpose: 
  This script performs quality checks to validate the integrity, consistency, and accuracy of the gold layer. 
  These checks ensure : 
  - Uniqueness of surrogate keys in dimension tables.
  - Referential integirty between fact and dimension tables.
  - Validation of relationships in the data model for analytical purposes.

Usage Notes: 
  - Run these checks after data loading silver layer.
  - Investigate and resolve any discrepencies found during the checks
==============
*/

--- Gold Layer Quality Control - Foreign Key Integrity (Dimensions)
SELECT * 
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_customer AS dc
ON dc.customer_key = fs.customer_key
WHERE dc.customer_key IS NULL -- to identify no matching row
