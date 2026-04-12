/*
============
Quality Checks
==============
Script Purpose :
  This script performs various quality checks for data consistency, accuracy and standarization across the 'silver' schema. 
  It includes checks for : 
  - Null or duplicate primary keys
  - unwanted spaces in string fields
  - data standarization and consistency
  - invalid data ranges and orders
  - data consistency bewteen related fields

Usage Notes:
  - Run these checks after data loading Silver Layer
  - Investigate and resolve any discrepencies found during the checks
========
*/

