---
title: "DM_Group_30"
output: html_document
date: "2024-03-05"
---
  
library(readr)
library(RSQLite)
library(dplyr)

my_connection <- RSQLite::dbConnect(RSQLite::SQLite(),"database.db")

# Loading data
Customer <- readr::read_csv("data_upload/customer.csv")
Product <- readr::read_csv("data_upload/product.csv")
Supplier <- readr::read_csv("data_upload/supplier.csv")
Warehouse <- readr::read_csv("data_upload/warehouse.csv")

# Create the Category Entity
RSQLite::dbExecute(my_connection, "DROP TABLE IF EXISTS Category")

RSQLite::dbExecute(my_connection, "
  CREATE TABLE Category (
    category_id VARCHAR(5) PRIMARY KEY,
    category_name VARCHAR(50),
    parent_category_id VARCHAR(5),
    parent_category_name VARCHAR(255)
  )
")