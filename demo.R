

library(readr)
library(RSQLite)
library(dplyr)
library(readxl)
library(openxlsx)



my_connection <- RSQLite::dbConnect(RSQLite::SQLite(),"database.db")


# Physical Schema
tables <- RSQLite::dbListTables(my_connection)

# drop all tables
for (table in tables) {
  query <- paste("DROP TABLE IF EXISTS", table)
  RSQLite::dbExecute(my_connection, query)
}


RSQLite::dbExecute(my_connection,"
  CREATE TABLE 'Customers' (
    'customer_id' VARCHAR(4) PRIMARY KEY,
    'customer_first_name' VARCHAR(200) NOT NULL,
    'customer_last_name' VARCHAR(200) NOT NULL,
    'customer_gender' VARCHAR(200) NOT NULL,
    'customer_date_of_birth' Date OT NULL,
    'customer_address1' INT NOT NULL,
    'customer_address2' VARCHAR(200) NOT NULL,
    'customer_postcode' VARCHAR(6)NOT NULL,
    'customer_phone_number' VARCHAR(12) NOT NULL,
    'customer_email' VARCHAR(200) NOT NULL,
    'customer_password' VARCHAR(200) NOT NULL);
")

RSQLite::dbExecute(my_connection," 
  CREATE TABLE 'Vouchers' (
    'voucher_id' VARCHAR(5) PRIMARY KEY ,
    'voucher_name' VARCHAR(200) NOT NULL,
    'voucher_value' INT Not NULL,
    'voucher_starts_date' DATE Not NULL,
    'voucher_expiry_date' DATE Not NULL);
")

RSQLite::dbExecute(my_connection,"
  CREATE TABLE 'Suppliers' (
    'supplier_id' VARCHAR(7) PRIMARY KEY,
    'supplier_name' VARCHAR(200) NOT NULL,
    'supplier_phone_number' VARCHAR(12) NOT NULL,
    'supplier_email' VARCHAR(200) NOT NULL,
    'supplier_password' VARCHAR(200) NOT NULL,
    'supplier_address1' INT NOT NULL,
    'supplier_address2' VARCHAR(200) NOT NULL,
    'supplier_postcode' VARCHAR(6) NOT NULL,
    'supplier_sort_code' VARCHAR(10) NOT NULL,
    'supplier_account_name' VARCHAR(200) NOT NULL,
    'supplier_account_number' VARCHAR(16) NOT NULL);
")

RSQLite::dbExecute(my_connection,"
  CREATE TABLE 'Categories' (
    'parent_category_id' VARCHAR(3),
    'category_id' VARCHAR(5) PRIMARY KEY,
    'category_name' VARCHAR(200) NOT NULL,
    'category_description' TEXT);
")

RSQLite::dbExecute(my_connection," 
  CREATE TABLE 'Cards'(
    'card_number' INT PRIMARY KEY,
    'card_holder_name' VARCHAR(200) NOT NULL,
    'card_types' VARCHAR(200) NOT NULL,
    'card_cvv' INT NOT NULL,
    'card_expiry_date' DATE NOT NULL,
    'customer_id' VARCHAR(4),
    FOREIGN KEY ('customer_id') REFERENCES Customers('customer_id'));
")

RSQLite::dbExecute(my_connection,"
  CREATE TABLE 'Products' (
    'product_id' VARCHAR(7) PRIMARY KEY,
    'product_name' VARCHAR(200) NOT NULL,
    'product_price' DOUBLE NOT NULL,
    'product_quantity' INT NOT NULL,
    'product_description' TEXT,
    'supplier_id' VARCHAR(7),
    'category_id' VARCHAR(5),
    FOREIGN KEY ('supplier_id') REFERENCES Suppliers('supplier_id'),
    FOREIGN KEY ('category_id') REFERENCES Categories('category_id'));
")

RSQLite::dbExecute(my_connection,"
  CREATE TABLE 'Orders_Details' (
    'order_id' VARCHAR(5) PRIMARY KEY,
    'order_date' DATE NOT NULL,
    'order_status' VARCHAR(200) NOT NULL,
    'recipient_name' VARCHAR(200) NOT NULL,
    'recipient_phone_number' VARCHAR(200) NOT NULL,
    'delivery_address1' INT NOT NULL,
    'delivery_address2' VARCHAR(200) NOT NULL,
    'delivery_postcode' VARCHAR(6) NOT NULL,
    'delivery_type' VARCHAR(200) NOT NULL,
    'expected_delivery_date' DATE,
    'delivered_date' DATE,
    'delivery_instructions' TEXT,
    'voucher_id' VARCHAR(5),
    FOREIGN KEY ('voucher_id') REFERENCES Vouchers ('voucher_id'));
")

RSQLite::dbExecute(my_connection,"
  CREATE TABLE 'Order_Relationship' (
    'order_id' VARCHAR(5),
    'customer_id' VARCHAR(4),
    'product_id' VARCHAR(6),
    'order_quantity' INT,
    PRIMARY KEY (order_id,customer_id,product_id),
    FOREIGN KEY ('order_id') REFERENCES Order_Details ('order_id')
    FOREIGN KEY ('customer_id') REFERENCES Customers('customer_id'),
    FOREIGN KEY ('product_id') REFERENCES Products('product_id'));
")

RSQLite::dbExecute(my_connection,"
  CREATE TABLE 'Transactions' (
    'transaction_id' VARCHAR(5) PRIMARY KEY,
    'transaction_date' DATE NOT NULL,
    'transaction_status' VARCHAR(200) NOT NULL,
    'card_number' VARCHAR(200) NOT NULL,
    'order_id' VARCHAR(5) NOT NULL,
    FOREIGN KEY ('card_number') REFERENCES Cards ('card_number'),
    FOREIGN KEY ('order_id') REFERENCES Order_Details ('order_id'));
")


# load data

Customers <- read_excel("old_data/MOCK_DATA.xlsx",sheet = "Customers")
Vouchers <- read_excel("old_data/MOCK_DATA.xlsx",sheet = "Vouchers")
Suppliers <- read_excel("old_data/MOCK_DATA.xlsx",sheet = "Suppliers")
Categories <- read_excel("old_data/MOCK_DATA.xlsx",sheet = "Categories")
Cards <- read_excel("old_data/MOCK_DATA.xlsx",sheet = "Cards")
Products <- read_excel("old_data/MOCK_DATA.xlsx",sheet = "Products")
Order_Details <- read_excel("old_data/MOCK_DATA.xlsx",sheet = "Order_Details")
Order_Relationship <- read_excel("old_data/MOCK_DATA.xlsx",sheet = "Order_Relationship")
Transactions <- read_excel("old_data/MOCK_DATA.xlsx",sheet = "Transactions")



# Insert the old data into tables

RSQLite::dbWriteTable(my_connection,"Customers",Customers,append=TRUE)
RSQLite::dbWriteTable(my_connection,"Vouchers",Vouchers,append=TRUE)
RSQLite::dbWriteTable(my_connection,"Suppliers",Suppliers,append=TRUE)
RSQLite::dbWriteTable(my_connection,"Categories",Categories,append=TRUE)
RSQLite::dbWriteTable(my_connection,"Cards",Cards,append=TRUE)
RSQLite::dbWriteTable(my_connection,"Products",Products,append=TRUE)
RSQLite::dbWriteTable(my_connection,"Order_Details",Order_Details,append=TRUE)
RSQLite::dbWriteTable(my_connection,"Order_Relationship",Order_Relationship,append=TRUE)
RSQLite::dbWriteTable(my_connection,"Transactions",Transactions,append=TRUE)


# load new data and append new data into tables

Order_Details_new <- read_excel("new_data/New_order.xlsx",sheet = "Order_Details")
Order_Relationship_new <- read_excel("new_data/New_order.xlsx",sheet = "Order_Relationships")

RSQLite::dbWriteTable(my_connection,"Order_Details",Order_Details_new,append=TRUE)
RSQLite::dbWriteTable(my_connection,"Order_Relationship",Order_Relationship_new,append=TRUE)



# Analysis
## top 10 product based on quantity

top_10_product_based_on_quantity <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,"
            SELECT p.product_id, p.product_name,
                   SUM(or_rel.order_quantity) AS total_quantity_sold
            FROM Products p
            JOIN Order_Relationship or_rel ON p.product_id = or_rel.product_id
            GROUP BY p.product_id, p.product_name
            ORDER BY total_quantity_sold DESC
            LIMIT 10;
          ")
  )
}


## top 10 product based on revenue

top_10_product_based_on_revenue <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,"
              SELECT p.product_id, p.product_name,
                     SUM(or_rel.order_quantity * p.product_price) AS total_revenue
              FROM Products p
              JOIN Order_Relationship or_rel ON p.product_id = or_rel.product_id
              GROUP BY p.product_id, p.product_name
              ORDER BY total_revenue DESC
              LIMIT 10;
              ")
  )
}


## top 3 categories based on quantity

top_3_categories_based_on_quantity <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,"
              SELECT c.category_id, c.category_name,
                     SUM(or_rel.order_quantity) AS total_quantity_sold
              FROM Categories c
              JOIN Products p ON c.category_id = p.category_id
              JOIN Order_Relationship or_rel ON p.product_id = or_rel.product_id
              GROUP BY c.category_id, c.category_name
              ORDER BY total_quantity_sold DESC
              LIMIT 3;
            ")
  )
}


## top 3 categories based on revenue

top_3_categories_based_on_revenue <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,"
              SELECT c.category_id, c.category_name,
                     SUM(or_rel.order_quantity * p.product_price) AS total_revenue_generated
              FROM Categories c
              JOIN Products p ON c.category_id = p.category_id
              JOIN Order_Relationship or_rel ON p.product_id = or_rel.product_id
              GROUP BY c.category_id, c.category_name
              ORDER BY total_revenue_generated DESC
              LIMIT 3;
            ")
  )
}



## top 10 customers based on the total amount spent on orders

top_10_customers_based_on_the_total_amount_spent_on_orders <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,"
              SELECT c.customer_id, c.customer_first_name, c.customer_last_name,
                     SUM(p.product_price * or_rel.order_quantity) AS total_amount_spent
              FROM Customers c
              JOIN Order_Relationship or_rel ON c.customer_id = or_rel.customer_id
              JOIN Products p ON or_rel.product_id = p.product_id
              JOIN Transactions t ON or_rel.order_id = t.order_id
              GROUP BY c.customer_id, c.customer_first_name, c.customer_last_name
              ORDER BY total_amount_spent DESC
              LIMIT 10;
            ")
  )
}


## the most popular delivery_type based on the number of orders

the_most_popular_delivery_type_based_on_the_number_of_orders <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,"
              SELECT delivery_type, COUNT(*) AS num_orders
              FROM Order_Details
              WHERE delivery_type IS NOT NULL
              GROUP BY delivery_type
              ORDER BY num_orders DESC
              LIMIT 1;
            ")
  )
}


## products that are running low (quantity less than 50)

products_that_are_running_low <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,"
              SELECT *
              FROM Products
              WHERE product_quantity < 50;
            ")
  )
}


## supplier performance based on the number of products supplied

supplier_performance_based_on_the_number_of_products_supplied <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,"
              SELECT 
                  s.supplier_id,
                  s.supplier_name,
                  COUNT(DISTINCT p.product_id) AS num_products_supplied
              FROM 
                  Suppliers s
              LEFT JOIN 
                  Products p ON s.supplier_id = p.supplier_id
              GROUP BY 
                  s.supplier_id, s.supplier_name;
            ")
  )
}


## Analyze the distribution of customers by gender and age groups
analyzing_distribution_of_customers_by_gender <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,"
              SELECT 
                  customer_gender,
                  COUNT(*) AS customer_count
              FROM 
                  Customers
              GROUP BY 
                  customer_gender;
            ")
  )
}

analyzing_distribution_of_customers_by_age_group <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,"
              SELECT 
                  CASE
                      WHEN strftime('%Y', 'now') - strftime('%Y', formatted_date_of_birth) - 
                           (strftime('%m-%d', 'now') < strftime('%m-%d', formatted_date_of_birth)) BETWEEN 0 AND 18 THEN '0-18'
                      WHEN strftime('%Y', 'now') - strftime('%Y', formatted_date_of_birth) - 
                           (strftime('%m-%d', 'now') < strftime('%m-%d', formatted_date_of_birth)) BETWEEN 19 AND 30 THEN '19-30'
                      WHEN strftime('%Y', 'now') - strftime('%Y', formatted_date_of_birth) - 
                           (strftime('%m-%d', 'now') < strftime('%m-%d', formatted_date_of_birth)) BETWEEN 31 AND 45 THEN '31-45'
                      WHEN strftime('%Y', 'now') - strftime('%Y', formatted_date_of_birth) - 
                           (strftime('%m-%d', 'now') < strftime('%m-%d', formatted_date_of_birth)) BETWEEN 46 AND 60 THEN '46-60'
                      ELSE '61+'
                  END AS age_group,
                  COUNT(*) AS num_customers
              FROM 
                  (SELECT 
                      strftime('%Y-%m-%d', substr(customer_date_of_birth, 7) || '-' || substr(customer_date_of_birth, 4, 2) || '-' || substr(customer_date_of_birth, 1, 2)) AS formatted_date_of_birth
                  FROM 
                      Customers) AS converted_dates
              GROUP BY 
                  age_group
              ORDER BY 
                  MIN(strftime('%Y', 'now') - strftime('%Y', formatted_date_of_birth) - 
                      (strftime('%m-%d', 'now') < strftime('%m-%d', formatted_date_of_birth)));
            ")
  )
}



# Execute the functions and store results
result_top_10_product_based_on_quantity <- top_10_product_based_on_quantity()
result_top_10_product_based_on_revenue <- top_10_product_based_on_revenue()
result_top_3_categories_based_on_quantity <- top_3_categories_based_on_quantity()
result_top_3_categories_based_on_revenue <- top_3_categories_based_on_revenue()
result_top_10_customers_based_on_the_total_amount_spent_on_orders <- top_10_customers_based_on_the_total_amount_spent_on_orders()
result_the_most_popular_delivery_type_based_on_the_number_of_orders <- the_most_popular_delivery_type_based_on_the_number_of_orders()
result_products_that_are_running_low <- products_that_are_running_low()
result_supplier_performance_based_on_the_number_of_products_supplied <- supplier_performance_based_on_the_number_of_products_supplied()
result_analyzing_distribution_of_customers_by_gender <- analyzing_distribution_of_customers_by_gender()
result_analyzing_distribution_of_customers_by_age_group <- analyzing_distribution_of_customers_by_age_group()

# Create a new workbook
wb <- openxlsx::createWorkbook()

# Add sheets to the workbook
openxlsx::addWorksheet(wb, "Top 10 Products Quantity")
openxlsx::addWorksheet(wb, "Top 10 Products Revenue")
openxlsx::addWorksheet(wb, "Top 3 Categories Quantity")
openxlsx::addWorksheet(wb, "Top 3 Categories Revenue")
openxlsx::addWorksheet(wb, "Top 10 Customers Spent")
openxlsx::addWorksheet(wb, "Popular Delivery Type")
openxlsx::addWorksheet(wb, "Products Running Low")
openxlsx::addWorksheet(wb, "Supplier Performance")
openxlsx::addWorksheet(wb, "Customer Distribution by Gender")
openxlsx::addWorksheet(wb, "Customer Age Distribution")

# Write results to sheets
openxlsx::writeData(wb, "Top 10 Products Quantity", result_top_10_product_based_on_quantity)
openxlsx::writeData(wb, "Top 10 Products Revenue", result_top_10_product_based_on_revenue)
openxlsx::writeData(wb, "Top 3 Categories Quantity", result_top_3_categories_based_on_quantity)
openxlsx::writeData(wb, "Top 3 Categories Revenue", result_top_3_categories_based_on_revenue)
openxlsx::writeData(wb, "Top 10 Customers Spent", result_top_10_customers_based_on_the_total_amount_spent_on_orders)
openxlsx::writeData(wb, "Popular Delivery Type", result_the_most_popular_delivery_type_based_on_the_number_of_orders)
openxlsx::writeData(wb, "Products Running Low", result_products_that_are_running_low)
openxlsx::writeData(wb, "Supplier Performance", result_supplier_performance_based_on_the_number_of_products_supplied)
openxlsx::writeData(wb, "Customer Distribution by Gender", result_analyzing_distribution_of_customers_by_gender)
openxlsx::writeData(wb, "Customer Age Distribution", result_analyzing_distribution_of_customers_by_age_group)

# Save the workbook
openxlsx::saveWorkbook(wb, "analysis_results.xlsx", overwrite = TRUE)


