library(readr)
library(RSQLite)
library(dplyr)
library(readxl)
library(openxlsx)
library(ggplot2)
library(scales)

my_connection <- RSQLite::dbConnect(RSQLite::SQLite(),"database.db")


## Physical Schema
## Create SQL Database Schema Tables

tables <- RSQLite::dbListTables(my_connection)

# drop all tables
for (table in tables) {
  query <- paste("DROP TABLE IF EXISTS", table)
  RSQLite::dbExecute(my_connection, query)
}

# Create 'Customers' table.
RSQLite::dbExecute(my_connection,"
  CREATE TABLE IF NOT EXISTS 'Customers' (
    'customer_id' VARCHAR(4) PRIMARY KEY NOT NULL,
    'customer_first_name' VARCHAR(200) NOT NULL,
    'customer_last_name' VARCHAR(200) NOT NULL,
    'customer_gender' VARCHAR(200) NOT NULL,
    'customer_date_of_birth' Date,
    'customer_address1' INT NOT NULL,
    'customer_address2' VARCHAR(200) NOT NULL,
    'customer_postcode' VARCHAR(6)NOT NULL,
    'customer_phone_number' VARCHAR(12) NOT NULL,
    'customer_email' VARCHAR(200) NOT NULL,
    'customer_password' VARCHAR(200) NOT NULL);
")

# Create 'Vouchers' table. 
RSQLite::dbExecute(my_connection," 
  CREATE TABLE IF NOT EXISTS 'Vouchers' (
    'voucher_id' VARCHAR(5) PRIMARY KEY NOT NULL,
    'voucher_name' VARCHAR(200) NOT NULL,
    'voucher_value' INT Not NULL,
    'voucher_starts_date' DATE Not NULL,
    'voucher_expiry_date' DATE Not NULL);
")

# Create 'Suppliers' table. 
RSQLite::dbExecute(my_connection,"
  CREATE TABLE IF NOT EXISTS 'Suppliers' (
    'supplier_id' VARCHAR(7) PRIMARY KEY NOT NULL,
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

# Create 'Categories' table.  
RSQLite::dbExecute(my_connection,"
  CREATE TABLE IF NOT EXISTS 'Categories' (
    'parent_category_id' VARCHAR(3),
    'category_id' VARCHAR(5) PRIMARY KEY NOT NULL,
    'category_name' VARCHAR(200) NOT NULL,
    'category_description' TEXT,
    FOREIGN KEY ('parent_category_id') REFERENCES Categories ('category_id'));
")

# Create 'Cards' table. 
RSQLite::dbExecute(my_connection," 
  CREATE TABLE IF NOT EXISTS 'Cards'(
    'card_number' INT PRIMARY KEY NOT NULL,
    'card_holder_first_name' VARCHAR(200) NOT NULL,
    'card_holder_last_name' VARCHAR(200) NOT NULL,
    'card_types' VARCHAR(200) NOT NULL,
    'card_cvv' INT NOT NULL,
    'card_expiry_date' DATE NOT NULL,
    'customer_id' VARCHAR(4),
    FOREIGN KEY ('customer_id') REFERENCES Customers('customer_id'));
")

# Create 'Products' table.
RSQLite::dbExecute(my_connection,"
  CREATE TABLE IF NOT EXISTS 'Products' (
    'product_id' VARCHAR(7) PRIMARY KEY NOT NULL,
    'product_name' VARCHAR(200) NOT NULL,
    'product_price' DOUBLE NOT NULL,
    'product_quantity' INT NOT NULL,
    'product_description' TEXT,
    'supplier_id' VARCHAR(7),
    'category_id' VARCHAR(5),
    FOREIGN KEY ('supplier_id') REFERENCES Suppliers('supplier_id'),
    FOREIGN KEY ('category_id') REFERENCES Categories('category_id'));
")

# Create 'Orders_Details' table.
RSQLite::dbExecute(my_connection,"
  CREATE TABLE IF NOT EXISTS 'Orders_Details' (
    'order_id' VARCHAR(5) PRIMARY KEY NOT NULL,
    'order_date' DATE NOT NULL,
    'order_status' VARCHAR(200) NOT NULL,
    'recipient_first_name' VARCHAR(200) NOT NULL,
    'recipient_last_name' VARCHAR(200) NOT NULL,
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

# Create 'Order_Relationship' table.
RSQLite::dbExecute(my_connection,"
  CREATE TABLE IF NOT EXISTS 'Order_Relationship' (
    'order_id' VARCHAR(5),
    'customer_id' VARCHAR(4),
    'product_id' VARCHAR(6),
    'order_quantity' INT,
    PRIMARY KEY (order_id,customer_id,product_id),
    FOREIGN KEY ('order_id') REFERENCES Order_Details ('order_id')
    FOREIGN KEY ('customer_id') REFERENCES Customers('customer_id'),
    FOREIGN KEY ('product_id') REFERENCES Products('product_id'));
")

# Create 'Transactions' table.
RSQLite::dbExecute(my_connection,"
  CREATE TABLE IF NOT EXISTS 'Transactions' (
    'transaction_id' VARCHAR(5) PRIMARY KEY NOT NULL,
    'transaction_date' DATE NOT NULL,
    'transaction_status' VARCHAR(200) NOT NULL,
    'card_number' VARCHAR(200) NOT NULL,
    'order_id' VARCHAR(5) NOT NULL,
    FOREIGN KEY ('card_number') REFERENCES Cards ('card_number'),
    FOREIGN KEY ('order_id') REFERENCES Order_Details ('order_id'));
")



## Load Synthetic Data
all_Customers <- list.files("data_upload/Customers/")
combined_Customers <- data.frame()

for (variable in all_Customers){
  this_filepath <- paste0("data_upload/Customers/",variable)
  this_file_contents <- readr::read_csv(this_filepath)
  
  combined_Customers <- rbind(combined_Customers,this_file_contents)
  number_of_combined_rows <- nrow(combined_Customers)
  number_of_rows <- nrow(this_file_contents)
  
  # check if the pk in new csv exists in old csv
  if ( nrow(unique(this_file_contents[,1]))==number_of_rows ){
    
    
    # check if the pk is unique in each csv
    if ( nrow(unique(combined_Customers[,1]))==number_of_combined_rows ) {
      
      # Data Transformation
      #  this_file_contents$customer_date_of_birth <- as.Date(this_file_contents$customer_date_of_birth, format = '%d/%m/%y')
      this_file_contents$customer_date_of_birth <- as.character(this_file_contents$customer_date_of_birth)
      this_file_contents$customer_address1 <- as.character(this_file_contents$customer_address1)
      
      # Check the format of the 'customer_id' column in the 'Customers' table.
      valid_cust_id <- grepl('^C[0-9]{3}$', this_file_contents$customer_id)
      this_file_contents <- this_file_contents[valid_cust_id,]
      
      
      # Check the format of the 'customer_phone_number' column in the 'Customers' table.
      valid_cust_phone_number <- grepl('^\\d{3}-\\d{3}-\\d{4}$', this_file_contents$customer_phone_number)
      this_file_contents <- this_file_contents[valid_cust_phone_number,]
      
      # Check the format of the 'customer_email' column in the 'Customers' table.
      valid_cust_email <- grep('^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$', this_file_contents$customer_email)
      this_file_contents <- this_file_contents[valid_cust_email,]
      
      #Check the format of the customer_postcode in the 'Customers' table.
      valid_cust_postcode <- grep('^CV[A-Z0-9]{3,5}$', this_file_contents$customer_postcode)
      this_file_contents <- this_file_contents[valid_cust_postcode,]
      
      #gender check
      valid_genders <- c('Male', 'Female', 'Non-binary')
      this_file_contents <- this_file_contents[this_file_contents$customer_gender %in% valid_genders,]
      
      # Insert the data into database
      RSQLite::dbWriteTable(my_connection,"Customers",this_file_contents,append=TRUE)
    }
    else {break}
  }
  else {break}
}

# double check data type, column names, primary key, and not null rule again
RSQLite::dbGetQuery(my_connection, "PRAGMA table_info(Customers);")


all_Vouchers <- list.files("data_upload/Vouchers/")
combined_Vouchers <- data.frame()

for (variable in all_Vouchers){
  this_filepath <- paste0("data_upload/Vouchers/",variable)
  this_file_contents <- readr::read_csv(this_filepath)
  
  combined_Vouchers <- rbind(combined_Vouchers,this_file_contents)
  number_of_combined_rows <- nrow(combined_Vouchers)
  number_of_rows <- nrow(this_file_contents)
  
  # check if the pk in new csv exists in old csv
  if ( nrow(unique(this_file_contents[,1]))==number_of_rows  ){
    
    # check if the pk is unique in each csv
    if ( nrow(unique(combined_Vouchers[,1]))==number_of_combined_rows) {
      
      # Data Transformation
      this_file_contents$voucher_starts_date <- as.Date(this_file_contents$voucher_starts_date, format = '%d/%m/%y')
      this_file_contents$voucher_starts_date <- as.character(this_file_contents$voucher_starts_date) 
      this_file_contents$voucher_expiry_date <- as.Date(this_file_contents$voucher_expiry_date, format = '%d/%m/%y')
      this_file_contents$voucher_expiry_date <- as.character(this_file_contents$voucher_expiry_date)
      # Check the format of the voucher_id in the 'Vouchers' table.
      valid_voucher_id <- grepl('^V[0-9]{4}$', this_file_contents$voucher_id)
      this_file_contents <- this_file_contents[valid_voucher_id, ]
      
      # Insert the data into database
      RSQLite::dbWriteTable(my_connection,"Vouchers",this_file_contents,append=TRUE)
    }
    else {break}
  }
  else {break}
}

# double check data type, column names, primary key, and not null rule again
RSQLite::dbGetQuery(my_connection, "PRAGMA table_info(Vouchers);")


all_Suppliers <- list.files("data_upload/Suppliers/")
combined_Suppliers <- data.frame()

for (variable in all_Suppliers){
  this_filepath <- paste0("data_upload/Suppliers/",variable)
  this_file_contents <- readr::read_csv(this_filepath)
  
  combined_Suppliers <- rbind(combined_Suppliers,this_file_contents)
  number_of_combined_rows <- nrow(combined_Suppliers)
  number_of_rows <- nrow(this_file_contents)
  
  # check if the pk in new csv exists in old csv
  if ( nrow(unique(this_file_contents[,1]))==number_of_rows ){
    
    # check if the pk is unique in each csv
    if ( nrow(unique(combined_Suppliers[,1]))==number_of_combined_rows ) {
      
      # Data Transformation
      this_file_contents$supplier_address1 <- as.character(this_file_contents$supplier_address1)
      this_file_contents$supplier_account_number <- as.character(this_file_contents$supplier_account_number)
      
      # Check the format of the 'supplier_id' column in the 'Suppliers' table.
      valid_sup_id <- grepl ('^SP[0-9]{4}[A-Z]{3}$', this_file_contents$supplier_id)
      this_file_contents <- this_file_contents [valid_sup_id,]
      
      # Check the format of the 'supplier_phone_number' column in the 'Suppliers' table.
      valid_supplier_phone_number <- grepl('^\\d{3}-\\d{3}-\\d{4}$', this_file_contents$supplier_phone_number)
      this_file_contents <- this_file_contents [valid_supplier_phone_number,]
      
      # Check the format of the 'supplier_email' column in the 'Suppliers' table.
      valid_supplier_email <- grep('^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$', this_file_contents$supplier_email)
      this_file_contents <- this_file_contents [valid_supplier_email,]
      
      #Check the format of the supplier_postcode in the 'Suppliers' table.
      valid_supplier_postcode <- grep('^CV[A-Z0-9]{3,5}$', this_file_contents$supplier_postcode)
      this_file_contents <- this_file_contents [valid_supplier_postcode,]
      
      # Check the format of the 'supplier_sort_code' column in the 'Suppliers' table.
      valid_supplier_sort_code <- grepl('^\\d{2}-\\d{2}-\\d{2}$', this_file_contents$supplier_sort_code)
      this_file_contents <- this_file_contents[valid_supplier_sort_code, ]
      
      # Check the format of the 'supplier_account_number' column in the 'Suppliers' table.
      valid_supplier_account_number <- grepl('^\\d{8,17}$', this_file_contents$supplier_account_number)
      this_file_contents <- this_file_contents[valid_supplier_account_number, ]
      
      
      # Insert the data into database
      RSQLite::dbWriteTable(my_connection,"Suppliers",this_file_contents,append=TRUE)
    }
    else {break}
  }
  else {break}
}

# double check data type, column names, primary key, and not null rule again
RSQLite::dbGetQuery(my_connection, "PRAGMA table_info(Suppliers);")


all_Categories <- list.files("data_upload/Categories/")
combined_Categories <- data.frame()

for (variable in all_Categories){
  this_filepath <- paste0("data_upload/Categories/",variable)
  this_file_contents <- readr::read_csv(this_filepath)
  
  combined_Categories <- rbind(combined_Categories,this_file_contents)
  number_of_combined_rows <- nrow(combined_Categories)
  number_of_rows <- nrow(this_file_contents)
  
  # check if the pk in new csv exists in old csv
  if ( nrow(unique(this_file_contents[,2]))==number_of_rows ){
    
    # check if the pk is unique in each csv
    if ( nrow(unique(combined_Categories[,2]))==number_of_combined_rows) {
      
      # Check the format of the 'category_id' column in the 'Categories' table.
      valid_category_id <- grepl('^CG[0-9]+$', this_file_contents$category_id)
      this_file_contents <- this_file_contents[valid_category_id, ]
      
      # Insert the data into database
      RSQLite::dbWriteTable(my_connection,"Categories",this_file_contents,append=TRUE)
    }
    else {break}
  }
  else {break}
}

# double check data type, column names, primary key, and not null rule again
RSQLite::dbGetQuery(my_connection, "PRAGMA table_info(Categories);")


all_Cards <- list.files("data_upload/Cards/")
combined_Cards <- data.frame()

for (variable in all_Cards){
  this_filepath <- paste0("data_upload/Cards/",variable)
  this_file_contents <- readr::read_csv(this_filepath)
  
  combined_Cards <- rbind(combined_Cards,this_file_contents)
  number_of_combined_rows <- nrow(combined_Cards)
  number_of_rows <- nrow(this_file_contents)
  
  # check if the pk in new csv exists in old csv
  if ( nrow(unique(this_file_contents[,1]))==number_of_rows){
    
    # check if the pk is unique in each csv
    if (nrow(unique(combined_Cards[,1]))==number_of_combined_rows ) {
      
      # Change data class within 'Cards'.
      this_file_contents$card_number <- as.character(this_file_contents$card_number)
      this_file_contents$card_cvv <- as.character(this_file_contents$card_cvv)
      this_file_contents$card_expiry_date <- as.Date(this_file_contents$card_expiry_date, format = '%d/%m/%y')
      this_file_contents$card_expiry_date <- as.character(this_file_contents$card_expiry_date)
      #valid_card_number <- grepl('^\\d{16}$', Cards$card_number)
      valid_card_number <- grepl('^\\d{16}$', this_file_contents$card_number)
      this_file_contents <- this_file_contents[valid_card_number, ]
      
      valid_card_cvv <- grepl('^\\d{3}$', this_file_contents$card_cvv)
      this_file_contents <- this_file_contents[valid_card_cvv, ]
      
      # Insert the data into database
      RSQLite::dbWriteTable(my_connection,"Cards",this_file_contents,append=TRUE)
    }
    else {break}
  }
  else {break}
}

# double check data type, column names, primary key, and not null rule again
RSQLite::dbGetQuery(my_connection, "PRAGMA table_info(Cards);")


all_Products <- list.files("data_upload/Products/")

combined_products <- data.frame()

for (variable in all_Products){
  this_filepath <- paste0("data_upload/Products/",variable)
  this_file_contents <- readr::read_csv(this_filepath)
  
  combined_products <- rbind(combined_products,this_file_contents)
  
  number_of_combined_rows <- nrow(combined_products)
  
  number_of_rows <- nrow(this_file_contents)
  
  if ( nrow(unique(this_file_contents[,1]))==number_of_rows ){
    
    if ( nrow(unique(combined_products[,1]))==number_of_combined_rows) {
      
      RSQLite::dbWriteTable(my_connection,"Products",this_file_contents,append=TRUE)
    }
    else {break}
  }
  else {break}
  
}

# double check data type, column names, primary key, and not null rule again
RSQLite::dbGetQuery(my_connection, "PRAGMA table_info(Products);")


all_Order_Details <- list.files("data_upload/Order_Details/")

combined_Order_Details <- data.frame()

for (variable in all_Order_Details){
  this_filepath <- paste0("data_upload/Order_Details/",variable)
  this_file_contents <- readr::read_csv(this_filepath)
  
  combined_Order_Details <- rbind(combined_Order_Details,this_file_contents)
  
  number_of_combined_rows <- nrow(combined_Order_Details)
  
  number_of_rows <- nrow(this_file_contents)
  
  if ( nrow(unique(this_file_contents[,1]))==number_of_rows ){
    
    if (  nrow(unique(combined_Order_Details[,1]))==number_of_combined_rows ) {
      
      # Data Transformation
      #this_file_contents$expected_delivery_date <- as.Date(this_file_contents$expected_delivery_date, format = '%d/%m/%y')
      #this_file_contents$delivered_date <- as.Date(this_file_contents$delivered_date, format = '%d/%m/%y')
      this_file_contents$expected_delivery_date <- as.character(this_file_contents$expected_delivery_date)
      this_file_contents$delivered_date <- as.character(this_file_contents$delivered_date)
      # Check the format of the 'order_id' column in the 'Order Details' table.
      valid_order_id <- grepl('^OD[0-9]+$',this_file_contents$order_id)
      this_file_contents <- this_file_contents[valid_order_id, ]
      
      #Check the format of the recipient phone number in the 'Order_Details' table.
      valid_rec_phone_number <- grepl('^\\d{3}-\\d{3}-\\d{4}$', this_file_contents$recipient_phone_number)
      this_file_contents <- this_file_contents[valid_rec_phone_number, ]
      
      #Check the format of the delivery_postcode in the 'Order_Details' table.
      valid_rec_postcode <- grep('^CV[A-Z0-9]{3,5}$', this_file_contents$delivery_postcode)
      this_file_contents <- this_file_contents[valid_rec_postcode,]
      
      RSQLite::dbWriteTable(my_connection,"Order_Details",this_file_contents,append=TRUE)
    }
    else {break}
  }
  else {break}
  
}

# double check data type, column names, primary key, and not null rule again
RSQLite::dbGetQuery(my_connection, "PRAGMA table_info(Order_Details);")


all_Order_Relationship <- list.files("data_upload/Order_Relationship/")

combined_Order_Relationship <- data.frame()

for (variable in all_Order_Relationship){
  this_filepath <- paste0("data_upload/Order_Relationship/",variable)
  this_file_contents <- readr::read_csv(this_filepath)
  
  this_file_contents$combined_id <- paste(this_file_contents$order_id, 
                                          this_file_contents$customer_id, 
                                          this_file_contents$product_id)
  
  combined_Order_Relationship <- rbind(combined_Order_Relationship,this_file_contents)
  
  number_of_combined_rows <- nrow(combined_Order_Relationship)
  
  number_of_rows <- nrow(this_file_contents)
  
  if ( nrow(unique(this_file_contents[,5]))==number_of_rows ){
    
    if ( nrow(unique(combined_Order_Relationship[,5]))==number_of_combined_rows) {
      
      this_file_contents$combined_id <- NULL
      
      RSQLite::dbWriteTable(my_connection,"Order_Relationship",this_file_contents,append=TRUE)
    }
    else {break}
  }
  else {break}
  
}

# double check data type, column names, primary key, and not null rule again
RSQLite::dbGetQuery(my_connection, "PRAGMA table_info(Order_Relationship);")


all_Transactions <- list.files("data_upload/Transactions/")
combined_Transactions <- data.frame()

for (variable in all_Transactions){
  this_filepath <- paste0("data_upload/Transactions/",variable)
  this_file_contents <- readr::read_csv(this_filepath)
  
  combined_Transactions <- rbind(combined_Transactions,this_file_contents)
  
  number_of_combined_rows <- nrow(combined_Transactions)
  
  number_of_rows <- nrow(this_file_contents)
  
  if ( nrow(unique(this_file_contents[,1]))==number_of_rows ){
    
    if ( nrow(unique(combined_Transactions[,1]))==number_of_combined_rows) {
      
      # Data Transformation
      this_file_contents$transaction_date <- as.Date(this_file_contents$transaction_date, format = '%d/%m/%y')
      this_file_contents$transaction_date <- as.character(this_file_contents$transaction_date) 
      this_file_contents$card_number <- as.character(this_file_contents$card_number)
      
      # Check the format of the transaction_id in the 'Transactions' table.
      valid_transaction_id <- grepl('TR[0-9]+$', this_file_contents$transaction_id)
      this_file_contents <- this_file_contents[valid_transaction_id, ]
      
      RSQLite::dbWriteTable(my_connection,"Transactions",this_file_contents,append=TRUE)
    }
    else {break}
  }
  else {break}
}

# double check data type, column names, primary key, and not null rule again
RSQLite::dbGetQuery(my_connection, "PRAGMA table_info(Transactions);")
