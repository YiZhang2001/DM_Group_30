library(readr)
library(RSQLite)
library(dplyr)
library(readxl)
library(openxlsx)
library(ggplot2)
library(scales)

my_connection <- RSQLite::dbConnect(RSQLite::SQLite(),"database.db")

## Analysis
### Top 10 Products Based on Quantity
top_10_product_based_on_quantity <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,'
            SELECT p.product_id, p.product_name,
                   SUM(or_rel.order_quantity) AS total_quantity_sold
            FROM Products p
            JOIN Order_Relationship or_rel ON p.product_id = or_rel.product_id
            GROUP BY p.product_id, p.product_name
            ORDER BY total_quantity_sold DESC
            LIMIT 10;
          ')
  )
}


### Top 10 Products Based on Revenue
top_10_product_based_on_revenue <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,'
              SELECT p.product_id, p.product_name,
                     SUM(or_rel.order_quantity * p.product_price) AS total_revenue
              FROM Products p
              JOIN Order_Relationship or_rel ON p.product_id = or_rel.product_id
              GROUP BY p.product_id, p.product_name
              ORDER BY total_revenue DESC
              LIMIT 10;
              ')
  )
}


### Top 3 Categories Based on Quantity
top_3_categories_based_on_quantity <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,'
              SELECT c.category_id, c.category_name,
                     SUM(or_rel.order_quantity) AS total_quantity_sold
              FROM Categories c
              JOIN Products p ON c.category_id = p.category_id
              JOIN Order_Relationship or_rel ON p.product_id = or_rel.product_id
              GROUP BY c.category_id, c.category_name
              ORDER BY total_quantity_sold DESC
              LIMIT 3;
            ')
  )
}


### Top 3 Categories Based on Quantity
top_3_categories_based_on_revenue <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,'
              SELECT c.category_id, c.category_name,
                     SUM(or_rel.order_quantity * p.product_price) AS total_revenue_generated
              FROM Categories c
              JOIN Products p ON c.category_id = p.category_id
              JOIN Order_Relationship or_rel ON p.product_id = or_rel.product_id
              GROUP BY c.category_id, c.category_name
              ORDER BY total_revenue_generated DESC
              LIMIT 3;
            ')
  )
}



### Top 10 Custmers
top_10_customers_based_on_the_total_amount_spent_on_orders <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,'
              SELECT c.customer_id, c.customer_first_name, c.customer_last_name,
                     SUM(p.product_price * or_rel.order_quantity) AS total_amount_spent
              FROM Customers c
              JOIN Order_Relationship or_rel ON c.customer_id = or_rel.customer_id
              JOIN Products p ON or_rel.product_id = p.product_id
              JOIN Transactions t ON or_rel.order_id = t.order_id
              GROUP BY c.customer_id, c.customer_first_name, c.customer_last_name
              ORDER BY total_amount_spent DESC
              LIMIT 10;
            ')
  )
}


### Most Popular Delivery Type
the_most_popular_delivery_type_based_on_the_number_of_orders <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,'
              SELECT delivery_type, COUNT(*) AS num_orders
              FROM Order_Details
              WHERE delivery_type IS NOT NULL
              GROUP BY delivery_type
              ORDER BY num_orders DESC
              LIMIT 1;
            ')
  )
}


### Products needed for Inventory Replenishment (Quantity Less Than 50 Items)
products_that_are_running_low <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,'
              SELECT *
              FROM Products
              WHERE product_quantity < 50
              ORDER BY product_quantity DESC;
            ')
  )
}


### Supplier Performance
supplier_performance_based_on_the_number_of_products_supplied <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,'
              SELECT 
                  s.supplier_id,
                  s.supplier_name,
                  COUNT(DISTINCT p.product_id) AS num_products_supplied
              FROM 
                  Suppliers s
              LEFT JOIN 
                  Products p ON s.supplier_id = p.supplier_id
              GROUP BY 
                  s.supplier_id, s.supplier_name
              ORDER BY num_products_supplied DESC;
            ')
  )
}


## Analyze the distribution of customers by gender and age groups
analyzing_distribution_of_customers_by_gender <- function() {
  return(
    RSQLite::dbGetQuery(my_connection,'
              SELECT 
                  customer_gender,
                  COUNT(*) AS customer_count
              FROM 
                  Customers
              GROUP BY 
                  customer_gender
              ORDER BY customer_count DESC;
            ')
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
                      (strftime('%m-%d', 'now') < strftime('%m-%d', formatted_date_of_birth))) DESC;
            ")
  )
}



# write the analysis results into excel
## Execute the functions and store results
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

## Create a new workbook
wb <- openxlsx::createWorkbook()

## Add sheets to the workbook
openxlsx::addWorksheet(wb, 'Top 10 Products Quantity')
openxlsx::addWorksheet(wb, 'Top 10 Products Revenue')
openxlsx::addWorksheet(wb, 'Top 3 Categories Quantity')
openxlsx::addWorksheet(wb, 'Top 3 Categories Revenue')
openxlsx::addWorksheet(wb, 'Top 10 Customers Spent')
openxlsx::addWorksheet(wb, 'Popular Delivery Type')
openxlsx::addWorksheet(wb, 'Products Running Low')
openxlsx::addWorksheet(wb, 'Supplier Performance')
openxlsx::addWorksheet(wb, 'Customer Distribution by Gender')
openxlsx::addWorksheet(wb, 'Customer Age Distribution')

## Write results to sheets
openxlsx::writeData(wb, 'Top 10 Products Quantity', result_top_10_product_based_on_quantity)
openxlsx::writeData(wb, 'Top 10 Products Revenue', result_top_10_product_based_on_revenue)
openxlsx::writeData(wb, 'Top 3 Categories Quantity', result_top_3_categories_based_on_quantity)
openxlsx::writeData(wb, 'Top 3 Categories Revenue', result_top_3_categories_based_on_revenue)
openxlsx::writeData(wb, 'Top 10 Customers Spent', result_top_10_customers_based_on_the_total_amount_spent_on_orders)
openxlsx::writeData(wb, 'Popular Delivery Type', result_the_most_popular_delivery_type_based_on_the_number_of_orders)
openxlsx::writeData(wb, 'Products Running Low', result_products_that_are_running_low)
openxlsx::writeData(wb, 'Supplier Performance', result_supplier_performance_based_on_the_number_of_products_supplied)
openxlsx::writeData(wb, 'Customer Distribution by Gender', result_analyzing_distribution_of_customers_by_gender)
openxlsx::writeData(wb, 'Customer Age Distribution', result_analyzing_distribution_of_customers_by_age_group)

## Save the workbook
this_filename_date <- as.character(Sys.Date())
this_filename_time <- as.character(format(Sys.time(), format = '%H_%M'))
openxlsx::saveWorkbook(wb, paste0('Analysis_Results/analysis_results_',this_filename_date,'_',this_filename_time,'.xlsx'), overwrite = TRUE)

sales <- dbGetQuery(my_connection, '
  SELECT product_id, SUM(order_quantity) AS total_sales
  FROM Order_Relationship
  GROUP BY product_id
  ORDER BY total_sales DESC
  LIMIT 1
')

top_product_id <- sales$product_id[1]

product_sales <- dbGetQuery(my_connection, "
  SELECT od.order_date, or_rel.order_quantity
  FROM Order_Details od
  INNER JOIN Order_Relationship or_rel ON od.order_id = or_rel.order_id
  WHERE or_rel.product_id = ?
", params = list(top_product_id))

product_sales$order_date <- as.Date(product_sales$order_date, format = "%d/%m/%Y")

ggplot(product_sales, aes(x = order_date, y = order_quantity)) +
  geom_line() +
  labs(title = paste('Sales Trend of Product', top_product_id),
       x = 'Date',
       y = 'Sales Quantity')

# Customer:
## gender pie chart
gender_counts_df <- as.data.frame(result_analyzing_distribution_of_customers_by_gender)
names(gender_counts_df) <- c('Gender', 'Count')

gender_counts_df$Percent <- gender_counts_df$Count / sum(gender_counts_df$Count) * 100


( gender_pie_chart <- ggplot(gender_counts_df, aes(x = "", y = Percent, fill = Gender)) +
    geom_bar(stat = 'identity') +
    coord_polar('y', start = 0) +
    geom_text(aes(label = paste(round(Percent), '%')), position = position_stack(vjust = 0.5)) +
    labs(title = 'Customer Gender Distribution', fill = 'Gender') +
    theme_minimal() +
    theme(legend.position = 'right') +
    theme_void() +
    scale_fill_discrete(name = 'Gender') )


## age violin chart
age_data <- RSQLite::dbGetQuery(my_connection, "
    SELECT 
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_gender,
    customer_date_of_birth,
    CAST(strftime('%Y', 'now') AS INTEGER) - CAST(SUBSTR(customer_date_of_birth, 7, 4) AS INTEGER) -
    CASE 
        WHEN strftime('%m-%d', 'now') < SUBSTR(customer_date_of_birth, 6, 5) 
        THEN 1
        ELSE 0
    END AS age
    FROM 
    Customers;
")

age_data$age_group <- cut(age_data$age, breaks = c(0, 30,  Inf),
                          labels = c('0-30', '31+'))

# violin
( age_violin_chart <- ggplot(age_data, aes(x = age_group, y = age)) +
    geom_violin() +
    labs(title = 'Distribution of Customer Age by Age Group',
         x = 'Age Group', y = 'Age') +
    theme_minimal() )


# Product
# product price distribution based on parent category 
query <- '
  SELECT pc.category_name AS parent_category_name, p.product_price
  FROM Products p
  JOIN Categories c ON p.category_id = c.category_id
  JOIN Categories pc ON c.parent_category_id = pc.category_id;
'
product_price_data <- RSQLite::dbGetQuery(my_connection, query)

( product_price_distribution <- ggplot(product_price_data, aes(x = parent_category_name, y = product_price)) +
    geom_violin(trim = FALSE) + 
    labs(title = 'Product Price Distribution Based on Parent Category', x = 'Parent Category', y = 'Product Price') +
    theme_minimal() )


## top 3 category based on sales
( top_3_category_based_on_sales <- ggplot(result_top_3_categories_based_on_revenue, aes(x = category_name, y = total_revenue_generated)) +
    geom_bar(stat = 'summary', fun = 'mean', fill = 'skyblue', color = 'black') +
    labs(title = 'top 3 category based on sales',
         x = 'category name',
         y = 'total sales generated') +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1)) )


## top 10 products based on sales
( top_10_products_based_on_sales <- ggplot(result_top_10_product_based_on_revenue, aes(x = product_name, y = total_revenue)) +
    geom_bar(stat = 'summary', fun = 'mean', fill = 'skyblue', color = 'black') +
    labs(title = 'top 10 products based on sales',
         x = 'product name',
         y = 'total sales generated') +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1)) )


# Cards
## the total of customers on number of cards
number_of_customers_based_on_cards <- RSQLite::dbGetQuery(my_connection, '
  SELECT
      num_cards,
      COUNT(*) AS num_customers
  FROM (
      SELECT
          customer_id,
          COUNT(*) AS num_cards
      FROM
          Cards
      GROUP BY
          customer_id
  ) AS card_counts
  GROUP BY
      num_cards
  ORDER BY
      num_cards DESC;
')

( total_customers_on_number_of_cards <- ggplot(number_of_customers_based_on_cards, aes(x = num_cards, y = num_customers)) +
    geom_bar(stat = 'summary', fun = 'mean', fill = 'skyblue', color = 'black') +
    labs(title = 'the total of customers on number of cards',
         x = 'number of cards',
         y = 'the total of customers') +
    theme_minimal() )


# Suppliers
## top 3 suppliers
( top_3_suppliers <- ggplot(result_supplier_performance_based_on_the_number_of_products_supplied[1:3,], aes(x = supplier_name, y = num_products_supplied)) +
    geom_bar(stat = 'summary', fun = 'mean', fill = 'skyblue', color = 'black') +
    labs(title = 'top 3 suppliers',
         x = 'supplier_name',
         y = 'number of products supplied') +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1)) )


# Order_details
## time series of order daily for 3 months
time_series <- RSQLite::dbGetQuery(my_connection, "
    SELECT 
        order_Date,
        COUNT(order_id) AS order_amount
    FROM 
        Order_Details
    GROUP BY 
        order_date
    ORDER BY 
        order_date;
")

time_series$order_date <- as.POSIXct(time_series$order_date, origin = "1970-01-01")

( time_series_of_order_daily <- ggplot(time_series, aes(x = order_date, y = order_amount)) +
    geom_line() +
    labs(title = 'time series of order daily for 3 months',
         x = 'order date',
         y = 'order amount')+
    theme_minimal()+
    scale_x_datetime(date_labels = '%d/%m/%y') )


# Vouchers
## 5 most used voucher from orde
voucher_usage <- RSQLite::dbGetQuery(my_connection, '
  SELECT voucher_id, COUNT(*) AS usage_count
  FROM Order_Details
  WHERE voucher_id IS NOT NULL
  GROUP BY voucher_id
  ORDER BY usage_count DESC
  LIMIT 5
')

# Use mutate to calculate row number and mark the top two most used vouchers as 'highlight'
voucher_usage <- voucher_usage %>%
  mutate(highlight = ifelse(row_number() <= 2, 'highlight', 'normal'))

# Get the names of vouchers
voucher_names <- RSQLite::dbGetQuery(my_connection, '
  SELECT voucher_id, voucher_name
  FROM Vouchers
')

# Merge voucher usage count and names
voucher_usage <- merge(voucher_usage, voucher_names, by = 'voucher_id')

# Draw the bar chart
(most_used_vouchers_from_order <- ggplot(voucher_usage, aes(x = reorder(voucher_name, -usage_count), y = usage_count, fill = highlight)) +
    geom_bar(stat = 'identity') +
    scale_fill_manual(values = c('highlight' = 'pink', 'normal' = 'skyblue')) +
    labs(title = 'Top 5 Most Used Vouchers',
         x = 'Voucher',
         y = 'Usage Count') +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1)) )

# save each plot as a png file
this_filename_date <- as.character(Sys.Date())
this_filename_time <- as.character(format(Sys.time(), format = '%H_%M'))
ggsave(paste0('analysis_graphs/gender_pie_chart_',this_filename_date,'_',this_filename_time,'.png'), plot = gender_pie_chart, width = 8, height = 6, units = 'in')
ggsave(paste0('analysis_graphs/age_violin_chart_',this_filename_date,'_',this_filename_time,'.png'), plot = age_violin_chart, width = 8, height = 6, units = 'in')
ggsave(paste0('analysis_graphs/product_price_distribution_',this_filename_date,'_',this_filename_time,'.png'), plot = product_price_distribution, width = 8, height = 6, units = 'in')
ggsave(paste0('analysis_graphs/top_3_category_based_on_sales_',this_filename_date,'_',this_filename_time,'.png'), plot = top_3_category_based_on_sales, width = 8, height = 6, units = 'in')
ggsave(paste0('analysis_graphs/top_10_products_based_on_sales_',this_filename_date,'_',this_filename_time,'.png'), plot = top_10_products_based_on_sales, width = 8, height = 6, units = 'in')
ggsave(paste0('analysis_graphs/total_customers_on_number_of_cards_',this_filename_date,'_',this_filename_time,'.png'), plot = total_customers_on_number_of_cards, width = 8, height = 6, units = 'in')
ggsave(paste0('analysis_graphs/top_3_suppliers_',this_filename_date,'_',this_filename_time,'.png'), plot = top_3_suppliers, width = 8, height = 6, units = 'in')
ggsave(paste0('analysis_graphs/time_series_of_order_daily_',this_filename_date,'_',this_filename_time,'.png'), plot = time_series_of_order_daily, width = 8, height = 6, units = 'in')
ggsave(paste0('analysis_graphs/most_used_vouchers_from_order_',this_filename_date,'_',this_filename_time,'.png'), plot = most_used_vouchers_from_order, width = 8, height = 6, units = 'in')
