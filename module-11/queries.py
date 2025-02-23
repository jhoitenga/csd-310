#   Title: queries.py
#   Authors: Casey Rose, Darreon Tolen and Jennifer Hoitenga
#   Date: 02/23/2025
#   Description: Queries for Reporting Needs.
#   Source: Creating bar charts in Python - https://www.w3schools.com/python/matplotlib_bars.asp
#   Source: MatplotLib - https://matplotlib.org/stable/gallery/index


# Import Statements
import mysql.connector  # to connect
from mysql.connector import errorcode
from db_config import connect_db  # Import shared db_config file
from log_config import logger  # Import shared logging configuration
import traceback  # for detailed error diagnostics
import logging  # for logging errors
import dotenv  # to use .env file
from dotenv import dotenv_values
from datetime import datetime, date  # Import date for date formatting
from decimal import Decimal  # Import for decimal formatting
import matplotlib.pyplot as plt  # Import for charting
import numpy as np

try:
    # Try/catch block for handling potential MySQL database errors

    # Connect to the winery database
    db = connect_db(database="winery")  # Connect to database

    # Output the connection status
    print("\n You are connected to the Winery MySQL Database!\n")

    # Open a new cursor for executing database queries.
    cursor = db.cursor()

    # Supplier Reports
    def get_supplier_delivery_performance():
        # SQL query to show supplier name, dates and total delay days
        query = """
        SELECT
            DATE_FORMAT(s.order_date, '%m-%d-%Y') AS order_month,
            sup.supplier_name,
            ANY_VALUE(DATE_FORMAT(s.expected_date, '%m-%d-%Y')) as expected_date,
            ANY_VALUE(DATE_FORMAT(s.delivery_date, '%m-%d-%Y')) as delivery_date,
            SUM(DATEDIFF(s.delivery_date, s.expected_date)) AS total_delay_days
        FROM supply s
        JOIN supplier sup ON s.supplier_id = sup.supplier_id
        GROUP BY order_month, sup.supplier_name
        ORDER BY DATE_FORMAT(MIN(s.order_date), '%Y-%m') ASC, total_delay_days DESC;
        """

        # Execute Query
        cursor.execute(query)
        results = cursor.fetchall()

        # Define column headers
        headers = ["Ordered Date", "Supplier Name",
                   "Expected Date", "Delivered Date", "Total Delay Days"]

        # Determine column widths dynamically
        col_widths = [max(len(str(row[i])) for row in results + [headers])
                      for i in range(len(headers))]

        # Print table header
        print(" | ".join(header.ljust(col_widths[i])
                         for i, header in enumerate(headers)))
        # Print a separator line
        print("-" * (sum(col_widths) + (len(headers) - 1) * 3))

        # Print each row in the table
        for row in results:
            print(" | ".join(str(row[i]).ljust(col_widths[i])
                             for i in range(len(row))))

    # Bar Chart for Supplier Reports
    def plot_supplier_delivery_trends():
        # SQL Query
        query = """
        SELECT
            DATE_FORMAT(s.order_date, '%Y-%m') AS order_month,
            sup.supplier_name,
            SUM(DATEDIFF(s.delivery_date, s.expected_date)) AS total_delay_days
        FROM supply s
        JOIN supplier sup ON s.supplier_id = sup.supplier_id
        GROUP BY order_month, sup.supplier_name
        ORDER BY order_month ASC;
        """

        # Execute Query
        cursor.execute(query)
        results = cursor.fetchall()

        # Organize data for plotting
        months = []
        suppliers = {}

        for row in results:
            month, supplier, delay = row
            if month not in months:
                months.append(month)
            if supplier not in suppliers:
                suppliers[supplier] = []
            suppliers[supplier].append(delay)

        # Plot Bar Chart
        plt.figure(figsize=(12, 6))

        # Bar positions
        bar_width = 0.2  # Width of each bar
        x_indexes = range(len(months))

        # Plot each supplier's data
        for i, (supplier, delays) in enumerate(suppliers.items()):
            plt.bar([x + (i * bar_width) for x in x_indexes],
                    delays, width=bar_width, label=supplier)

        # Formatting the chart
        plt.xlabel("Month")
        plt.ylabel("Total Delay Days")
        plt.title("Monthly Supplier Delivery Delays")
        plt.xticks([x + (bar_width * (len(suppliers) / 2))
                    for x in x_indexes], months, rotation=45)
        plt.legend(title="Suppliers")
        plt.grid(axis="y", linestyle="--", alpha=0.7)

        # Show the plot
        plt.tight_layout()
        plt.show()

    # Wine Reports
    def get_wine_performance():
        # SQL query to show sale date, quantity, wine type and distributor
        query = """
        SELECT 
            DATE_FORMAT(s.sale_date, '%m-%d-%Y') AS sale_date,
            s.sale_id,
            s.quantity,
            wt.wine_type_name,
            d.distributor_name
        FROM sales s
        JOIN wines w ON s.wine_id = w.wine_id
        JOIN wine_type wt ON w.wine_type_id = wt.wine_type_id
        JOIN distributor d ON s.distributor_id = d.distributor_id
        ORDER BY s.sale_date ASC, s.sale_id ASC, d.distributor_name ASC;
        """

        # Execute Query
        cursor.execute(query)
        results = cursor.fetchall()

        # Define column headers
        headers = ["Sale Date", "Sale ID", "Quantity",
                   "Wine Type", "Distributor Name"]

        # Determine column widths dynamically
        col_widths = [max(len(str(row[i])) for row in results + [headers])
                      for i in range(len(headers))]

        # Print table header
        print(" | ".join(header.ljust(col_widths[i])
              for i, header in enumerate(headers)))
        # Print a separator line
        print("-" * (sum(col_widths) + (len(headers) - 1) * 3))

        # Print each row in the table
        for row in results:
            print(" | ".join(str(row[i]).ljust(col_widths[i])
                  for i in range(len(row))))

    # Bar Chart for Wine Reports
    def plot_sales_trends():
        # SQL Query
        query = """
        SELECT 
            DATE_FORMAT(s.sale_date, '%m-%Y') AS sale_month,
            d.distributor_name,
            wt.wine_type_name,
            SUM(s.quantity) AS total_quantity
        FROM sales s
        JOIN wines w ON s.wine_id = w.wine_id
        JOIN wine_type wt ON w.wine_type_id = wt.wine_type_id
        JOIN distributor d ON s.distributor_id = d.distributor_id
        GROUP BY sale_month, d.distributor_name, wt.wine_type_name
        ORDER BY DATE_FORMAT(MIN(sale_month), '%Y-%m') ASC;
        """

        # Execute Query
        cursor.execute(query)
        results = cursor.fetchall()

        # Organize data for plotting
        months = []
        distributors = {}

        for row in results:
            month, distributor, wine_type, quantity = row
            if month not in months:
                months.append(month)
            if distributor not in distributors:
                distributors[distributor] = {}
            if wine_type not in distributors[distributor]:
                distributors[distributor][wine_type] = []
            distributors[distributor][wine_type].append(quantity)

        # Plot Bar Chart
        plt.figure(figsize=(12, 6))

        # Bar positions
        bar_width = 0.2  # Width of each bar
        x_indexes = range(len(months))

        # Plot the data
        for i, (distributor, wine_data) in enumerate(distributors.items()):
            for j, (wine_type, quantities) in enumerate(wine_data.items()):
                plt.bar([x + ((i + j * 0.2) * bar_width) for x in x_indexes],
                        quantities, width=bar_width, label=f"{distributor} - {wine_type}")

        # Formatting the chart
        plt.xlabel("Month")
        plt.ylabel("Total Wines Sold")
        plt.title("Monthly Wine Sales by Distributor and Wine Type")
        plt.xticks([x + (bar_width * (len(distributors) / 2))
                    for x in x_indexes], months, rotation=45)
        plt.legend(title="Distributors - Wine Type")
        plt.grid(axis="y", linestyle="--", alpha=0.7)

        # Show the plot
        plt.tight_layout()
        plt.show()

    # Employee Reports
    def get_employee_performance():
        # SQL query to show sale date, quantity, wine type and distributor
        query = """
        SELECT 
            e.first_name,
            e.last_name,
            SUM(CASE WHEN QUARTER(wh.work_date) = 1 THEN wh.hours_worked ELSE 0 END) AS Q1_total,
            SUM(CASE WHEN QUARTER(wh.work_date) = 2 THEN wh.hours_worked ELSE 0 END) AS Q2_total,
            SUM(CASE WHEN QUARTER(wh.work_date) = 3 THEN wh.hours_worked ELSE 0 END) AS Q3_total,
            SUM(CASE WHEN QUARTER(wh.work_date) = 4 THEN wh.hours_worked ELSE 0 END) AS Q4_total
        FROM employee e
        JOIN work_hours wh ON e.employee_id = wh.employee_id
        WHERE 
            NOT (YEAR(wh.work_date) = YEAR(CURDATE()) 
            AND QUARTER(wh.work_date) = QUARTER(CURDATE())) 
        GROUP BY e.employee_id
        ORDER BY e.employee_id ASC;
        """

        # Execute Query
        cursor.execute(query)
        results = cursor.fetchall()

        # Define column headers
        headers = ["First Name", "Last Name", "Q1", "Q2", "Q3", "Q4"]

        # Determine column widths dynamically
        col_widths = [max(len(str(row[i])) for row in results + [headers])
                      for i in range(len(headers))]

        # Print table header
        print(" | ".join(header.ljust(col_widths[i])
              for i, header in enumerate(headers)))
        # Print a separator line
        print("-" * (sum(col_widths) + (len(headers) - 1) * 3))

        # Print each row in the table
        for row in results:
            print(" | ".join(str(row[i]).ljust(col_widths[i])
                  for i in range(len(row))))

    # Creating a menu to display the reports

    def select_reports():
        print("\n Report Menu:")
        print("\n 1. Supplier Report")
        print("\n 2. Wine Report")
        print("\n 3. Employee Report")
        print("\n 4. Exit\n")

        choice = input("Please make a selection 1-4: ")
        return choice

    while True:
        choice = select_reports()

        if choice == "1":
            print("\nGenerating Supplier Report... \n")
            get_supplier_delivery_performance()
            plot_supplier_delivery_trends()

        elif choice == "2":
            print("\nGenerating Wine Report... \n")
            get_wine_performance()
            plot_sales_trends()

        elif choice == "3":
            print("\nGenerating Employee Report... \n")
            get_employee_performance()

        elif choice == "4":
            print("\nExiting... \n")
            break

        else:
            print("\nInvalid choice! Please select a valid option... \n")


# Close the cursor.
    cursor.close()


except mysql.connector.Error as err:
    error_message = ""

    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        error_message = f"Error: The supplied username or password are invalid. MySQL Error Code: {err.errno}"

    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        error_message = f"Error: The specified database does not exist. MySQL Error Code: {err.errno}"

    else:
        error_message = f"General MySQL Error: {err}"

    print(error_message)  # Prints the error for immediate feedback.
    logging.error(error_message)  # Logs the error message.
    # Logs the full traceback for debugging.
    logging.error(traceback.format_exc())

finally:
    # Close the connection to MySQL
    if 'db' in locals() and db.is_connected():
        db.close()
        print("\n  Connection closed safely.")
