# Title: movies_queries.py
# Author: Jennifer Hoitenga
# Date: 01/23/2025

""" import statements """
import mysql.connector  # to connect
from mysql.connector import errorcode

import dotenv  # to use .env file
from dotenv import dotenv_values

# using our .env file
secrets = dotenv_values("C:\csd\csd-310\module-6/.env")

""" database config object """
config = {
    "user": secrets["USER"],
    "password": secrets["PASSWORD"],
    "host": secrets["HOST"],
    "database": secrets["DATABASE"],
    "raise_on_warnings": True  # not in .env file
}

try:
    """ try/catch block for handling potential MySQL database errors """

    # connect to the movies database
    db = mysql.connector.connect(**config)

    # output the connection status
    print("\n  Database user {} connected to MySQL on host {} with database {}".format(
        config["user"], config["host"], config["database"]))

    input("\n\n  Press any key to continue...")

    # Open a new cursor for executing database queries.
    cursor = db.cursor()

    # Select all fields for the studio table.
    cursor.execute("SELECT * FROM studio")
    studios = cursor.fetchall()
    # Print a header.
    print("\n-- DISPLAYING Studio RECORDS --")
    # Iterate through each record in the result set.
    for studio in studios:
        # Print the Studio ID and Studio Name for each record.
        print("Studio ID: {}\nStudio Name: {}\n".format(
            studio[0], studio[1]))

    # Select all fields for the genre table.
    cursor.execute("SELECT * FROM genre")
    genres = cursor.fetchall()
    # Print a header.
    print("\n-- DISPLAYING Genre RECORDS --")
    # Iterate through each record in the result set.
    for genre in genres:
        # Print the Genre ID and Genre Name for each record.
        print("Genre ID: {}\nGenre Name: {} \n".format(
            genre[0], genre[1]))

    # Select the movies that have a run time of less than two hours.
    cursor.execute(
        "SELECT film_name, film_runtime FROM film WHERE film_runtime < 120")  # 120 mins
    films = cursor.fetchall()
    # Print a header.
    print("\n-- DISPLAYING Short Film RECORDS --")
    # Iterate through each record in the result set.
    for film in films:
        # Print the Film Name and Runtime for each record.
        print("Film Name: {}\nRuntime: {}\n".format(
            film[0], film[1]))

    # Select a list of film names and directors grouped by director.
    cursor.execute(
        "SELECT film_name, film_director FROM film ORDER BY film_director;")
    films = cursor.fetchall()
    # Print a header.
    print("\n-- DISPLAYING Director RECORDS in Order --")
    # Iterate through each record in the result set.
    for film in films:
        # Print the Film Name and Director for each record.
        print("Film Name: {}\nDirector: {}\n".format(
            film[0], film[1]))

    # Close the cursor.
    cursor.close()

except mysql.connector.Error as err:
    """ on error code """

    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("  The supplied username or password are invalid")

    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("  The specified database does not exist")

    else:
        print(err)

finally:
    """ close the connection to MySQL """


# Close the database connection.
db.close()
