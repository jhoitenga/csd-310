# Title: movies_queries.py
# Author: Jennifer Hoitenga
# Date: 01/29/2025

# Import Statements
import mysql.connector  # to connect
from mysql.connector import errorcode
import traceback  # for detailed error diagnostics
import logging  # for logging errors
import dotenv  # to use .env file
from dotenv import dotenv_values

# Configure Logging
LOG_FILE = "error_log.txt"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Using our .env file
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
    # Try/catch block for handling potential MySQL database errors

    # Connect to the movies database
    db = mysql.connector.connect(**config)

    # Output the connection status
    print("\n  Database user {} connected to MySQL on host {} with database {}".format(
        config["user"], config["host"], config["database"]))

    # Open a new cursor for executing database queries.
    cursor = db.cursor()

    # Function to display films.
    def show_films(cursor, title):

        # Inner join query to pull all films.
        query = """
        SELECT film.film_name AS name, 
            film.film_director AS director, 
            genre.genre_name AS genre, 
            studio.studio_name AS studio_name 
        FROM film 
            INNER JOIN genre ON film.genre_id = genre.genre_id 
            INNER JOIN studio ON film.studio_id = studio.studio_id;
        """
        cursor.execute(query)
        films = cursor.fetchall()
        print("\n -- {} --".format(title))
        # Iterate over the film data set and display the results.
        for film in films:
            print("Film Name: {}\nDirector: {}\nGenre Name ID: {}\nStudio Name: {}\n". format(
                film[0], film[1], film[2], film[3]))

    # Call the show_films function to display the initial films.
    show_films(cursor, "DISPLAYING FILMS")

    # Insert a new record into the film table.
    insert_query = """
    INSERT INTO film (film_name, film_releaseDate, film_runtime, film_director, studio_id, genre_id)
    VALUES('Speak No Evil', '2024', '110', 'James Watkins','2','1')
    """
    cursor.execute(insert_query)
    db.commit()

    # Call the show_films function to display films after insert.
    show_films(cursor, "DISPLAYING FILMS AFTER INSERT")

    # Update the film "Alien" to a Horror genre.
    cursor.execute("UPDATE film SET genre_id = '1' WHERE film_name = 'Alien';")
    db.commit()

    # Call the show_films function to display films after update.
    show_films(cursor, "DISPLAYING FILMS AFTER UPDATE")

    # Delete the film "Gladiator".
    cursor.execute("DELETE FROM film WHERE film_name = 'Gladiator';")
    db.commit()

    # Call the show_films function to display films after delete.
    show_films(cursor, "DISPLAYING FILMS AFTER DELETE")

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
        print("Connection closed safely.")
