import sqlite3
import logging

def create_database(database_path):
    """
    Creates an SQLite database and initializes the required tables.

    :param database_path: Path to the SQLite database file
    """
    try:
        # Connect to the SQLite database (it will create the file if it doesn't exist)
        connection = sqlite3.connect(database_path)
        cursor = connection.cursor()

        # Create the 'scheduled_tasks' table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS scheduled_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_name TEXT NOT NULL,
            query TEXT NOT NULL,
            output_file TEXT NOT NULL,
            remote_path TEXT,
            sftp_host TEXT,
            sftp_user TEXT,
            cron_expression TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'pending',
            last_execution TIMESTAMP DEFAULT NULL,
            next_execution TIMESTAMP DEFAULT NULL
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
        logging.info("Database and table 'scheduled_tasks' created successfully.")

        # Close the connection
        cursor.close()
        connection.close()
    except sqlite3.Error as e:
        logging.error(f"Error creating the database: {e}")
        raise

# Define the path to the SQLite database file
database_path = "scheduled_tasks.db"

# Create the database and table
create_database(database_path)
