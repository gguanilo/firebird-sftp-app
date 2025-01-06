import logging
import sqlite3

from utils.errors import SQLiteConnectionError, SQLiteQueryError


class SQLiteHandler:
    def __init__(self, database_path):
        """
        Initializes the SQLite connection handler.

        :param database_path: Path to the SQLite database file
        """
        self.database_path = database_path
        self.connection = None

    def connect(self):
        """
        Establishes a connection to the SQLite database.
        """
        try:
            self.connection = sqlite3.connect(self.database_path)
            logging.info("Successfully connected to SQLite.")
        except sqlite3.Error as e:
            logging.error(f"Error connecting to the database: {e}")
            raise SQLiteConnectionError(f"Error connecting to the database: {e}")

    def insert_task(self, task_name, query, output_file, remote_path, sftp_host, sftp_user, cron_expression):
        """
        Inserts a scheduled task into the database.

        :param task_name: Name of the task
        :param query: SQL query associated with the task
        :param output_file: Output file name for the task
        :param remote_path: Remote path for the task
        :param sftp_host: SFTP host for the task
        :param sftp_user: SFTP user for the task
        :param cron_expression: Cron expression for the task's schedule
        """
        try:
            if not self.connection:
                raise SQLiteConnectionError("No connection established with the database.")

            cursor = self.connection.cursor()
            insert_query = (
                "INSERT INTO scheduled_tasks (task_name, query, output_file, remote_path, sftp_host, sftp_user, cron_expression) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            )

            cursor.execute(insert_query,
                           (task_name, query, output_file, remote_path, sftp_host, sftp_user, cron_expression))
            self.connection.commit()
            logging.info(f"Task '{task_name}' inserted successfully.")

            cursor.close()
        except SQLiteConnectionError as e:
            logging.error(f"Error Connection: {e}")
            raise
        except sqlite3.Error as e:
            logging.error(f"Error inserting task: {e}")
            raise SQLiteQueryError(f"Error inserting task: {e}")
        except Exception as ex:
            logging.error(f"General error inserting the task: {ex}")
            raise Exception(f"General error inserting the task: {ex}")

    def get_tasks(self):
        """
        Fetches all scheduled tasks from the database.

        :return: A list of dictionaries representing tasks
        """
        try:
            if not self.connection:
                raise SQLiteConnectionError("No connection established with the database.")

            cursor = self.connection.cursor()
            query = "SELECT id, task_name, query, output_file, remote_path, sftp_host, sftp_user, cron_expression, created_at, status FROM scheduled_tasks"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            tasks = [dict(zip(columns, row)) for row in rows]

            logging.info(f"Fetched {len(tasks)} tasks from the database.")

            cursor.close()
            return tasks
        except SQLiteConnectionError as e:
            logging.error(f"Error Connection: {e}")
            raise
        except sqlite3.Error as e:
            logging.error(f"Error fetching tasks: {e}")
            raise SQLiteQueryError(f"Error fetching tasks: {e}")
        except Exception as ex:
            logging.error(f"General error fetching tasks: {ex}")
            raise Exception(f"General error fetching tasks: {ex}")

    def close(self):
        """
        Closes the connection to the SQLite database.
        """
        if self.connection:
            self.connection.close()
            logging.info("Connection closed.")
