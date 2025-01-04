import logging

import fdb
import pandas as pd

from utils.errors import FirebirdConnectionError, FirebirdQueryError


class FirebirdHandler:
    def __init__(self, host, port, database, user, password):
        """
        Initializes the Firebird connection handler.

        :param host: Firebird server address
        :param port: Firebird server port
        :param database: Full path to the database
        :param user: Database user
        :param password: Database password
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None

    def connect(self):
        """
        Establishes a connection to the Firebird database.
        """
        try:
            self.connection = fdb.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            logging.info("Successfully connected to Firebird.")
        except fdb.DatabaseError as e:
            logging.error(f"Error connecting to the database: {e}")
            raise FirebirdConnectionError(f"Error connecting to the database: {e}")

    def execute_query_to_csv(self, query, output_file):
        """
        Executes a query on the Firebird database and saves the results to a CSV file.

        :param query: SQL query to execute
        :param output_file: Name of the CSV file to save the results
        """
        try:
            if not self.connection:
                raise FirebirdConnectionError("No connection established with the database.")

            # Create a cursor to execute queries
            cursor = self.connection.cursor()

            # Execute the query
            cursor.execute(query)

            # Fetch results and column names
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            # Create a DataFrame with the results
            df = pd.DataFrame(rows, columns=columns)

            # Save the DataFrame to a CSV file
            df.to_csv(output_file, index=False, encoding='utf-8')

            logging.info(f"Results saved to {output_file}.")

            # Close the cursor
            cursor.close()
        except FirebirdConnectionError as e:
            logging.error(f"Error Connection: {e}")
            raise
        except fdb.DatabaseError as e:
            logging.error(f"Error executing query: {e}")
            raise FirebirdQueryError(f"Error executing query: {e}")
        except Exception as ex:
            logging.error(f"General error processing the query: {ex}")
            raise Exception(f"General error processing the query: {ex}")

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
                raise FirebirdConnectionError("No connection established with the database.")

            cursor = self.connection.cursor()
            insert_query = (
                "INSERT INTO scheduled_tasks (task_name, query, output_file, remote_path, sftp_host, sftp_user, cron_expression) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            )

            cursor.execute(insert_query, (task_name, query, output_file, remote_path, sftp_host, sftp_user, cron_expression))
            self.connection.commit()
            logging.info(f"Task '{task_name}' inserted successfully.")

            cursor.close()
        except FirebirdConnectionError as e:
            logging.error(f"Error Connection: {e}")
            raise
        except fdb.DatabaseError as e:
            logging.error(f"Error inserting task: {e}")
            raise FirebirdQueryError(f"Error inserting task: {e}")
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
                raise FirebirdConnectionError("No connection established with the database.")

            cursor = self.connection.cursor()
            query = "SELECT id, task_name, query, output_file, remote_path, sftp_host, sftp_user, cron_expression, created_at, status FROM scheduled_tasks"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            tasks = [dict(zip(columns, row)) for row in rows]

            logging.info(f"Fetched {len(tasks)} tasks from the database.")

            cursor.close()
            return tasks
        except FirebirdConnectionError as e:
            logging.error(f"Error Connection: {e}")
            raise
        except fdb.DatabaseError as e:
            logging.error(f"Error fetching tasks: {e}")
            raise FirebirdQueryError(f"Error fetching tasks: {e}")
        except Exception as ex:
            logging.error(f"General error fetching tasks: {ex}")
            raise Exception(f"General error fetching tasks: {ex}")

    def close(self):
        """
        Closes the connection to the Firebird database.
        """
        if self.connection:
            self.connection.close()
            logging.info("Connection closed.")