import logging
import traceback

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
        Instrumentado para medir el tiempo de ejecución y registrar información relevante.

        :param query: SQL query to execute
        :param output_file: Name of the CSV file to save the results
        """
        import time
        try:
            start_time = time.time()
            if not self.connection:
                raise FirebirdConnectionError("No connection established with the database.")

            # Crear un cursor para ejecutar la consulta
            cursor = self.connection.cursor()

            logging.info(f"Ejecutando consulta: {query}")
            # Ejecutar la consulta
            cursor.execute(query)

            # Obtener resultados y nombres de columnas
            rows = cursor.fetchall()
            num_rows = len(rows)
            columns = [desc[0] for desc in cursor.description]

            # Crear un DataFrame con los resultados
            df = pd.DataFrame(rows, columns=columns)

            # Guardar el DataFrame en un archivo CSV
            df.to_csv(output_file, index=False, encoding='utf-8')

            elapsed_time = time.time() - start_time
            logging.info(
                f"Consulta ejecutada y resultados guardados en {output_file} en {elapsed_time:.2f} segundos. Filas obtenidas: {num_rows}")

            # Cerrar el cursor
            cursor.close()
        except FirebirdConnectionError as e:
            traceback.print_exc()
            logging.error(f"Error de conexión: {e}")
            raise
        except fdb.DatabaseError as e:
            traceback.print_exc()
            logging.error(f"Error al ejecutar la consulta: {e}")
            raise FirebirdQueryError(f"Error al ejecutar la consulta: {e}")
        except Exception as ex:
            traceback.print_exc()
            logging.error(f"Error general al procesar la consulta: {ex}")
            raise Exception(f"Error general al procesar la consulta: {ex}")

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
            traceback.print_exc()
            logging.error(f"Error Connection: {e}")
            raise
        except fdb.DatabaseError as e:
            traceback.print_exc()
            logging.error(f"Error fetching tasks: {e}")
            raise FirebirdQueryError(f"Error fetching tasks: {e}")
        except Exception as ex:
            traceback.print_exc()
            logging.error(f"General error fetching tasks: {ex}")
            raise Exception(f"General error fetching tasks: {ex}")

    def close(self):
        """
        Closes the connection to the Firebird database.
        """
        if self.connection:
            self.connection.close()
            logging.info("Connection closed.")