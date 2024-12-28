import logging
import os
from dotenv import load_dotenv
from firebird.FirebirdHandler import FirebirdHandler
from utils.Logger import Logger
from utils.errors import FirebirdConnectionError, FirebirdQueryError

if __name__ == "__main__":
    load_dotenv()

    Logger.setup_logging()

    # Connection data
    firebird_config = {
        "host": os.getenv("FIREBIRD_HOST"),
        "port": int(os.getenv("FIREBIRD_PORT")),
        "database": os.getenv("FIREBIRD_DATABASE"),
        "user": os.getenv("FIREBIRD_USER"),
        "password": os.getenv("FIREBIRD_PASSWORD")
    }


    query = "SELECT * FROM employees"

    try:
        firebird_handler = FirebirdHandler(**firebird_config)

        firebird_handler.connect()

        output_file = "query_result.csv"
        firebird_handler.execute_query_to_csv(query, output_file)

        firebird_handler.close()
        logging.info("Query executed successfully.")
    except FirebirdConnectionError as e:
        logging.error(e)
    except FirebirdQueryError as e:
        logging.error(e)
    except Exception as ex:
        logging.error(f"Unexpected error: {ex}")

