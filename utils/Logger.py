import logging


class Logger:
    """
    Class to handle logging configuration for the entire project.
    """

    @staticmethod
    def setup_logging(log_file="app.log"):
        """
        Configures the logging module.

        :param log_file: Name of the file where logs will be saved
        """
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s [%(filename)s]",
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
