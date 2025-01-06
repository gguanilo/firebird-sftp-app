import logging
from colorlog import ColoredFormatter

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
        # File handler (for saving logs to a file)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s [%(filename)s]"
        ))

        # Console handler (for colored output in the terminal)
        console_handler = logging.StreamHandler()
        console_formatter = ColoredFormatter(
            "%(log_color)s%(asctime)s - %(levelname)s - %(message)s [%(filename)s]",
            log_colors={
                "DEBUG": "white",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        )
        console_handler.setFormatter(console_formatter)

        # Configure the root logger
        logging.basicConfig(
            level=logging.INFO,
            handlers=[file_handler, console_handler]
        )
