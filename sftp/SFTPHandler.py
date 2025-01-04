import paramiko
import logging


class SFTPHandler:
    def __init__(self, host, username, password, port=22):
        """
        Initialize the SFTPHandler with the necessary connection details.

        :param host: SFTP server host
        :param username: Username for authentication
        :param password: Password for authentication
        :param port: Port for the SFTP server (default is 22)
        """
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.client = None
        self.sftp = None

    def connect(self):
        """
        Establish a connection to the SFTP server.
        """
        try:
            self.client = paramiko.Transport((self.host, self.port))
            self.client.connect(username=self.username, password=self.password)
            self.sftp = paramiko.SFTPClient.from_transport(self.client)
            logging.info("Successfully connected to the SFTP server.")
        except Exception as e:
            logging.error(f"Failed to connect to the SFTP server: {e}")
            raise

    def upload_file(self, local_path, remote_path):
        """
        Upload a file to the SFTP server.

        :param local_path: Path to the local file
        :param remote_path: Path on the SFTP server where the file will be uploaded
        """
        try:
            if not self.sftp:
                raise ConnectionError("SFTP connection is not established.")

            self.sftp.put(local_path, remote_path)
            logging.info(f"File '{local_path}' uploaded successfully to '{remote_path}'.")
        except Exception as e:
            logging.error(f"Failed to upload file: {e}")
            raise

    def close_connection(self):
        """
        Close the SFTP connection.
        """
        try:
            if self.sftp:
                self.sftp.close()
            if self.client:
                self.client.close()
            logging.info("SFTP connection closed.")
        except Exception as e:
            logging.error(f"Failed to close the connection: {e}")
