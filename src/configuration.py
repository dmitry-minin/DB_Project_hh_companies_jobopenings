from abc import ABC, abstractmethod
from configparser import ConfigParser
import os


class BasicConfigHandler(ABC):
    """
        Basic interface for configuration handling.
    """
    @abstractmethod
    def config(self) -> dict:
        """
            Read and return configuration parameters from the configuration file.
        """
        pass


class ConfigHandler(BasicConfigHandler):
    """
        Concrete implementation of BasicConfigHandler interface.
    """
    def __init__(self):
        """
         Initialize configuration handler with the path to the database.ini file.
        """
        self.config_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "database.ini")

    def config(self):
        """
            Read and return database connection parameters from the database.ini file.
        """
        parser = ConfigParser()
        parser.read(self.config_file_path)
        section = "postgresql"
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception(
                'Section {0} is not found in the {1} file.'.format(section, self.config_file_path))
        return db
