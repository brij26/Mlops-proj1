import os
import sys
import pymongo
import certifi

from src.exception import MyException
from src.logger import logging
from src.constants import DATABASE_NAME, MONGODB_URL_KEY

# Load the certificate authority file to avoid timeout errors when connecting to MongoDB
ca = certifi.where()


class MongodbClient:
    """
    mongodbclient is responsible for establishing a connection to the MongoDB database.

    Attributes:
    -----------
    client : MongoClient
        A shared MongoClient instance for the class.
    database : Database
        The specific database instance that MongoDBClient connects to.

    Methods:
    --------
    __init__(database_name:str)->None:
        Initialize the MongoDB connection using the given database name.
    """

    client = None  # Shared MongoClient instance across all MongoDBClient instances

    def __init__(self, database_name: str = DATABASE_NAME) -> None:
        """
        Initialize a connection to the MongoDB Database. If no existing connection is found, it establish a new one.

        Parameters:
        ----------
        database_name:str, optional
            Name of the mongodb database to connect to. Default is set by DATABASE_NAME constant.

        Raises:
        --------
        MyException:
            If there is any issue connecting to MongoDB or if the environment variable for the MongoDB URL is not set.
        """
        try:
            # Check if a MongoDB client connection has already been established; if not, create a new one
            if MongodbClient.client is None:
                # Retrive MongoDB URL from environment variables
                mongo_db_url = os.getenv(MONGODB_URL_KEY)
                if mongo_db_url is None:
                    raise Exception(
                        f"Environment variable '{MONGODB_URL_KEY}' is not set.")

                # establish a new MongoDB client connection
                MongodbClient.client = pymongo.MongoClient(
                    mongo_db_url, tlsCAFile=ca)

            # Use the shared MongoClient fot this instance
            self.client = MongodbClient.client
            # connect to the specified database
            self.database = self.client[database_name]
            self.database_name = database_name
            logging.info("MongoDB connection is successful.")

        except Exception as e:
            # Raise a custom exception with traceback details if connection fails
            raise MyException(e, sys)
