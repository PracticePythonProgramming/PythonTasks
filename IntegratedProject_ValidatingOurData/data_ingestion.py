#!/usr/bin/python3
from sqlalchemy import create_engine, text
import logging
import pandas as pd
# Name logger for module-specific logs in data_ingestion module.
logger = logging.getLogger('data_ingestion')
# Configure logger to print timestamp, logger name, and message.
logging.basicConfig(level=logging.INFO, format='%(asctime)s \
    - %(name)s - %(levelname)s - %(message)s')


def create_db_engine(db_path):
    """
    Creates a SQLAlchemy database engine object.

    Parameters:
    db_path (str): The path to the SQLite database.

    Returns:
    engine: The SQLAlchemy database engine object.

    Raises:
    ImportError: If SQLAlchemy is not installed.
    Exception: If failed to create the database engine.
    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine  # Return the engine object if it all works well
    except ImportError:  # Handle missing SQLAlchemy with ImportError
        logger.error("SQLAlchemy is required to use this function. \
            Please install it first.")
        raise e
    except Exception as e:  # If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e


def query_data(engine, sql_query):
    """
    Executes a SQL query on the given database engine \
        and returns the result as a DataFrame.

    Parameters:
    engine: The SQLAlchemy database engine object.
    sql_query (str): The SQL query to be executed.

    Returns:
    DataFrame: The result of the SQL query as a DataFrame.

    Raises:
    ValueError: If the query returns an empty DataFrame.
    Exception: If an error occurs while executing the query.
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e:
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. \
            Error: {e}")
        raise e


def read_from_web_CSV(URL):
    """
    Reads a CSV file from the web and returns its contents as a DataFrame.

    Parameters:
    URL (str): The URL of the CSV file to be read.

    Returns:
    DataFrame: The contents of the CSV file as a DataFrame.

    Raises:
    pd.errors.EmptyDataError: If the URL does not point to a valid CSV file.
    Exception: If failed to read the CSV file from the web.
    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. \
            Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e
