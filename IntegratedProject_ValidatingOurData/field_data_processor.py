import re
import numpy as np
import pandas as pd
import logging
from data_ingestion import create_db_engine, query_data, read_from_web_CSV


class FieldDataProcessor:

    def __init__(self, config_params, logging_level="INFO"):
        """
        Initialize the FieldDataProcessor class with attributes.

        Parameters:
        - config_params (dict): Dictionary containing configuration parameters.
        - logging_level (str): Optional parameter to specify the logging level.
        Defaults to "INFO".
        """
        self.db_path = config_params['db_path']
        self.sql_query = config_params['sql_query']
        self.columns_to_rename = config_params['columns_to_rename']
        self.values_to_rename = config_params['values_to_rename']
        self.weather_map_data = config_params['weather_map_data']

        self.initialize_logging(logging_level)

        self.df = None
        self.engine = None

    def initialize_logging(self, logging_level):
        """
        Initialize logging for the FieldDataProcessor class.

        Parameters:
        - logging_level (str): The desired logging level.
        """
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False

        # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO

        self.logger.setLevel(log_level)

        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - \
                %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def ingest_sql_data(self):
        """
        Fetches data from the SQL database using the specified query.
        Returns:
            DataFrame: The DataFrame containing the fetched data.
        """
        try:
            self.engine = create_db_engine(self.db_path)
            self.df = query_data(self.engine, self.sql_query)
            self.logger.info("Sucessfully loaded data.")
            return self.df
        except Exception as e:
            self.logger.error(f"Failed to ingest SQL data. Error: {e}")
            raise e

    def rename_columns(self):
        """
        Renames specified columns in the DataFrame.

        This method swaps the column names according to the
        configuration provided.
        """
        # Extract the columns to rename from the configuration
        column1, column2 = list(self.columns_to_rename.keys())[0], \
            list(self.columns_to_rename.values())[0]

        # Temporarily rename one of the columns to avoid a naming conflict
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"

        # Perform the swap
        self.df = self.df.rename(columns={column1: temp_name,
                                          column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})

        # Log the column name swap
        self.logger.info(f"Swapped columns: {column1} with {column2}")

    def apply_corrections(self, column_name='Crop_type',
                          abs_column='Elevation'):
        """
        Applies corrections to specified columns in the DataFrame.

        This method performs corrections such as taking absolute values and
        replacing values based on a mapping.

        Parameters:
        - column_name (str): The name of the column to apply corrections to.
        Defaults to 'Crop_type'.
        - abs_column (str): The name of the column where absolute values
        should be taken. Defaults to 'Elevation'.
        """
        # Take absolute values for the specified column
        self.df[abs_column] = self.df[abs_column].abs()

        # Apply value replacements based on the mapping
        self.df[column_name] = self.df[column_name]\
            .apply(lambda crop: self.values_to_rename.get(crop, crop))

    def weather_station_mapping(self):
        """
        Fetches weather station mapping data from a CSV file.

        Returns:
            DataFrame: The DataFrame containing the weather station
            mapping data.
        """
        self.df = read_from_web_CSV(self.weather_map_data)

    def process(self):
        """
        Executes the data processing pipeline.

        This method sequentially calls all the necessary methods
        to process the data.
        """
        # Step 1: Ingest SQL data
        self.ingest_sql_data()

        # Step 2: Rename columns
        self.rename_columns()

        # Step 3: Apply corrections
        self.apply_corrections()

        # Step 4: Weather station mapping
        self.weather_station_mapping()
