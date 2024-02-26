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
        self.weather_map_data = config_params['weather_mapping_csv']

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

        This method swaps the column names 'Annual_yield' and 'Crop_type' in the DataFrame.
        """
        if 'Annual_yield' in self.df.columns and 'Crop_type' in self.df.columns:
            # Rename 'Annual_yield' to temporary name
            self.df.rename(columns={'Annual_yield': 'Crop_type_Temp'}, inplace=True)
            # Rename 'Crop_type' to 'Annual_yield'
            self.df.rename(columns={'Crop_type': 'Annual_yield'}, inplace=True)
            # Rename 'Crop_type_Temp' to 'Crop_type'
            self.df.rename(columns={'Crop_type_Temp': 'Crop_type'}, inplace=True)
            self.logger.info("Swapped columns: 'Annual_yield' with 'Crop_type'")
        else:
            self.logger.warning("Columns 'Annual_yield' and 'Crop_type' not found in DataFrame.")

    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
        Applies corrections to specified columns in the DataFrame.

        This method takes the absolute values for the specified column and applies
        corrections to the specified column using a predefined function.

        Parameters:
        - column_name (str): The name of the column to apply corrections to. Defaults to 'Crop_type'.
        - abs_column (str): The name of the column where absolute values should be taken. Defaults to 'Elevation'.
        """
        # Take absolute values for the specified column
        if abs_column in self.df.columns:
            self.df[abs_column] = self.df[abs_column].abs()
            self.logger.info(f"Took absolute values for '{abs_column}' column.")
        else:
            self.logger.warning(f"Column '{abs_column}' not found in DataFrame.")

        # Define the correction function for the Crop_type column
        def correct_crop_type(crop):
            crop = crop.strip()  # Remove trailing spaces
            corrections = {
                'cassaval': 'cassava',
                'wheatn': 'wheat',
                'teaa': 'tea'
            }
            return corrections.get(crop, crop)  # Get the corrected crop type, or return the original if not in corrections

        # Apply the correction function to the specified column
        if column_name in self.df.columns:
            self.df[column_name] = self.df[column_name].apply(correct_crop_type)
            self.logger.info(f"Applied corrections to '{column_name}' column.")
        else:
            self.logger.warning(f"Column '{column_name}' not found in DataFrame.")

    def weather_station_mapping(self):
        """
        Fetches weather station mapping data from a CSV file.

        Returns:
            DataFrame: The DataFrame containing the weather station mapping data.
        """
        weather_map_df = read_from_web_CSV(self.weather_map_data)
        self.df = self.df.merge(weather_map_df, on='Field_ID',how='left')

    def process(self):
        """
        Executes the data processing pipeline.

        This method sequentially calls all the necessary methods to process the data.
        """
        # Step 1: Ingest SQL data
        self.ingest_sql_data()

        # Step 2: Rename columns
        self.rename_columns()

        # Step 3: Apply corrections
        self.apply_corrections()

        # Step 4: Weather station mapping
        self.weather_station_mapping()
        
        # Remove any columns with names containing 'Unnamed' followed by any number
        unnamed_columns = [col for col in self.df.columns if re.match(r'^Unnamed:\s*\d*$', col)]
        self.df.drop(columns=unnamed_columns, inplace=True)
