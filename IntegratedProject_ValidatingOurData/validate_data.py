import re
import numpy as np
import pandas as pd
from field_data_processor import FieldDataProcessor
from weather_data_processor import WeatherDataProcessor
from data_ingestion import create_db_engine, query_data, read_from_web_CSV
import logging 
import pytest

# Load the sampled CSV files
weather_df = pd.read_csv('sampled_weather_df.csv')
field_df = pd.read_csv('sampled_field_df.csv')

def test_read_weather_DataFrame_shape():
    assert weather_df.shape[0] > 0 and weather_df.shape[1] > 0, "Weather DataFrame shape is incorrect."

def test_read_field_DataFrame_shape():
    assert field_df.shape[0] > 0 and field_df.shape[1] > 0, "Field DataFrame shape is incorrect."

def test_weather_DataFrame_columns():
    expected_columns = ['Weather_station_ID', 'Message', 'Measurement', 'Value']  # Replace with your actual expected columns
    assert list(weather_df.columns) == expected_columns, "Weather DataFrame columns are incorrect."

def test_field_DataFrame_columns():
    expected_columns = ['Field_ID', 'Elevation', 'Latitude', 'Longitude', 'Location', 'Slope',
       'Rainfall', 'Min_temperature_C', 'Max_temperature_C', 'Ave_temps',
       'Soil_fertility', 'Soil_type', 'pH', 'Pollution_level', 'Plot_size',
       'Annual_yield', 'Crop_type', 'Standard_yield', 'Weather_station']  # Replace with your actual expected columns
    assert list(field_df.columns) == expected_columns, "Field DataFrame columns are incorrect."

def test_field_DataFrame_non_negative_elevation():
    assert (field_df['Elevation'] >= 0).all(), "Field DataFrame has negative elevation values."

def test_crop_types_are_valid():
    valid_crop_types = ['cassava', 'tea', 'wheat', 'potato', 'banana', 'coffee', 'rice','maize']  # Replace with your actual valid crop types
    assert field_df['Crop_type'].isin(valid_crop_types).all(), "Field DataFrame contains invalid crop types."

def test_positive_rainfall_values():
    assert (field_df['Rainfall'] > 0).all(), "Weather DataFrame has non-positive rainfall values."
