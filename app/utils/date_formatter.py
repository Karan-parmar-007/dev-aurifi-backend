# Create a new file: app/utils/date_formatter.py
import pandas as pd
import numpy as np

class DateFormatter:
    """Utility class to handle date formatting consistently across the application"""
    
    @staticmethod
    def standardize_date_column(series, format='%d/%m/%Y'):
        """
        Standardize a date column to the specified format.
        
        Args:
            series: Pandas Series containing date values
            format: Desired output format (default: dd/mm/yyyy)
            
        Returns:
            Pandas Series with standardized date strings
        """
        try:
            # Try to parse dates with dayfirst=True
            dates = pd.to_datetime(series, dayfirst=True, errors='coerce')
            
            # Convert to desired format
            formatted = dates.dt.strftime(format)
            
            # Replace NaT with original values or empty string
            mask = dates.isna()
            if mask.any():
                formatted[mask] = series[mask].fillna('')
                
            return formatted
        except Exception as e:
            # If conversion fails, return original series
            return series
    
    @staticmethod
    def format_dataframe_dates(df, date_columns, format='%d/%m/%Y'):
        """
        Format all date columns in a dataframe.
        
        Args:
            df: Pandas DataFrame
            date_columns: List of column names that contain dates
            format: Desired output format
            
        Returns:
            DataFrame with formatted date columns
        """
        df_copy = df.copy()
        
        for col in date_columns:
            if col in df_copy.columns:
                df_copy[col] = DateFormatter.standardize_date_column(df_copy[col], format)
                
        return df_copy
    
    @staticmethod
    def is_valid_date_format(value, expected_format='%d/%m/%Y'):
        """
        Check if a value matches the expected date format.
        
        Args:
            value: String value to check
            expected_format: Expected format
            
        Returns:
            Boolean indicating if format matches
        """
        try:
            pd.to_datetime(value, format=expected_format)
            return True
        except:
            return False