# app/utils/datatype_converter.py
import pandas as pd
import numpy as np
from datetime import datetime
from app.utils.logger import logger

class DataTypeConverter:
    """Utility class for converting column data types based on system column definitions"""
    
    @staticmethod
    def convert_column_datatype(series, target_datatype):
        """
        Convert a pandas Series to the target datatype
        
        Args:
            series: pandas Series to convert
            target_datatype: string indicating target datatype (e.g., 'number', 'integer', 'float', 'date', 'string', 'boolean')
            
        Returns:
            tuple: (converted_series, success, error_message)
        """
        try:
            if target_datatype.lower() in ['number', 'numeric', 'float', 'decimal']:
                # Convert to float to handle both integers and decimals
                converted = pd.to_numeric(series, errors='coerce')
                # Check if too many values failed conversion
                nan_count = converted.isna().sum() - series.isna().sum()
                if nan_count > len(series) * 0.1:  # More than 10% failed
                    return series, False, f"Too many values ({nan_count}) could not be converted to number"
                return converted, True, None
                
            elif target_datatype.lower() in ['integer', 'int']:
                # First convert to numeric, then try to convert to integer
                numeric_converted = pd.to_numeric(series, errors='coerce')
                # Check if values have decimal parts
                has_decimals = (numeric_converted % 1 != 0).any()
                if has_decimals:
                    return series, False, "Column contains decimal values, cannot convert to integer"
                
                converted = numeric_converted.astype('Int64')  # Use nullable integer type
                # Check if too many values failed conversion
                nan_count = converted.isna().sum() - series.isna().sum()
                if nan_count > len(series) * 0.1:  # More than 10% failed
                    return series, False, f"Too many values ({nan_count}) could not be converted to integer"
                return converted, True, None
                
            elif target_datatype.lower() in ['date', 'datetime']:
                # Try multiple date formats
                date_formats = [
                    '%Y-%m-%d',
                    '%d-%m-%Y',
                    '%m-%d-%Y',
                    '%Y/%m/%d',
                    '%d/%m/%Y',
                    '%m/%d/%Y',
                    '%Y-%m-%d %H:%M:%S',
                    '%d-%m-%Y %H:%M:%S',
                    '%m-%d-%Y %H:%M:%S'
                ]
                
                converted = None
                for fmt in date_formats:
                    try:
                        converted = pd.to_datetime(series, format=fmt, errors='coerce')
                        # If most values converted successfully, use this format
                        if converted.notna().sum() > len(series) * 0.5:
                            break
                    except:
                        continue
                
                # If no specific format worked well, use general parser
                if converted is None or converted.isna().sum() > len(series) * 0.5:
                    converted = pd.to_datetime(series, errors='coerce', infer_datetime_format=True)
                
                # Check if too many values failed conversion
                nat_count = converted.isna().sum() - series.isna().sum()
                if nat_count > len(series) * 0.1:  # More than 10% failed
                    return series, False, f"Too many values ({nat_count}) could not be converted to date"
                return converted, True, None
                
            elif target_datatype.lower() in ['boolean', 'bool']:
                # Map common boolean representations
                bool_map = {
                    'true': True, 'false': False,
                    'yes': True, 'no': False,
                    '1': True, '0': False,
                    't': True, 'f': False,
                    'y': True, 'n': False,
                    'TRUE': True, 'FALSE': False,
                    'YES': True, 'NO': False,
                    'True': True, 'False': False,
                    'Yes': True, 'No': False
                }
                # First try direct mapping
                converted = series.map(bool_map)
                
                # For unmapped values, try case-insensitive
                unmapped_mask = converted.isna() & series.notna()
                if unmapped_mask.any():
                    converted.loc[unmapped_mask] = series.loc[unmapped_mask].str.lower().map(bool_map)
                
                # Check if too many values couldn't be mapped
                unmapped = converted.isna().sum() - series.isna().sum()
                if unmapped > len(series) * 0.1:  # More than 10% failed
                    return series, False, f"Too many values ({unmapped}) could not be converted to boolean"
                return converted, True, None
                
            elif target_datatype.lower() in ['string', 'text', 'varchar']:
                # Convert to string, handling NaN values
                converted = series.fillna('').astype(str).replace('nan', '')
                return converted, True, None
                
            elif target_datatype.lower() in ['currency', 'money']:
                # Handle currency symbols and convert to numeric
                # Remove common currency symbols and commas
                cleaned = series.str.replace(r'[$£€¥₹,]', '', regex=True)
                converted = pd.to_numeric(cleaned, errors='coerce')
                
                # Check if too many values failed conversion
                nan_count = converted.isna().sum() - series.isna().sum()
                if nan_count > len(series) * 0.1:  # More than 10% failed
                    return series, False, f"Too many values ({nan_count}) could not be converted to currency"
                return converted, True, None
                
            else:
                return series, False, f"Unknown datatype: {target_datatype}"
                
        except Exception as e:
            logger.error(f"Error converting column datatype: {str(e)}")
            return series, False, str(e)
    
    @staticmethod
    def convert_dataframe_columns(df, column_datatype_mapping):
        """
        Convert multiple columns in a dataframe based on mapping
        
        Args:
            df: pandas DataFrame
            column_datatype_mapping: dict mapping column names to target datatypes
            
        Returns:
            tuple: (converted_df, success, error_details)
        """
        converted_df = df.copy()
        errors = {}
        overall_success = True
        
        for column, datatype in column_datatype_mapping.items():
            if column in converted_df.columns:
                logger.info(f"Converting column '{column}' to datatype '{datatype}'")
                converted_series, success, error_msg = DataTypeConverter.convert_column_datatype(
                    converted_df[column], datatype
                )
                if success:
                    converted_df[column] = converted_series
                    logger.info(f"Successfully converted column '{column}' to '{datatype}'")
                else:
                    overall_success = False
                    errors[column] = error_msg
                    logger.error(f"Failed to convert column '{column}': {error_msg}")
            else:
                errors[column] = f"Column '{column}' not found in dataframe"
                overall_success = False
                
        return converted_df, overall_success, errors
    
    @staticmethod
    def infer_datatype(series):
        """
        Try to infer the best datatype for a pandas Series
        
        Args:
            series: pandas Series to analyze
            
        Returns:
            str: Suggested datatype
        """
        # Remove NaN values for analysis
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return 'string'
        
        # Try boolean first (most restrictive)
        bool_values = {'true', 'false', 'yes', 'no', '1', '0', 't', 'f', 'y', 'n'}
        if all(str(v).lower() in bool_values for v in non_null):
            return 'boolean'
        
        # Try integer
        try:
            numeric_vals = pd.to_numeric(non_null, errors='coerce')
            if numeric_vals.notna().all() and (numeric_vals % 1 == 0).all():
                return 'integer'
        except:
            pass
        
        # Try float/number
        try:
            numeric_vals = pd.to_numeric(non_null, errors='coerce')
            if numeric_vals.notna().sum() / len(non_null) > 0.9:  # 90% can be converted
                return 'number'
        except:
            pass
        
        # Try date
        try:
            date_vals = pd.to_datetime(non_null, errors='coerce', infer_datetime_format=True)
            if date_vals.notna().sum() / len(non_null) > 0.9:  # 90% can be converted
                return 'date'
        except:
            pass
        
        # Default to string
        return 'string'