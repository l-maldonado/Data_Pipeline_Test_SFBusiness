# Data Cleaning and Transformation
# Perform any necessary data cleaning and transformation activities based on the characteristics of the dataset, as described in the notebook of Data_Analysis.ipynb.


import pandas as pd

# data is a pandas dataframe given as an argument to the method

def clean_transform_data(data):
    """
    Clean and preprocess a pandas DataFrame.

    Parameters:
    - data: pandas DataFrame to be cleaned.

    Returns:
    - df_cleaned: Cleaned pandas DataFrame to be returned.
    """
    
    if data:
        df = pd.DataFrame(data)
        # Perform data cleaning and transformations as needed
        # For example, drop columns, handle missing values, convert data types
        
        df = df.drop(columns=['lic', 'lic_code_description', 'naic_code', 'naic_code_description',
                              ':@computed_region_6qbp_sg9q', ':@computed_region_qgnn_b9vv', ':@computed_region_26cr_cadq',
                              ':@computed_region_ajp5_b2md', ':@computed_region_jwn9_ihcz'])
        
        # Drop rows with missing values
        #df_cleaned = df.dropna()
        df = df.dropna(how = 'all', axis = 1)
        #df = df.drop_duplicates()


        # Convert specified columns to numeric
        columns_to_convert = ['business_zip', 'mail_zipcode', 'supervisor_district']
        df[columns_to_convert] = df[columns_to_convert].apply(pd.to_numeric, errors='coerce')


        # Convert specified  date columns to datetime
        columns_to_datetime = ['dba_start_date', 'dba_end_date', 'location_start_date', 'location_end_date']
        df[columns_to_datetime] = df[columns_to_datetime].apply(pd.to_datetime, errors='coerce')
        
        # Fill all Nan values with "-", just to upload to Mongo, with the obvious loss of data. 
        df.fillna("-",inplace=True)
        
        
        df_cleaned = df
    
    
        return df_cleaned
    else:
        return None

    
#=========================================================================
    
