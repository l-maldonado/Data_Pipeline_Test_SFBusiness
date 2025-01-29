# Collect Data from the API Endpoint
# Using the "requests" library to fetch data from the API endpoint. We have to ensure that the API provides the data in a JSON format.


import requests

api_url = 'https://data.sfgov.org/resource/g8m3-pdis.json'

def fetch_data(api_url, query=None):
    """
    Fetch data from an API endpoint using the requests library.

    Parameters:
    - api_endpoint: API endpoint URL.
    - query: Query to be added to the endpoint (optional).
        More info at: https://dev.socrata.com/docs/filtering.html

    Returns:
    - data: Fetched data in JSON format.
    """
    
    # Construct the full endpoint URL with the query
    full_endpoint = f"{api_url}"
    
    if query:
        full_endpoint += f"?${query}"
        print(full_endpoint)

    try:
        # Make a GET request to the API endpoint
        response = requests.get(full_endpoint)
        
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            api_data = response.json()
            return api_data
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            api_data = None
            return api_data
   
    except Exception as e:
        print(f"Error: {e}")
        return None
    

#=========================================================================
# Example of Use:
if __name__ == "__main__":
    
    # Example API endpoint URL
    api_endpoint = "https://data.sfgov.org/resource/g8m3-pdis.json"

    # Example query 
    query = "within_box(location, 37.885001, -122.645939, 37.467011, -122.218516) limit 100"

    # Fetch data with the specified query
    fetched_data = fetch_data(api_endpoint, query)

    # Display the fetched data
    print("Fetched Data:")
    print(fetched_data)
    
    

# Example of use:
# Load data into Pandas Dataframe

#api_data = fetch_data(api_url)
#if api_data:
#    df = pd.DataFrame(api_data)
# Optionally, display the first few rows to inspect the data
#    print(df.head())
#else:
#    df = None
    
    
    
    
    
    


    
    
    
    
    




    
    

    
    

    
    
    
    
    

