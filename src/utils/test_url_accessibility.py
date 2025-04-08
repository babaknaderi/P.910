import pandas as pd
import requests

def test_urls(csv_file, url_column):
    """
    Test the accessibility of URLs in a given CSV file.

    Args:
        csv_file (str): Path to the CSV file.
        url_column (str): Name of the column containing the URLs.

    Returns:
        pd.DataFrame: DataFrame with an additional column indicating URL status.
    """
    # Read the CSV file
    df = pd.read_csv(csv_file)

    # Check if the URL column exists
    if url_column not in df.columns:
        raise ValueError(f"Column '{url_column}' not found in the CSV file.")

    # Function to test URL accessibility
    def check_url(url):
        try:
            response = requests.head(url, allow_redirects=True, timeout=5)
            if response.status_code == 200 and "video/mp4" in response.headers.get("Content-Type", ""):
                return "Accessible"
            else:
                return f"Error: {response.status_code}"
        except requests.RequestException as e:
            return f"Error: {str(e)}"

    # Apply the function to the URL column
    df["URL_Status"] = df[url_column].apply(check_url)

    return df

if __name__ == "__main__":
    # Path to the CSV file and the column containing URLs
    input_csv = r"C:\Users\vigopal\source\repos\P.910\src\04_02_2025\rating_source_b.csv"
    url_column_name = "pvs"

    # Test the URLs and save the results
    result_df = test_urls(input_csv, url_column_name)
    output_csv = input_csv.replace(".csv", "_url_status.csv")
    result_df.to_csv(output_csv, index=False)
    print(f"URL status saved to: {output_csv}")
