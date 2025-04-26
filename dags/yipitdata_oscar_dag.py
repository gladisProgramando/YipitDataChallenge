import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
import re

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.state import State

# Import cleaning functions (assuming utils is in the same dags folder orPYTHONPATH is set)
try:
    from utils.cleaning import get_decade, clean_budget
except ImportError:
    logging.error("Could not import cleaning functions. Ensure utils/cleaning.py is accessible.")
    # Define dummy functions to allow DAG parsing
    def get_decade(year_input: any) -> int | None: return None

    def clean_budget(budget_input: any) -> int: return 0


# --- Configuration ---
API_BASE_URL = "http://oscars.yipitdata.com/"
OUTPUT_DIR = Path(os.environ.get("AIRFLOW_OUTPUT_DIR", "/opt/airflow/output"))
OUTPUT_FILENAME = "oscar_movies_transformed.csv"

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --- DAG Definition ---
@dag(
    dag_id='yipitdata_oscar_pipeline',
    default_args=DEFAULT_ARGS,
    description='Scrape Oscar movie data, transform it, and save as CSV.',
    schedule_interval=None, # Manual trigger
    start_date=datetime(2023, 10, 26),
    catchup=False,
    tags=['yipitdata', 'etl', 'scraping', 'docker'],
)
def yipitdata_oscar_pipeline():
    """
    ### YipitData Oscar Movie ETL Pipeline

    This DAG performs the following steps:
    1.  **Extract**: Scrapes the initial list of movies from the YipitData Oscar API, handling pagination.
    2.  **Enrich**: Fetches detailed data (including budget) for each movie from its detail URL.
    3.  **Transform**: Cleans the data, including:
        *   Calculating the decade for each film's year.
        *   Cleaning the budget, converting to a USD integer (handling ranges, currencies via fixed rates, NaNs).
        *   Selecting and renaming required columns.
    4.  **Export**: Saves the transformed data to a CSV file in the specified output directory.
    """

    @task()
    def extract_initial_data() -> List[Dict[str, Any]]:
        """
        Fetches all movie entries from the paginated API endpoint.
        """
        all_movies = []
        url = API_BASE_URL
        page = 1
        max_pages = 100 # Safety break to prevent infinite loops

        while url and page <= max_pages:
            logging.info(f"Fetching data from: {url} (Page {page})")
            try:
                response = requests.get(url, timeout=30, allow_redirects=True) # Allow redirects is important here
                # Check if the response content type is JSON before trying to decode
                if 'application/json' not in response.headers.get('Content-Type', ''):
                     logging.error(f"Non-JSON response received from {url}. Status: {response.status_code}. Content-Type: {response.headers.get('Content-Type')}")
                     # Log first 500 chars of unexpected response for debugging
                     logging.error(f"Response text (partial): {response.text[:500]}")
                     # Decide how to handle: raise error, return partial data, or try next page?
                     # For robustness, let's try to continue if possible, but log severe warning.
                     # Or raise error to stop the DAG:
                     response.raise_for_status() # This might raise if status is bad, but maybe content type is wrong on 200 OK?
                     raise ValueError(f"Expected JSON content type, got {response.headers.get('Content-Type')}")


                response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
                data = response.json()

                movies = data.get('results', [])
                if not isinstance(movies, list):
                     logging.error(f"API response['results'] is not a list. Type: {type(movies)}. Url: {url}")
                     # Maybe the structure is different? Log the keys found.
                     logging.error(f"Keys found in response data: {data.keys()}")
                     raise ValueError("Expected 'results' key containing a list of movies.")

                all_movies.extend(movies)
                logging.info(f"Fetched {len(movies)} movies from page {page}. Total fetched: {len(all_movies)}")

                url = data.get('next') # Get URL for the next page
                page += 1

                if url:
                     time.sleep(0.5) 

            except requests.exceptions.RequestException as e:
                logging.error(f"API request failed for {url}: {e}")
                # Decide: retry? fail? return partial data? For now, fail the task.
                raise
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode JSON response from {url}: {e}")
                logging.error(f"Response text (partial): {response.text[:500]}")
                raise
            except (ValueError, KeyError) as e:
                 logging.error(f"Error processing API response structure from {url}: {e}")
                 raise


        if page > max_pages:
            logging.warning(f"Reached maximum page limit ({max_pages}). Stopping pagination.")

        logging.info(f"Finished extraction. Total movies fetched: {len(all_movies)}")
        if not all_movies:
            logging.warning("Extraction resulted in an empty list of movies.")
        return all_movies

    @task(max_active_tis_per_dag=5) # Limit concurrent detail fetches if needed
    def fetch_and_enrich_details(movies_groups: List[Dict[str, Any]]) -> List[Dict[str, Any]]: # Renamed input for clarity
        """
        Fetches detailed data for each individual movie using its 'Detail URL'
        from the grouped input structure and merges it.
        """
        enriched_movies_final = [] # This will be the final flat list of all processed movies
        if not movies_groups:
            logging.warning("No movie groups received for enrichment.")
            return []

        # --- Calculate total number of *individual* films first ---
        total_individual_films = 0
        for group in movies_groups:
            films_in_group = group.get('films', [])
            if isinstance(films_in_group, list):
                total_individual_films += len(films_in_group)
            else:
                logging.warning(f"Item in input list did not have a 'films' list: {group}")

        logging.info(f"Starting enrichment for {total_individual_films} individual films found within {len(movies_groups)} groups.")
        current_film_index = 0 # Counter for logging progress
        # --- Outer loop: Iterate through the list of groups/years ---
        for i, group_data in enumerate(movies_groups):
            # Safely get the list of films for the current group
            individual_films_list = group_data.get('films', [])
            year_info = group_data.get('year') # Get year for logging context
            # Check if we actually got a list
            if not isinstance(individual_films_list, list):
                logging.warning(f"Expected a list under 'films' key for group '{year_info}', but got {type(individual_films_list)}. Skipping this group.")
                continue            
            # --- Inner loop: Iterate through the actual movies in this group ---
            for movie in individual_films_list: 
                current_film_index += 1
                # Now 'movie' should be a dictionary like {'Film': 'Wings', 'Detail URL': '...', ...}
                logging.info(f"Processing film {current_film_index}/{total_individual_films} from group '{year_info}': {movie.get('Film', 'N/A')}")

                detail_url = movie.get('Detail URL')
                movie_title = movie.get('Film', f'Unknown Film #{current_film_index}') # Use Film name for logging

                if not detail_url:
                    logging.warning(f"Skipping enrichment for '{movie_title}': Missing 'Detail URL'.")
                    enriched_movies_final.append(movie) # Keep original data
                    continue

                logging.info(f"Fetching details for '{movie_title}' from {detail_url}")
                try:
                    if not detail_url.startswith(('http://', 'https://')):
                        logging.warning(f"Detail URL '{detail_url}' seems relative. Assuming base URL {API_BASE_URL}.")
                        detail_url = requests.compat.urljoin(API_BASE_URL, detail_url)

                    response = requests.get(detail_url, timeout=20)

                    detail_data = {} # Default to empty details
                    if 'application/json' in response.headers.get('Content-Type', ''):
                         response.raise_for_status()
                         detail_data = response.json()
                         logging.debug(f"Successfully fetched JSON details for '{movie_title}'.")
                    elif 'text/html' in response.headers.get('Content-Type', ''):
                         response.raise_for_status()
                         logging.warning(f"Received HTML page for '{movie_title}' ({detail_url}). Budget extraction might require HTML parsing.")

                    else:
                        logging.warning(f"Unexpected content type '{response.headers.get('Content-Type')}' for '{movie_title}' from {detail_url}. Status: {response.status_code}")
                        # response.raise_for_status() # Optionally raise error even for unknown type if status indicates failure

                    # Merge original data with detailed data
                    
                    movie_copy = movie.copy()
                    movie_copy['Year'] = year_info # Add the group's year info

                    merged_movie = {**movie_copy, **detail_data}
                    merged_movie['Year'] = movie_copy['Year']
                    enriched_movies_final.append(merged_movie)

                except requests.exceptions.RequestException as e:
                    logging.error(f"Failed to fetch details for '{movie_title}' from {detail_url}: {e}")
                    enriched_movies_final.append(movie) # Keep original data on failure
                except json.JSONDecodeError as e:
                     logging.error(f"Failed to decode JSON for '{movie_title}' detail response ({detail_url}): {e}")
                     enriched_movies_final.append(movie) # Keep original data
                except Exception as e: # Catch unexpected errors
                     logging.error(f"Unexpected error processing details for '{movie_title}' ({detail_url}): {e}")
                     enriched_movies_final.append(movie) # Keep original data

                time.sleep(0.3) # Be polite

        logging.info(f"Finished enrichment. Processed {len(enriched_movies_final)} individual movie records.")
        return enriched_movies_final # Return the flat list of all processed movies


    @task()
    def transform_data(movies: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Transforms the list of movie dictionaries into a cleaned pandas DataFrame.
        Applies cleaning functions for Year (Decade) and Budget.
        Selects and renames required columns.
        """
        if not movies:
            logging.warning("No movies received for transformation.")
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=['film', 'year', 'decade', 'wikipedia_url', 'is_oscar_winner', 'original_budget', 'budget_usd'])

        df = pd.DataFrame(movies)
        logging.info(f"Starting transformation of {len(df)} records. Initial columns: {df.columns.tolist()}")

        # Rename columns early if they differ from expected names (adjust based on actual API response)
        # Example renames (adjust based on actual keys in 'movies' dicts):
        rename_map = {
            'Film': 'film',
            'Year': 'year_raw', # Keep raw year for reference if needed
            'Winner': 'is_oscar_winner',
            'Detail URL': 'detail_url', # Keep for reference
            'Budget': 'original_budget', # Assume detail fetch puts budget here
            # Add other renames if needed, e.g., 'Wiki URL': 'wikipedia_url'
        }
        # Find the actual Wikipedia URL column name if it's different
        wiki_col_actual = 'wikipedia_url' # Default assumption
        possible_wiki_cols = ['Wiki URL', 'Wikipedia URL', 'wiki_url']
        for col in possible_wiki_cols:
            if col in df.columns:
                wiki_col_actual = col
                break
        if wiki_col_actual not in rename_map and wiki_col_actual != 'wikipedia_url':
             rename_map[wiki_col_actual] = 'wikipedia_url'

        df.rename(columns=rename_map, inplace=True)

        # Ensure required columns exist, adding empty ones if missing
        required_cols = ['film', 'year_raw', 'is_oscar_winner', 'wikipedia_url', 'original_budget']
        for col in required_cols:
            if col not in df.columns:
                logging.warning(f"Column '{col}' not found in data, adding as empty.")
                df[col] = pd.NA

        # 1. Clean Year and Create Decade column
        if 'year_raw' in df.columns :
                if df['year_raw'].notna().any():
                    df['year'] = df['year_raw'].apply(lambda x: re.search(r'\b(\d{4})\b', str(x)).group(1) if re.search(r'\b(\d{4})\b', str(x)) else None).astype('Int64')
                    decade_series = df['year'].apply(get_decade)
                    logging.info(f"Tipo de dato de decade_series antes de astype: {decade_series.dtype}")
                    df['decade'] = decade_series.astype('Int64')
                else:
                    logging.error("Column 'year_raw  (renamed from 'Year')' None.") 
                    df['year'] = None
                    df['decade'] = None
        else:
            logging.error("Column 'year_raw' (renamed from 'Year') not found. Cannot process year/decade.")
            df['year'] = None
            df['decade'] = None

        # 2. Clean Budget (Bonus)
        if 'original_budget' in df.columns:
             logging.info("Cleaning budget column...")
             # Make a copy of original budget before cleaning for the final schema
             df['budget_raw_preserved'] = df['original_budget']
             df['budget_usd'] = df['original_budget'].apply(clean_budget)
             # Overwrite 'original_budget' with the preserved raw value for the final schema
             df['original_budget'] = df['budget_raw_preserved']
             df.drop(columns=['budget_raw_preserved'], inplace=True)
             logging.info("Budget cleaning finished.")
        else:
             logging.warning("Column 'original_budget' (expected from detail enrichment) not found. Budget columns will be empty.")
             df['original_budget'] = None # Ensure column exists per schema request
             df['budget_usd'] = 0 # Default to 0 as per requirement

        # 3. Clean Oscar Winner (convert to boolean if possible)
        if 'is_oscar_winner' in df.columns:
             # Assuming 'Yes'/'No' or True/False or 1/0
             df['is_oscar_winner'] = df['is_oscar_winner'].apply(lambda x: True if str(x).strip().lower() in ['yes', 'true', '1'] else False).astype(bool)
        else:
             df['is_oscar_winner'] = False # Assume False if missing


        # 4. Select and Order Final Columns according to requirements
        final_schema = [
            'film',             # From 'Film'
            'year',             # Cleaned 4-digit year
            'decade',           # Calculated decade
            'wikipedia_url',    # From 'Wiki URL' or similar
            'is_oscar_winner',  # From 'Oscar Winner', cleaned to boolean
            'original_budget',  # Raw budget string from detail page
            'budget_usd'        # Cleaned budget as USD integer
        ]

        # Ensure all required columns are present, even if empty/None
        for col in final_schema:
            if col not in df.columns:
                logging.warning(f"Final schema column '{col}' was missing after processing, adding as None/default.")
                if col == 'budget_usd':
                     df[col] = 0
                elif col == 'is_oscar_winner':
                     df[col] = False
                else:
                     df[col] = None

        df_transformed = df[final_schema]

        logging.info(f"Transformation complete. Final DataFrame shape: {df_transformed.shape}")
        logging.info(f"Final columns: {df_transformed.columns.tolist()}")
        logging.info(f"Sample transformed data:\n{df_transformed.head().to_string()}")

        # Convert DataFrame to list of dicts for XComs (Airflow best practice for moderate data size)
        # If data is very large, save to intermediate file and pass path via XComs.
        #a = df_transformed.to_dict('records')
        #logging.info(f"DICTIONARY: >>>>>>>>>>>>>> {a}")
        df_final_for_xcom = df_transformed.astype(object).where(pd.notnull(df_transformed), None)
        logging.info(f"Sample data after NaN -> None conversion:\n{df_final_for_xcom.head().to_string()}")
        # -------------------------------------------------------------------------------

        # Convert the cleaned DataFrame (without NaNs) to list of dicts for XComs
        result_list = df_final_for_xcom.to_dict('records')

        # Optional: Log a sample of the final list to ensure None is present
        if result_list:
             logging.info(f"Sample record being pushed to XCom: {result_list[0]}")
        else:
            logging.warning("Result list for XCom is empty.")

        return result_list 
        #return df_transformed.to_dict('records')

    @task()
    def export_to_csv(movies_transformed: List[Dict[str, Any]]):

        logging.info(f"movies_transformed !!!!!!!!!!!!!!! {movies_transformed}")
        """
        Saves the transformed movie data to a CSV file.
        """
        if not movies_transformed:
            logging.warning("No transformed movie data received, skipping CSV export.")
            return


        df_final = pd.DataFrame(movies_transformed)
        output_path = OUTPUT_DIR / OUTPUT_FILENAME

        try:
            logging.info(f"Exporting {len(df_final)} records to CSV: {output_path}")
            df_final.to_csv(output_path, index=False, encoding='utf-8')
            logging.info(f"Successfully exported data to {output_path}")

            # Log summary statistics of the final data
            logging.info("--- Final Data Summary ---")
            logging.info(f"Total Records: {len(df_final)}")
            if 'decade' in df_final.columns:
                 logging.info(f"Movies per Decade:\n{df_final['decade'].value_counts().sort_index().to_string()}")
            if 'budget_usd' in df_final.columns:
                 logging.info(f"Budget (USD) Stats:\n{df_final['budget_usd'].describe().to_string()}")
            if 'is_oscar_winner' in df_final.columns:
                 logging.info(f"Oscar Winners:\n{df_final['is_oscar_winner'].value_counts().to_string()}")
            logging.info("-------------------------")


        except Exception as e:
            logging.error(f"Failed to export data to CSV at {output_path}: {e}")
            raise

    # --- Task Dependencies ---
    extracted_data = extract_initial_data()
    enriched_data = fetch_and_enrich_details(extracted_data)
    transformed_data_list = transform_data(enriched_data)
    export_to_csv(transformed_data_list)


# Instantiate the DAG
yipitdata_oscar_pipeline_dag = yipitdata_oscar_pipeline()