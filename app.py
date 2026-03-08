import streamlit as st
from google.cloud import bigquery
import pandas as pd
import requests

# --- CONFIGURATION ---
PROJECT_ID = "dh-assignment1"
DATASET_ID = "movie_db" # Change if your dataset is named differently
TMDB_API_KEY = "dee031756bbb4499f30c60fc595ce12b" # Get this from https://www.themoviedb.org/

# Initialize BigQuery Client
# On Cloud Run, this automatically authenticates. Locally, use gcloud auth application-default login
client = bigquery.Client(project=PROJECT_ID)

# --- HELPER FUNCTIONS ---
def execute_query(query: str) -> pd.DataFrame:
    """Executes a SQL query, prints it to the terminal, and returns a DataFrame."""
    print("\n" + "="*40)
    print("EXECUTING SQL QUERY:")
    print(query)
    print("="*40 + "\n")
    
    try:
        query_job = client.query(query)
        return query_job.to_dataframe()
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

def fetch_tmdb_details(tmdb_id):
    """Fetches movie poster and overview from TMDB."""
    if not TMDB_API_KEY or TMDB_API_KEY == "YOUR_TMDB_API_KEY_HERE":
        return None, "TMDB API key not configured."
        
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        poster_url = f"https://image.tmdb.org/t/p/w500{data.get('poster_path')}" if data.get('poster_path') else None
        overview = data.get('overview', 'No description available.')
        return poster_url, overview
    except requests.RequestException:
        return None, "Failed to fetch details from TMDB."

# --- MAIN UI ---
def main():
    st.set_page_config(page_title="Movie Explorer", layout="wide")
    st.title("🎬 Movie Database Explorer")
    st.markdown("Search and filter movies from our BigQuery database.")

    # Sidebar for Filters
    st.sidebar.header("Filters")
    
    # 1. Title Autocomplete (Simple Example)
    search_title = st.sidebar.text_input("Search by Title:")
    
    # 2. Genre Filter
    genre = st.sidebar.selectbox("Genre", ["All", "Comedy", "Drama", "Horror", "Action"]) # Expand this list!
    
    # 3. Year Filter
    min_year = st.sidebar.number_input("Released after year:", min_value=1900, max_value=2026, value=2010)

    # --- BUILD SQL QUERY ---
    # Start with a base query
    base_query = f"""
        SELECT movieId, title, genres, tmdbId, release_year
        FROM `{PROJECT_ID}.{DATASET_ID}.movies`
        WHERE 1=1
    """
    
    # Add conditions based on user input
    if search_title:
        base_query += f" AND LOWER(title) LIKE LOWER('%{search_title}%')"
    if genre != "All":
        base_query += f" AND genres LIKE '%{genre}%'"
    if min_year:
        base_query += f" AND release_year >= {min_year}"
        
    base_query += " LIMIT 20" # Limit results for UI performance

    # --- EXECUTE AND RENDER ---
    if st.sidebar.button("Search"):
        with st.spinner("Querying BigQuery..."):
            results_df = execute_query(base_query)
            
        if results_df.empty:
            st.warning("No movies found matching your criteria.")
        else:
            st.success(f"Found {len(results_df)} movies!")
            
            # Display results
            for index, row in results_df.iterrows():
                with st.expander(f"{row['title']} ({row['release_year']}) - {row['genres']}"):
                    col1, col2 = st.columns([1, 3])
                    
                    # Fetch extra details from TMDB
                    poster_url, overview = fetch_tmdb_details(row['tmdbId'])
                    
                    with col1:
                        if poster_url:
                            st.image(poster_url, use_column_width=True)
                        else:
                            st.write("No image available.")
                    
                    with col2:
                        st.write("**Overview:**")
                        st.write(overview)
                        st.write(f"**TMDB ID:** {row['tmdbId']}")
                        st.write(f"**Internal Movie ID:** {row['movieId']}")

if __name__ == "__main__":
    main()