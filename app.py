import streamlit as st
from google.cloud import bigquery
import pandas as pd
import requests

# config
PROJECT_ID = "dh-assignment1"
DATASET_ID = "movie_db" 
TMDB_API_KEY = "dee031756bbb4499f30c60fc595ce12b" 

client = bigquery.Client(project=PROJECT_ID)

# languages
LANGUAGE_MAP = {
    "All": "",
    "English": "en",
    "French": "fr",
    "Italian": "it",
    "Spanish": "es",
    "German": "de",
    "Japanese": "ja",
    "Albanian": "sq",
    "Amharic": "am",
    "Arabic": "ar",
    "Armenian": "hy",
    "Aymara": "ay",
    "Bambara": "bm",
    "Basque": "eu",
    "Bengali": "bn",
    "Bosnian": "bs",
    "Bulgarian": "bg",
    "Catalan": "ca",
    "Chinese (Simplified)": "zh",
    "Chinese (Traditional)": "cn",
    "Croatian": "hr",
    "Czech": "cs",
    "Danish": "da",
    "Dutch": "nl",
    "Dzongkha": "dz",
    "Esperanto": "eo",
    "Estonian": "et",
    "Finnish": "fi",
    "Fulah": "ff",
    "Galician": "gl",
    "Georgian": "ka",
    "Greek": "el",
    "Hebrew": "he",
    "Hindi": "hi",
    "Hungarian": "hu",
    "Icelandic": "is",
    "Indonesian": "id",
    "Inuktitut": "iu",
    "Javanese": "jv",
    "Kazakh": "kk",
    "Kinyarwanda": "rw",
    "Korean": "ko",
    "Kurdish": "ku",
    "Lao": "lo",
    "Latin": "la",
    "Latvian": "lv",
    "Lingala": "ln",
    "Lithuanian": "lt",
    "Macedonian": "mk",
    "Malay": "ms",
    "Malayalam": "ml",
    "Marathi": "mr",
    "Mongolian": "mn",
    "Nepali": "ne",
    "Northern Sami": "se",
    "Norwegian": "no",
    "Norwegian Bokmål": "nb",
    "Pashto": "ps",
    "Persian": "fa",
    "Polish": "pl",
    "Portuguese": "pt",
    "Quechua": "qu",
    "Romanian": "ro",
    "Russian": "ru",
    "Sardinian": "sc",
    "Serbian": "sr",
    "Serbo-Croatian": "sh",
    "Slovak": "sk",
    "Slovenian": "sl",
    "Swedish": "sv",
    "Tagalog": "tl",
    "Tajik": "tg",
    "Tamil": "ta",
    "Telugu": "te",
    "Thai": "th",
    "Tibetan": "bo",
    "Tswana": "tn",
    "Turkish": "tr",
    "Ukrainian": "uk",
    "Urdu": "ur",
    "Uzbek": "uz",
    "Vietnamese": "vi",
    "Welsh": "cy",
    "Western Frisian": "fy",
    "Wolof": "wo",
    "Zulu": "zu"
}

# functions

# executing query
def execute_query(query: str) -> pd.DataFrame:
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

@st.cache_data(ttl=3600)
# spliting genre
def get_unique_genres():
    query = f"""
        SELECT DISTINCT genre 
        FROM `{PROJECT_ID}.{DATASET_ID}.movies`, 
        UNNEST(SPLIT(genres, '|')) as genre 
        WHERE genre != '(no genres listed)'
        ORDER BY genre
    """
    df = execute_query(query)
    if not df.empty and 'genre' in df.columns:
        return ["All"] + df['genre'].tolist()
    return ["All", "Action", "Adventure", "Comedy", "Drama", "Documentary", "Horror", "Western", "Romance", "War", "Fantasy", "Mistery", "Sci-Fi", "Thriller"]

@st.cache_data(ttl=86400)
# integration with API
def fetch_tmdb_details(tmdb_id):
    if not TMDB_API_KEY:
        return None, "TMDB API key not configured."
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        poster_url = f"https://image.tmdb.org/t/p/w500{data.get('poster_path')}" if data.get('poster_path') else None
        overview = data.get('overview', 'No overview available.')
        return poster_url, overview
    except requests.RequestException:
        return None, "Failed to fetch details from TMDB."

# UI logic
def sync_slider_to_inputs():
    st.session_state.min_year_input = st.session_state.year_range[0]
    st.session_state.max_year_input = st.session_state.year_range[1]

def sync_inputs_to_slider():
    min_y = st.session_state.min_year_input
    max_y = st.session_state.max_year_input
    if min_y > max_y: 
        min_y = max_y 
    st.session_state.year_range = (min_y, max_y)

# this execute sql
def trigger_search():
    st.session_state.run_search = True

# main ui
def main():
    st.set_page_config(page_title="Movie Explorer", layout="wide")
    st.title("🎬 Search the Best Movies in the World")
    st.markdown("Search and filter movies based on the largest dataset of movies and user ratings.")

    if "year_range" not in st.session_state:
        st.session_state.year_range = (1980, 2026)
    if "min_year_input" not in st.session_state:
        st.session_state.min_year_input = 1980
    if "max_year_input" not in st.session_state:
        st.session_state.max_year_input = 2026
    if "run_search" not in st.session_state:
        st.session_state.run_search = False
    if "results_df" not in st.session_state:
        st.session_state.results_df = None


    unique_genres = get_unique_genres()

    # sidebar filters
    st.sidebar.header("Search Filters")
    
    # text input trigger_search
    search_term = st.sidebar.text_input(
        "Search by title (Press Enter to search):", 
        "", 
        on_change=trigger_search
    )
    
    selected_language = st.sidebar.selectbox("Language:", list(LANGUAGE_MAP.keys()))
    language_code = LANGUAGE_MAP[selected_language]
    genre = st.sidebar.selectbox("Genre:", unique_genres)
    
    # year fields
    st.sidebar.write("### Release Year Range")
    st.sidebar.slider(
        "Use slider:", 1800, 2026, 
        key="year_range", on_change=sync_slider_to_inputs, label_visibility="collapsed"
    )
    
    col1, col2 = st.sidebar.columns(2)
    col1.number_input("Min Year", min_value=1800, max_value=2026, key="min_year_input", on_change=sync_inputs_to_slider)
    col2.number_input("Max Year", min_value=1800, max_value=2026, key="max_year_input", on_change=sync_inputs_to_slider)
    
    min_year = st.session_state.year_range[0]
    max_year = st.session_state.year_range[1]
    
    st.sidebar.markdown("---")
    min_rating, max_rating = st.sidebar.slider("Average rating range:", 0.0, 5.0, (3.0, 5.0), step=0.1)
    
    # Changed from Max to Min Votes
    min_votes = st.sidebar.number_input("Minimum number of votes:", min_value=1, max_value=10000, value=100)

    # search triggers trigger_search also
    st.sidebar.button("Search Movies", on_click=trigger_search)

    # building SQL query
    query = f"""
        SELECT 
            m.movieId, 
            m.title, 
            m.genres, 
            m.tmdbId, 
            m.language,
            m.release_year,
            ROUND(AVG(r.rating), 2) as avg_rating,
            COUNT(r.rating) as rating_count
        FROM `{PROJECT_ID}.{DATASET_ID}.movies` m
        JOIN `{PROJECT_ID}.{DATASET_ID}.ratings` r ON m.movieId = r.movieId
        WHERE m.release_year BETWEEN {min_year} AND {max_year}
    """
    
    if search_term:
        safe_term = search_term.replace("'", "\\'")
        query += f" AND LOWER(m.title) LIKE LOWER('%{safe_term}%')"
        
    if language_code:
        query += f" AND m.language = '{language_code}'"
    if genre != "All":
        query += f" AND m.genres LIKE '%{genre}%'"
        
    query += f"""
        GROUP BY 
            m.movieId, m.title, m.genres, m.tmdbId, m.language, m.release_year
        HAVING 
            avg_rating BETWEEN {min_rating} AND {max_rating}
            AND rating_count >= {min_votes}
        ORDER BY 
            avg_rating DESC, rating_count DESC
        LIMIT 50
    """

    # execute and rented
    if st.session_state.run_search:
        with st.spinner("Querying millions of ratings in BigQuery..."):
            st.session_state.results_df = execute_query(query)
        st.session_state.run_search = False

    # results
    if st.session_state.results_df is not None:
        df = st.session_state.results_df
        if df.empty:
            st.warning("No movies found matching these criteria.")
        else:
            st.success(f"Found {len(df)} movies! (Showing top results by number of votes)")
            
            REVERSE_LANGUAGE_MAP = {v: k for k, v in LANGUAGE_MAP.items() if v != ""}


            for index, row in df.iterrows():
                with st.expander(f"⭐ {row['avg_rating']} ({row['rating_count']} votes) | {row['title']} - {row['genres']}"):
                    col1, col2 = st.columns([1, 3])
                    
                    poster_url, overview = fetch_tmdb_details(row['tmdbId'])
                    
                    with col1:
                        if poster_url:
                            st.image(poster_url, use_container_width=True)
                        else:
                            st.write("No poster available")
                    
                    with col2:
                        full_language_name = REVERSE_LANGUAGE_MAP.get(row['language'], row['language'])
                        st.write("**Overview:**")
                        st.write(overview)
                        st.write(f"**Original Language:** {full_language_name}")
                        st.write(f"**Release Year:** {row['release_year']}")
                        st.write(f"**Average User Rating:** {row['avg_rating']} / 5.0 (based on {row['rating_count']} user ratings)")

if __name__ == "__main__":
    main()
