import pandas as pd
import time
import re
import os
from io import StringIO
from typing import Optional, Dict
import random # For sleep

# --- IMPORTS ---
from bs4 import BeautifulSoup, Comment
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# --- Configuration ---
INDIVIDUAL_LEAGUES = {
    "Premier-League": {"id": 9, "name_in_url": "Premier-League"},
    "La-Liga": {"id": 12, "name_in_url": "La-Liga"},
    "Serie-A": {"id": 11, "name_in_url": "Serie-A"},
    "Bundesliga": {"id": 20, "name_in_url": "Bundesliga"},
    "Ligue-1": {"id": 13, "name_in_url": "Ligue-1"},
}

BIG5_CONFIG = {
    "id_in_url": "Big5",
    "name_in_url": "Big-5-European-Leagues"
}

BIG5_STAT_CATEGORIES_URL_MAP: Dict[str, str] = {
    "standard": "stats", "keeper": "keepers", "keeper_adv": "keepersadv",
    "shooting": "shooting", "passing": "passing", "passing_types": "passing_types",
    "gca": "gca", "defense": "defense", "possession": "possession",
    "playing_time": "playingtime", "misc": "misc"
}
STAT_CATEGORY_ITERATION_KEYS = list(BIG5_STAT_CATEGORIES_URL_MAP.keys())
PLAYER_STAT_URL_COMPONENTS = BIG5_STAT_CATEGORIES_URL_MAP
SQUAD_STAT_URL_COMPONENTS = BIG5_STAT_CATEGORIES_URL_MAP

# --- Helper Functions ---
def clean_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        new_cols = []
        for i, col_parts in enumerate(df.columns.values):
            str_col_parts = [str(p).strip() for p in col_parts]
            is_player_candidate = (len(str_col_parts) > 0 and str_col_parts[-1].lower() == 'player' and \
                                  (str_col_parts[0].lower().startswith('unnamed') or \
                                   not str_col_parts[0] or i == 0 or \
                                   (len(str_col_parts) > 1 and not str_col_parts[0] and str_col_parts[1].lower()=='player')))
            is_squad_candidate = (len(str_col_parts) > 0 and str_col_parts[-1].lower() == 'squad' and \
                                (str_col_parts[0].lower().startswith('unnamed') or \
                                 not str_col_parts[0] or i == 0 or \
                                 (len(str_col_parts) > 1 and not str_col_parts[0] and str_col_parts[1].lower()=='squad')))
            is_unnamed_top_level = str_col_parts[0].lower().startswith('unnamed')

            if is_player_candidate: joined_col = 'Player'
            elif is_squad_candidate: joined_col = 'Squad'
            elif len(str_col_parts) == 2 and is_unnamed_top_level and str_col_parts[1] and not str_col_parts[1].lower().startswith('unnamed'):
                joined_col = str_col_parts[1]
            else:
                filtered_parts = [p for p in str_col_parts if p and not p.lower().startswith('unnamed')]
                if not filtered_parts: filtered_parts = [p for p in str_col_parts if p] 
                joined_col = '_'.join(s for s in filtered_parts if s).strip().rstrip('_')
            new_cols.append(joined_col if joined_col else f"column_{i}")
        df.columns = new_cols
    else:
        df.columns = [str(col).strip() for col in df.columns]
    
    df.columns = [re.sub(r'_+', '_', col) for col in df.columns]
    df.columns = [re.sub(r'[^0-9a-zA-Z%#+_\s\-/]+', '', col).strip() for col in df.columns]
    df.columns = [col.replace(' ', '_') for col in df.columns]
    df.columns = [re.sub(r'_+', '_', col) for col in df.columns]
    df.columns = [col if col else f"unnamed_col_{i}" for i, col in enumerate(df.columns)]
    
    cols = list(df.columns); counts = {}; new_column_names = []
    for col_name in cols:
        original_col_name = col_name; current_count = counts.get(original_col_name, 0)
        if current_count > 0: new_column_names.append(f"{original_col_name}_{current_count}")
        else: new_column_names.append(original_col_name)
        counts[original_col_name] = current_count + 1
    df.columns = new_column_names
    return df

create_output_dir = lambda dir_name: os.makedirs(dir_name, exist_ok=True)

def find_table_directly_or_in_comment(soup_obj: BeautifulSoup,
                                        direct_table_id_exact: Optional[str] = None,
                                        comment_marker_string: Optional[str] = None,
                                        context_url: str = "N/A_URL") -> tuple[pd.DataFrame | None, BeautifulSoup | None]:
    table_element = None; df = None
    if direct_table_id_exact:
        wrapper_div = soup_obj.select_one(f'div[id="div_{direct_table_id_exact}"]')
        if wrapper_div:
            table_element = wrapper_div.select_one(f'table[id="{direct_table_id_exact}"]')
            if table_element: print(f"      -> Found direct table (id='{direct_table_id_exact}') within wrapper (id='div_{direct_table_id_exact}') on {context_url}")
        if not table_element:
            table_element = soup_obj.select_one(f'table[id="{direct_table_id_exact}"]')
            if table_element: print(f"      -> Found direct table with id exact '{direct_table_id_exact}' on {context_url}")
        if table_element:
            try:
                header_level = [0,1] if len(table_element.select('thead tr')) > 1 else 0
                df = pd.read_html(StringIO(str(table_element)), header=header_level)[0]
                return df, table_element
            except Exception as e: print(f"      -- Error parsing direct table (id='{direct_table_id_exact}'): {e}")

    if comment_marker_string and not df:
        comments = soup_obj.find_all(string=lambda text: isinstance(text, Comment) and comment_marker_string in str(text))
        if comments:
            try:
                comment_soup = BeautifulSoup(str(comments[0]), 'lxml')
                table_element_in_comment = comment_soup.find('table', id=direct_table_id_exact if direct_table_id_exact else None)
                if not table_element_in_comment: table_element_in_comment = comment_soup.find('table')
                if table_element_in_comment:
                    print(f"      -> Found table in comment using marker '{comment_marker_string}' on {context_url}")
                    header_level_comment = [0,1] if len(table_element_in_comment.select('thead tr')) > 1 else 0
                    df = pd.read_html(StringIO(str(table_element_in_comment)), header=header_level_comment)[0]
                    return df, table_element_in_comment
            except Exception as e: print(f"      -- Error parsing table from comment (marker='{comment_marker_string}'): {e}")
        # else: print(f"      -- No comment found with marker '{comment_marker_string}'.") # Reduce verbosity
    
    # Fallback: Try to find *any* table if specific searches fail and it's a schedule context
    if df is None and "schedule" in context_url.lower() : # Apply this fallback more selectively
        all_tables_on_page = soup_obj.find_all('table')
        if len(all_tables_on_page) == 1 and len(all_tables_on_page[0].select('thead tr')) > 0 : # Only if it's the single prominent table
            print(f"      -- Specific table find failed, attempting to parse the only prominent table on schedule page: {context_url}")
            try:
                table_element = all_tables_on_page[0]
                header_level = [0,1] if len(table_element.select('thead tr')) > 1 else 0
                df = pd.read_html(StringIO(str(table_element)), header=header_level)[0]
                if df is not None: print(f"      -> Found table via broad fallback on schedule page.")
                return df, table_element
            except Exception as e_fallback:
                print(f"      -- Error parsing fallback table: {e_fallback}")


    if df is None: print(f"      !! FAILED to find target table for '{direct_table_id_exact or comment_marker_string}' on {context_url}.")
    return df, table_element

def scrape_big5_data_table(driver, data_url: str, category_key: str, data_type: str, season_year_part: str, output_dir: str):
    print(f"    Scraping Big 5 {data_type} '{category_key}' stats from: {data_url}")
    page_source = ""; table_id_to_find = ""
    try:
        driver.get(data_url)
        if data_type == "player": table_id_to_find = f"stats_{category_key}" 
        elif data_type == "squad": table_id_to_find = f"stats_teams_{category_key}_for"
        else: print(f"        Unknown data_type: {data_type}."); return
        
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, f"table[id='{table_id_to_find}'], div[id='div_{table_id_to_find}']")))
        time.sleep(random.uniform(4, 7)); page_source = driver.page_source; soup = BeautifulSoup(page_source, 'lxml')
        df_data, _ = find_table_directly_or_in_comment(soup, direct_table_id_exact=table_id_to_find, comment_marker_string=f'id="{table_id_to_find}"', context_url=data_url)
        
        if df_data is not None and not df_data.empty:
            df_cleaned = clean_dataframe_columns(df_data.copy())
            key_col = 'Player' if data_type == "player" else 'Squad'
            if key_col in df_cleaned.columns:
                df_cleaned = df_cleaned[df_cleaned[key_col].notna() & (~df_cleaned[key_col].astype(str).str.fullmatch(key_col, case=False, na=False))]
                if 'Rk' in df_cleaned.columns: df_cleaned = df_cleaned[df_cleaned['Rk'].astype(str).str.fullmatch('Rk', case=False, na=False) == False]
            
            if df_cleaned.empty:
                print(f"        -- Table for {category_key} ({data_type}) empty after cleaning."); # HTML save logic...
                return
            filename = os.path.join(output_dir, f"{BIG5_CONFIG['name_in_url']}_{season_year_part}_{category_key}_{data_type}_stats.csv")
            df_cleaned.to_csv(filename, index=False)
            print(f"        ✅ Saved {data_type} '{category_key}' stats to {filename} ({len(df_cleaned)} rows)")
        else: 
            print(f"        !! Failed to extract {data_type} '{category_key}' table from {data_url}")
            debug_fname_no_table = os.path.join(output_dir, f"debug_{season_year_part}_{category_key}_{data_type}_NO_TABLE.html")
            with open(debug_fname_no_table, "w", encoding="utf-8") as f: f.write(page_source or "No page source.")
    except Exception as e: 
        print(f"        !! Error processing {data_type} '{category_key}' at {data_url}: {e}")
        debug_fname_err = os.path.join(output_dir, f"debug_{season_year_part}_{category_key}_{data_type}_ERROR.html")
        with open(debug_fname_err, "w", encoding="utf-8") as f: f.write(page_source or "No page source.")

# --- Updated Function for Individual League Scores & Fixtures ---
def scrape_league_scores_and_fixtures(driver, fixtures_url: str, league_name_key: str, season_year_part: str, output_dir: str):
    print(f"    Scraping Scores & Fixtures for {league_name_key} ({season_year_part}) from: {fixtures_url}")
    page_source = ""
    try:
        driver.get(fixtures_url)
        WebDriverWait(driver, 20).until(
             EC.presence_of_element_located((By.CSS_SELECTOR, "table[id^='sched'], table[id^='results'], table[id='schedule']"))
        )
        time.sleep(random.uniform(4,7))
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'lxml')

        df_schedule = None; table_found_details = ""; table_id_to_try = None
        league_id = INDIVIDUAL_LEAGUES[league_name_key]['id']
        
        # Attempt 1: Specific schedule ID format (e.g., sched_2023-2024_9_1 or sched_2023_2024_9_1)
        # FBRef sometimes uses hyphens, sometimes underscores in IDs from season_year_part
        season_formats = [season_year_part, season_year_part.replace('-', '_')]
        for season_fmt in season_formats:
            table_id_to_try = f"sched_{season_fmt}_{league_id}_1"
            df_schedule, _ = find_table_directly_or_in_comment(soup, direct_table_id_exact=table_id_to_try, comment_marker_string=f'id="{table_id_to_try}"', context_url=fixtures_url)
            if df_schedule is not None: table_found_details = f"found using id='{table_id_to_try}'"; break
        
        # Attempt 2: Try 'sched_all' (works for some leagues like Bundesliga, Ligue 1, and CL example)
        if df_schedule is None:
            table_id_to_try = "sched_all"
            df_schedule, _ = find_table_directly_or_in_comment(soup, direct_table_id_exact=table_id_to_try, comment_marker_string=f'id="{table_id_to_try}"', context_url=fixtures_url)
            if df_schedule is not None: table_found_details = f"found using id='{table_id_to_try}'"

        # Attempt 3: Try generic 'schedule' ID
        if df_schedule is None:
            table_id_to_try = "schedule"
            df_schedule, _ = find_table_directly_or_in_comment(soup, direct_table_id_exact=table_id_to_try, comment_marker_string=f'id="{table_id_to_try}"', context_url=fixtures_url)
            if df_schedule is not None: table_found_details = f"found using id='{table_id_to_try}'"
        
        # Attempt 4: If still no df, try find_table_directly_or_in_comment with None for exact ID, letting its internal fallbacks work
        if df_schedule is None:
            print(f"    -- Specific IDs failed for {league_name_key}. Trying generic table search on schedule page.")
            df_schedule, _ = find_table_directly_or_in_comment(soup, context_url=fixtures_url) # Broadest fallback
            if df_schedule is not None: table_found_details = "found via generic fallback"


        if df_schedule is not None and not df_schedule.empty:
            print(f"        -> Table for {league_name_key} {table_found_details}.")
            df_cleaned = clean_dataframe_columns(df_schedule.copy())
            if 'Wk' in df_cleaned.columns: df_cleaned = df_cleaned[df_cleaned['Wk'].astype(str).str.lower() != 'wk']
            if df_cleaned.empty:
                print(f"        -- Schedule table found for {league_name_key} but was empty after cleaning."); return
            filename = os.path.join(output_dir, f"{league_name_key.replace(' ', '_')}_{season_year_part}_scores_fixtures.csv")
            df_cleaned.to_csv(filename, index=False)
            print(f"        ✅ Saved Scores & Fixtures for {league_name_key} {season_year_part} to {filename} ({len(df_cleaned)} rows)")
        else:
            print(f"        !! Failed to extract Scores & Fixtures table for {league_name_key} from {fixtures_url}")
            if page_source : 
                debug_html_sfail = os.path.join(output_dir, f"debug_{league_name_key.replace(' ','_')}_{season_year_part}_SF_NO_TABLE.html")
                with open(debug_html_sfail, "w", encoding="utf-8") as f: f.write(page_source)
    except Exception as e:
        print(f"        !! Error scraping Scores & Fixtures for {league_name_key} ({fixtures_url}): {e}")
        if page_source : 
            debug_html_sferr = os.path.join(output_dir, f"debug_{league_name_key.replace(' ','_')}_{season_year_part}_SF_ERROR.html")
            with open(debug_html_sferr, "w", encoding="utf-8") as f: f.write(page_source)

# --- Main Orchestrator ---
def main():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless'); options.add_argument('--log-level=3')
    options.add_argument('--disable-gpu'); options.add_argument('--no-sandbox'); options.add_argument('--disable-dev-shm-usage')
    service = ChromeService(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    try: 
        driver.get("https://fbref.com/en/"); wait = WebDriverWait(driver, 15)
        possible_texts = ["Accept all cookies", "Accept All", "I Accept"]; xpath_selectors = [f"//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text.lower()}')]" for text in possible_texts]; xpath_selectors.append("//button[contains(@class, 'osano-cm-accept-all')]")
        accept_button = None
        for sel in xpath_selectors:
            try: accept_button = wait.until(EC.element_to_be_clickable((By.XPATH, sel))); break
            except: continue
        if accept_button:
            try: accept_button.click(); print("✅ Cookie consent accepted.")
            except: 
                try: driver.execute_script("arguments[0].click();", accept_button); print("✅ Cookie consent accepted via JS click.")
                except Exception as e_js: print(f"-> Cookie click failed: {e_js}")
            time.sleep(random.uniform(2,4)) 
        else: print("-> Cookie consent button not found.")
    except Exception as e: print(f"-> Cookie consent not processed: {e}")

    LATEST_COMPLETED_SEASON_END_YEAR = 2025; NUM_SEASONS_TO_SCRAPE = 11 
    big5_output_dir_base = f"{BIG5_CONFIG['name_in_url']}_aggregate_stats"; create_output_dir(big5_output_dir_base)
    print(f"\n{'='*30}\nScraping Aggregate Stats for: {BIG5_CONFIG['name_in_url']}\n{'='*30}")
    for i in range(NUM_SEASONS_TO_SCRAPE):
        season_end_year = LATEST_COMPLETED_SEASON_END_YEAR - i; season_start_year = season_end_year - 1
        url_season_part = f"{season_start_year}-{season_end_year}"
        print(f"\n--- Scraping Season: {url_season_part} ---")
        current_season_output_dir = os.path.join(big5_output_dir_base, url_season_part); create_output_dir(current_season_output_dir)
        player_stats_dir = os.path.join(current_season_output_dir, "player_aggregate_stats"); create_output_dir(player_stats_dir)
        print(f"\n  [Player Aggregate Stats for {url_season_part}]")
        for cat_key, url_seg in PLAYER_STAT_URL_COMPONENTS.items():
            player_cat_url = f"https://fbref.com/en/comps/{BIG5_CONFIG['id_in_url']}/{url_season_part}/{url_seg}/players/{url_season_part}-{BIG5_CONFIG['name_in_url']}-Stats"
            scrape_big5_data_table(driver, player_cat_url, cat_key, "player", url_season_part, player_stats_dir)
            time.sleep(random.uniform(5, 8))
        squad_stats_dir = os.path.join(current_season_output_dir, "squad_aggregate_stats"); create_output_dir(squad_stats_dir)
        print(f"\n  [Squad Aggregate Stats for {url_season_part}]")
        for cat_key, url_seg in SQUAD_STAT_URL_COMPONENTS.items():
            squad_cat_url = f"https://fbref.com/en/comps/{BIG5_CONFIG['id_in_url']}/{url_season_part}/{url_seg}/squads/{url_season_part}-{BIG5_CONFIG['name_in_url']}-Stats"
            scrape_big5_data_table(driver, squad_cat_url, cat_key, "squad", url_season_part, squad_stats_dir)
            time.sleep(random.uniform(5, 8)) 

    individual_leagues_output_base = "Individual_League_Scores_Fixtures"; create_output_dir(individual_leagues_output_base)
    print(f"\n{'='*30}\nScraping Individual League Scores & Fixtures\n{'='*30}")
    for i in range(NUM_SEASONS_TO_SCRAPE):
        season_end_year = LATEST_COMPLETED_SEASON_END_YEAR - i; season_start_year = season_end_year - 1
        url_season_part = f"{season_start_year}-{season_end_year}"
        print(f"\n--- Scraping Season: {url_season_part} for Individual Leagues ---")
        for league_key, league_details in INDIVIDUAL_LEAGUES.items():
            league_name_url_format = league_details["name_in_url"]; league_id = league_details["id"]
            league_season_output_dir = os.path.join(individual_leagues_output_base, league_key.replace(" ", "_"), url_season_part); create_output_dir(league_season_output_dir)
            fixtures_url = f"https://fbref.com/en/comps/{league_id}/{url_season_part}/schedule/{url_season_part}-{league_name_url_format}-Scores-and-Fixtures"
            scrape_league_scores_and_fixtures(driver, fixtures_url, league_key, url_season_part, league_season_output_dir)
            time.sleep(random.uniform(5,8))
    print("\nScript finished."); driver.quit()

if __name__ == "__main__":
    main()