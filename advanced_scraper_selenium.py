import pandas as pd
import time
import re
import os
from io import StringIO
from typing import Optional, Dict
import random 
import argparse 
import datetime 

from bs4 import BeautifulSoup, Comment
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException


INDIVIDUAL_LEAGUES = {
    "Premier-League": {"id": "9", "name_in_url": "Premier-League", "display_name": "Premier League"},
    "La-Liga": {"id": "12", "name_in_url": "La-Liga", "display_name": "La Liga"},
    "Serie-A": {"id": "11", "name_in_url": "Serie-A", "display_name": "Serie A"},
    "Bundesliga": {"id": "20", "name_in_url": "Bundesliga", "display_name": "Bundesliga"},
    "Ligue-1": {"id": "13", "name_in_url": "Ligue-1", "display_name": "Ligue 1"},
}

BIG5_CONFIG = {
    "id_in_url": "Big5", "name_in_url": "Big-5-European-Leagues", "display_name": "Big 5 European Leagues"
}

CHAMPIONS_LEAGUE_CONFIG = {
    "id_in_url": "8", "name_in_url": "Champions-League", "display_name": "Champions League"
}

ALL_SCRAPING_TARGETS = {
    "Big5_Agg_Player": {"type": "aggregate_player", "config": BIG5_CONFIG, "label": "Big 5 Player Aggregate Stats"},
    "Big5_Agg_Squad": {"type": "aggregate_squad", "config": BIG5_CONFIG, "label": "Big 5 Squad Aggregate Stats"},
    "CL_Agg_Player": {"type": "aggregate_player", "config": CHAMPIONS_LEAGUE_CONFIG, "label": "Champions League Player Aggregate Stats"},
    "CL_Agg_Squad": {"type": "aggregate_squad", "config": CHAMPIONS_LEAGUE_CONFIG, "label": "Champions League Squad Aggregate Stats"},
    "CL_Fixtures": {"type": "fixtures", "config": CHAMPIONS_LEAGUE_CONFIG, "label": "Champions League Scores & Fixtures"},
}
for league_key, league_conf in INDIVIDUAL_LEAGUES.items():
    ALL_SCRAPING_TARGETS[f"{league_key}_Fixtures"] = {
        "type": "fixtures", "config": league_conf, "label": f"{league_conf['display_name']} Scores & Fixtures"
    }

STAT_CATEGORIES_URL_MAP: Dict[str, str] = {
    "standard": "stats", "keeper": "keepers", "keeper_adv": "keepersadv",
    "shooting": "shooting", "passing": "passing", "passing_types": "passing_types",
    "gca": "gca", "defense": "defense", "possession": "possession",
    "playing_time": "playingtime", "misc": "misc"
}
PLAYER_STAT_URL_COMPONENTS = STAT_CATEGORIES_URL_MAP
SQUAD_STAT_URL_COMPONENTS = STAT_CATEGORIES_URL_MAP

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
    
    if df is None and ("schedule" in context_url.lower() or "fixtures" in context_url.lower()) : 
        all_tables_on_page = soup_obj.find_all('table')
        if len(all_tables_on_page) > 0 : # Looser condition: try first prominent table
            print(f"      -- Specific table find failed for schedule, attempting to parse first prominent table on page: {context_url}")
            try:
                table_element = all_tables_on_page[0]
                if len(table_element.select('thead tr')) > 0 and len(table_element.select('tbody tr')) > 1 : # Basic heuristic
                    header_level = [0,1] if len(table_element.select('thead tr')) > 1 else 0
                    df = pd.read_html(StringIO(str(table_element)), header=header_level)[0]
                    if df is not None: print(f"      -> Found table via broad fallback on schedule page.")
                    return df, table_element
            except Exception as e_fallback: print(f"      -- Error parsing fallback table: {e_fallback}")

    if df is None: print(f"      !! FAILED to find target table for '{direct_table_id_exact or comment_marker_string}' on {context_url}.")
    return df, table_element

def scrape_competition_aggregate_stats_table(driver, data_url: str, comp_config: Dict, category_key: str, data_type: str, season_year_part: str, output_dir: str):
    print(f"    Scraping {comp_config['display_name']} {data_type} '{category_key}' stats from: {data_url}")
    page_source = ""; table_id_to_find = ""
    try:
        driver.get(data_url)
        
        comp_id_for_patterns = comp_config.get('id_in_url')

        if data_type == "player":
            table_id_to_find = f"stats_{category_key}" 
        elif data_type == "squad":
            if comp_id_for_patterns == "Big5":
                table_id_to_find = f"stats_teams_{category_key}_for"
            else: 
                table_id_to_find = f"stats_squads_{category_key}_for"
        else: 
            print(f"        Unknown data_type: {data_type}."); return
        
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
                print(f"        -- Table for {category_key} ({data_type}) empty after cleaning.")
                debug_fname_empty = os.path.join(output_dir, f"debug_{comp_config['name_in_url']}_{season_year_part}_{category_key}_{data_type}_EMPTY.html")
                with open(debug_fname_empty, "w", encoding="utf-8") as f: f.write(page_source or "No page source.")
                return
            filename = os.path.join(output_dir, f"{comp_config['name_in_url']}_{season_year_part}_{category_key}_{data_type}_stats.csv")
            df_cleaned.to_csv(filename, index=False)
            print(f"        ✅ Saved {data_type} '{category_key}' stats to {filename} ({len(df_cleaned)} rows)")
        else: 
            print(f"        !! Failed to extract {data_type} '{category_key}' table from {data_url}")
            debug_fname_no_table = os.path.join(output_dir, f"debug_{comp_config['name_in_url']}_{season_year_part}_{category_key}_{data_type}_NO_TABLE.html")
            if page_source: 
                with open(debug_fname_no_table, "w", encoding="utf-8") as f: f.write(page_source)
            else: 
                with open(debug_fname_no_table, "w", encoding="utf-8") as f: f.write("No page source captured.")

    except TimeoutException:
        print(f"        !! TimeoutException waiting for table '{table_id_to_find}' for {data_type} '{category_key}' at {data_url}")
        if page_source:
            debug_fname_timeout = os.path.join(output_dir, f"debug_{comp_config['name_in_url']}_{season_year_part}_{category_key}_{data_type}_TIMEOUT.html")
            with open(debug_fname_timeout, "w", encoding="utf-8") as f: f.write(page_source)
    except Exception as e: 
        print(f"        !! Error processing {data_type} '{category_key}' at {data_url}: {type(e).__name__} {e}")
        debug_fname_err = os.path.join(output_dir, f"debug_{comp_config['name_in_url']}_{season_year_part}_{category_key}_{data_type}_ERROR.html")
        if page_source: 
            with open(debug_fname_err, "w", encoding="utf-8") as f: f.write(page_source)
        else: 
            with open(debug_fname_err, "w", encoding="utf-8") as f: f.write("No page source captured on error.")


def scrape_competition_scores_and_fixtures(driver, fixtures_url: str, 
                                        comp_display_name: str, 
                                        comp_id_for_table_pattern: str, 
                                        comp_name_in_url: str, 
                                        season_year_part: str, 
                                        output_dir: str):
    print(f"    Scraping Scores & Fixtures for {comp_display_name} ({season_year_part}) from: {fixtures_url}")
    page_source = ""
    try:
        driver.get(fixtures_url)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table[id^='sched'], table[id^='results'], table[id='schedule']")))
        time.sleep(random.uniform(4,7)); page_source = driver.page_source; soup = BeautifulSoup(page_source, 'lxml')
        df_schedule = None; table_found_details = ""; table_id_to_try = None
        
        season_formats = [season_year_part, season_year_part.replace('-', '_')]
        specific_sched_ids_to_try = []
        for season_fmt in season_formats:
            specific_sched_ids_to_try.append(f"sched_{season_fmt}_{comp_id_for_table_pattern}_1")

        for table_id in specific_sched_ids_to_try:
            df_schedule, _ = find_table_directly_or_in_comment(soup, direct_table_id_exact=table_id, comment_marker_string=f'id="{table_id}"', context_url=fixtures_url)
            if df_schedule is not None: table_found_details = f"found using id='{table_id}'"; break
        
        if df_schedule is None:
            table_id_to_try = "sched_all"
            df_schedule, _ = find_table_directly_or_in_comment(soup, direct_table_id_exact=table_id_to_try, comment_marker_string=f'id="{table_id_to_try}"', context_url=fixtures_url)
            if df_schedule is not None: table_found_details = f"found using id='{table_id_to_try}'"

        if df_schedule is None:
            table_id_to_try = "schedule"
            df_schedule, _ = find_table_directly_or_in_comment(soup, direct_table_id_exact=table_id_to_try, comment_marker_string=f'id="{table_id_to_try}"', context_url=fixtures_url)
            if df_schedule is not None: table_found_details = f"found using id='{table_id_to_try}'"
        
        if df_schedule is None:
            results_table_id = f"results{season_year_part}{comp_id_for_table_pattern}1_overall"
            df_schedule, _ = find_table_directly_or_in_comment(soup, direct_table_id_exact=results_table_id, comment_marker_string=f'id="{results_table_id}"', context_url=fixtures_url)
            if df_schedule is not None: table_found_details = f"found using id='{results_table_id}' (results table)"
        
        if df_schedule is None: 
            print(f"    -- Specific IDs failed for {comp_display_name}. Trying generic table search on schedule page.")
            df_schedule, _ = find_table_directly_or_in_comment(soup, context_url=fixtures_url) 
            if df_schedule is not None: table_found_details = "found via generic fallback"

        if df_schedule is not None and not df_schedule.empty:
            print(f"        -> Table for {comp_display_name} {table_found_details}.")
            df_cleaned = clean_dataframe_columns(df_schedule.copy())
            if 'Wk' in df_cleaned.columns: df_cleaned = df_cleaned[df_cleaned['Wk'].astype(str).str.lower() != 'wk']
            if df_cleaned.empty:
                print(f"        -- Schedule table found for {comp_display_name} but was empty after cleaning."); return
            filename = os.path.join(output_dir, f"{comp_name_in_url.replace(' ', '_')}_{season_year_part}_scores_fixtures.csv")
            df_cleaned.to_csv(filename, index=False)
            print(f"        ✅ Saved Scores & Fixtures for {comp_display_name} {season_year_part} to {filename} ({len(df_cleaned)} rows)")
        else:
            print(f"        !! Failed to extract Scores & Fixtures table for {comp_display_name} from {fixtures_url}")
            if page_source : 
                debug_html_sfail = os.path.join(output_dir, f"debug_{comp_name_in_url.replace(' ','_')}_{season_year_part}_SF_NO_TABLE.html")
                with open(debug_html_sfail, "w", encoding="utf-8") as f: f.write(page_source)
    except TimeoutException:
        print(f"        !! TimeoutException waiting for schedule table for {comp_display_name} ({fixtures_url})")
        if page_source:
            debug_html_stout = os.path.join(output_dir, f"debug_{comp_name_in_url.replace(' ','_')}_{season_year_part}_SF_TIMEOUT.html")
            with open(debug_html_stout, "w", encoding="utf-8") as f: f.write(page_source)
    except Exception as e:
        print(f"        !! Error scraping Scores & Fixtures for {comp_display_name} ({fixtures_url}): {type(e).__name__} {e}")
        if page_source : 
            debug_html_sferr = os.path.join(output_dir, f"debug_{comp_name_in_url.replace(' ','_')}_{season_year_part}_SF_ERROR.html")
            with open(debug_html_sferr, "w", encoding="utf-8") as f: f.write(page_source)

def main():
    parser = argparse.ArgumentParser(description="Scrape football statistics from FBRef.")
    parser.add_argument("--targets", nargs='+', required=True, help=f"List of scraping targets. Available: {' , '.join(ALL_SCRAPING_TARGETS.keys())} or 'ALL'.")
    parser.add_argument("--seasons", type=int, required=True, help="Number of past seasons to scrape.")
    parser.add_argument("--latest_year", type=int, default=None, help="End year of the most recent season (e.g., 2024 for 2023-2024). Defaults based on current date.")
    args = parser.parse_args()

    tasks_to_run = []
    if "ALL" in [t.upper() for t in args.targets]: tasks_to_run = list(ALL_SCRAPING_TARGETS.keys())
    else:
        for target_key in args.targets:
            if target_key in ALL_SCRAPING_TARGETS: tasks_to_run.append(target_key)
            else: print(f"Warning: Unknown target '{target_key}'. Skipping. Available: {', '.join(ALL_SCRAPING_TARGETS.keys())}")
    if not tasks_to_run: print("No valid targets. Exiting."); return

    if args.latest_year: LATEST_COMPLETED_SEASON_END_YEAR = args.latest_year
    else:
        current_month = datetime.datetime.now().month; current_year = datetime.datetime.now().year
        LATEST_COMPLETED_SEASON_END_YEAR = current_year - 1 if current_month <= 7 else current_year
        print(f"Defaulting latest_year to: {LATEST_COMPLETED_SEASON_END_YEAR}")
    NUM_SEASONS_TO_SCRAPE = args.seasons

    options = webdriver.ChromeOptions()
    options.add_argument('--headless'); options.add_argument('--log-level=3')
    options.add_argument('--disable-gpu'); options.add_argument('--no-sandbox'); options.add_argument('--disable-dev-shm-usage')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")
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

    base_output_directory = "output_data"; create_output_dir(base_output_directory)

    for i in range(NUM_SEASONS_TO_SCRAPE):
        season_end_year = LATEST_COMPLETED_SEASON_END_YEAR - i
        season_start_year = season_end_year - 1; url_season_part = f"{season_start_year}-{season_end_year}"
        print(f"\n{'='*40}\nProcessing Season: {url_season_part}\n{'='*40}")

        for task_key in tasks_to_run:
            task_details = ALL_SCRAPING_TARGETS[task_key]; comp_config = task_details['config']
            task_type = task_details['type']; task_label = task_details['label']
            task_folder_name = task_key.replace(" ", "_") 
            task_season_output_dir = os.path.join(base_output_directory, task_folder_name, url_season_part); create_output_dir(task_season_output_dir)
            print(f"\n  Scraping: {task_label} for {url_season_part}")

            if task_type == "aggregate_player":
                for cat_key_internal, url_seg in STAT_CATEGORIES_URL_MAP.items():
                    data_url = f"https://fbref.com/en/comps/{comp_config['id_in_url']}/{url_season_part}/{url_seg}/players/{url_season_part}-{comp_config['name_in_url']}-Stats"
                    scrape_competition_aggregate_stats_table(driver, data_url, comp_config, cat_key_internal, "player", url_season_part, task_season_output_dir)
                    time.sleep(random.uniform(5, 8))
            elif task_type == "aggregate_squad":
                for cat_key_internal, url_seg in STAT_CATEGORIES_URL_MAP.items():
                    data_url = f"https://fbref.com/en/comps/{comp_config['id_in_url']}/{url_season_part}/{url_seg}/squads/{url_season_part}-{comp_config['name_in_url']}-Stats"
                    scrape_competition_aggregate_stats_table(driver, data_url, comp_config, cat_key_internal, "squad", url_season_part, task_season_output_dir)
                    time.sleep(random.uniform(5, 8))
            elif task_type == "fixtures":
                comp_id_for_url = comp_config.get('id', comp_config.get('id_in_url'))
                fixtures_url = f"https://fbref.com/en/comps/{comp_id_for_url}/{url_season_part}/schedule/{url_season_part}-{comp_config['name_in_url']}-Scores-and-Fixtures"
                scrape_competition_scores_and_fixtures(driver, fixtures_url, comp_config['display_name'], comp_id_for_url, comp_config['name_in_url'], url_season_part, task_season_output_dir)
                time.sleep(random.uniform(5,8))
            
    print("\nScript finished."); driver.quit()

if __name__ == "__main__":
    main()