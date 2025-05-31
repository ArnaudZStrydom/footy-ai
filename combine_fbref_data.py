import pandas as pd
import os
import glob
from functools import reduce
import re
import argparse
from pathlib import Path 


BIG5_COMP_NAME_IN_FILE = "Big-5-European-Leagues"
CL_COMP_NAME_IN_FILE = "Champions-League"
CHAMPIONS_LEAGUE_CONFIG_NAME_FOR_FILES = "Champions-League" 

INDIVIDUAL_LEAGUE_NAMES_IN_FILES = [
    "Premier-League", "La-Liga", "Serie-A", "Bundesliga", "Ligue-1"
]

def clean_final_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans column names of the final combined DataFrame, ensuring uniqueness robustly."""
    cols = list(df.columns)
    new_cols = []
    counts = {}
    for col_name_original_case in cols:
        col_name = str(col_name_original_case).strip() 
        
        # Basic cleaning
        col_name = re.sub(r'[#%/()-]+', '', col_name) 
        col_name = re.sub(r'\s+', '_', col_name)    
        col_name = re.sub(r'_+', '_', col_name)     
        col_name = col_name.strip('_')
        
        if not col_name: 
            col_name = f"unnamed_col_{len(new_cols)}"

       
        current_count = counts.get(col_name.lower(), 0) 
        final_col_name_to_append = col_name
        if current_count > 0:
            final_col_name_to_append = f"{col_name}_{current_count + 1}"
        
        new_cols.append(final_col_name_to_append)
        counts[col_name.lower()] = current_count + 1
        
    df.columns = new_cols
    return df

def extract_info_from_path(file_path_str: str, base_data_dir_abs: str, type_folder_name: str):
    file_path = Path(file_path_str)
    relative_path = file_path.relative_to(base_data_dir_abs)
    parts = list(relative_path.parts) 
                                    
    filename = file_path.name

    season = "UnknownSeason"
    competition_name = "UnknownCompetition"
    category = "UnknownCategory"
    data_sub_type = None 

    if type_folder_name == "aggregate_stats":
        if len(parts) == 4: 
            season = parts[1]
            sub_type_folder_name = parts[2]
            if "player" in sub_type_folder_name.lower():
                data_sub_type = "player"
            elif "squad" in sub_type_folder_name.lower():
                data_sub_type = "squad"


            if filename.startswith(BIG5_COMP_NAME_IN_FILE):
                competition_name = "Big 5 European Leagues"
              
                match_cat = re.search(fr"^{re.escape(BIG5_COMP_NAME_IN_FILE)}_{season}_(.+?)_(player|squad)_stats\.csv$", filename)
                if match_cat: category = match_cat.group(1)
            elif filename.startswith(CHAMPIONS_LEAGUE_CONFIG_NAME_FOR_FILES):
                competition_name = "Champions League"
                match_cat = re.search(fr"^{re.escape(CHAMPIONS_LEAGUE_CONFIG_NAME_FOR_FILES)}_{season}_(.+?)_(player|squad)_stats\.csv$", filename)
                if match_cat: category = match_cat.group(1)
        else:
            print(f"Warning: Unexpected path structure for aggregate_stats: {file_path_str}")

    elif type_folder_name == "scores_fixtures":
        if len(parts) == 4:
            competition_name = parts[1].replace("_", " ")
            season = parts[2]
            category = "scores_fixtures" 
            data_sub_type = "fixtures" 
        else:
            print(f"Warning: Unexpected path structure for scores_fixtures: {file_path_str}")
            
    return season, competition_name, category, data_sub_type


def combine_stats_data(data_root_dir: str, target_data_type: str):
    
    all_dfs_by_key = {} 
    total_source_rows = 0
    num_contributing_files = 0
    file_pattern = os.path.join(data_root_dir, "*", f"{target_data_type}_aggregate_stats", "*.csv")
    
    data_files = glob.glob(file_pattern)
    
    if not data_files:
        print(f"No {target_data_type} files found with pattern: {file_pattern}")
        return None
    print(f"Found {len(data_files)} files for {target_data_type} data in {data_root_dir}.")

    for f_path in data_files:
        try:
            
            season, comp_name, category, _ = extract_info_from_path(f_path, str(Path(data_root_dir).parent), "aggregate_stats")
            
            if category == "UnknownCategory" or comp_name == "UnknownCompetition":
                print(f"Skipping file due to unknown metadata: {f_path}")
                continue

            df = pd.read_csv(f_path)
            if df.empty:
                print(f"Skipping empty file: {f_path}")
                continue
            
            total_source_rows += len(df)
            num_contributing_files += 1
            df['Season'] = season
            df['Competition'] = comp_name
            
            key = (comp_name, season)
            if key not in all_dfs_by_key:
                all_dfs_by_key[key] = []
            all_dfs_by_key[key].append({"category": category, "dataframe": df})
            
        except Exception as e:
            print(f"Error processing {target_data_type} file {f_path}: {e}")
            continue
    
    final_merged_dfs_list = []
    id_column = 'Player' if target_data_type == "player" else 'Squad'
    
    base_identifier_cols_player = ['Nation', 'Pos', 'Age', 'Born', 'Squad']
    base_identifier_cols_squad = ['#_Pl', 'Age', 'Poss', 'Comp'] 

    for key, cat_df_infos in all_dfs_by_key.items():
        if not cat_df_infos: continue
        
        comp_name_iter, season_iter = key
        cat_df_infos.sort(key=lambda x: 0 if x['category'] == 'standard' else 1)

        if not cat_df_infos[0]['dataframe'].empty and id_column in cat_df_infos[0]['dataframe'].columns:
            merged_df = cat_df_infos[0]['dataframe'].copy()
            potential_keys = base_identifier_cols_player if target_data_type == 'player' else base_identifier_cols_squad
            
            for df_info in cat_df_infos[1:]:
                df_to_merge = df_info['dataframe'].copy()
                category_suffix = f"_{df_info['category']}"
                if df_to_merge.empty or id_column not in df_to_merge.columns: continue

                current_merge_keys = [id_column]
                for pk in potential_keys:
                    if pk in merged_df.columns and pk in df_to_merge.columns:
                        if merged_df[pk].dtype == df_to_merge[pk].dtype: current_merge_keys.append(pk)
                current_merge_keys = list(dict.fromkeys(current_merge_keys))
                cols_to_bring = [col for col in df_to_merge.columns if col in current_merge_keys or col not in merged_df.columns]
                
                if not any(c not in current_merge_keys for c in cols_to_bring): continue
                try:
                    merged_df = pd.merge(merged_df, df_to_merge[cols_to_bring], on=current_merge_keys, how='outer', suffixes=('', category_suffix))
                except Exception as e_merge:
                    print(f"Error merging {target_data_type} for {key}, category {df_info['category']} on {current_merge_keys}: {e_merge}. Trying '{id_column}' only.")
                    try:
                        cols_to_bring_fallback = [id_column] + [col for col in df_to_merge.columns if col != id_column and col not in merged_df.columns]
                        cols_to_bring_fallback = list(dict.fromkeys(cols_to_bring_fallback))
                        if len(cols_to_bring_fallback) > 1 :
                             merged_df = pd.merge(merged_df, df_to_merge[cols_to_bring_fallback], on=id_column, how='outer', suffixes=('', category_suffix + "_fb"))
                    except Exception as e_fb_merge:
                        print(f"  Fallback merge on '{id_column}' also failed for {key}, cat {df_info['category']}: {e_fb_merge}")
            final_merged_dfs_list.append(merged_df)
        else:
            print(f"  Base DataFrame for {key} (category: {cat_df_infos[0]['category']}) is empty or missing ID column '{id_column}'.")

    if not final_merged_dfs_list:
        print(f"No {target_data_type} dataframes to combine into a master file.")
        return None
        
    combined_master_df = pd.concat(final_merged_dfs_list, ignore_index=True)
    combined_master_df = clean_final_dataframe_columns(combined_master_df)
    print(f"{target_data_type.capitalize()} data: Combined {len(combined_master_df)} rows into master DataFrame, from {total_source_rows} total rows in {num_contributing_files} source files.")
    return combined_master_df

def combine_match_fixtures(data_root_dir: str):
    
    all_fixture_dfs = []
    total_source_rows = 0
    num_contributing_files = 0
    file_pattern = os.path.join(data_root_dir, "*", "*", "*.csv")
    fixture_files = glob.glob(file_pattern)


    if not fixture_files:
        print(f"No fixture files found with pattern: {file_pattern}")
        return None
    print(f"Found {len(fixture_files)} fixture files.")

    for f_path in fixture_files:
        try:
            season, comp_name, _, _ = extract_info_from_path(f_path, str(Path(data_root_dir).parent), "scores_fixtures")
            if comp_name == "UnknownCompetition": 
                print(f"Skipping fixture file due to unknown competition: {f_path}")
                continue
            
            df = pd.read_csv(f_path)
            if df.empty:
                print(f"Skipping empty fixture file: {f_path}")
                continue
            
            total_source_rows += len(df)
            num_contributing_files += 1
            df['Season'] = season
            df['Competition'] = comp_name 
            all_fixture_dfs.append(df)
            
        except Exception as e:
            print(f"Error processing fixture file {f_path}: {e}")
            continue
            
    if not all_fixture_dfs:
        print("No fixture dataframes to combine.")
        return None
        
    combined_df = pd.concat(all_fixture_dfs, ignore_index=True)
    combined_df = clean_final_dataframe_columns(combined_df)
    print(f"Match fixtures: Combined {len(combined_df)} rows into master DataFrame, from {total_source_rows} total rows in {num_contributing_files} source files.")
    return combined_df

def main_combiner_logic(base_data_dir_arg):
    print(f"Starting data combination process from base directory: {base_data_dir_arg}")
    abs_base_data_dir = os.path.abspath(base_data_dir_arg)
    if not os.path.isdir(abs_base_data_dir):
        print(f"Error: Base data directory '{abs_base_data_dir}' not found.")
        return

    aggregate_stats_path = os.path.join(abs_base_data_dir, "aggregate_stats")
    scores_fixtures_path = os.path.join(abs_base_data_dir, "scores_fixtures")

    if os.path.isdir(aggregate_stats_path):
        print("\nCombining Player Stats...")
        combined_players = combine_stats_data(aggregate_stats_path, target_data_type="player")
        if combined_players is not None and not combined_players.empty:
            output_path = os.path.join(abs_base_data_dir, "MASTER_PLAYER_STATS.csv")
            combined_players.to_csv(output_path, index=False)
            print(f"Saved MASTER_PLAYER_STATS.csv with {len(combined_players)} rows to {output_path}")
        else:
            print("No master player stats file generated or result was empty.")

        print("\nCombining Squad Stats...")
        combined_squads = combine_stats_data(aggregate_stats_path, target_data_type="squad")
        if combined_squads is not None and not combined_squads.empty:
            output_path = os.path.join(abs_base_data_dir, "MASTER_SQUAD_STATS.csv")
            combined_squads.to_csv(output_path, index=False)
            print(f"Saved MASTER_SQUAD_STATS.csv with {len(combined_squads)} rows to {output_path}")
        else:
            print("No master squad stats file generated or result was empty.")
    else:
        print(f"Directory for aggregate stats not found: {aggregate_stats_path}")


    if os.path.isdir(scores_fixtures_path):
        print("\nCombining Match Fixtures...")
        combined_fixtures = combine_match_fixtures(scores_fixtures_path)
        if combined_fixtures is not None and not combined_fixtures.empty:
            output_path = os.path.join(abs_base_data_dir, "MASTER_MATCH_FIXTURES.csv")
            combined_fixtures.to_csv(output_path, index=False)
            print(f"Saved MASTER_MATCH_FIXTURES.csv with {len(combined_fixtures)} rows to {output_path}")
        else:
            print("No master match fixtures file generated or result was empty.")
    else:
        print(f"Directory for scores & fixtures not found: {scores_fixtures_path}")
    
    print("\nData combination process finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combine scraped FBRef CSV data into master files.")
    parser.add_argument(
        "--data_dir", 
        type=str, 
        default="output_data", 
        help="The base directory where the 'aggregate_stats' and 'scores_fixtures' subdirectories are located (default: 'output_data/')."
    )
    args = parser.parse_args()
    
    main_combiner_logic(args.data_dir)