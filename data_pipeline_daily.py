#!/usr/bin/env python3

import argparse
import ast
import os
import uuid 
import shutil 

import requests
import pandas as pd

def get_data(input:str) -> pd.DataFrame:
    """
    Function to pull data into pandas 
    
    Input - date of inspection should be YYYY-MM-DD
    """
    url = requests.get(f'https://data.cityofchicago.org/resource/4ijn-s7e5.json?inspection_date={url}T00:00:00.000')
    if len(ast.literal_eval(url.text)) > 0:
        df = pd.DataFrame.from_records(ast.literal_eval(url.text))
        return df

def run_city_inspections_data_pipeline(
    date: str, output_loc: str, run_id: str
) -> None:
    """
    Script to run pipeline with transformation for daily retrieval
    """
    output_path = os.path.join(output_loc, run_id)
    if os.path.exists(output_path):
        shutil.rmtree(output_path)

    # data retrieval by date
    try:
        df = get_data(date)
    except:
        print("No data")
    else:
        # data transformations 
        df["run_id"] = run_id
        recorded_violations = df.violations.str.findall(pat=r"[0-9][0-9]\.\s").str.len()
        recorded_violations = recorded_violations.fillna(0)
        recorded_violations = pd.to_numeric(recorded_violations).astype(int)
        recorded_violations.name = "recorded_violations"
        df_fin = df.join(recorded_violations)
        df_fin.to_parquet(output_path, partition_cols=["run_id"])
        


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=str,
        help="The date for data retrieval",
    )
    parser.add_argument(
        "--output-loc",
        type=str,
        help="The output folder",
    )

    opts = parser.parse_args()
    run_id = uuid.uuid4()
    run_city_inspections_data_pipeline(
        input_file=opts.date, output_loc=opts.output_loc, run_id=run_id
    )