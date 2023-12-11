#!/usr/bin/env python3

import argparse
import ast
import os
import uuid
import shutil

import requests
import pandas as pd


def get_data(input: str) -> pd.DataFrame:
    """
    Function to pull data into pandas

    input - date of inspection should be YYYY-MM-DD
    TODO - add more checks to the data format
    """
    url = requests.get(
        f"https://data.cityofchicago.org/resource/4ijn-s7e5.json?inspection_date={input}T00:00:00.000"
    )
    if len(ast.literal_eval(url.text)) > 0:
        df = pd.DataFrame.from_records(ast.literal_eval(url.text))
        return df


def run_city_inspections_data_pipeline(date: str, run_id: str) -> None:
    """
    Script to run pipeline with transformation for daily retrieval
    """
    output_loc = "/pipeline_docker_test/data"
    output_path = os.path.join(output_loc, date)
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
        df_fin.to_parquet(output_path, partition_cols=["inspection_date"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=str,
        help="The date for city inspections data retrieval",
    )

    opts = parser.parse_args()
    run_id = str(uuid.uuid4())
    run_city_inspections_data_pipeline(date=opts.date, run_id=run_id)
