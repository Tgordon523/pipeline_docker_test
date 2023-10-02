#!/usr/bin/env python3

import argparse
import ast
import os
import uuid 
import shutil 

import requests
import pandas as pd

def get_data(input:str) -> pd.DataFrame:


def run_city_inspections_data_pipeline(
    date: str, output_loc: str, run_id: str
) -> None:
    output_path = os.path.join(output_loc, run_id)
    if os.path.exists(output_path):
        shutil.rmtree(output_path)

    df = get_data(date)
    # your transformations
    states_to_remove = ["99"]
    df_fin = df[~df["Registration State"].isin(states_to_remove)]
    df_fin.to_parquet(output_path, partition_cols=["Registration State"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-file",
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