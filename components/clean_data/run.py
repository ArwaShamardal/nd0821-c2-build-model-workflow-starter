#!/usr/bin/env python
"""
cleaning the dathe min and max range provided
"""
import argparse
import logging
import pandas as pd
import os

import wandb

from wandb_utils.log_artifact import log_artifact



logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    logger.info("step clean_data started")


    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    logger.info("Downloading artifact")
    artifact = run.use_artifact(args.in_artifact_name)
    artifact_path = artifact.file()

    logger.info("Reading artifact")
    df = pd.read_csv(artifact_path)  

    logger.info("Data cleaning") 
    df['last_review'] = pd.to_datetime(df['last_review'])
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    filename = "clean_sample"
    df.to_csv(filename, index=False)
    
    logger.info("Creating artifact")
    artifact = wandb.Artifact(
        name=args.out_artifact_name,
        type=args.out_artifact_type,
        description=args.out_artifact_desc,
    )
    artifact.add_file(filename)

    logger.info("Logging artifact")
    run.log_artifact(artifact)
    
    os.remove(filename)



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="cleaning data")


    parser.add_argument(
        "--min_price", 
        type=float,
        help="the minimum price of the house for outlier cleaning",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type= float,
        help="the maximum price of the house for outlier cleaning",
        required=True
    )

    parser.add_argument(
        "--in_artifact_name", 
        type= str,
        help="Input artifact name: of the file to download",
        required=True
    )

    parser.add_argument(
        "--out_artifact_name", 
        type= str,
        help="Input artifact name: of the file to upload",
        required=True
    )


    parser.add_argument(
        "--out_artifact_type", 
        type= str,
        help="output artifact type",
        required=True
    )

    parser.add_argument(
        "--out_artifact_desc", 
        type= str,
        help="output artifact description",
        required=True
    )


    args = parser.parse_args()

    go(args)
