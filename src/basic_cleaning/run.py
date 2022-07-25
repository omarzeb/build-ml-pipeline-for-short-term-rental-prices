#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    logger.info("Downloading the artifact from W&B")
    local_path = run.use_artifact(args.input_artifact).file()

    logger.info("Reading the artifact")
    df = pd.read_csv(local_path)

    logger.info("Removing price outliers")
    price_min = args.min_price
    price_max = args.max_price

    idx = df['price'].between(price_min, price_max)
    df = df[idx].copy()

    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    logger.info("Saving the clean data")
    df.to_csv("clean_data.csv", index=False)


    logger.info("Uploading artifact to W&B")

    artifact = wandb.Artifact(
        args.output_artifact,
        type = args.output_type,
        description= args.output_description
    )

    artifact.add_file("clean_data.csv")
    run.log_artifact(artifact)

    artifact.wait()
 


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description=" very basic data cleaning")

    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Name of Input Artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name of Output Artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Output Artifact type",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Output Artifact Description",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Min price to consider for cleaning",
        required=False
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Max price to consider for cleaning",
        required=False
    )

    args = parser.parse_args()

    go(args)
