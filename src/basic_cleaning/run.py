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

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()
    local_path = wandb.use_artifact("sample.csv:latest").file()
    df = pd.read_csv(local_path)
    logger.info("Input artifact loaded")

    idx = df["price"].between(args.min_price, args.max_price)
    df = df[idx].copy()
    df["last_review"] = pd.to_datetime(df["last_review"])
    idx = df["longitude"].between(args.min_long, args.max_long) & df[
        "latitude"
    ].between(args.min_lat, args.max_lat)
    df = df[idx].copy()
    logger.info("Applied data cleaning")

    df.to_csv("clean_sample.csv", index=False)
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)
    logger.info("Data uploaded")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument(
        "--input_artifact", type=str, help="Name of the input artifact.", required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Name of the output artifact.",
        required=True,
    )

    parser.add_argument(
        "--output_type", type=str, help="Type of the output.", required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="Description of the output.",
        required=True,
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="Minimum threshold for the price.",
        required=True,
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="Maximum threshold for the price.",
        required=True,
    )

    parser.add_argument(
        "--min_long",
        type=float,
        help="Minimum threshold for the longitude.",
        required=True,
    )

    parser.add_argument(
        "--max_long",
        type=float,
        help="Maximum threshold for the longitude.",
        required=True,
    )

    parser.add_argument(
        "--min_lat",
        type=float,
        help="Minimum threshold for the latitude.",
        required=True,
    )

    parser.add_argument(
        "--max_lat",
        type=float,
        help="Maximum threshold for the latitude.",
        required=True,
    )
    args = parser.parse_args()

    go(args)
