import argparse
from transform.transform import DataTransformer
import logging

def main(config_file, usecase, task, package, datetime, env):
    # Initialize the DataTransformer with the provided config file
    data_transformer = DataTransformer(config_file)
    
    # Log the execution details
    logging.info(f"Running ETL pipeline for use case: {usecase}, task: {task}, package: {package}, datetime: {datetime}, environment: {env}")
    
    # Run the ETL pipeline
    data_transformer.run_pipeline(data_transformer)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Analytics Engine ETL pipeline.")
    parser.add_argument("config", type=str, help="Path to the configuration file (YAML)")
    parser.add_argument("-usecase", type=str, required=True, help="Specify the use case for the ETL job.")
    parser.add_argument("-task", type=str, required=True, help="Define the specific task within the use case.")
    parser.add_argument("-package", type=str, required=True, help="Specify the package or module to be used.")
    parser.add_argument("-datetime", type=str, required=True, help="Set the datetime for the ETL run (e.g., 2024-08-24T12:00:00).")
    parser.add_argument("-env", type=str, choices=['prod', 'dev'], required=True, help="Specify the environment: 'prod' or 'dev'.")
    
    args = parser.parse_args()
    main(args.config, args.usecase, args.task, args.package, args.datetime, args.env)
