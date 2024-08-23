import logging
import yaml
import joblib
import requests
from pyspark.sql import SparkSession
from time import sleep
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

class BaseAnalyticsEngine:
    def __init__(self, config_file):
        # Load the configuration file
        self.config = self.load_config(config_file)
        # Setup logging based on the configuration
        self.setup_logging(self.config.get('log', {}))
        # Initialize Spark session
        self.spark = self.init_spark_session(self.config.get('spark', {}))
        # Dictionary to store dataframes from various sources
        self.inputs = {}

    def load_config(self, config_file):
        """Load and parse the YAML configuration file."""
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
        return config

    def setup_logging(self, log_config):
        """Set up logging configuration."""
        logging.basicConfig(
            filename=log_config.get('file', 'etl.log'),
            level=log_config.get('level', 'INFO'),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        logging.info("Logging setup complete.")

    def init_spark_session(self, spark_config):
        """Initialize and return a Spark session with settings from YAML."""
        spark_builder = SparkSession.builder.appName(spark_config.get('app_name', 'ETL_PySpark_App')).master(spark_config.get('master', 'local[*]'))

        # Load additional Spark configurations from YAML
        for key, value in spark_config.get('config', {}).items():
            spark_builder = spark_builder.config(key, value)

        if 'jars' in spark_config:
            spark_builder = spark_builder.config("spark.jars", spark_config['jars'])

        return spark_builder.getOrCreate()

    def retry_on_failure(self, func, retries=None, delay=None):
        """Retry function execution in case of failure."""
        max_attempts = retries or self.config.get('retries', {}).get('max_attempts', 3)
        delay_time = delay or self.config.get('retries', {}).get('delay', 5)
        for attempt in range(max_attempts):
            try:
                return func()
            except Exception as e:
                logging.error(f"Attempt {attempt + 1} failed: {e}")
                if attempt < max_attempts - 1:
                    sleep(delay_time)
                else:
                    raise

    def api_get_request(self, url, headers=None, params=None):
        """Make a GET request to an API with retries."""
        def request():
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        return self.retry_on_failure(request)

    def api_post_request(self, url, headers=None, data=None, json=None):
        """Make a POST request to an API with retries."""
        def request():
            response = requests.post(url, headers=headers, data=data, json=json)
            response.raise_for_status()
            return response.json()
        return self.retry_on_failure(request)

    def validate_data(self, df):
        """Perform basic data validation checks."""
        if df is None or df.rdd.isEmpty():
            raise ValueError("Data validation failed: DataFrame is empty.")
        if df.columns is None or len(df.columns) == 0:
            raise ValueError("Data validation failed: DataFrame has no columns.")
        # Add additional validation checks as needed
        logging.info("Data validation passed.")

    def notify_failure(self, subject, body):
        """Send notification in case of failure."""
        notifications = self.config.get('notifications', {})
        if not notifications.get('enabled', False):
            logging.info("Notifications are disabled.")
            return

        method = notifications.get('method', 'email')
        if method == 'email':
            try:
                email_config = notifications.get('email', {})
                sender_email = email_config['sender']
                receiver_email = email_config['receiver']
                password = email_config['password']
                smtp_server = email_config['smtp_server']
                port = email_config['port']

                message = MIMEMultipart("alternative")
                message["Subject"] = subject
                message["From"] = sender_email
                message["To"] = receiver_email

                part = MIMEText(body, "plain")
                message.attach(part)

                with smtplib.SMTP(smtp_server, port) as server:
                    server.starttls()
                    server.login(sender_email, password)
                    server.sendmail(sender_email, receiver_email, message.as_string())

                logging.info("Failure notification sent.")
            except Exception as e:
                logging.error(f"Failed to send notification: {e}")
        # Add support for other notification methods (e.g., Slack) here

    def read_data(self, source_config):
        """Read data from the specified source using PySpark with retries."""
        def load_data():
            if source_config['type'] == 'jdbc':
                df = self.spark.read.format("jdbc").options(
                    url=source_config['jdbc_url'],
                    dbtable=f"({source_config['query']}) as subquery",
                    driver=source_config.get('driver', ''),
                    user=source_config.get('user', ''),
                    password=source_config.get('password', '')
                ).load()
                self.validate_data(df)
                return df
            # Handle other source types as needed
        return self.retry_on_failure(load_data)

    def save_model(self, model, model_name, save_path):
        """Save the trained model to a file with versioning."""
        version = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_filename = f"{model_name}_v{version}.pkl"
        joblib.dump(model, f"{save_path}/{model_filename}")
        logging.info(f"Model saved: {model_filename}")
        return model_filename

    def load_model(self, model_path, model_version=None):
        """Load a model from the file system."""
        if model_version:
            model_filename = f"{model_path}/{model_version}.pkl"
        else:
            model_filename = model_path
        model = joblib.load(model_filename)
        logging.info(f"Model loaded: {model_filename}")
        return model

    def load_inputs_from_config(self):
        """Load all input sources defined in the YAML configuration."""
        for source in self.config['sources']:
            self.inputs[source['name']] = self.read_data(source)

    def execute_transforms(self, data_transformer):
        """Execute the transformation process defined in the derived class."""
        try:
            data = self.load_inputs_from_config()
            transformed_data = data_transformer.transforms(data)
            self.write_data(transformed_data)
        except Exception as e:
            logging.error(f"Transformation failed: {e}")
            self.notify_failure("ETL Pipeline Failure", str(e))
            raise

    def write_data(self, transformed_data):
        """Write the transformed data to the target destination using PySpark."""
        target_config = self.config['target']
        def save_data():
            transformed_data.write.format("jdbc").options(
                url=target_config['jdbc_url'],
                dbtable=target_config['table_name'],
                mode='overwrite' if target_config.get('overwrite', False) else 'append',
                driver=target_config.get('driver', ''),
                user=target_config.get('user', ''),
                password=target_config.get('password', '')
            ).save()
            logging.info(f"Data written to {target_config['table_name']} in the database.")
        return self.retry_on_failure(save_data)

    def run_pipeline(self, data_transformer):
        """Run the entire ETL pipeline with monitoring placeholders."""
        logging.info(f"Starting ETL pipeline in {self.config.get('environment', 'dev')} environment...")
        try:
            self.execute_transforms(data_transformer)
            logging.info("ETL pipeline completed successfully.")
            # Placeholder for monitoring integration
            # Example: push metrics to Prometheus or another monitoring tool
        except Exception as e:
            logging.error(f"ETL pipeline failed: {e}")
            self.notify_failure("ETL Pipeline Execution Failure", str(e))
            raise
