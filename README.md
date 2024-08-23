# Analytics Engine Framework

## Overview
The Analytics Engine framework is designed to handle ETL processes and machine learning model management using PySpark. It supports connections to multiple JDBC sources, handles data transformations, and can be easily configured for different environments through a YAML file.

## Features
- **Configurable via YAML**: Manage Spark settings, notifications, retries, and other configurations directly in a YAML file.
- **Multi-environment Support**: Easily switch between development, production, and other environments.
- **Integrations**: Seamless integration with Apache Airflow, Jenkins, Docker, and OpenShift.
- **Jupyter Notebook Generation**: Automatically generate notebooks for interactive ETL processes.
- **Notifications**: Configurable email notifications for pipeline failures.
- **Retry Logic**: Built-in retry mechanisms for robust ETL execution.

## Setup and Usage

### 1. Basic Setup
1. **Clone the repository**: 
   ```bash
   git clone <repository-url>
   cd analytics-engine
   ```

2. **Install the package**:
   ```bash
   pip install .
   ```

3. **Prepare the YAML configuration file**: Customize the `config.yaml` file located in the `config/` directory. This file contains all necessary configurations for Spark, data sources, targets, notifications, and more.

4. **Run the pipeline**:
   ```bash
   python main.py config/config.yaml
   ```

### 2. Running on AWS
- **Spark on EMR**: Configure the `spark` section in the YAML file to connect to AWS EMR, specifying the master URL and any additional Spark settings needed for EMR.
- **S3 Integration**: Use S3 paths in the `sources` and `target` sections of the YAML file for data storage.
- **Deployment**: Optionally, use AWS CodePipeline for CI/CD or Jenkins for deployment automation.

### 3. Running on Databricks
- **Cluster Setup**: Configure the `spark` section in the YAML file with Databricks-specific settings, including the cluster ID and token for authentication.
- **Databricks CLI**: Use the Databricks CLI or REST API to submit jobs to a Databricks cluster.
- **Deployment**: Integrate with Databricks Jobs or use Airflow for job orchestration.

### 4. Running on Azure
- **Azure HDInsight**: Configure the `spark` section in the YAML file for Azure HDInsight or Azure Databricks.
- **Blob Storage**: Use Azure Blob Storage paths in the `sources` and `target` sections for data storage.
- **Deployment**: Use Azure DevOps Pipelines for CI/CD or integrate with Jenkins.

### 5. Running on On-Premises Cluster
- **Spark Configuration**: Customize the `spark` section in the YAML file for your on-premises Spark cluster.
- **Data Storage**: Configure the `sources` and `target` sections to point to your on-premises data storage systems.
- **Deployment**: Use Jenkins or another CI/CD tool for automated deployment.

## Jupyter Notebook Generation
### Command: `ae_notebook`
You can generate a Jupyter notebook with the following command:

```bash
python -c "from your_cli_module import ae_notebook; ae_notebook('usecase1', 'package1', 'task1')"
```

This will create a notebook in the `notebooks/` directory with the filename `usecase-package-task.ipynb`. The notebook will include:

- A section to load the configuration file based on the use case, package, and task.
- Widgets to extract data and perform transformations interactively.

### 6. Integration with Apache Airflow
1. **DAG Creation**: Create an Airflow DAG that includes tasks for running the Analytics Engine pipeline.
2. **Custom Operator**: Implement a custom Airflow operator to execute the pipeline, passing in the YAML configuration file.
3. **Scheduling and Monitoring**: Use Airflow’s built-in scheduling and monitoring to manage the pipeline.

### 7. Integration with Jenkins
1. **Pipeline Script**: Write a Jenkinsfile to automate the deployment and execution of the Analytics Engine.
2. **Automated Testing**: Include stages for testing the ETL pipeline before deployment.
3. **Deployment**: Use Jenkins to deploy the pipeline to different environments (e.g., AWS, Azure, on-premises).

### 8. Docker and OpenShift Integration
1. **Dockerization**: Create a Dockerfile to containerize the Analytics Engine, including all dependencies.
2. **OpenShift Deployment**: Deploy the Docker container to OpenShift, using Kubernetes manifests or Helm charts for orchestration.
3. **Scaling**: Use OpenShift’s auto-scaling features to dynamically adjust resources based on the load of the ETL pipelines.

## Example Configuration
Here’s an example of a YAML configuration file for an on-premises cluster with basic Spark settings:

```yaml
environment: dev

notifications:
  enabled: true
  method: email
  email:
    sender: "your-email@example.com"
    receiver: "receiver-email@example.com"
    smtp_server: "smtp.example.com"
    port: 587
    password: "your-password"

retries:
  max_attempts: 3
  delay: 5

monitoring:
  enabled: false
  system: prometheus
  endpoint: "http://prometheus.example.com"

spark:
  master: "spark://your-spark-master:7077"
  app_name: "ETL_PySpark_App"
  jars: "/path/to/jdbc/drivers"
  config:
    spark.executor.memory: "2g"
    spark.executor.cores: 2
    spark.driver.memory: "4g"
    spark.sql.shuffle.partitions: 200

sources:
  - name: source1
    type: jdbc
    jdbc_url: "jdbc:postgresql://localhost:5432/dbname1"
    query: "SELECT * FROM table1"
    driver: "org.postgresql.Driver"
    user: "user"
    password: "password"
  
  - name: source2
    type: jdbc
    jdbc_url: "jdbc:mysql://localhost:3306/dbname2"
    query: "SELECT * FROM table2"
    driver: "com.mysql.cj.jdbc.Driver"
    user: "user"
    password: "password"

target:
  type: jdbc
  jdbc_url: "jdbc:postgresql://localhost:5432/dbname"
  table_name: "output_table"
  overwrite: true
  driver: "org.postgresql.Driver"
  user: "user"
  password: "password"
```

## License
This project is licensed under the MIT License.

## Contributing
Feel free to submit issues, fork the repository, and send pull requests. Contributions are welcome!
