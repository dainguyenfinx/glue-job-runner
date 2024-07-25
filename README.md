# AWS Glue Job Runner

This repository contains a Python script for managing AWS Glue jobs based on priority and configuration settings. The script reads job configurations from a YAML file, handles default values, and executes jobs in sequence or parallel as needed.

## Features

- **Priority-Based Execution**: Runs Glue jobs in order of their priority. Jobs with the same priority run in parallel.
- **Default Configuration**: Uses default values for worker type, `from-date`, and `to-date` if not specified in the job configuration.
- **Logging**: Logs job start times, statuses, and other relevant information with timestamps.

## Configuration

### `config/glue-config.yml`

The `config/glue-config.yml` file is used to define the AWS credentials, default job configurations, and specific job settings.

#### Example

```yaml
defaults:
  worker_type: "Standard"
  from_date: "2024-01-01"
  to_date: "2024-12-31"

aws:
  access_key_id: "your-access-key-id"
  secret_access_key: "your-secret-access-key"
  session_token: "your-session-token"

jobs:
  party:
    priority: 1
    # Uses default worker_type, from_date, and to_date
  account:
    priority: 2
    worker_type: "G.1X"
    from_date: "2024-02-01"
    to_date: "2024-11-30"
  tm:
    priority: 2
    worker_type: "G.2X"
    # Uses default from_date and to_date
  f-tm:
    priority: 3
    # Uses default worker_type, from_date, and to_date
```
- **defaults**: Defines default values for jobs.
- **aws**: Contains AWS credentials and session tokens.
- **jobs**: Defines each job with its priority and specific settings.

## Prerequisites
- Python 3.6+
- AWS Boto3: For interacting with AWS Glue.
- PyYAML: For parsing YAML configuration files.

## Installation
1. Clone the repository:

```bash
git clone https://github.com/your-repo/aws-glue-job-scheduler.git
cd glue-job-runner
```
2. Install required Python packages:

```bash
pip install boto3 pyyaml
````
3. Update the config/glue-config.yml file with your AWS credentials and job configurations.

## Usage
Run the Python script to start the Glue jobs based on the configuration:
To run the script with the default configuration file ([glue-config.yml](config%2Fglue-config.yml)):
```bash
python job-runner.py
```
To run the script with a custom configuration file:
```bash
python priority_jobs.py --config path/to/your/config.yaml
```