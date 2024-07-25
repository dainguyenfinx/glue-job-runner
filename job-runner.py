import boto3
import yaml
import time
import datetime
import argparse
from concurrent.futures import ThreadPoolExecutor


def load_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)


def log(message):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] {message}")


def run_glue_job(job_name, job_details, defaults, glue_client):
    priority = job_details.get('priority', 0)
    worker_type = job_details.get('worker_type', defaults.get('worker_type', 'Standard'))
    number_of_workers = job_details.get('number_of_workers', defaults.get('number_of_workers', 2))
    from_date = job_details.get('from_date', defaults.get('from_date', '9999-69-96'))
    to_date = job_details.get('to_date', defaults.get('to_date', '9999-96-69'))

    log(f"Starting Glue job with priority {priority}, worker type {worker_type}, from-date {from_date}, to-date {to_date}: {job_name}")

    start_response = glue_client.start_job_run(
        JobName=job_name,
        WorkerType=worker_type,
        NumberOfWorkers=number_of_workers,
        Arguments={
            '--from-date': from_date,
            '--to-date': to_date
        }
    )

    job_run_id = start_response['JobRunId']

    while True:
        job_status = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)['JobRun']['JobRunState']
        if job_status == 'SUCCEEDED':
            log(f"Glue job {job_name} with priority {priority}, worker type {worker_type}, from-date {from_date}, to-date {to_date} completed successfully.")
            break
        elif job_status in ['FAILED', 'TIMEOUT']:
            log(f"Glue job {job_name} with priority {priority}, worker type {worker_type}, from-date {from_date}, to-date {to_date} failed with status: {job_status}.")
            return False
        else:
            log(f"Glue job {job_name} with priority {priority}, worker type {worker_type}, from-date {from_date}, to-date {to_date} is still running. Status: {job_status}. Waiting...")
            time.sleep(30)
    return True


def main():
    parser = argparse.ArgumentParser(description='Run AWS Glue jobs based on priority and configuration.')
    parser.add_argument('--config', default='config/glue-config.yml',
                        help='Path to the configuration file (default: config/glue-config.yml)')
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    aws_credentials = config['aws']
    jobs_config = config['jobs']
    defaults = config.get('defaults', {})

    # Initialize boto3 client
    glue_client = boto3.client(
        'glue',
        aws_access_key_id=aws_credentials['aws_access_key_id'],
        aws_secret_access_key=aws_credentials['aws_secret_access_key'],
        aws_session_token=aws_credentials['aws_session_token']
    )

    # Group jobs by priority
    jobs_by_priority = {}
    for job_name, job_details in jobs_config.items():
        priority = job_details.get('priority', 0)
        if priority not in jobs_by_priority:
            jobs_by_priority[priority] = []
        jobs_by_priority[priority].append((job_name, job_details))

    # Run jobs by priority
    for priority in sorted(jobs_by_priority.keys()):
        print("\n")
        log(f"Running jobs with priority {priority}")
        with ThreadPoolExecutor() as executor:
            results = list(
                executor.map(lambda job: run_glue_job(*job, defaults, glue_client), jobs_by_priority[priority]))
            if not all(results):
                log("One or more jobs failed. Exiting...")
                break

    log("All Glue jobs completed.")


if __name__ == '__main__':
    main()
