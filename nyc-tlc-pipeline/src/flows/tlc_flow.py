from prefect import flow, task
import subprocess

@task
def download():
    subprocess.run(["python", "nyc-tlc-pipeline/src/ingestion/download_tlc.py"])

@task
def clean():
    subprocess.run(["python", "nyc-tlc-pipeline/src/transforms/clean_tlc.py"])

@flow
def pipeline():
    download()
    clean()

if __name__ == "__main__":
    pipeline()