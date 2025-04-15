# scripts/pipeline.py

from data_cleaning import clean_and_merge_data


def run_pipeline():
    print("Running e-commerce data pipeline...")
    clean_and_merge_data()
    print("Pipeline complete. Processed data ready for dashboard.")


if __name__ == "__main__":
    run_pipeline()
