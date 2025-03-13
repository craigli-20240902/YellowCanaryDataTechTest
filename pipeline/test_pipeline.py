import pytest
import luigi
import os
import pandas as pd
from pipeline import (
    ConvertExcelToCSV,
    CalculateMetrics,
    RAW_DATA_DIR,
    METRICS_DIR,
    METRICS_FILE,
    DISBURSEMENTS_FILE,
    PAYSLIPS_FILE,
    PAYCODES_FILE,
    EXTRACTED_DATA_DIR,
)
from typing import Callable

SAMPLE_EXCEL_FILE = "sample.xlsx"


@pytest.fixture
def temp_directory():
    """Fixture to create a temporary directory for storing test outputs."""
    os.makedirs("/tmp/data/raw", exist_ok=True)
    os.makedirs("/tmp/data/extracted", exist_ok=True)
    os.makedirs("/tmp/metrics", exist_ok=True)
    return str("/tmp")


@pytest.fixture
def sample_excel_file(temp_directory: str):
    """Creates a sample Excel file with dummy data."""
    file_path = os.path.join(temp_directory, "sample.xlsx")
    with pd.ExcelWriter(file_path) as writer:
        pd.DataFrame(
            {"pay_code": ["PC1", "PC2"], "ote_treatment": [1, 0]}
        ).to_excel(writer, sheet_name="PayCodes", index=False)
        pd.DataFrame({"Amount": [100, 200]}).to_excel(
            writer, sheet_name="Disbursements", index=False
        )
        pd.DataFrame({"ID": [1, 2], "Salary": [5000, 6000]}).to_excel(
            writer, sheet_name="Payslips", index=False
        )
    return file_path


@pytest.fixture
def disbursements():
    return pd.DataFrame(
        {
            "employee_code": [1115, 1118, 1115],
            "payment_made": [
                "2023-02-15T00:00:00",
                "2023-05-15T00:00:00",
                "2023-08-15T00:00:00",
            ],
            "sgc_amount": [100, 200, 150],
        }
    )


@pytest.fixture
def payslips():
    return pd.DataFrame(
        {
            "employee_code": [1115, 1118, 1115],
            "code": ["C1", "C2", "C1"],
            "amount": [1000, 2000, 1500],
            "end": ["2023-01-30", "2023-04-30", "2023-05-23"],
        }
    )


@pytest.fixture
def paycodes():
    return pd.DataFrame(
        {"pay_code": ["C1", "C2"], "ote_treament": ["OTE", "Not OTE"]}
    )


@pytest.fixture
def run_luigi():
    """Helper function to run Luigi tasks using local_scheduler."""

    def _run(task):
        luigi.build([task], local_scheduler=True, workers=1)

    return _run


def test_convert_excel_to_csv(
    run_luigi: Callable[..., None], sample_excel_file: str, temp_directory: str
):
    """Tests ConvertExcelToCSV task"""
    task = ConvertExcelToCSV(
        source_file=sample_excel_file, target_directory=temp_directory
    )
    task.run()

    # Verify the expected CSV files are created
    expected_files = [DISBURSEMENTS_FILE, PAYSLIPS_FILE, PAYCODES_FILE]
    for filename in expected_files:
        file_path = os.path.join(temp_directory, filename)
        assert os.path.exists(file_path)
        os.remove(file_path)
        print(f"File {file_path} deleted.")


def test_calculate_metrics(
    run_luigi: Callable[..., None],
    temp_directory: str,
    sample_excel_file: str,
    paycodes: pd.DataFrame,
    payslips: pd.DataFrame,
    disbursements: pd.DataFrame,
):

    metrics_file = os.path.join(temp_directory, METRICS_DIR, METRICS_FILE)

    excel_file_path = os.path.join(
        temp_directory, RAW_DATA_DIR, SAMPLE_EXCEL_FILE
    )
    # Create the sample Excel file
    with pd.ExcelWriter(excel_file_path) as writer:
        paycodes.to_excel(
            writer, sheet_name=PAYCODES_FILE.replace(".csv", ""), index=False
        )
        disbursements.to_excel(
            writer,
            sheet_name=DISBURSEMENTS_FILE.replace(".csv", ""),
            index=False,
        )
        payslips.to_excel(
            writer, sheet_name=PAYSLIPS_FILE.replace(".csv", ""), index=False
        )
    assert os.path.exists(excel_file_path), "Raw Excel file was not created."
    # Run the CalculateMetrics task
    task = CalculateMetrics(
        base_path=temp_directory,
        excel_super_data=SAMPLE_EXCEL_FILE,
    )
    run_luigi(task)
    # Check if the metrics file has the expected content
    metrics_df = pd.read_csv(metrics_file)  # Read the metrics file
    print("Actual metrics: ", metrics_df)
    expected_metrics = pd.DataFrame(
        {
            "employee_code": [1115, 1115, 1115, 1118],
            "year": [2023, 2023, 2023, 2023],
            "quarter": ["Q1", "Q2", "Q3", "Q2"],
            "total_ote": [1000.0, 1500.0, 0.0, 0.0],
            "total_super_payable": [95.0, 142.5, 0.0, 0.0],
            "total_disbursed": [100.0, 0.0, 150.0, 200.0],
            "variance": [-5.0, 142.5, -150.0, -200.0],
        }
    )
    print("Expected metrics: ", expected_metrics)
    pd.testing.assert_frame_equal(metrics_df, expected_metrics)
    # remove the files created for reproducibility
    if os.path.exists(metrics_file):
        os.remove(metrics_file)
        os.remove(metrics_file.replace(".csv", ".xlsx"))
        print(f"File {metrics_file} deleted.")
        os.remove(excel_file_path)
        print(f"File {excel_file_path} deleted.")
        os.remove(
            os.path.join(
                temp_directory, EXTRACTED_DATA_DIR, DISBURSEMENTS_FILE
            )
        )
        os.remove(
            os.path.join(temp_directory, EXTRACTED_DATA_DIR, PAYSLIPS_FILE)
        )
        os.remove(
            os.path.join(temp_directory, EXTRACTED_DATA_DIR, PAYCODES_FILE)
        )
    else:
        print("File not found.")


def test_calculate_metrics_requires():
    """Test requires() method of CalculateMetrics task."""
    base_path = "/tmp"
    excel_super_data = "sample.xlsx"
    task = CalculateMetrics(
        base_path=base_path, excel_super_data=excel_super_data
    )
    dependency = task.requires()
    assert isinstance(dependency, ConvertExcelToCSV)
    assert dependency.source_file == f"{base_path}/data/raw/{excel_super_data}"
    assert dependency.target_directory == f"{base_path}/data/extracted"


def test_calculate_metrics_output():
    """Test output() method of CalculateMetrics task."""
    base_path = "/tmp"
    excel_super_data = "sample.xlsx"
    task = CalculateMetrics(
        base_path=base_path, excel_super_data=excel_super_data
    )
    expected_output_path = f"{base_path}/metrics/metrics.csv"
    assert task.output().path == expected_output_path
