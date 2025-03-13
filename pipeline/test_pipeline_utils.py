import pytest
import pandas as pd
from pipeline_utils import (
    read_csv,
    calculate_ote_and_super,
    calculate_disbursed,
    calculate_variance,
    get_disbursed_quarter,
    refine_merged_df,
    get_seasonal_quarter,
)


@pytest.fixture
def csv_file(tmp_path):
    data = """name,age,city
John,30,New York
Jane,25,Los Angeles
Doe,22,Chicago"""
    file_path = tmp_path / "test.csv"
    file_path.write_text(data)
    return file_path


def test_read_csv(csv_file):
    result = read_csv(csv_file)
    expected = [
        {"name": "John", "age": 30, "city": "New York"},
        {"name": "Jane", "age": 25, "city": "Los Angeles"},
        {"name": "Doe", "age": 22, "city": "Chicago"},
    ]
    expected = pd.DataFrame(expected)
    pd.testing.assert_frame_equal(result, expected)


@pytest.fixture
def payslips():
    return pd.DataFrame(
        {
            "employee_code": ["E1", "E2", "E1"],
            "code": ["C1", "C2", "C1"],
            "amount": [1000, 2000, 1500],
            "quarter": ["Q1", "Q1", "Q2"],
            "end": ["2023-01-30", "2023-04-30", "2023-05-23"],
        }
    )


@pytest.fixture
def paycodes():
    return pd.DataFrame(
        {"pay_code": ["C1", "C2"], "ote_treament": ["OTE", "NON-OTE"]}
    )


def test_calculate_ote_and_super(payslips, paycodes):
    result = calculate_ote_and_super(payslips, paycodes)
    print("result: ", result)
    expected = pd.DataFrame(
        {
            "employee_code": ["E1", "E1"],
            "year": [2023, 2023],
            "quarter": ["Q1", "Q2"],
            "total_ote": [1000, 1500],
            "total_super_payable": [95, 142.5],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


@pytest.fixture
def disbursements():
    return pd.DataFrame(
        {
            "employee_code": ["E1", "E2", "E1"],
            "payment_made": [
                "2023-02-15T00:00:00",
                "2023-05-15T00:00:00",
                "2023-08-15T00:00:00",
            ],
            "sgc_amount": [100, 200, 150],
        }
    )


def test_calculate_disbursed(disbursements, mocker):
    result = calculate_disbursed(disbursements)
    print("result: ", result)
    expected = pd.DataFrame(
        {
            "employee_code": ["E1", "E1", "E2"],
            "year": [2023, 2023, 2023],
            "quarter": ["Q1", "Q3", "Q2"],
            "total_disbursed": [100, 150, 200],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_calculate_variance():
    ote_grouped = pd.DataFrame(
        {
            "employee_code": ["E1", "E1"],
            "year": [2023, 2023],
            "quarter": ["Q1", "Q2"],
            "total_ote": [1000, 1500],
            "total_super_payable": [95, 142.5],
        }
    )
    disbursements_grouped = pd.DataFrame(
        {
            "employee_code": ["E1", "E1"],
            "year": [2023, 2023],
            "quarter": ["Q1", "Q2"],
            "total_disbursed": [100, 200],
        }
    )
    result = calculate_variance(ote_grouped, disbursements_grouped)
    print(result)
    expected = pd.DataFrame(
        {
            "employee_code": ["E1", "E1"],
            "year": [2023, 2023],
            "quarter": ["Q1", "Q2"],
            "total_ote": [1000, 1500],
            "total_super_payable": [95, 142.5],
            "total_disbursed": [100, 200],
            "variance": [-5.0, -57.5],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_get_disbursed_quarter():
    assert get_disbursed_quarter("2023-01-30T00:00:00") == "Q1"
    assert get_disbursed_quarter("2023-04-30T00:00:00") == "Q2"
    assert get_disbursed_quarter("2023-07-30T00:00:00") == "Q3"
    assert get_disbursed_quarter("2023-04-28T00:00:00") == "Q1"
    assert get_disbursed_quarter("2023-07-28T00:00:00") == "Q2"
    assert get_disbursed_quarter("2023-10-28T00:00:00") == "Q3"
    assert get_disbursed_quarter("2023-01-29T00:00:00") == "Q1"
    assert get_disbursed_quarter("2023-04-29T00:00:00") == "Q2"
    assert get_disbursed_quarter("2023-07-29T00:00:00") == "Q3"
    assert get_disbursed_quarter("2023-10-30T00:00:00") == "Q4"
    assert get_disbursed_quarter("2023-01-28T00:00:00") == "Q4"
    assert get_disbursed_quarter("2023-10-29T00:00:00") == "Q4"
    assert get_disbursed_quarter("2023-01-26T00:00:00") == "Q4"


def test_refine_merged_df():
    merged_df = pd.DataFrame(
        {
            "employee_code": ["E1", "E1", "E2"],
            "year": [2023, 2023, 2023],
            "quarter": ["Q1", "Q2", "Q1"],
            "total_ote": [1000, 1500, 2000],
            "total_super_payable": [95, 142.5, 190],
            "total_disbursed": [100, 200, 250],
            "variance": [-5.0, -57.5, -60.0],
        }
    )
    result = refine_merged_df(merged_df)
    expected = pd.DataFrame(
        {
            "employee_code": ["E1", "E1", "E2"],
            "year": [2023, 2023, 2023],
            "quarter": ["Q1", "Q2", "Q1"],
            "total_ote": [1000, 1500, 2000],
            "total_super_payable": [95, 142.5, 190],
            "total_disbursed": [100, 200, 250],
            "variance": [-5.0, -57.5, -60.0],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_get_seasonal_quarter():
    assert get_seasonal_quarter("2023-01-15") == "Q1"
    assert get_seasonal_quarter("2023-03-31") == "Q1"
    assert get_seasonal_quarter("2023-04-01") == "Q2"
    assert get_seasonal_quarter("2023-06-30") == "Q2"
    assert get_seasonal_quarter("2023-07-01") == "Q3"
    assert get_seasonal_quarter("2023-09-30") == "Q3"
    assert get_seasonal_quarter("2023-10-01") == "Q4"
    assert get_seasonal_quarter("2023-12-31") == "Q4"
