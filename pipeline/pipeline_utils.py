from datetime import datetime
import pandas as pd
from enum import Enum

# Define the quarter periods
QUARTERS = {
    "Q1": {
        "payment_start": "01-29",
        "payment_end": "04-28",
    },
    "Q2": {
        "payment_start": "04-29",
        "payment_end": "07-28",
    },
    "Q3": {
        "payment_start": "07-29",
        "payment_end": "10-28",
    },
    "Q4": {
        "payment_start": "10-29",
        "payment_end": "01-28",
    },
}
OTE_SUPER_RATE = 0.095
GROUP_BY_CRITERIA = ["employee_code", "year", "quarter"]


class Quarter(Enum):
    Q1 = "Q1"
    Q2 = "Q2"
    Q3 = "Q3"
    Q4 = "Q4"


def read_csv(file_path: str) -> pd.DataFrame:
    """Read a CSV file and return a pandas DataFrame."""
    return pd.read_csv(file_path)


def get_seasonal_quarter(date_str: str) -> str:
    date = datetime.strptime(date_str, "%Y-%m-%d")
    if 1 <= date.month <= 3:
        return Quarter.Q1.value
    elif 4 <= date.month <= 6:
        return Quarter.Q2.value
    elif 7 <= date.month <= 9:
        return Quarter.Q3.value
    else:
        return Quarter.Q4.value


def get_disbursed_year(date_str: str) -> int:
    """Get the year of the disbursement based on the date of
    the disbursement."""
    date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
    if date.month == 1 and date.day < 29:
        return date.year - 1
    else:
        return date.year


def get_year(date_str: str) -> int:
    date = datetime.strptime(date_str, "%Y-%m-%d")
    return date.year

# A function that determines from when the disbursement is from


def get_disbursed_quarter(date_str: str) -> str:
    """Get the quarter of the year based on the date of the disbursement."""
    # assume the date time appeared in the sample payment excel file
    # is all in the timezone of Australia/Sydney
    date_time = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
    year = date_time.year
    for quarter, periods in QUARTERS.items():
        # handle the special case of Q4 to have correct year for payment
        # start and end
        if quarter == "Q4":
            if date_time.month == 1 and date_time.day < 29:
                # handle payment start time between Jan 1 and Jan 28
                payment_start = datetime.strptime(
                    f"{year-1}-{periods['payment_start']}", "%Y-%m-%d"
                )
                # handle payment end time between Jan 1 and Jan 28
                payment_end = datetime.strptime(
                    f"{year}-{periods['payment_end']}", "%Y-%m-%d"
                )
            else:
                # handle payment end time between October to December
                payment_start = datetime.strptime(
                    f"{year}-{periods['payment_start']}", "%Y-%m-%d"
                )
                payment_end = datetime.strptime(
                    f"{year+1}-{periods['payment_end']}", "%Y-%m-%d"
                )
        else:
            payment_start = datetime.strptime(
                f"{year}-{periods['payment_start']}", "%Y-%m-%d"
            )
            payment_end = datetime.strptime(
                f"{year}-{periods['payment_end']}", "%Y-%m-%d"
            )
        print(
            "time: ",
            datetime.strptime(date_time.strftime("%Y-%m-%d"), "%Y-%m-%d"),
        )
        if (
            payment_start
            <= datetime.strptime(date_time.strftime("%Y-%m-%d"), "%Y-%m-%d")
            <= payment_end
        ):
            return quarter
    return None


def calculate_ote_and_super(
    payslips: pd.DataFrame, paycodes: pd.DataFrame
) -> pd.DataFrame:
    """Calculate the total OTE and super payable amount for each
    employee per year and quarter."""
    ote_df = filter_ote_payable(payslips, paycodes)
    # Group by EmployeeCode, Year, and Quarter
    ote_grouped = (
        ote_df.groupby(["employee_code", "year", "quarter"])
        .agg({"amount": "sum", "super_payable": "sum"})
        .reset_index()
    )
    ote_grouped.columns = [
        "employee_code",
        "year",
        "quarter",
        "total_ote",
        "total_super_payable",
    ]
    return ote_grouped

# The function that can calculate what super is payable 


def filter_ote_payable(
    payslips: pd.DataFrame, paycodes: pd.DataFrame
) -> pd.DataFrame:
    """filter the OTE and super payable amount for each
    employee."""
    # Merge payslips with paycodes to determine if each pay code is OTE
    merged_df = pd.merge(
        payslips, paycodes, left_on="code", right_on="pay_code"
    )
    ote_df = merged_df[merged_df["ote_treament"] == "OTE"]
    # Calculate the super payable amount based on the OTE amount and 0.095 rate
    ote_df["super_payable"] = ote_df["amount"] * OTE_SUPER_RATE
    # Get the natural quarter and year of the payslip when the payment ends
    ote_df["quarter"] = ote_df["end"].apply(get_seasonal_quarter)
    ote_df["year"] = ote_df["end"].apply(get_year)
    return ote_df


def calculate_disbursed(disbursements: pd.DataFrame) -> pd.DataFrame:
    """Calculate the total disbursed amount for each employee per
    year and quarter."""
    # Get the natural quarter and year of the disbursement
    disbursements["quarter"] = disbursements["payment_made"].apply(
        get_disbursed_quarter
    )
    disbursements["year"] = disbursements["payment_made"].apply(
        get_disbursed_year
    )
    disbursements_grouped = (
        disbursements.groupby(["employee_code", "year", "quarter"])
        .agg({"sgc_amount": "sum"})
        .reset_index()
    )
    disbursements_grouped.columns = [
        "employee_code",
        "year",
        "quarter",
        "total_disbursed",
    ]
    return disbursements_grouped


# The function that establishes the variance between what was
# payable and what was disbursed

def calculate_variance(
    ote_super: pd.DataFrame, disbursed: pd.DataFrame
) -> pd.DataFrame:
    merged_df = pd.merge(
        ote_super, disbursed, on=GROUP_BY_CRITERIA, how="outer"
    ).fillna(0)
    merged_df["variance"] = (
        merged_df["total_super_payable"] - merged_df["total_disbursed"]
    )

    return merged_df


def refine_merged_df(merged_df: pd.DataFrame) -> pd.DataFrame:
    """Refine the merged DataFrame by selecting the required columns,
    removing suffixes, rounding the required columns, and sorting the
    DataFrame by employee_code, year, and quarter."""
    # Select the required columns
    selected_columns = [
        "employee_code",
        "year",
        "quarter",
        "total_ote",
        "total_super_payable",
        "total_disbursed",
        "variance",
    ]
    merged_df = merged_df[selected_columns]
    # Round the required columns to 4 decimal places using apply function
    columns_to_round = [
        "total_ote",
        "total_super_payable",
        "total_disbursed",
        "variance",
    ]
    merged_df[columns_to_round] = merged_df[columns_to_round].apply(
        lambda x: x.round(2)
    )
    # Sort the DataFrame by employee_code, year, and quarter
    merged_df = merged_df.sort_values(by=["employee_code", "year", "quarter"])
    return merged_df
