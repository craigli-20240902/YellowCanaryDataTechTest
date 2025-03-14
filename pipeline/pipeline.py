import luigi
import pandas as pd
from pathlib import Path
from pipeline_utils import (
    calculate_ote_and_super,
    calculate_variance,
    calculate_disbursed,
    refine_merged_df,
)

RAW_DATA_DIR = "data/raw"
EXTRACTED_DATA_DIR = "data/extracted"
METRICS_DIR = "metrics"
METRICS_FILE = "metrics.csv"
PAYSLIPS_FILE = "Payslips.csv"
DISBURSEMENTS_FILE = "Disbursements.csv"
PAYCODES_FILE = "PayCodes.csv"


class ConvertExcelToCSV(luigi.Task):
    """
    Luigi Task to convert specific sheets from an Excel file to CSV files.
    Attributes:
        source_file (luigi.Parameter): The path to the source Excel file.
        target_directory (luigi.Parameter): The directory where the CSV files will be saved.
    Methods:
        output(): Specifies the output targets for the task.
        run(): Reads the specified sheets from the Excel file and writes them as CSV files to the target directory.
    """

    source_file = luigi.Parameter()
    target_directory = luigi.Parameter()

    def output(self):
        return [
            luigi.LocalTarget(f"{self.target_directory}/{DISBURSEMENTS_FILE}"),
            luigi.LocalTarget(f"{self.target_directory}/{PAYSLIPS_FILE}"),
            luigi.LocalTarget(f"{self.target_directory}/{PAYCODES_FILE}"),
        ]

    def run(self):
        disbursements_df = pd.read_excel(
            self.source_file, sheet_name=DISBURSEMENTS_FILE.replace(".csv", "")
        )
        payslips_df = pd.read_excel(
            self.source_file, sheet_name=PAYSLIPS_FILE.replace(".csv", "")
        )
        paycodes_df = pd.read_excel(
            self.source_file, sheet_name=PAYCODES_FILE.replace(".csv", "")
        )
        disbursements_df.to_csv(self.output()[0].path, index=False)
        payslips_df.to_csv(self.output()[1].path, index=False)
        paycodes_df.to_csv(self.output()[2].path, index=False)

        print("CSV files have been created successfully.")


class CalculateMetrics(luigi.Task):
    """
    A Luigi Task to calculate metrics from raw data.

    Attributes:
        base_path (luigi.Parameter): The base directory path where data is stored.
        excel_super_data (luigi.Parameter): The name of the Excel file containing the super data.

    Methods:
        requires(): Specifies the task dependencies.
        output(): Specifies the output target of the task.
        run(): Executes the task to calculate metrics and save the results.

    Requires:
        ConvertExcelToCSV: A task to convert the Excel file to CSV format.

    Outputs:
        A CSV file and an Excel file containing the calculated metrics.

    Example:
        luigi.build([CalculateMetrics(base_path='/path/to/base', excel_super_data='data.xlsx')])
    """
    base_path = luigi.Parameter()
    excel_super_data = luigi.Parameter()

    def requires(self):
        source_file = (
            f"{self.base_path}/{RAW_DATA_DIR}/{self.excel_super_data}"
        )
        return ConvertExcelToCSV(
            source_file=source_file,
            target_directory=f"{self.base_path}/{EXTRACTED_DATA_DIR}",
        )

    def output(self):
        return luigi.LocalTarget(
            f"{self.base_path}/{METRICS_DIR}/{METRICS_FILE}"
        )

    def run(self):
        payslips = pd.read_csv(
            f"{self.base_path}/{EXTRACTED_DATA_DIR}/{PAYSLIPS_FILE}"
        )
        disbursements = pd.read_csv(
            f"{self.base_path}/{EXTRACTED_DATA_DIR}/{DISBURSEMENTS_FILE}"
        )
        pay_codes = pd.read_csv(
            f"{self.base_path}/{EXTRACTED_DATA_DIR}/{PAYCODES_FILE}"
        )
        ote_super = calculate_ote_and_super(payslips, pay_codes)
        disbursed = calculate_disbursed(disbursements)
        # calculate the variance based on ote_super and disbursed
        merged_df = calculate_variance(ote_super, disbursed)
        print("Merged data: ", merged_df)
        merged_df = refine_merged_df(merged_df)
        merged_df.to_csv(self.output().path, index=False)
        merged_df.to_excel(
            self.output().path.replace(".csv", ".xlsx"), index=False
        )
        print("Metrics have been calculated and saved successfully.")


if __name__ == "__main__":
    base_path = Path(
        input(
            "Enter the path to the base directory containing the data folder: "
        )
    )
    excel_super_data = input("Enter the name of the Super data excel file: ")
    luigi.build(
        [
            CalculateMetrics(
                base_path=base_path, excel_super_data=excel_super_data
            )
        ],
        local_scheduler=True,
    )
