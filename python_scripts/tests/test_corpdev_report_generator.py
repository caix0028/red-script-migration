import os
import sys

# This is needed to run debug with VSCode
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import shutil
import unittest
from datetime import date, datetime, timedelta
from unittest.mock import patch

import pandas as pd
from corpdev_report.corpdev_report_generator import main


class TestCorpdevReportGenerator(unittest.TestCase):
    @patch("corpdev_report.corpdev_report_generator.pd.read_csv")
    @patch("corpdev_report.corpdev_report_generator.f_get_table")
    def test_main(self, mock_f_get_table, mock_read_csv):
        mock_read_csv.return_value = pd.DataFrame(
            {
                "input_df": [
                    "1",
                    "1",
                ]
            }
        )
        mock_f_get_table.side_effect = [
            pd.DataFrame(
                {
                    "proj_id": [1],
                    "proj_name": ["test"],
                    "turn_on_date": ["2020-01-01"],
                    "year": [2020],
                    "month": [1],
                    "energy_generated": [1],
                }
            ),
            pd.DataFrame(
                {
                    "proj_id": [1],
                    "proj_name": ["test"],
                    "turn_on_date": ["2020-01-01"],
                    "year": [2020],
                    "month": [1],
                    "energy_exported": [1],
                }
            ),
            pd.DataFrame(
                {
                    "proj_id": [1],
                    "proj_name": ["test"],
                    "year": [2020],
                    "system_size": [1],
                }
            ),
            pd.DataFrame(
                {
                    "proj_id": [1],
                    "proj_name": ["test"],
                    "year": [2020],
                    "month": [1],
                    "sunhours": [2],
                }
            ),
        ]
        main()

        date_end = date.today().replace(day=1) - timedelta(days=1)
        assert os.path.exists(
            f"Corpdev_report_{datetime.now().strftime('%Y%b')}/Sunseap Leasing Solar Portfolio till "
            + date_end.strftime("%B %Y")
            + ".xlsx"
        )

        shutil.rmtree(f"Corpdev_report_{datetime.now().strftime('%Y%b')}")


if __name__ == "__main__":
    unittest.main()
