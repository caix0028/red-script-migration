import os
import sys

# This is needed to run debug with VSCode
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import shutil
import unittest
from datetime import date, timedelta
from unittest.mock import patch

import pandas as pd
from stk_scheduled_task.stk_scheduled_task import f_folder_creator, main


class TestStkScheduledTask(unittest.TestCase):
    @patch("stk_scheduled_task.stk_scheduled_task.os.path.exists", return_value=False)
    @patch("stk_scheduled_task.stk_scheduled_task.os.makedirs")
    @patch("stk_scheduled_task.stk_scheduled_task.datetime")
    def test_folder_creator_with_append_timestamp(
        self, mock_datetime, mock_makedirs, _
    ):
        mock_datetime.now.return_value.strftime.return_value = "_123456"
        mock_datetime.strftime.return_value = "2022Jan"
        date_format = f_folder_creator(mock_datetime, True)
        mock_makedirs.assert_called_once_with("2022Jan_123456")
        self.assertEqual(date_format, "2022Jan_123456")

    @patch("stk_scheduled_task.stk_scheduled_task.os.path.exists", return_value=False)
    @patch("stk_scheduled_task.stk_scheduled_task.os.makedirs")
    @patch("stk_scheduled_task.stk_scheduled_task.datetime")
    def test_folder_creator_without_append_timestamp(
        self, mock_datetime, mock_makedirs, _
    ):
        mock_datetime.strftime.return_value = "2022Jan"
        date_format = f_folder_creator(mock_datetime, False)
        mock_makedirs.assert_called_once_with("2022Jan")
        self.assertEqual(date_format, "2022Jan")

    @patch("stk_scheduled_task.stk_scheduled_task.os.path.exists", return_value=True)
    @patch("stk_scheduled_task.stk_scheduled_task.os.makedirs")
    @patch("stk_scheduled_task.stk_scheduled_task.datetime")
    def test_folder_creator_folder_exists(self, mock_datetime, mock_makedirs, _):
        mock_datetime.now.return_value.strftime.return_value = "_123456"
        mock_datetime.strftime.return_value = "2022Jan"
        date_format = f_folder_creator(mock_datetime, True)
        mock_makedirs.assert_not_called()
        self.assertEqual(date_format, "2022Jan_123456")

    @patch("stk_scheduled_task.stk_scheduled_task.f_folder_creator")
    @patch("stk_scheduled_task.stk_scheduled_task.f_get_table")
    def test_main(
        self,
        mock_get_table,
        mock_folder_creator,
    ):
        mock_folder_creator.return_value = "target_folder"
        os.mkdir("target_folder")
        # Mocking the return values
        mock_get_table.side_effect = [
            pd.DataFrame(
                {
                    "address": ["address1", "address2"],
                    "meter_name": ["meter_name1", "meter_name2"],
                    "excel_name": ["excel_name1", "excel_name2"],
                    "meter_id": ["meter_id1", "meter_id2"],
                    "meter_type": ["Solar Generation", "Solar Export"],
                }
            ),
            pd.DataFrame(
                {
                    "meter_id": ["meter_id1", "meter_id2"],
                    "date": ["2023-01-02", "2023-01-02"],
                    "energy": [100, 50],
                }
            ),
        ]

        # Call the function
        main()

        self.assertTrue(os.path.exists("lostcomms_detected_log.log"))
        os.remove("lostcomms_detected_log.log")

        self.assertTrue(os.path.exists("data_availability.csv"))
        os.remove("data_availability.csv")

        last_day_prev_mth = date.today().replace(day=1) - timedelta(days=1)
        date_start = last_day_prev_mth.replace(day=1)
        self.assertTrue(
            os.path.exists(
                os.path.join(
                    "target_folder",
                    f"STK-{date_start.strftime('%b%Y')}-MonthlyReport-GwExport.xlsx",
                )
            )
        )
        shutil.rmtree("target_folder")


if __name__ == "__main__":
    unittest.main()
