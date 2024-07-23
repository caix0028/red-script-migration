import os
import sys

# This is needed to run debug with VSCode
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
from datetime import datetime
from unittest.mock import Mock, call, patch

import pandas as pd
from envision_accumulative_reading.get_envision_accumulative_meter_readings import (
    get_energy_data,
    main,
)


class TestGetEnergyData(unittest.TestCase):
    @patch(
        "envision_accumulative_reading.get_envision_accumulative_meter_readings.retry_get_request"
    )
    def test_get_energy_data_success(self, mock_retry_get_request):
        mock_response = Mock()
        mock_response.json.return_value = {
            "result": {
                "abc": {
                    "points": {
                        "123": {
                            "APProductionKWH": {
                                "timestamp": 1622527200000,
                                "value": 100,
                            },
                            "APConsumedKWH": {"value": 200},
                        }
                    }
                }
            }
        }
        mock_retry_get_request.return_value = mock_response

        mdmids = ["mdmid1"]
        metric = "123metric1"
        env_meter_list_temp = pd.DataFrame(
            {"api_metrics": [metric], "mdmids": ["abc"], "meter_id": ["123"]}
        )

        df = get_energy_data(mdmids, metric, env_meter_list_temp)

        expected_df = pd.DataFrame(
            {
                "mdmids": ["abc"],
                "timestamp": [pd.to_datetime(1622527200000, unit="ms")],
                "exported": [100],
                "imported": [200],
                "api_metrics": [metric],
                "meter_id": ["123"],
                "lost_comms": [True],
            }
        )
        assert df.equals(expected_df)

    @patch(
        "envision_accumulative_reading.get_envision_accumulative_meter_readings.retry_get_request"
    )
    def test_get_energy_data_failure(self, mock_retry_get_request):
        mock_response = Mock()
        mock_response.json.return_value = {"status": "500", "msg": "Error message"}
        mock_retry_get_request.return_value = mock_response

        mdmids = ["mdmid1"]
        metric = "metric1"
        env_meter_list_temp = pd.DataFrame(
            {"api_metrics": ["metric1"], "mdmids": ["mdmid1"]}
        )

        with self.assertRaises(SystemExit) as cm:
            get_energy_data(mdmids, metric, env_meter_list_temp)
        self.assertEqual(cm.exception.code, 1)

    @patch(
        "envision_accumulative_reading.get_envision_accumulative_meter_readings.f_get_table"
    )
    @patch(
        "envision_accumulative_reading.get_envision_accumulative_meter_readings.get_energy_data"
    )
    @patch(
        "envision_accumulative_reading.get_envision_accumulative_meter_readings.pd.concat"
    )
    def test_main(self, mock_concat, mock_get_energy_data, mock_f_get_table):
        envision_meter_list = pd.DataFrame(
            {"api_metrics": ["metric1", "metric2"], "mdmids": ["mdmid1", "mdmid2"]}
        )

        mock_f_get_table.return_value = envision_meter_list
        mock_get_energy_data.return_value = pd.DataFrame()
        mock_concat.return_value = pd.DataFrame()

        main()

        filename = (
            f"Envision_meter_readings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        self.assertTrue(os.path.exists(filename))
        os.remove(filename)  # cleanup


if __name__ == "__main__":
    unittest.main()
