import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
from datetime import date, timedelta
from unittest.mock import Mock, patch

import pandas as pd
from dateutil.relativedelta import relativedelta
from fusion_solar_data_ingestion.update_fusion_solar_pyranometer_data import (
    get_energy_data,
    main,
)


class TestUpdateFusionSolarPyranometerData(unittest.TestCase):
    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_pyranometer_data.retry_post_request"
    )
    def test_get_energy_data_success(self, mock_retry_post_request):
        mock_response = Mock()
        mock_response.json.return_value = {
            "success": True,
            "data": [
                {
                    "collectTime": 1622527200000,
                    "dataItemMap": {"radiation_intensity": 100},
                    "stationCode": "station1",
                },
                {
                    "collectTime": 1622527200000,
                    "dataItemMap": {"radiation_intensity": 200},
                    "stationCode": "station2",
                },
            ],
        }
        mock_retry_post_request.return_value = mock_response
        stationCodes = ["station1", "station2"]
        collectTime = "2021-06-01"
        api_token = "test_token"
        fs_pyr_list = pd.DataFrame(
            {"stationcodes": ["station1", "station2"], "pyr_id": [1, 2]}
        )

        df = get_energy_data(stationCodes, collectTime, api_token, fs_pyr_list)

        self.assertEqual(len(df), 2)
        self.assertEqual(df["date"].tolist(), ["2021-06-01", "2021-06-01"])
        self.assertEqual(df["pyr_id"].tolist(), [1, 2])
        self.assertEqual(df["sunhours"].tolist(), [100.0, 200.0])

    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_pyranometer_data.retry_post_request"
    )
    def test_get_energy_data_failure(self, mock_retry_post_request):
        mock_response = Mock()
        mock_response.json.return_value = {
            "success": False,
            "failCode": "500",
            "message": "Error message",
        }
        mock_retry_post_request.return_value = mock_response
        stationCodes = ["station1", "station2"]
        collectTime = "2021-06-01"
        api_token = "test_token"
        fs_pyr_list = pd.DataFrame(
            {"stationcodes": ["station1", "station2"], "pyr_id": [1, 2]}
        )

        with self.assertRaises(SystemExit) as cm:
            get_energy_data(stationCodes, collectTime, api_token, fs_pyr_list)
        self.assertEqual(cm.exception.code, 1)

    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_pyranometer_data.f_get_table"
    )
    def test_main_with_energy_data_already_updated(
        self,
        mock_f_get_table,
    ):
        mock_f_get_table.side_effect = [
            pd.DataFrame(
                {
                    "pyr_id": [1, 2],
                    "stationcodes": ["station1", "station2"],
                    "location": ["location1", "location2"],
                }
            ),
            pd.DataFrame(
                {
                    "date": [date.today() - relativedelta(days=1)],
                }
            ),
        ]

        with self.assertRaises(SystemExit) as cm:
            main()
        self.assertEqual(cm.exception.code, 0)

    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_pyranometer_data.f_get_table"
    )
    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_pyranometer_data.get_api_token"
    )
    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_pyranometer_data.get_querytimes"
    )
    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_pyranometer_data.get_energy_data"
    )
    def test_main_lost_comm(
        self,
        mock_get_energy_data,
        mock_get_querytimes,
        mock_get_api_token,
        mock_f_get_table,
    ):
        mock_f_get_table.side_effect = [
            pd.DataFrame(
                {
                    "pyr_id": [1, 2],
                    "stationcodes": ["station1", "station2"],
                    "location": ["location1", "location2"],
                }
            ),
            pd.DataFrame(
                {
                    "date": [date.today() - relativedelta(days=2)],
                }
            ),
        ]
        mock_get_api_token.return_value = "test_token"
        mock_get_querytimes.return_value = ["2021-06-01"]
        mock_get_energy_data.return_value = pd.DataFrame(
            {
                "date": ["2021-06-01", "2021-06-02"],
                "sunhours": [100.0, 200.0],
                "pyr_id": [1, 2],
            }
        )

        main()

    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_pyranometer_data.f_get_table"
    )
    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_pyranometer_data.get_api_token"
    )
    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_pyranometer_data.get_querytimes"
    )
    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_pyranometer_data.get_energy_data"
    )
    def test_main_with_data_append(
        self,
        mock_get_energy_data,
        mock_get_querytimes,
        mock_get_api_token,
        mock_f_get_table,
    ):
        mock_f_get_table.side_effect = [
            pd.DataFrame(
                {
                    "pyr_id": [1, 2],
                    "stationcodes": ["station1", "station2"],
                    "location": ["location1", "location2"],
                }
            ),
            pd.DataFrame(
                {
                    "date": [date.today() - relativedelta(days=2)],
                }
            ),
        ]
        mock_get_api_token.return_value = "test_token"
        mock_get_querytimes.return_value = [
            (date.today() - relativedelta(days=1)).strftime("%Y-%m-%d")
        ]
        mock_get_energy_data.return_value = pd.DataFrame(
            {
                "date": [
                    (date.today() - relativedelta(days=1)).strftime("%Y-%m-%d"),
                    "2021-06-02",
                ],
                "sunhours": [100.0, 200.0],
                "pyr_id": [1, 2],
            }
        )

        main()


if __name__ == "__main__":
    unittest.main()
