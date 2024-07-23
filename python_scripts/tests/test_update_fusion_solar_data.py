import os
import sys

# This is needed to run debug with VSCode
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
from datetime import date
from unittest.mock import Mock, patch

import pandas as pd
from dateutil.relativedelta import relativedelta
from fusion_solar_data_ingestion.update_fusion_solar_data import (
    get_energy_data,
    get_plant_status,
    main,
)


class TestUpdateFusionSolarData(unittest.TestCase):
    @patch("fusion_solar_data_ingestion.update_fusion_solar_data.retry_post_request")
    def test_get_energy_data_success(self, mock_retry_post_request):
        mock_response = Mock()
        mock_response.json.return_value = {
            "success": True,
            "data": [
                {
                    "collectTime": 1622527200000,
                    "dataItemMap": {"inverter_power": 100},
                    "stationCode": "station1",
                },
                {
                    "collectTime": 1622527200000,
                    "dataItemMap": {"inverter_power": 200},
                    "stationCode": "station2",
                },
            ],
        }
        mock_retry_post_request.return_value = mock_response
        stationCodes = ["station1", "station2"]
        collectTime = "2021-06-01"
        api_token = "test_token"
        fs_meter_list = pd.DataFrame(
            {"stationcodes": ["station1", "station2"], "meter_id": [1, 2]}
        )

        df = get_energy_data(stationCodes, collectTime, api_token, fs_meter_list)

        self.assertEqual(len(df), 2)
        self.assertEqual(df["date"].tolist(), ["2021-06-01", "2021-06-01"])
        self.assertEqual(df["meter_id"].tolist(), [1, 2])
        self.assertEqual(df["energy"].tolist(), [100.0, 200.0])

    @patch("fusion_solar_data_ingestion.update_fusion_solar_data.retry_post_request")
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
        fs_meter_list = pd.DataFrame(
            {"stationcodes": ["station1", "station2"], "meter_id": [1, 2]}
        )

        with self.assertRaises(SystemExit) as cm:
            get_energy_data(stationCodes, collectTime, api_token, fs_meter_list)
        self.assertEqual(cm.exception.code, 1)

    @patch("fusion_solar_data_ingestion.update_fusion_solar_data.retry_post_request")
    def test_get_plant_status_success(self, mock_retry_post_request):
        mock_response = Mock()
        mock_response.json.return_value = {
            "success": True,
            "data": [
                {
                    "stationCode": "station1",
                    "dataItemMap": {"real_health_state": "1"},
                },
                {
                    "stationCode": "station2",
                    "dataItemMap": {"real_health_state": "2"},
                },
            ],
        }
        mock_retry_post_request.return_value = mock_response
        stationCodes = ["station1", "station2"]
        api_token = "test_token"
        fs_meter_list = pd.DataFrame(
            {"stationcodes": ["station1", "station2"], "meter_id": [1, 2]}
        )

        df = get_plant_status(stationCodes, api_token, fs_meter_list)

        self.assertEqual(len(df), 2)
        self.assertEqual(df["date"].tolist(), [date.today(), date.today()])
        self.assertEqual(df["meter_id"].tolist(), [1, 2])
        self.assertEqual(df["status"].tolist(), ["Disconnected", "Faulty"])

    @patch("fusion_solar_data_ingestion.update_fusion_solar_data.retry_post_request")
    def test_get_plant_status_failure(self, mock_retry_post_request):
        mock_response = Mock()
        mock_response.json.return_value = {
            "success": False,
            "failCode": "500",
            "message": "Error message",
        }
        mock_retry_post_request.return_value = mock_response
        stationCodes = ["station1", "station2"]
        api_token = "test_token"
        fs_meter_list = pd.DataFrame(
            {"stationcodes": ["station1", "station2"], "meter_id": [1, 2]}
        )

        with self.assertRaises(SystemExit) as cm:
            get_plant_status(stationCodes, api_token, fs_meter_list)
        self.assertEqual(cm.exception.code, 1)

    @patch("fusion_solar_data_ingestion.update_fusion_solar_data.f_get_table")
    def test_main_no_energy_data(
        self,
        mock_f_get_table,
    ):
        mock_f_get_table.side_effect = [
            pd.DataFrame({"meter_id": [1], "stationcodes": [1]}),
            pd.DataFrame(
                {
                    "meter_id": [1],
                    "meter_name": ["meter1"],
                    "meter_type": ["Type1"],
                    "proj_name": ["Project1"],
                    "proj_id": [1],
                    "monitoring_portal": ["FusionSolar"],
                }
            ),
            pd.DataFrame(),
        ]

        with self.assertRaises(SystemExit) as cm:
            main()

        self.assertEqual(cm.exception.code, 0)
        mock_f_get_table.assert_called()

    @patch("fusion_solar_data_ingestion.update_fusion_solar_data.f_get_table")
    def test_main_with_energy_data_already_updated(
        self,
        mock_f_get_table,
    ):
        mock_f_get_table.side_effect = [
            pd.DataFrame({"meter_id": [1], "stationcodes": [1]}),
            pd.DataFrame(
                {
                    "meter_id": [1],
                    "meter_name": ["meter1"],
                    "meter_type": ["Type1"],
                    "proj_name": ["Project1"],
                    "proj_id": [1],
                    "monitoring_portal": ["FusionSolar"],
                }
            ),
            pd.DataFrame(
                {
                    "date": [date.today() - relativedelta(days=1)],
                    "meter_id": [1],
                    "energy": [100.0],
                }
            ),
        ]

        with self.assertRaises(SystemExit) as cm:
            main()
        self.assertEqual(cm.exception.code, 0)

    @patch("fusion_solar_data_ingestion.update_fusion_solar_data.f_get_table")
    @patch("fusion_solar_data_ingestion.update_fusion_solar_data.get_api_token")
    @patch("fusion_solar_data_ingestion.update_fusion_solar_data.get_querytimes")
    @patch("fusion_solar_data_ingestion.update_fusion_solar_data.get_energy_data")
    @patch("fusion_solar_data_ingestion.update_fusion_solar_data.get_plant_status")
    def test_main_with_no_data_append(
        self,
        mock_get_plant_status,
        mock_get_energy_data,
        mock_get_querytimes,
        mock_get_api_token,
        mock_f_get_table,
    ):
        mock_f_get_table.side_effect = [
            pd.DataFrame({"meter_id": [1], "stationcodes": [1]}),
            pd.DataFrame(
                {
                    "meter_id": [1, 2],
                    "meter_name": ["meter1", "meter2"],
                    "meter_type": ["Type1", "Type2"],
                    "proj_name": ["Project1", "Project2"],
                    "proj_id": [1, 2],
                    "monitoring_portal": ["FusionSolar", "FusionSolar"],
                }
            ),
            pd.DataFrame(
                {
                    "date": [date.today() - relativedelta(days=2)],
                    "meter_id": [1],
                    "energy": [100.0],
                }
            ),
        ]
        mock_get_api_token.return_value = "test_token"
        mock_get_querytimes.return_value = ["2021-06-01"]
        mock_get_energy_data.return_value = pd.DataFrame(
            {"date": ["2021-06-01"], "meter_id": [1], "energy": [100.0]}
        )
        mock_get_plant_status.return_value = pd.DataFrame(
            {
                "status": ["Disconnected", "Faulty"],
                "meter_id": [1, 2],
                "meter_name": ["meter1", "meter2"],
                "date": [date.today(), date.today()],
            }
        )

        main()

        assert mock_get_energy_data.call_args[0][0] == "1"
        assert mock_get_energy_data.call_args[0][1] == "2021-06-01"
        assert mock_get_energy_data.call_args[0][2] == "test_token"
        assert mock_get_energy_data.call_args[0][3].equals(
            pd.DataFrame({"meter_id": [1], "stationcodes": [1]})
        )
        assert mock_get_plant_status.call_args[0][0] == "1"
        assert mock_get_plant_status.call_args[0][1] == "test_token"
        assert mock_get_plant_status.call_args[0][2].equals(
            pd.DataFrame({"meter_id": [1], "stationcodes": [1]})
        )

    @patch("fusion_solar_data_ingestion.update_fusion_solar_data.f_get_table")
    @patch("fusion_solar_data_ingestion.update_fusion_solar_data.get_api_token")
    @patch("fusion_solar_data_ingestion.update_fusion_solar_data.get_querytimes")
    @patch("fusion_solar_data_ingestion.update_fusion_solar_data.get_energy_data")
    @patch("fusion_solar_data_ingestion.update_fusion_solar_data.get_plant_status")
    def test_main_with_data_append(
        self,
        mock_get_plant_status,
        mock_get_energy_data,
        mock_get_querytimes,
        mock_get_api_token,
        mock_f_get_table,
    ):
        mock_f_get_table.side_effect = [
            pd.DataFrame({"meter_id": [1], "stationcodes": [1]}),
            pd.DataFrame(
                {
                    "meter_id": [1, 2],
                    "meter_name": ["meter1", "meter2"],
                    "meter_type": ["Type1", "Type2"],
                    "proj_name": ["Project1", "Project2"],
                    "proj_id": [1, 2],
                    "monitoring_portal": ["FusionSolar", "FusionSolar"],
                }
            ),
            pd.DataFrame(
                {
                    "date": [date.today() - relativedelta(days=2)],
                    "meter_id": [1],
                    "energy": [100.0],
                }
            ),
        ]
        mock_get_api_token.return_value = "test_token"
        mock_get_querytimes.return_value = [
            (date.today() - relativedelta(days=1)).strftime("%Y-%m-%d")
        ]
        mock_get_energy_data.return_value = pd.DataFrame(
            {
                "date": [(date.today() - relativedelta(days=1)).strftime("%Y-%m-%d")],
                "meter_id": [1],
                "energy": [100.0],
            }
        )
        mock_get_plant_status.return_value = pd.DataFrame(
            {
                "status": ["Disconnected", "Faulty"],
                "meter_id": [1, 2],
                "meter_name": ["meter1", "meter2"],
                "date": [date.today(), date.today()],
            }
        )

        main()

        assert mock_get_energy_data.call_args[0][0] == "1"
        assert mock_get_energy_data.call_args[0][1] == (
            date.today() - relativedelta(days=1)
        ).strftime("%Y-%m-%d")
        assert mock_get_energy_data.call_args[0][2] == "test_token"
        assert mock_get_energy_data.call_args[0][3].equals(
            pd.DataFrame({"meter_id": [1], "stationcodes": [1]})
        )
        assert mock_get_plant_status.call_args[0][0] == "1"
        assert mock_get_plant_status.call_args[0][1] == "test_token"
        assert mock_get_plant_status.call_args[0][2].equals(
            pd.DataFrame({"meter_id": [1], "stationcodes": [1]})
        )


if __name__ == "__main__":
    unittest.main()
