import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
from datetime import date, timedelta
from unittest.mock import Mock, patch

import pandas as pd
from dateutil.relativedelta import relativedelta
from fusion_solar_data_ingestion.update_fusion_solar_inverter_data import (
    get_energy_data,
    get_plant_status,
    main,
)


class TestUpdateFusionSolarInverterData(unittest.TestCase):
    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_inverter_data.retry_post_request"
    )
    def test_get_energy_data_success(self, mock_retry_post_request):
        mock_response = Mock()
        mock_response.json.return_value = {
            "success": True,
            "data": [
                {
                    "collectTime": 1622527200000,
                    "dataItemMap": {"product_power": 100},
                    "devId": "dev1",
                },
                {
                    "collectTime": 1622527200000,
                    "dataItemMap": {"product_power": 200},
                    "devId": "dev2",
                },
            ],
        }
        mock_retry_post_request.return_value = mock_response
        devIds = ["dev1", "dev2"]
        collectTime = "2021-06-01"
        api_token = "test_token"
        fs_inv_list = pd.DataFrame({"devIds": ["dev1", "dev2"], "inv_id": [1, 2]})

        df = get_energy_data(devIds, collectTime, api_token, fs_inv_list)

        self.assertEqual(len(df), 2)
        self.assertEqual(df["date"].tolist(), ["2021-06-01", "2021-06-01"])
        self.assertEqual(df["inv_id"].tolist(), [1, 2])
        self.assertEqual(df["inv_energy"].tolist(), [100.0, 200.0])

    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_inverter_data.retry_post_request"
    )
    def test_get_energy_data_failure(self, mock_retry_post_request):
        mock_response = Mock()
        mock_response.json.return_value = {
            "success": False,
            "failCode": "500",
            "message": "Error message",
        }
        mock_retry_post_request.return_value = mock_response
        devIds = ["dev1", "dev2"]
        collectTime = "2021-06-01"
        api_token = "test_token"
        fs_inv_list = pd.DataFrame({"devIds": ["dev1", "dev2"], "inv_id": [1, 2]})

        with self.assertRaises(SystemExit) as cm:
            get_energy_data(devIds, collectTime, api_token, fs_inv_list)
        self.assertEqual(cm.exception.code, 1)

    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_inverter_data.retry_post_request"
    )
    @patch("fusion_solar_data_ingestion.update_fusion_solar_inverter_data.pd.read_csv")
    def test_get_plant_status_success(self, mock_read_csv, mock_retry_post_request):
        mock_response = Mock()
        mock_response.json.return_value = {
            "success": True,
            "data": [
                {
                    "devId": "dev1",
                    "dataItemMap": {"run_state": 1},
                },
                {
                    "devId": "dev2",
                    "dataItemMap": {"run_state": 2},
                },
            ],
        }
        mock_retry_post_request.return_value = mock_response
        mock_read_csv.return_value = pd.DataFrame({"inverter_state": [1, 2]})
        devIds = ["dev1", "dev2"]
        api_token = "test_token"
        fs_inv_list = pd.DataFrame({"devIds": ["dev1", "dev2"], "inv_id": [1, 2]})

        df = get_plant_status(devIds, api_token, fs_inv_list)

        self.assertEqual(len(df), 2)
        self.assertEqual(df["date"].tolist(), [date.today(), date.today()])
        self.assertEqual(df["inv_id"].tolist(), [1, 2])
        self.assertEqual(df["status"].tolist(), [1, 2])

    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_inverter_data.retry_post_request"
    )
    def test_get_plant_status_failure(self, mock_retry_post_request):
        mock_response = Mock()
        mock_response.json.return_value = {
            "success": False,
            "failCode": "500",
            "message": "Error message",
        }
        mock_retry_post_request.return_value = mock_response
        devIds = ["dev1", "dev2"]
        api_token = "test_token"
        fs_inv_list = pd.DataFrame({"devIds": ["dev1", "dev2"], "inv_id": [1, 2]})

        with self.assertRaises(SystemExit) as cm:
            get_plant_status(devIds, api_token, fs_inv_list)
        self.assertEqual(cm.exception.code, 1)

    @patch("fusion_solar_data_ingestion.update_fusion_solar_inverter_data.f_get_table")
    def test_main_with_energy_data_already_updated(
        self,
        mock_f_get_table,
    ):
        mock_f_get_table.side_effect = [
            pd.DataFrame({"inv_id": [1], "devIds": ["dev1"], "inv_name": ["inv1"]}),
            pd.DataFrame(
                {
                    "date": [date.today() - relativedelta(days=1)],
                    "inv_id": [1],
                    "status": [1],
                }
            ),
        ]

        with self.assertRaises(SystemExit) as cm:
            main()
        self.assertEqual(cm.exception.code, 0)

    @patch("fusion_solar_data_ingestion.update_fusion_solar_inverter_data.f_get_table")
    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_inverter_data.get_api_token"
    )
    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_inverter_data.get_querytimes"
    )
    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_inverter_data.get_energy_data"
    )
    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_inverter_data.get_plant_status"
    )
    def test_main_with_no_data_append(
        self,
        mock_get_plant_status,
        mock_get_energy_data,
        mock_get_querytimes,
        mock_get_api_token,
        mock_f_get_table,
    ):
        mock_f_get_table.side_effect = [
            pd.DataFrame({"inv_id": [1], "devIds": ["dev1"], "inv_name": ["inv1"]}),
            pd.DataFrame(
                {
                    "date": [date.today() - relativedelta(days=2)],
                    "inv_id": [1],
                    "status": [1],
                }
            ),
            pd.DataFrame(
                {
                    "inv_id": [1],
                    "inv_name": ["inv1"],
                    "meter_id": [1],
                    "meter_name": ["meter1"],
                    "meter_type": ["type1"],
                    "proj_name": ["proj1"],
                    "proj_id": [1],
                    "monitoring_portal": ["FusionSolar"],
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
                "inv_id": [1, 2],
                "inv_name": ["inv1", "inv2"],
                "status": ["Disconnected", "Faulty"],
                "meter_id": [1, 2],
                "meter_name": ["meter1", "meter2"],
                "date": [date.today(), date.today()],
                "Description": ["Disconnected", "Faulty"],
            }
        )

        main()

        mock_get_energy_data.assert_called_once()
        assert mock_get_energy_data.call_args[0][0] == "dev1"
        assert mock_get_energy_data.call_args[0][1] == "2021-06-01"
        assert mock_get_energy_data.call_args[0][2] == "test_token"
        assert mock_get_energy_data.call_args[0][3].equals(
            pd.DataFrame({"inv_id": [1], "devIds": ["dev1"], "inv_name": ["inv1"]})
        )

    @patch("fusion_solar_data_ingestion.update_fusion_solar_inverter_data.f_get_table")
    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_inverter_data.get_api_token"
    )
    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_inverter_data.get_querytimes"
    )
    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_inverter_data.get_energy_data"
    )
    @patch(
        "fusion_solar_data_ingestion.update_fusion_solar_inverter_data.get_plant_status"
    )
    def test_main_with_data_append(
        self,
        mock_get_plant_status,
        mock_get_energy_data,
        mock_get_querytimes,
        mock_get_api_token,
        mock_f_get_table,
    ):
        mock_f_get_table.side_effect = [
            pd.DataFrame({"inv_id": [1], "devIds": ["dev1"], "inv_name": ["inv1"]}),
            pd.DataFrame(
                {
                    "date": [date.today() - relativedelta(days=2)],
                    "inv_id": [1],
                    "status": [1],
                }
            ),
            pd.DataFrame(
                {
                    "inv_id": [1],
                    "inv_name": ["inv1"],
                    "meter_id": [1],
                    "meter_name": ["meter1"],
                    "meter_type": ["type1"],
                    "proj_name": ["proj1"],
                    "proj_id": [1],
                    "monitoring_portal": ["FusionSolar"],
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
                "inv_id": [1, 2],
                "inv_name": ["inv1", "inv2"],
                "status": ["Disconnected", "Faulty"],
                "meter_id": [1, 2],
                "meter_name": ["meter1", "meter2"],
                "date": [date.today(), date.today()],
                "Description": ["Disconnected", "Faulty"],
            }
        )

        main()

        mock_get_energy_data.assert_called_once()
        assert mock_get_energy_data.call_args[0][0] == "dev1"
        assert mock_get_energy_data.call_args[0][1] == (
            date.today() - relativedelta(days=1)
        ).strftime("%Y-%m-%d")
        assert mock_get_energy_data.call_args[0][2] == "test_token"
        assert mock_get_energy_data.call_args[0][3].equals(
            pd.DataFrame({"inv_id": [1], "devIds": ["dev1"], "inv_name": ["inv1"]})
        )


if __name__ == "__main__":
    unittest.main()
