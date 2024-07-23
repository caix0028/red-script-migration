import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import logging
import math
from datetime import datetime

import pandas as pd
from shared.db_utils import f_get_table
from shared.request_utils import get_envision_api_token, retry_get_request

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

base_rest_url = "https://app-portal-eu2.envisioniot.com"
status_path = "/solar-api/v1.0/domainService/getmdmidspoints"
url_status_api = base_rest_url + status_path

ORG_ID = "o16221928963871049"
api_token = get_envision_api_token()


def get_energy_data(mdmids, metric, env_meter_list_temp):
    device = metric[:3]
    querystring = {
        "mdmids": mdmids,
        "points": f"{device}.APProductionKWH,{device}.APConsumedKWH",
        "token": api_token,
        "orgId": ORG_ID,
        "field": "value",
    }
    response = retry_get_request(url_status_api, querystring)
    if response is None:
        raise Exception("Failed to get meter connectivity status")
    rs = response.json()
    if "result" not in rs:
        logging.error(
            f"Getting data from {url_status_api} failed - failcode: {rs.get('status')} - message: {rs.get('msg') or '--'}"
        )
        exit(1)

    dfs = []
    for mdm_id, mdm_data in rs["result"].items():
        normalized_df = pd.json_normalize(mdm_data)
        normalized_df["mdmids"] = mdm_id
        dfs.append(normalized_df)
    df = pd.concat(dfs, ignore_index=True)

    mdmids = df.index.tolist()
    selected_columns = [
        "mdmids",
        f"points.{device}.APProductionKWH.timestamp",
        f"points.{device}.APProductionKWH.value",
        f"points.{device}.APConsumedKWH.value",
    ]
    df = df[selected_columns]
    df[selected_columns[1:]] = df[selected_columns[1:]].apply(pd.to_numeric)
    if df.shape == (3, 1):
        df = df.transpose()
    df.columns = ["mdmids", "timestamp", "exported", "imported"]
    df.index = mdmids
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df = pd.merge(
        df,
        env_meter_list_temp[env_meter_list_temp["api_metrics"] == metric],
        on="mdmids",
        how="left",
    )
    df = df.sort_values(by="meter_id")
    current_time = pd.Timestamp.now()
    df["lost_comms"] = (df["timestamp"].dt.date < current_time.date()) | (
        abs(current_time.hour - df["timestamp"].dt.hour) > 1
    )
    return df


def main():
    envision_meter_list_query = """
        SELECT meter_id, api_metrics, portal_ref AS mdmids, meter_name 
        FROM meter
        WHERE monitoring_portal='Envision' AND portal_ref IS NOT NULL AND portal_ref!='Inverter'
    """

    # t_proj_query = """
    #     SELECT
    #         meter_id,
    #         meter_name,
    #         meter_type,
    #         proj_name,
    #         proj.proj_id,
    #         monitoring_portal
    #     FROM
    #         meter
    #     LEFT JOIN
    #         site ON site.site_id = meter.site_id
    #     LEFT JOIN
    #         proj ON proj.proj_id = site.proj_id;
    # """

    envision_meter_list = f_get_table(envision_meter_list_query)
    # t_proj = f_get_table(t_proj_query)

    api_metrics = envision_meter_list["api_metrics"].dropna().unique()

    meter_readings_list = []
    for metric in api_metrics:
        env_meter_list_temp = envision_meter_list[
            envision_meter_list["api_metrics"] == metric
        ]

        mdmids_length = len(env_meter_list_temp["mdmids"])
        slice_num = 50
        round = math.ceil(mdmids_length / slice_num)

        for j in range(round):
            if j < round - 1:
                mdmids = ",".join(
                    map(
                        str,
                        env_meter_list_temp["mdmids"][
                            slice_num * j : slice_num * (j + 1)
                        ],
                    )
                )
            elif j == round - 1:
                mdmids = ",".join(
                    map(
                        str,
                        env_meter_list_temp["mdmids"][slice_num * j : mdmids_length],
                    )
                )

            logging.info("Envision - getting meter connectivity status ...")
            df = get_energy_data(mdmids, metric, env_meter_list_temp)
            meter_readings_list.append(df)
    meter_readings = pd.concat(meter_readings_list, ignore_index=True)

    filename = f"Envision_meter_readings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    meter_readings.to_csv(filename, index=False)


if __name__ == "__main__":  # pragma: no cover
    main()
