import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
from datetime import date, timedelta

import pandas as pd
from dateutil.relativedelta import relativedelta
from shared.const import FS_BASE_REST_URL
from shared.datetime_utils import get_querytimes
from shared.db_utils import close_db_conn, f_append_to_table, f_get_table
from shared.request_utils import get_api_token, retry_post_request

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def get_energy_data(stationCodes, collectTime, api_token, fs_meter_list):
    data = {"stationCodes": stationCodes, "collectTime": collectTime}
    headers = {"Content-Type": "application/json", "XSRF-TOKEN": api_token}
    url = FS_BASE_REST_URL + "/thirdData/getKpiStationDay"
    response = retry_post_request(url, data=json.dumps(data), headers=headers)
    rs = response.json()
    if not rs.get("success", False):
        logging.error(
            f"Getting data from {url} failed - failcode: {rs.get('failCode')} - message: {rs.get('message') or '--'}"
        )
        exit(1)
    df = pd.json_normalize(rs.get("data"))
    df = df[["collectTime", "dataItemMap.inverter_power", "stationCode"]]
    df["collectTime"] = pd.to_datetime(df["collectTime"], unit="ms")
    df.columns = ["date", "energy", "stationcodes"]
    df = df.merge(fs_meter_list, on="stationcodes", how="left")
    df["energy"] = df["energy"].astype(float).round(5)
    df["date"] = df["date"].dt.date.astype(str)
    return df[["date", "meter_id", "energy"]]


def get_plant_status(stationCodes, api_token, fs_meter_list):
    data = {"stationCodes": stationCodes}
    headers = {"Content-Type": "application/json", "XSRF-TOKEN": api_token}
    url = FS_BASE_REST_URL + "/thirdData/getStationRealKpi"
    response = retry_post_request(url, data=json.dumps(data), headers=headers)
    rs = response.json()
    if not rs.get("success", False):
        logging.error(
            f"Getting data from {url} failed - failcode: {rs.get('failCode')} - message: {rs.get('message') or '--'}"
        )
        exit(1)
    df = pd.json_normalize(rs.get("data"))
    df = df[["stationCode", "dataItemMap.real_health_state"]]
    df.columns = ["stationcodes", "status"]
    df = df.merge(fs_meter_list, on="stationcodes", how="left")
    df["date"] = date.today()
    df = df.sort_values(by="meter_id")
    df["status"] = df["status"].astype(str)
    df.loc[df["status"] == "1", "status"] = "Disconnected"
    df.loc[df["status"] == "2", "status"] = "Faulty"
    df.loc[df["status"] == "3", "status"] = "Healthy"
    return df


def main():
    query = """
        SELECT meter_id, 
        portal_ref AS stationcodes, 
        meter_name 
        FROM meter 
        WHERE monitoring_portal='FusionSolar' 
        AND portal_ref IS NOT NULL
    """
    fs_meter_list = f_get_table(query)
    meter_id_fs = ",".join(map(str, fs_meter_list["meter_id"]))

    query = """
        SELECT 
            meter.meter_id, 
            meter.meter_name, 
            meter.meter_type, 
            proj.proj_name, 
            proj.proj_id, 
            monitoring_portal 
        FROM 
            meter 
        LEFT JOIN 
            site ON meter.site_id = site.site_id
        LEFT JOIN 
            proj ON site.proj_id = proj.proj_id
    """
    t_proj = f_get_table(query)

    time_frame = 5
    query = f"select * from energy_patched where date >= '{date.today() - relativedelta(months=time_frame)}' and meter_id in ({meter_id_fs})"
    t_fs_energy = f_get_table(query)
    if len(t_fs_energy) == 0:
        logging.info(f"FusionSolar - No energy data found from {time_frame} months ago")
        exit(0)

    fs_energy_date = pd.to_datetime(t_fs_energy["date"]).dt.date
    fs_date_start: date = fs_energy_date.unique().max() + pd.Timedelta(days=1)
    fs_date_end: date = date.today() - timedelta(days=1)

    if fs_date_start == date.today():
        logging.info(
            "FusionSolar energy data for yesterday already exists, not updating energy_patched"
        )
        exit(0)

    logging.info(
        f"Updating energy_patched table (FusionSolar) from {fs_date_start} to {fs_date_end}"
    )
    no_of_days = (fs_date_end - fs_date_start).days + 1
    date_vec = [
        (fs_date_start + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(no_of_days)
    ]

    logging.info("FusionSolar - getting API token ...")
    fs_token = get_api_token()

    # get energy data
    querytimes = get_querytimes(fs_date_start, fs_date_end)

    logging.info("FusionSolar - getting energy data from FusionSolar ...")
    stationcodes = ",".join(map(str, fs_meter_list["stationcodes"]))
    energy_portal_list = []
    for querytime in querytimes:
        df = get_energy_data(stationcodes, querytime, fs_token, fs_meter_list)
        if df is not None:
            energy_portal_list.append(df)

    energy_portal = pd.concat(energy_portal_list, ignore_index=True)
    fs_energy_to_patch = energy_portal[energy_portal["date"].isin(date_vec)]
    if len(fs_energy_to_patch) == 0:
        logging.info("FusionSolar - No data appended")
    else:
        # TODO: uncomment when ready
        # f_append_to_table(fs_energy_to_patch, "energy_patched")
        pass

    # get plant status
    logging.info("FusionSolar - getting plant status ...")
    connectivity = get_plant_status(stationcodes, fs_token, fs_meter_list)
    lost_comm_fs_meters = connectivity[connectivity["status"] == "Disconnected"].merge(
        t_proj, on=["meter_id", "meter_name"], how="left"
    )[
        [
            "date",
            "meter_name",
            "meter_id",
            "proj_name",
            "proj_id",
            "status",
            "monitoring_portal",
        ]
    ]
    faulty_fs_meters = connectivity[connectivity["status"] == "Faulty"].merge(
        t_proj, on=["meter_id", "meter_name"], how="left"
    )[
        [
            "date",
            "meter_name",
            "meter_id",
            "proj_name",
            "proj_id",
            "status",
            "monitoring_portal",
        ]
    ]

    if len(lost_comm_fs_meters) > 0:
        print("lost_comm_fs_meters")
        print(lost_comm_fs_meters.to_markdown())
    if len(faulty_fs_meters) > 0:
        print("faulty_fs_meters")
        print(faulty_fs_meters.to_markdown())


if __name__ == "__main__":  # pragma: no cover
    try:
        main()
    except Exception as e:
        logging.error(e)
    finally:
        close_db_conn()
