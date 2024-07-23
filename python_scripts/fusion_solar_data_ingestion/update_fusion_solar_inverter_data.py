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

INVERTER_STATE_PATH = "D:/mov-related/red-script-migration-intern/red_script_migration/autorun-monitoring-file-updater/fusionsolar_inverter_state.csv"


def get_energy_data(devIds, collectTime, api_token, fs_inv_list):
    data = {"devIds": devIds, "collectTime": collectTime, "devTypeId": 1}
    headers = {"Content-Type": "application/json", "XSRF-TOKEN": api_token}
    url = FS_BASE_REST_URL + "/thirdData/getDevKpiDay"
    response = retry_post_request(url, data=json.dumps(data), headers=headers)
    rs = response.json()
    if not rs.get("success", False):
        logging.error(
            f"Getting data from {url} failed - failcode: {rs.get('failCode')} - message: {rs.get('message') or '--'}"
        )
        exit(1)
    df = pd.json_normalize(rs.get("data", []))
    df = df[["collectTime", "dataItemMap.product_power", "devId"]]
    df["collectTime"] = pd.to_datetime(df["collectTime"], unit="ms")
    df.columns = ["date", "inv_energy", "devIds"]
    df["devIds"] = df["devIds"].astype(str)
    df = df.merge(fs_inv_list, on="devIds", how="left")
    df["inv_energy"] = df["inv_energy"].astype(float).round(5)
    df["date"] = df["date"].dt.date.astype(str)
    return df[["date", "inv_id", "inv_energy"]]


def get_plant_status(devIds, api_token, fs_inv_list):
    data = {"devIds": devIds, "devTypeId": 1}
    headers = {"Content-Type": "application/json", "XSRF-TOKEN": api_token}
    url = FS_BASE_REST_URL + "/thirdData/getDevRealKpi"
    response = retry_post_request(url, data=json.dumps(data), headers=headers)
    rs = response.json()
    if not rs.get("success", False):
        logging.error(
            f"Getting data from {url} failed - failcode: {rs.get('failCode')} - message: {rs.get('message') or '--'}"
        )
        exit(1)
    df = pd.json_normalize(response.json().get("data", []))
    df = df[["devId", "dataItemMap.run_state"]]
    df.columns = ["devIds", "status"]
    df["devIds"] = df["devIds"].astype(str)
    df = df.merge(fs_inv_list, on="devIds", how="left")
    df["date"] = date.today()
    df = df.sort_values(by="inv_id")
    inverter_state = pd.read_csv(INVERTER_STATE_PATH)
    df = pd.merge(
        df, inverter_state, how="left", left_on="status", right_on="inverter_state"
    )
    df["inv_id"] = df["inv_id"].astype(int)
    return df


def main():
    fs_inv_list_query = """
        select 
            inv_id, 
            portal_ref as devIds, 
            inv_name 
        from 
            inv 
        where 
            monitoring_portal='FusionSolar' 
            and portal_ref is not null
    """
    fs_inv_list = f_get_table(fs_inv_list_query)
    inv_id_fs = ",".join(map(str, fs_inv_list["inv_id"]))

    time_frame = 5
    t_fs_inv_energy_query = f"select * from inv_energy_patched where date >= '{date.today() - relativedelta(months=time_frame)}' and inv_id in ({inv_id_fs})"
    t_fs_inv_energy = f_get_table(t_fs_inv_energy_query)
    fs_inv_energy_date = pd.to_datetime(t_fs_inv_energy["date"]).dt.date
    fs_date_start: date = fs_inv_energy_date.unique().max() + pd.Timedelta(days=1)
    fs_date_end: date = date.today() - timedelta(days=1)

    if fs_date_start == date.today():
        logging.info(
            "FusionSolar inv energy data for yesterday already exists, not updating inv_energy_patche"
        )
        exit(0)

    logging.info(
        f"Updating inv_energy_patched table (FusionSolar) from {fs_date_start} to {fs_date_end}"
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

    logging.info("FusionSolar - getting inv energy data from FusionSolar ...")
    devIds = ",".join(map(str, fs_inv_list["devIds"]))
    inv_energy_portal_list = []
    for querytime in querytimes:
        df = get_energy_data(devIds, querytime, fs_token, fs_inv_list)
        inv_energy_portal_list.append(df)
    inv_energy_portal = pd.concat(inv_energy_portal_list, ignore_index=True)
    fs_inv_energy_to_patch = inv_energy_portal[inv_energy_portal["date"].isin(date_vec)]
    if len(fs_inv_energy_to_patch) == 0:
        logging.info("FusionSolar - No data appended")
    else:
        # TODO: uncomment when ready
        # f_append_to_table(fs_inv_energy_to_patch, "inv_energy_patched")
        pass

    # get plant status
    logging.info("FusionSolar - getting device status ...")
    connectivity = get_plant_status(devIds, fs_token, fs_inv_list)

    query = """
        SELECT 
            inv.inv_id, 
            inv.inv_name, 
            meter.meter_id, 
            meter.meter_name, 
            meter.meter_type, 
            proj.proj_name, 
            proj.proj_id, 
            inv.monitoring_portal AS monitoring_portal 
        FROM 
            inv 
        LEFT JOIN 
            meter ON inv.meter_id = meter.meter_id 
        LEFT JOIN 
            site ON meter.site_id = site.site_id 
        LEFT JOIN 
            proj ON site.proj_id = proj.proj_id
    """
    t_proj_fs = f_get_table(query)
    lost_comm_fs_inv = connectivity[connectivity["Description"].notna()].merge(
        t_proj_fs, how="left", on=["inv_id", "inv_name"]
    )[
        [
            "date",
            "inv_name",
            "inv_id",
            "proj_name",
            "proj_id",
            "Description",
            "monitoring_portal",
        ]
    ]
    if len(lost_comm_fs_inv) > 0:
        print("lost_comm_fs_inv")
        print(lost_comm_fs_inv.to_markdown())


if __name__ == "__main__":  # pragma: no cover
    try:
        main()
    except Exception as e:
        logging.error(e)
    finally:
        close_db_conn()
