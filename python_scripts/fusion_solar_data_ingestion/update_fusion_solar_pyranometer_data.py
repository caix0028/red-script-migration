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


def get_energy_data(stationCodes, collectTime, api_token, fs_pyr_list):
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
    df = df[["collectTime", "dataItemMap.radiation_intensity", "stationCode"]]
    df["collectTime"] = pd.to_datetime(df["collectTime"], unit="ms")
    df.columns = ["date", "sunhours", "stationcodes"]
    df = df.merge(fs_pyr_list, on="stationcodes", how="left")
    df["sunhours"] = df["sunhours"].astype(float).round(5)
    df["date"] = df["date"].dt.date.astype(str)
    return df


def main():
    fs_pyr_list_query = """
        select 
            pyr_id, 
            portal_ref as stationcodes, 
            location 
        from 
            pyr 
        where 
            monitoring_portal='FusionSolar' 
            and portal_ref is not null
    """
    fs_pyr_list = f_get_table(fs_pyr_list_query)
    pyr_id_fs = ",".join(map(str, fs_pyr_list["pyr_id"]))

    time_frame = 5
    query = f"select * from pyr_SH where date >= '{date.today() - relativedelta(months=time_frame)}' and pyr_id in ({pyr_id_fs})"
    t_fs_sunhours = f_get_table(query)
    t_fs_sunhours_date = pd.to_datetime(t_fs_sunhours["date"]).dt.date
    fs_date_start: date = t_fs_sunhours_date.unique().max() + pd.Timedelta(days=1)
    fs_date_end: date = date.today() - timedelta(days=1)

    if fs_date_start == date.today():
        logging.info(
            "FusionSolar sunhours data for yesterday already exists, not updating pyr_SH"
        )
        exit(0)

    logging.info(
        f"updating pyr_SH table (FusionSolar) from {fs_date_start} to {fs_date_end}"
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

    logging.info("FusionSolar - getting sunhours data from FusionSolar ...")
    stationcodes = ",".join(map(str, fs_pyr_list["stationcodes"]))
    sunhours_portal_list = []
    for querytime in querytimes:
        df = get_energy_data(stationcodes, querytime, fs_token, fs_pyr_list)
        sunhours_portal_list.append(df)
    sunhours_portal = pd.concat(sunhours_portal_list, ignore_index=True)
    fs_sunhours_to_patch = sunhours_portal[sunhours_portal["date"].isin(date_vec)]

    if (
        fs_sunhours_to_patch.empty
        or fs_sunhours_to_patch[
            fs_sunhours_to_patch["date"] == fs_sunhours_to_patch["date"].max()
        ]["sunhours"].values[0]
        == 0
    ):
        fs_pyr_lostcomms = fs_pyr_list[["pyr_id", "location"]].copy()
        fs_pyr_lostcomms["date"] = date.today()
        fs_pyr_lostcomms["status"] = "Lost Comms"
        fs_pyr_lostcomms = fs_pyr_lostcomms[["date", "pyr_id", "location", "status"]]

        logging.info("\nFUSIONSOLAR PYRANOMETER LOST COMMS")
        logging.info(f"Fusionsolar pyranometers lost comms as of {date.today()}")
        print(fs_pyr_lostcomms.to_markdown())

    if len(fs_sunhours_to_patch) > 0:
        # TODO: uncomment when ready
        # f_append_to_table(fs_sunhours_to_patch, "pyr_SH")
        pass


if __name__ == "__main__":  # pragma: no cover
    try:
        main()
    except Exception as e:
        logging.error(e)
    finally:
        close_db_conn()
