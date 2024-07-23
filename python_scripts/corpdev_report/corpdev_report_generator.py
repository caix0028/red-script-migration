import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import os
from datetime import date, datetime, timedelta

import pandas as pd
from shared.db_utils import close_db_conn, f_get_table

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def main():
    date_end = date.today().replace(day=1) - timedelta(days=1)
    exclusion_df = pd.read_csv("<path_to_input.txt>", comment="#")
    excluded_projs_str = exclusion_df["input_df"][0]
    excluded_meters_str = exclusion_df["input_df"][1]

    energy_gen_query = f"""
        SELECT 
            p.proj_id, 
            p.proj_name, 
            s.date_of_com as turn_on_date, 
            YEAR(ep.date) as year, 
            MONTH(ep.date) as month, 
            SUM(ep.energy) as energy_generated 
        FROM 
            energy_patched as ep 
        LEFT JOIN 
            meter as m ON m.meter_id = ep.meter_id 
        LEFT JOIN 
            site as s On s.site_id = m.site_id 
        LEFT JOIN 
            proj as p ON p.proj_id = s.proj_id 
        WHERE 
            p.proj_id NOT IN ({excluded_projs_str}) 
        AND m.meter_id NOT IN ({excluded_meters_str}) 
        AND (ep.date >= s.date_of_com AND ep.date <= '{date_end}') 
        AND m.meter_type = 'Solar Generation' 
        AND s.country = 'Singapore' 
        GROUP BY 
            p.proj_id, 
            p.proj_name,
            s.date_of_com,
            m.meter_type, 
            YEAR(ep.date), 
            MONTH(ep.date);
    """

    energy_ex_query = f"""
        SELECT 
            p.proj_id, 
            p.proj_name,
            YEAR(ep.date) AS year, 
            MONTH(ep.date) AS month, 
            SUM(ep.energy) AS energy_exported 
        FROM 
            energy_patched AS ep
        LEFT JOIN 
            meter AS m ON m.meter_id = ep.meter_id
        LEFT JOIN 
            site AS s On s.site_id = m.site_id
        LEFT JOIN 
            proj AS p ON p.proj_id = s.proj_id
        WHERE 
            p.proj_id NOT IN ({excluded_projs_str})
            AND m.meter_id NOT IN ({excluded_meters_str})
            AND (ep.date >= date_of_com AND ep.date <= '{date_end}')
            AND m.meter_type = 'Solar Export'
            AND s.country = 'Singapore'
        GROUP BY 
            p.proj_id,
            p.proj_name,
            m.meter_type,
            YEAR(ep.date),
            MONTH(ep.date);
    """

    system_size_query = f"""
        SELECT 
            p.proj_id, 
            p.proj_name, 
            ss.year, 
            SUM(ss.system_size) AS system_size
        FROM 
            system_size AS ss
        LEFT JOIN 
            meter AS m ON m.meter_id = ss.meter_id
        LEFT JOIN 
            site AS s On s.site_id = m.site_id
        LEFT JOIN 
            proj AS p ON p.proj_id = s.proj_id
        WHERE 
            p.proj_id NOT IN ({excluded_projs_str}) AND 
            m.meter_id NOT IN ({excluded_meters_str}) AND 
            meter_type = 'Solar Generation' AND 
            s.country = 'Singapore'
        GROUP BY 
            p.proj_id,
            p.proj_name,
            ss.year;
    """

    pyr_SH_query = f"""
        SELECT 
            p.proj_id, 
            p.proj_name, 
            YEAR(pyr_SH.date) AS year, 
            MONTH(pyr_SH.date) AS month,
            CASE
                WHEN COUNT(DISTINCT s.site_id) > 0 
                THEN SUM(pyr_SH.sunhours) / COUNT(DISTINCT s.site_id)
                ELSE 0
            END AS sunhours
        FROM 
            pyr_SH
        LEFT JOIN 
            site AS s ON s.pyr_id = pyr_SH.pyr_id
        LEFT JOIN 
            proj AS p ON p.proj_id = s.proj_id
        WHERE 
            p.proj_id NOT IN ({excluded_projs_str}) AND 
            (pyr_SH.date >= date_of_com AND pyr_SH.date <= '{date_end}')
        GROUP BY 
            p.proj_id,
            p.proj_name,
            YEAR(pyr_SH.date), 
            MONTH(pyr_SH.date);
    """

    df_EnergyGen = f_get_table(energy_gen_query)
    df_EnergyExp = f_get_table(energy_ex_query)
    df_SysSize = f_get_table(system_size_query)
    df_Sunhours = f_get_table(pyr_SH_query)

    # Combining the dataframes together using left joins
    df_EnergyGenExp = pd.merge(
        df_EnergyGen,
        df_EnergyExp,
        on=["proj_id", "proj_name", "year", "month"],
        how="left",
    )
    df_Energy_SS = pd.merge(
        df_EnergyGenExp, df_SysSize, on=["proj_id", "proj_name", "year"], how="left"
    )
    df_Energy_SS_SH = pd.merge(
        df_Energy_SS,
        df_Sunhours,
        on=["proj_id", "proj_name", "year", "month"],
        how="left",
    )

    # Remove records in the year 2012, as there isn't any sunhours information in 2012
    # Create three new columns - Export ratio, Yield, Performance Ratio
    df_final = df_Energy_SS_SH[df_Energy_SS_SH["year"] != 2012].copy()
    df_final["export_ratio"] = (
        df_final["energy_exported"] / df_final["energy_generated"]
    )
    df_final["yield"] = df_final["energy_generated"] / df_final["system_size"]
    df_final["performance_ratio"] = df_final["yield"] / df_final["sunhours"]

    df_final["export_ratio"] = df_final["export_ratio"].apply(lambda x: f"{x*100}%")
    df_final["performance_ratio"] = df_final["performance_ratio"].apply(
        lambda x: f"{x*100}%"
    )

    df_final.loc[df_final["proj_id"].isin([1, 7, 31]), "turn_on_date"] = (
        "-"  # HDB P1,2,3
    )
    df_final = df_final.sort_values(by=["turn_on_date", "proj_id", "year", "month"])

    folder_name = "Corpdev_report_" + datetime.now().strftime("%Y%b")
    os.makedirs(folder_name, exist_ok=True)
    excel_file_name = os.path.join(
        folder_name,
        "Sunseap Leasing Solar Portfolio till " + date_end.strftime("%B %Y") + ".xlsx",
    )
    df_final.to_excel(excel_file_name, index=False)


if __name__ == "__main__":  # pragma: no cover
    try:
        main()
    except Exception as e:
        logging.error(e)
    finally:
        close_db_conn()
