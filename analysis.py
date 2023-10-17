import pandas as pd
import duckdb
import os
import requests
import pgeocode

GEO = pgeocode.GeoDistance("us")
CON = duckdb.connect("file.db")
FILE_NAME = "horizon.xlsx"
EXCEL_URL = "https://storage.googleapis.com/hznp_public/Horizon_Data_Engineering.xlsx"


# download file from google, if I haven't downloaded it
if not os.path.exists(FILE_NAME):
    resp = requests.get(EXCEL_URL)
    with open(FILE_NAME, "wb") as f:
        f.write(resp.content)


hcp_df = pd.read_excel(FILE_NAME, "hcp")
hco_df = pd.read_excel(FILE_NAME, "hco")
ziptoter_df = pd.read_excel(FILE_NAME, "ziptoter")


# create a sql function for distance
def dist(x: str, y: str) -> float:
    return GEO.query_postal_code(x, y)


CON.create_function("dist", dist)

# Precompute(cache) distances and store in a temporary table as cross-joins are expensive, especially when calling an external UDF :P
CON.query(
    """
    CREATE TABLE IF NOT EXISTS temp_distance AS
    SELECT
        hcp.npi AS hcp_npi,
        hco.account_id AS hco_account_id,
        dist(hcp.zip_hcp, hco.zip_hco) AS distance
    FROM
        (SELECT npi, lpad(zip, 5, '0') AS zip_hcp FROM hcp_df) hcp
    CROSS JOIN
        (SELECT account_id, lpad(zipcode, 5, '0') AS zip_hco FROM hco_df WHERE account_type = 'HOSP') hco
    """
)


with pd.ExcelWriter(FILE_NAME, mode="a") as writer:
    ques_1 = CON.query(
        """
        SELECT
            ter,
            COUNT(DISTINCT(specialty)) AS distinct_specialties
        FROM hcp_df
        LEFT JOIN ziptoter_df
        ON hcp_df.zip = ziptoter_df.zipcode
        GROUP BY ter
        """
    ).to_df()
    print(ques_1)
    # ques_1.to_excel(writer, sheet_name="question1")

    ques_2 = CON.query(
        """
        WITH hcp_ter AS (
            SELECT
                npi,
                lpad(zipcode, 5, '0') as zip,
                ter
            FROM hcp_df
            LEFT JOIN ziptoter_df
            ON hcp_df.zip = ziptoter_df.zipcode
            WHERE specialty IN ('RHEUMATOLOGY', 'NEPHROLOGY')
        ), ic_ter AS (
            SELECT
                ter,
                account_id,
                lpad(hco_df.zipcode, 5, '0') as zip
            FROM hco_df
            LEFT JOIN ziptoter_df
            ON hco_df.zipcode = ziptoter_df.zipcode
            WHERE account_type = 'IC'
        )

        SELECT
            npi,
            MIN(dist(hcp.zip, hco.zip)) AS dist_km,
            MIN_BY(account_id, dist(hcp.zip, hco.zip)) as closest_ic
        FROM hcp_ter hcp
        JOIN ic_ter hco
        ON hcp.ter = hco.ter
        GROUP BY 1
        """
    ).to_df()
    print(ques_2)
    # ques_2.to_excel(writer, sheet_name="question2")

    ques_3 = CON.query(
        """
        WITH tbl AS (
            SELECT
                closest_ic,
                npi,
                dist_km,
                ROW_NUMBER() OVER (PARTITION BY closest_ic ORDER BY dist_km) AS rn
            FROM ques_2
        )

        SELECT 
            closest_ic AS infusion_center_id,
            ARRAY_AGG(npi) as three_closest_hcp
        FROM tbl
        WHERE rn <= 3
        GROUP BY 1
        """
    ).to_df()
    print(ques_3)
    # ques_3.to_excel(writer, sheet_name="question3")

    ques_4 = CON.query(
        """
        WITH closest_hosp_per_hcp AS (
            SELECT
                hcp_npi AS npi,
                MIN_BY(hco_account_id, distance) AS hosp
            FROM temp_distance
            WHERE distance <= 100
            GROUP BY 1
        )

        SELECT
            hosp,
            ARRAY_AGG(npi) as HCPs
        FROM closest_hosp_per_hcp
        GROUP BY 1
    """
    ).to_df()
    print(ques_4)
    # ques_4.to_excel(writer, sheet_name="question4")
