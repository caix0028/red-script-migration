import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import hashlib
import json
import time

import requests
from shared.const import (
    ENVISION_ACCESS_KEY,
    ENVISION_PASSWORD,
    ENVISION_SECRET_KEY,
    ENVISION_USERNAME,
    FS_COMPLETE_REST_URL,
    FUSIONSOLAR_PASSWORD,
    FUSIONSOLAR_USERNAME,
)


def retry_post_request(
    url, total=20, status_forcelist=[429, 500, 502, 503, 504], **kwargs
):
    for _ in range(total):
        try:
            response = requests.post(url, **kwargs)
            if response.status_code in status_forcelist:
                # retry request
                continue
            return response
        except:
            pass
    raise requests.exceptions.ConnectionError(f"Can not connect to {url}")


def retry_get_request(
    url, query_string, total=20, status_forcelist=[429, 500, 502, 503, 504]
):
    for _ in range(total):
        try:
            response = requests.get(url, params=query_string)
            if response.status_code in status_forcelist:
                # retry request
                continue
            return response
        except:
            pass
    raise requests.exceptions.ConnectionError(f"Can not connect to {url}")


def get_api_token():
    data = {"userName": FUSIONSOLAR_USERNAME, "systemCode": FUSIONSOLAR_PASSWORD}
    headers = {"Content-Type": "application/json"}
    response = retry_post_request(
        FS_COMPLETE_REST_URL, data=json.dumps(data), headers=headers
    )
    if response is None:
        raise Exception("Failed to get FusionSolar API token")
    return response.headers["xsrf-token"]


def get_envision_access_token():
    timestamp = int(time.time()) * 1000
    encryption = ENVISION_ACCESS_KEY + str(timestamp) + ENVISION_SECRET_KEY
    sha256_digest = hashlib.sha256(encryption.encode("utf-8")).hexdigest()
    data = {
        "appKey": ENVISION_ACCESS_KEY,
        "encryption": sha256_digest,
        "timestamp": timestamp,
    }

    baseRestURL = "https://ag-eu2.envisioniot.com"
    Path = "/apim-token-service/v2.0/token/get"
    url_get_accesstoken = baseRestURL + Path
    response = retry_post_request(url_get_accesstoken, json=data)
    if response is None:
        raise Exception("Failed to get Envision SolarAPI token")
    rs = response.json()
    if rs.get("status", 1) != 0:
        raise Exception(
            f"Failed to get Envision SolarAPI access token - {rs.get('msg', 'Unknown error')}"
        )
    return rs.get("data", {}).get("accessToken", "")


def get_envision_api_token():
    access_token = get_envision_access_token()
    paramsData = (
        f'{{"password": "{ENVISION_PASSWORD}", "username": "{ENVISION_USERNAME}"}}'
    )
    timestamp = int(time.time()) * 1000
    signature = access_token + paramsData + str(timestamp) + ENVISION_SECRET_KEY
    sha256_digest = hashlib.sha256(signature.encode("utf-8")).hexdigest()
    headers = {
        "apim-accesstoken": access_token,
        "apim-signature": sha256_digest,
        "apim-timestamp": str(timestamp),
    }
    data = {"username": ENVISION_USERNAME, "password": ENVISION_PASSWORD}

    baseRestURL = "https://app-portal-eu2.envisioniot.com"
    loginPath = "/solar-api/v1.0/loginService/login"
    url_login = baseRestURL + loginPath
    response = retry_post_request(url_login, headers=headers, json=data)
    rs = response.json()
    if rs.get("status", 1) != 0:
        raise Exception(
            f"Failed to get Envision SolarAPI api token - {rs.get('msg', 'Unknown error')}"
        )
    return rs.get("body")
