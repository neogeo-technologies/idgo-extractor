from osgeo import gdal
import json
import os
import requests
import time
import sys

END_POINT_ROOT = "http://localhost:5000"
SUBMIT_URL = END_POINT_ROOT + "/jobs"


def submit(req):
    return requests.post(
        SUBMIT_URL, json.dumps(req), headers={"Content-Type": "application/json"}
    )


print("Test invalid submit request 1")
req = {}
r = submit(req)
assert r.status_code == 400
resp = json.loads(r.text)
assert resp["status"] == "ERROR"
assert "detail" in resp
assert "user_id" in resp["detail"]

print("Test invalid submit request 2")
req = {"user_id": "my_id"}
r = submit(req)
assert r.status_code == 400
resp = json.loads(r.text)
assert resp["status"] == "ERROR"
assert "detail" in resp
assert "user_email_address" in resp["detail"]

print("Test invalid submit request 3")
req = {
    "user_id": "my_id",
    "user_email_address": "invalid",
    "source": "invalid",
    "dst_format": "invalid",
}
r = submit(req)
assert r.status_code == 400
resp = json.loads(r.text)
assert resp["status"] == "ERROR"
assert "detail" in resp
assert "user_email_address" in resp["detail"]

print("Test invalid submit request 4")
req = {
    "user_id": "my_id",
    "user_email_address": "foo@bar.com",
    "source": "invalid",
    "dst_format": "invalid",
}
r = submit(req)
assert r.status_code == 400
resp = json.loads(r.text)
assert resp["status"] == "ERROR"
assert "detail" in resp

print("Test invalid get status request 1")
r = requests.get(END_POINT_ROOT + "/jobs/inexisting_task_id")
assert r.status_code == 404

print("Test invalid download request 1")
r = requests.get(END_POINT_ROOT + "/jobs/inexisting_task_id/download")
assert r.status_code == 404

print("Test invalid abort request 1")
r = requests.put(
    END_POINT_ROOT + "/jobs/inexisting_task_id",
    json.dumps({"status": "STOP_REQUESTED"}),
    headers={"Content-Type": "application/json"},
)
assert r.status_code == 404


print("Test valid request 1")
req = {
    "user_id": "my_id",
    "user_email_address": "foo@bar.com",
    "source": os.getcwd() + "/byte.tif",
    "dst_format": {"gdal_driver": "GTiff", "options": {"TILED": "YES"}},
    # 'img_res': 0.0000002,
    # 'dst_srs': 'EPSG:26711',
    "footprint": "POLYGON((440100.000 3751000.000,440720.000 3750120.000,441920.000 3750120.000,441920.000 3751320.000,440100.000 3751000.000))",
    "footprint_srs": "EPSG:26711",
    "img_overviewed": True,
}
r = submit(req)
assert r.status_code == 201
resp = json.loads(r.text)
assert "task_id" in resp

# Wait for result request
while True:
    r = requests.get(resp["possible_requests"]["status"]["url"])
    assert r.status_code == 200
    resp = json.loads(r.text)
    print(resp["status"])
    if resp["status"] == "SUCCESS":
        break
    if not resp["status"] in ("SUBMITTED", "STARTED", "PROGRESS"):
        print(resp)
        sys.exit(1)

    time.sleep(1)


# Get zip name and check it exists
zip_name = resp["zip_name"]
os.stat(zip_name)

# Test the download REST API
r = requests.get(resp["possible_requests"]["download"]["url"])
zip_data = r.content

# Open result and check there's at least one feature
gdal.FileFromMemBuffer("/vsimem/result.zip", zip_data)
ds = gdal.Open("/vsizip//vsimem/result.zip/byte_extract.tif")
assert ds is not None
assert ds.RasterXSize == 20
assert ds.RasterYSize == 20
ds = None
gdal.Unlink("/vsimem/result.zip")

# Remove result file
os.unlink(zip_name)


print("Test valid request 2")
req = {
    "user_id": "my_id",
    "user_email_address": "foo@bar.com",
    "source": os.getcwd() + "/byte.tif",
    "dst_format": {"gdal_driver": "GTiff", "options": {"TILED": "YES"}},
    "img_res": 60.0,
    "dst_srs": "EPSG:26711",
    "footprint": "POLYGON((440100.000 3751000.000,440720.000 3750120.000,441920.000 3750120.000,441920.000 3751320.000,440100.000 3751000.000))",
    "footprint_srs": "EPSG:26711",
    "img_overviewed": True,
}
r = submit(req)
assert r.status_code == 201
resp = json.loads(r.text)
assert "task_id" in resp

# Wait for result request
while True:
    r = requests.get(resp["possible_requests"]["status"]["url"])
    assert r.status_code == 200
    resp = json.loads(r.text)
    print(resp["status"])
    if resp["status"] == "SUCCESS":
        break
    if not resp["status"] in ("SUBMITTED", "STARTED", "PROGRESS"):
        print(resp)
        sys.exit(1)

    time.sleep(1)


# Get zip name and check it exists
zip_name = resp["zip_name"]
os.stat(zip_name)

# Test the download REST API
r = requests.get(resp["possible_requests"]["download"]["url"])
zip_data = r.content

# Open result and check there's at least one feature
gdal.FileFromMemBuffer("/vsimem/result.zip", zip_data)
ds = gdal.Open("/vsizip//vsimem/result.zip/byte_extract.tif")
assert ds is not None
assert ds.RasterXSize == 30
assert ds.RasterYSize == 20
ds = None
gdal.Unlink("/vsimem/result.zip")

# Remove result file
os.unlink(zip_name)


# Launch a request that will fail on the backend side (image bigger than what JPEG supports)
print("Test a request that will fail on backend side")
req = {
    "user_id": "my_id",
    "user_email_address": "foo@bar.com",
    "source": os.getcwd() + "/byte.tif",
    "dst_format": {"gdal_driver": "JPEG"},
    "img_res": 0.0000002,
    "dst_srs": "EPSG:4326",
    "img_overviewed": True,
}
r = submit(req)
assert r.status_code == 201
resp = json.loads(r.text)
assert "task_id" in resp

# Wait for the request to fail
while True:
    r = requests.get(resp["possible_requests"]["status"]["url"])
    assert r.status_code == 200
    resp = json.loads(r.text)
    print(resp["status"])
    if resp["status"] == "FAILED":
        break
    elif not resp["status"] in ("SUBMITTED", "STARTED"):
        print(resp)
        sys.exit(1)

    time.sleep(1)


# Launch a request that will take a lot of time and abort it a bit after
print("Test aborting a request")
req = {
    "user_id": "my_id",
    "user_email_address": "foo@bar.com",
    "source": os.getcwd() + "/byte.tif",
    "dst_format": {"gdal_driver": "GTiff", "options": {"TILED": "YES"}},
    "img_res": 0.0000002,
    "dst_srs": "EPSG:4326",
    "img_resampling_method": "nearest",
    "img_overviewed": True,
}
r = submit(req)
assert r.status_code == 201
resp = json.loads(r.text)
assert "task_id" in resp

# Wait for the request to be in PROGRESS up to 10%
while True:
    r = requests.get(resp["possible_requests"]["status"]["url"])
    assert r.status_code == 200
    resp = json.loads(r.text)
    # print(resp)
    print(resp["status"])
    if resp["status"] == "PROGRESS":
        pct = resp["progress_pct"]
        print(pct)
        if pct > 10:
            break
    elif not resp["status"] in ("SUBMITTED", "STARTED"):
        print(resp)
        sys.exit(1)

    time.sleep(1)

r = requests.put(
    resp["possible_requests"]["abort"]["url"],
    json.dumps({"status": "STOP_REQUESTED", "soft_kill": True}),
    headers={"Content-Type": "application/json"},
)
assert r.status_code == 201

# Wait for request to be aborted
while True:
    r = requests.get(resp["possible_requests"]["status"]["url"])
    assert r.status_code == 200
    resp = json.loads(r.text)
    print(resp["status"])
    if resp["status"] == "STOPPED":
        break
    if not resp["status"] in ("STOP_REQUESTED", "STOPPED"):
        print(resp)
        sys.exit(1)

    time.sleep(1)
