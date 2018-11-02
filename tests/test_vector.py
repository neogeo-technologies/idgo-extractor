from osgeo import gdal, ogr
import json
import os
import requests
import time
import sys

in_docker = False
if in_docker:
    END_POINT_ROOT = "http://localhost:8080"
    src_gpkg = "/tests/poly.gpkg"
else:
    END_POINT_ROOT = "http://localhost:5000"
    src_gpkg = os.getcwd() + "/poly.gpkg"
SUBMIT_URL = END_POINT_ROOT + "/jobs"

pg_dbname = "PG:dbname=autotest"
pg_layer = "poly_32632"


def fix_url(url):
    if in_docker:
        return url.replace("http://app:5000", END_POINT_ROOT)
    else:
        return url


def submit(req):
    return requests.post(
        SUBMIT_URL, json.dumps(req), headers={"Content-Type": "application/json"}
    )


def test(
    source_name,
    layer_name=None,
    rectangular_footprint=False,
    output_format="ESRI Shapefile",
    extension=None,
):

    minx = 478316.000000
    miny = 4762880.000000
    maxx = 481645.000000
    maxy = 4765610.000000
    if rectangular_footprint:
        x0 = minx
        y0 = miny
    else:
        x0 = (minx + maxx) / 2
        y0 = (miny + maxy) / 2

    req = {
        "user_id": "my_id",
        "user_email_address": "foo@bar.com",
        "data_extractions": [
            {
                "source": source_name,
                "dst_format": {"gdal_driver": output_format},
                "footprint": "POLYGON((%.15g %.15g,%.15g %.15g,%.15g %.15g,%.15g %.15g,%.15g %.15g))"
                             % (x0, y0, minx, maxy, maxx, maxy, maxx, miny, x0, y0),
                "footprint_srs": "EPSG:32632",
            }
        ],
    }
    if extension is not None:
        req["dst_format"]["extension"] = extension
    if layer_name is not None:
        req["layer"] = layer_name
    r = submit(req)
    if r.status_code != 201:
        print(r.text)
    assert r.status_code == 201
    resp = json.loads(r.text)
    # print(resp)
    assert "task_id" in resp

    # Wait for result request
    while True:
        r = requests.get(fix_url(resp["possible_requests"]["status"]["url"]))
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
    if not in_docker:
        zip_name = resp["extract_location"]
        os.stat(zip_name)

    # Test the download REST API
    r = requests.get(fix_url(resp["possible_requests"]["download"]["url"]))
    zip_data = r.content

    # Open result and check there's at least one feature

    # There's a bug in GDAL 2.1 regarding reading zipped tab
    if output_format != "MapInfo File":
        gdal.FileFromMemBuffer("/vsimem/result.zip", zip_data)
        ds = ogr.Open("/vsizip//vsimem/result.zip")
        assert ds is not None
        lyr = ds.GetLayer(0)
        assert lyr.GetFeatureCount() != 0
        ds = None
        gdal.Unlink("/vsimem/result.zip")

    if not in_docker:
        # Remove result file
        os.unlink(zip_name)


# Test GeoPackage source
print("Testing extracting from GeoPackage source with non-rectangular footprint")
test(src_gpkg)

print("Testing extracting from GeoPackage source with rectangular footprint")
test(src_gpkg, rectangular_footprint=True)

print("Testing extracting into GeoJSON")
test(src_gpkg, output_format="GeoJSON", rectangular_footprint=True)

print("Testing extracting into MapInfo tab")
test(
    src_gpkg, output_format="MapInfo File", extension="tab", rectangular_footprint=True
)

if not in_docker:
    print("Testing extracting from PostgreSQL source")
    # Setup PostgreSQL db
    os.system(
        "ogr2ogr -update %s %s -nln %s -overwrite" % (pg_dbname, src_gpkg, pg_layer)
    )
    test(pg_dbname, pg_layer)
