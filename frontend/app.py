# -*- coding: utf-8 -*-

from flask import Flask, request, make_response, Response
from werkzeug.datastructures import Headers
import os
import logging
import json
import sys
import time
import uuid
from osgeo import gdal, ogr, osr
from email.utils import parseaddr

if os.path.exists("../common"):
    sys.path.append("../common")
elif os.path.exists("/common"):
    sys.path.append("/common")
from common import taskmanager
from celery import chord


# To make pyflakes happy. Make builtins.__dict__['_'] of gettext active again
def _(x):
    return x


del sys.modules[__name__].__dict__["_"]


# create the app:
app = Flask(__name__)

logger = logging.getLogger("app")

env = os.environ
DEBUG = env.get("DEBUG", "False")


def make_json_response(json_response, status):
    return make_response(
        json.dumps(json_response), status, {"Content-Type": "application/json"}
    )


def make_json_load_error(e):
    json_response = {
        "status": "ERROR",
        "detail": _("Exception while decoding incoming post data"),
        "incoming_post_data": str(request.data),
        "exception": str(e.__class__.__name__) + ": " + str(e),
    }
    return make_json_response(json_response, 400)


def make_error(error_msg, req=None, http_status=400):
    json_response = {"status": "ERROR", "detail": error_msg}
    if req:
        json_response["query"] = req
    return make_json_response(json_response, http_status)


def build_missing_parameter_error_message(parameter_name):
    return "Parameter {} is missing".format(parameter_name)


def build_invalid_parameter_type_error_message(parameter_name):
    return "Parameter {} has not expected type".format(parameter_name)


def missing_parameter_error(parameter_name, req):
    return make_error(_(build_missing_parameter_error_message(parameter_name)), req)


def invalid_parameter_type_error(parameter_name, req):
    return make_error(_(build_invalid_parameter_type_error_message(parameter_name)), req)


def get_current_datetime():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def upper(x):
    return x.upper()


@app.route("/jobs", methods=["POST"])
def submit():
    """Submit a new task from the JSon content in the POST payload"""

    try:
        req = json.loads(request.data.decode("UTF-8"))
    except Exception as e:
        return make_json_load_error(e)
    if type(req) != dict:
        return make_error(_("Payload should be a JSON dictionary"))

    required_parameters = [
        ("user_id", [str]),
        ("user_email_address", [str]),
    ]
    for (k, accepted_types) in required_parameters:
        if k not in req:
            return missing_parameter_error(k, req)
        if type(req[k]) not in accepted_types:
            return invalid_parameter_type_error(k, req)

    optional_parameters = [
        ("user_name", [str]),
        ("user_first_name", [str]),
        ("user_company", [str]),
        ("user_address", [str]),
        ("data_extractions", [list]),
        ("additional_files", [list]),
        ("extracts_volume", [str]),
    ]
    for (k, accepted_types) in optional_parameters:
        if k in req and type(req[k]) not in accepted_types:
            return invalid_parameter_type_error(k, req)

    user_email_address = req["user_email_address"]
    # Very light validation of email address
    (___, user_email_address) = parseaddr(user_email_address)
    if user_email_address == "" or "@" not in user_email_address:
        return make_error(_("user_email_address is invalid"), req)

    current_datetime = get_current_datetime()

    tasks_infos = []
    common_params_dict = dict()
    if req.get("extracts_volume"):
        common_params_dict["extracts_volume"] = req["extracts_volume"]

        # Check if the extract volume exists and if we have write permission
        if not os.path.exists(common_params_dict["extracts_volume"]):
            return make_error(
                _("Extracts directory not found: {}".format(common_params_dict["extracts_volume"])), req)
        elif not os.path.isdir(common_params_dict["extracts_volume"]):
            return make_error(
                _("Extracts directory path not a directory: {}".format(common_params_dict["extracts_volume"])), req)
        elif not os.access(common_params_dict["extracts_volume"], os.W_OK):
            return make_error(
                _("No write access to extracts directory: {}".format(common_params_dict["extracts_volume"])), req)

    if req.get("fake_processing"):
        common_params_dict["fake_processing"] = True

    if req.get("data_extractions"):
        data_extractions_tasks_infos = get_tasks_info_for_data_extracts(req["data_extractions"], common_params_dict)
        errors = []
        for task_info in data_extractions_tasks_infos:
            errors.extend(task_info.get("param_errors"))
        if errors:
            return make_error(_({"title": "Invalid data extraction parameters", "list": errors}), req)
        tasks_infos.extend(data_extractions_tasks_infos)

    if req.get("additional_files"):
        doc_copy_tasks_infos = get_tasks_info_for_doc_copy(req["additional_files"], common_params_dict)
        errors = []
        for task_info in doc_copy_tasks_infos:
            errors.extend(task_info.get("param_errors"))
        if errors:
            return make_error(_({"title": "Invalid doc copy parameters", "list": errors}), req)
        tasks_infos.extend(doc_copy_tasks_infos)

    if not tasks_infos:
        return make_error(_("Not any task in this request"), req)

    extract_id = str(uuid.uuid4())
    tasks = []
    for task_info in tasks_infos:
        if "fake_processing" in task_info["worker_params"]:
            task = taskmanager.signature(
                "idgo_extractor.fake_extraction",
                kwargs={"params": task_info["worker_params"],
                        "datetime": current_datetime,
                        "extract_id": extract_id})
        else:
            task = taskmanager.signature(
                task_info["worker_name"],
                kwargs={"params": task_info["worker_params"],
                        "datetime": current_datetime,
                        "extract_id": extract_id})
        tasks.append(task)

    zip_task = taskmanager.signature(
        "idgo_extractor.zip_dir",
        kwargs={"params": {},
                "datetime": current_datetime,
                "extract_id": extract_id},
        task_id=extract_id
    )
    task_result = chord(tasks, zip_task).apply_async()

    subtasks_ids = []
    for subtask in task_result.parent.children:
        subtasks_ids.append(subtask.id)

    if task_result:
        task_id = task_result.id
        task_result.backend.store_result(
            task_id=task_id, result={"query": req, "subtasks_ids": subtasks_ids}, state="SUBMITTED"
        )

        resp = {
            "status": "SUBMITTED",
            "submission_datetime": current_datetime,
            "query": req,
            "possible_requests": {
                "status": {"url": request.url_root + "jobs/" + str(task_id), "verb": "GET"},
                "abort": {
                    "url": request.url_root + "jobs/" + str(task_id),
                    "verb": "PUT",
                    "payload": {"status": "STOP_REQUESTED"},
                },
            },
            "tasks_details": tasks_infos,
            "task_id": str(task_id),
        }

    return make_json_response(resp, 201)


class DataExtractParamsChecker():

    def __init__(self, extract_params: dict):
        self.extract_params = extract_params

        self.errors = []
        self.warnings = []
        self.ds = None
        self.drv = None
        self.has_raster = None
        self.has_vector = None
        self.is_raster = None
        self.dst_format = None

    def run(self):
        self.check_datasource()
        self.check_driver_options()
        self.check_dst_srs()
        self.check_footprint()
        self.check_resampling_method()

    def check_datasource(self):
        source = self.extract_params["source"]
        gdal.PushErrorHandler()
        flags = gdal.OF_VERBOSE_ERROR
        # We don't want to hit the PostGIS Raster driver11
        if source.upper().startswith("PG:"):
            flags += gdal.OF_VECTOR
        self.ds = gdal.OpenEx(source, flags)
        gdal.PopErrorHandler()
        if self.ds is None:
            param_error = "Cannot open source '{}'. Error message : {}".format(
                source,
                gdal.GetLastErrorMsg()
            )
            self.errors.append(param_error)
            return

        self.has_raster = self.ds.RasterCount != 0
        self.has_vector = self.ds.GetLayerCount() != 0

        # No raster and no vector content
        if not self.has_raster and not self.has_vector:
            param_error = "Source has no raster or vector content"
            self.errors.append(param_error)
            return

        # Raster and vector content
        if self.has_raster and self.has_vector:
            # Could happen for example for a GeoPackage.
            # Currently we will process it as vector-only or raster-only depending
            # on the presence of layer.
            if  self.extract_params.get("layer"):
                logging.info(
                    "Source has both raster and vector. Processing it as vector due to 'layer' being specified"
                )
            else:
                logging.info(
                    "Source has both raster and vector. Processing it as vector due to 'layer' being absent"
                )
        self.is_raster = not self.has_vector or (self.has_raster and "layer" not in self.extract_params)
        self.extract_params["is_raster"] = self.is_raster

        # Check dst_format
        dst_format = self.extract_params["dst_format"]
        dst_format_name = None

        if isinstance(dst_format, dict):
            if "gdal_driver" not in dst_format:
                param_error = build_missing_parameter_error_message("dst_format.gdal_driver")
                self.errors.append(param_error)
                return

            dst_format_name = dst_format["gdal_driver"]
            if not isinstance(dst_format_name, str):
                param_error = build_invalid_parameter_type_error_message("dst_format.gdal_driver")
                self.errors.append(param_error)
                return
        else:
            dst_format_name = dst_format
            # Normalize for worker
            self.extract_params["dst_format"] = {}
            self.extract_params["dst_format"]["gdal_driver"] = dst_format_name
            dst_format = self.extract_params["dst_format"]

        # Check that extension is present for MapInfo File'
        if dst_format_name and dst_format_name.upper() == "MapInfo File".upper():
            if "extension" not in self.extract_params["dst_format"]:
                param_error = build_missing_parameter_error_message("dst_format.extension")
                self.errors.append(param_error)
                return

            extension = self.extract_params["dst_format"]["extension"]
            if not isinstance(dst_format_name, str):
                param_error = build_invalid_parameter_type_error_message("dst_format.extension")
                self.errors.append(param_error)
                return

            if extension.lower() not in ["tab", "mif"]:
                param_error = "dst_format.extension should be either 'mif' or 'tab'"
                self.errors.append(param_error)
                return

        # Check driver existence and capabilities
        self.drv = gdal.GetDriverByName(dst_format_name)
        if self.drv is None:
            param_error = "Driver '{}' is unknown".format(dst_format_name)
            self.errors.append(param_error)
            return
        drv_md = self.drv.GetMetadata()
        if self.is_raster:
            if "DCAP_RASTER" not in drv_md:
                param_error = "Driver '{}' has no raster capabilities".format(dst_format_name)
                self.errors.append(param_error)
                return
            if "DCAP_CREATE" not in drv_md and "DCAP_CREATECOPY" not in drv_md:
                param_error = "Driver '{}' has no creation capabilities".format(dst_format_name)
                self.errors.append(param_error)
                return
        else:
            if self.drv.GetMetadataItem("DCAP_VECTOR") is None:
                param_error = "Driver '{}' has no vector capabilities".format(dst_format_name)
                self.errors.append(param_error)
                return
            if "DCAP_CREATE" not in drv_md:
                param_error = "Driver '{}' has no creation capabilities".format(dst_format_name)
                self.errors.append(param_error)
                return

    def check_driver_options(self):
        dst_format = self.extract_params["dst_format"]
        dst_format_name = self.extract_params["dst_format"]["gdal_driver"]
        source = self.extract_params["source"]

        if not dst_format or not dst_format_name or not source:
            return

        if dst_format.get("options"):
            options = dst_format["options"]
            if not isinstance(options, dict):
                param_error = build_invalid_parameter_type_error_message("dst_format.options")
                self.errors.append(param_error)
            else:
                creation_options = self.drv.GetMetadataItem("DMD_CREATIONOPTIONLIST")
                for k in options:
                    if not creation_options or k.upper() not in creation_options:
                        param_warning = "Driver option '{}' is unknown".format(k)
                        self.warnings.append(param_warning)

        # Check if layer is required and correct when present
        if self.is_raster:
            if self.extract_params.get("layer"):
                param_error = "'{}' is present, but unexpected for a raster source".format("layer")
                self.errors.append(param_error)

            ds_wkt = self.ds.GetProjectionRef()
            if len(ds_wkt) == 0:
                param_error = "'{}' is not georeferenced".format("source")
                self.errors.append(param_error)
        else:
            if not self.extract_params.get("layer"):
                accept_several_out_layers = upper(dst_format_name) in (
                    upper("ESRI Shapefile"),
                    upper("MapInfo File"),
                    upper("GPKG"),
                )
                if self.ds.GetLayerCount() > 1 and not accept_several_out_layers:
                    param_error = build_missing_parameter_error_message("layer")
                    self.errors.append(param_error)
                    return
            else:
                layer = self.extract_params["layer"]
                if self.ds.GetLayerByName(layer) is None:
                    param_error = "Layer '{}' does not exist".format(layer)
                    self.errors.append(param_error)
                    return

            for k in self.extract_params:
                if k.startswith("img_"):
                    param_error = "'{}' is present, but unexpected for a raster source".format(k)
                    self.errors.append(param_error)
                    return

            if dst_format.get("layer_options"):
                layer_options = dst_format["layer_options"]
                if not isinstance(layer_options, dict):
                    param_error = build_invalid_parameter_type_error_message("dst_format.layer_options")
                    self.errors.append(param_error)
                else:
                    layer_creation_options = self.drv.GetMetadataItem("DMD_LAYERCREATIONOPTIONLIST")
                    for k in layer_options:
                        if not layer_creation_options or k.upper() not in layer_creation_options:
                            param_warning = "Layer option '{}' is unknown".format(k)
                            self.warnings.append(param_warning)

    def check_dst_srs(self):
        # Validate dst_srs
        if self.extract_params.get("dst_srs"):
            dst_srs = self.extract_params["dst_srs"]
            sr = osr.SpatialReference()
            if sr.SetFromUserInput(dst_srs) != 0:
                param_error = "'{}' is not a valid SRS".format(dst_srs)
                self.errors.append(param_error)

    def check_footprint(self):
        g = None

        # Validate footprint and footprint_srs
        if self.extract_params.get("footprint"):

            footprint = self.extract_params["footprint"]
            footprint_is_json = False
            if isinstance(footprint, dict):
                g = ogr.CreateGeometryFromJson(json.dumps(footprint))
                footprint_is_json = True
            else:
                g = ogr.CreateGeometryFromWkt(footprint)

            if g is None:
                param_error = "'{}' is not a valid footprint".format(str(footprint))
                self.errors.append(param_error)
                return

            gdal.PushErrorHandler()
            is_valid = g.IsValid()
            gdal.PopErrorHandler()
            if not is_valid:
                validity_error = gdal.GetLastErrorMsg()
                param_error = "'{}' is not a valid footprint: {}".format(str(footprint), validity_error)
                self.errors.append(param_error)
                return

            # Normalize footprint to WKT
            footprint_as_wkt = g.ExportToWkt()
            self.extract_params["footprint"] = footprint_as_wkt

            # SRS
            if "footprint_srs" not in self.extract_params:
                param_error = build_missing_parameter_error_message("footprint_srs")
                self.errors.append(param_error)
                return

            footprint_srs = self.extract_params["footprint_srs"]
            footprint_src_srs = osr.SpatialReference()
            if footprint_src_srs.SetFromUserInput(footprint_srs) != 0:
                param_error = "'{}' is not a valid SRS".format(str(footprint_srs))
                self.errors.append(param_error)
                return

            if footprint_is_json and footprint_src_srs.GetAuthorityCode(None) != "4326":
                param_error = "As footprint is a JSON geometry, footprint_srs should be EPSG:4326"
                self.errors.append(param_error)
                return

            # Add a footprint in GeoJSON in EPSG:4326
            if footprint_is_json:
                footprint_as_geojson = footprint
            else:
                footprint_dst_srs = osr.SpatialReference()
                footprint_dst_srs.ImportFromEPSG(4326)
                transform = osr.CoordinateTransformation(
                    footprint_src_srs, footprint_dst_srs
                )
                g.Transform(transform)
                footprint_as_geojson = json.loads(g.ExportToJson())

                self.extract_params["footprint_geojson"] = footprint_as_geojson

    def check_resampling_method(self):
        if self.extract_params.get("img_resampling_method"):
            img_resampling_method = self.extract_params["img_resampling_method"].lower()
            if img_resampling_method not in (
                "nearest",
                "bilinear",
                "cubic",
                "cubicspline",
                "lanczos",
                "average",
            ):
                param_error = "img_resampling_method value should be one " \
                              "of 'nearest', 'bilinear', 'cubic', " \
                              "'cubicspline', 'lanczos' or 'average'"
                self.errors.append(param_error)


def get_tasks_info_for_doc_copy(extracts_list, common_params_dict):
    tasks_info = []
    for extract_param_dict in extracts_list:
        tasks_info.append(
            get_task_info_doc_copy(
                extract_param_dict, common_params_dict)
        )
    return tasks_info


def get_task_info_doc_copy(extract_param_dict, common_params_dict):
    worker_params = extract_param_dict.copy()
    worker_params.update(common_params_dict)
    param_errors = []
    param_warnings = []

    task_info = {
        "worker_name": "idgo_extractor.file_copy",
        "worker_params": worker_params,
        "param_errors": param_errors,
        "param_warnings": param_warnings
    }

    return task_info


def get_tasks_info_for_data_extracts(extracts_list, common_params_dict):
    tasks_info = []
    for extract_param_dict in extracts_list:
        tasks_info.append(
            get_task_info_for_data_extract(
                extract_param_dict, common_params_dict)
        )
    return tasks_info


def get_task_info_for_data_extract(extract_param_dict, common_params_dict):
    worker_params = extract_param_dict.copy()
    worker_params.update(common_params_dict)
    param_errors = []
    param_warnings = []

    # Required parameters presence and types validation
    required_parameters = [
        ("source", [str]),
        ("dst_format", [str, dict]),
    ]
    for (k, accepted_types) in required_parameters:
        if k not in worker_params:
            param_error = build_missing_parameter_error_message(k)
            param_errors.append(param_error)
        elif type(worker_params[k]) not in accepted_types:
            param_error = build_invalid_parameter_type_error_message(k)
            param_errors.append(param_error)

    # Optional parameters types validation
    optional_parameters = [
        ("dst_srs", [str]),
        ("footprint", [str, dict]),
        ("footprint_srs", [str]),
        ("img_resampling_method", [str]),
        ("img_res", [int, float]),
        ("img_overviewed", [bool]),
        ("img_overview_min_size", [int]),
        ("layer", [str]),
        ("extracts_volume", [str]),
        ("extract_name", [str]),
    ]
    for (k, accepted_types) in optional_parameters:
        if worker_params.get(k) and type(worker_params[k]) not in accepted_types:
            param_error = build_invalid_parameter_type_error_message(k)
            param_errors.append(param_error)

    known_parameters = [
        "extract_name",
        "source",
        "layer",
        "dst_format",
        "dst_srs",
        "footprint",
        "footprint_srs",
        "footprint_geojson",
        "img_overviewed",
        "img_overview_min_size",
        "img_res",
        "img_resampling_method",
    ]
    for k in worker_params:
        if k not in known_parameters:
            param_warning = "Key {} found in request will be ignored".format(k)
            param_warnings.append(param_warning)

    if not param_errors:
        param_checker = DataExtractParamsChecker(worker_params)
        param_checker.run()
        param_errors.extend(param_checker.errors)
        param_warnings.extend(param_checker.warnings)

    for warning in param_warnings:
        logging.info(warning)

    task_info = {
        "worker_name": "idgo_extractor.extraction",
        "worker_params": worker_params,
        "param_errors": param_errors,
        "param_warnings": param_warnings
    }

    return task_info


def is_valid_task(result):
    # PENDING is normally returned by Celery for submitted as well as
    # non existing tasks. But as we explicitely changed the state as
    # SUBMITTED at submit time, PENDING means non existing here
    return result.state != "PENDING"


@app.route("/jobs/<string:task_id>", methods=["GET"])
def jobs_get(task_id):
    """Get the status of a task"""

    res = taskmanager.AsyncResult(task_id)

    if not is_valid_task(res):
        return make_error(_("Unknown task_id"), http_status=404)

    possible_requests = {
        "status": {"url": request.url_root + "jobs/" + str(task_id), "verb": "GET"}
    }
    if res.state in ("SUBMITTED", "STARTED", "PROGRESS"):
        possible_requests["abort"] = {
            "url": request.url_root + "jobs/" + str(task_id),
            "verb": "PUT",
            "payload": {"status": "STOP_REQUESTED"},
        }

    if res.state == "SUCCESS":
        possible_requests["download"] = {
            "url": request.url_root + "jobs/" + str(task_id) + "/download",
            "verb": "GET",
        }

    resp = {
        "task_id": str(task_id),
        "possible_requests": possible_requests,
        "status": str(res.state),
    }

    info = res.info
    if not isinstance(info, dict):
        if res.state == "FAILURE":
            resp["exception"] = str(info)
        else:
            logging.error("res.info is not a dict")
    else:
        resp.update(info)
    return make_json_response(resp, 200)


@app.route("/jobs/<string:task_id>", methods=["PUT"])
def jobs_put(task_id):
    """Cancel a task (if {"status": "STOP_REQUESTED"} payload provided)"""

    try:
        req = json.loads(request.data.decode("UTF-8"))
    except Exception as e:
        return make_json_load_error(e)
    if type(req) != dict:
        return make_error(_("Payload should be a JSON dictionary"))

    if "status" not in req:
        return missing_parameter_error("status", req)
    if req["status"] != "STOP_REQUESTED":
        return make_error(_('Expecting {"status": "STOP_REQUESTED"}'), req)

    res = taskmanager.AsyncResult(task_id)
    if not is_valid_task(res):
        return make_error(_("Unknown task_id"), http_status=404)
    elif res.state == "STOPPED":
        return make_error(_("Task already aborted"), http_status=409)
    elif res.state == "STOP_REQUESTED":
        return make_error(_("Task already in abortion"), http_status=409)
    elif res.state == "SUCCESS":
        return make_error(_("Task already finished successfully"), http_status=409)
    elif res.state == "FAILED":
        return make_error(_("Task already finished in failure"), http_status=409)

    terminate = False
    if "hard_kill" in req and req["hard_kill"]:
        terminate = True

    # Revocation of subtasks
    for subtask_id in res.result["subtasks_ids"]:
        subtask_res = taskmanager.AsyncResult(subtask_id)
        taskmanager.control.revoke(subtask_id, terminate=terminate)
        res.backend.store_result(subtask_id, subtask_res.info, "STOP_REQUESTED")

    taskmanager.control.revoke(task_id, terminate=terminate)
    res.backend.store_result(res.id, res.info, "STOP_REQUESTED")

    possible_requests = {
        "status": {"url": request.url_root + "jobs/" + str(task_id), "verb": "GET"}
    }

    resp = {
        "task_id": str(task_id),
        "possible_requests": possible_requests,
        "status": str(res.state),
    }
    info = res.info
    resp.update(info)
    return make_json_response(resp, 202)


@app.route("/jobs/<string:task_id>/download", methods=["GET"])
def jobs_download_result(task_id):
    """Return the result of a successful task as a .zip attachement"""

    res = taskmanager.AsyncResult(task_id)

    if not is_valid_task(res):
        return make_error(_("Unknown task_id"), http_status=404)
    elif res.state != "SUCCESS":
        return make_error(_("Task is not in SUCCESS state"), http_status=409)

    file_path = res.info["extract_location"]

    def generate(fp):
        with open(fp, "rb") as f:
            while True:
                data = f.read(4096)
                if not data:
                    break
                yield data

    headers = Headers()
    headers.add("Content-Type", "application/zip")
    headers.add("Content-Disposition", "attachment", filename="%s.zip" % task_id)
    headers.add("Content-Length", str(os.path.getsize(file_path)))
    return Response(generate(file_path), headers=headers)


if __name__ == "__main__":
    app.run(debug=DEBUG == "True")
