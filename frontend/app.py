# -*- coding: utf-8 -*-

from flask import Flask, request, make_response, Response
from werkzeug.datastructures import Headers
import os
import logging
import json
import sys
import time
from osgeo import gdal, ogr, osr
from email.utils import parseaddr

if os.path.exists('../common'):
    sys.path.append('../common')
elif os.path.exists('/common'):
    sys.path.append('/common')
from common import taskmanager


# To make pyflakes happy. Make builtins.__dict__['_'] of gettext active again
def _(x): return x


del sys.modules[__name__].__dict__['_']


# create the app:
app = Flask(__name__)

logger = logging.getLogger('app')

env = os.environ
DEBUG = env.get('DEBUG', 'False')


def make_json_response(json_response, status):
    return make_response(json.dumps(json_response),
                         status,
                         {'Content-Type': 'application/json'})


def make_json_load_error(e):
    json_response = {"status": "ERROR",
                     "detail": _("Exception while decoding incoming post data"),
                     "incoming_post_data": str(request.data),
                     "exception": str(e.__class__.__name__) + ': ' + str(e)}
    return make_json_response(json_response, 400)


def make_error(error_msg, req=None, http_status=400):
    json_response = {"status": "ERROR",
                     "detail": error_msg}
    if req:
        json_response['request'] = req
    return make_json_response(json_response, http_status)


def missing_parameter_error(parameter_name, req):
    return make_error(_("Parameter '%s' is missing") % parameter_name, req)


def invalid_parameter_type_error(parameter_name, req):
    return make_error(_("Parameter '%s' has not expected type") % parameter_name, req)


def get_current_datetime():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def upper(x):
    return x.upper()


@app.route('/jobs', methods=['POST'])
def submit():
    """Submit a new task from the JSon content in the POST payload"""

    try:
        req = json.loads(request.data.decode('UTF-8'))
    except Exception as e:
        return make_json_load_error(e)
    if type(req) != dict:
        return make_error(_("Payload should be a JSon dictionary"))

    required_parameters = [('user_id', [str]),
                           ('user_email_address', [str]),
                           ('source', [str]),
                           ('dst_format', [str, dict])]
    for (k, accepted_types) in required_parameters:
        if k not in req:
            return missing_parameter_error(k, req)
        if type(req[k]) not in accepted_types:
            return invalid_parameter_type_error(k, req)

    optional_parameters = [('dst_srs', [str]),
                           ('footprint', [str, dict]),
                           ('footprint_srs', [str]),
                           ('img_resampling_method', [str]),
                           ('img_res', [int, float]),
                           ('img_overviewed', [bool]),
                           ('img_overview_min_size', [int]),
                           ('extracts_volume', [str])
                           ]
    for (k, accepted_types) in optional_parameters:
        if k in req and type(req[k]) not in accepted_types:
            return invalid_parameter_type_error(k, req)

    user_email_address = req['user_email_address']
    # Very light validation of email address
    (___, user_email_address) = parseaddr(user_email_address)
    if user_email_address == '' or '@' not in user_email_address:
        return make_error(_("user_email_address is invalid"), req)

    # Open source to do some checks
    source = req['source']
    gdal.PushErrorHandler()
    flags = gdal.OF_VERBOSE_ERROR
    # We don't want to hit the PostGIS Raster driver11
    if source.upper().startswith('PG:'):
        flags += gdal.OF_VECTOR
    ds = gdal.OpenEx(source, flags)
    gdal.PopErrorHandler()
    if ds is None:
        return make_error(_("Cannot open source '%s'. Error message : %s") %
                          (source, gdal.GetLastErrorMsg()), req)

    has_raster = ds.RasterCount != 0
    has_vector = ds.GetLayerCount() != 0
    if not has_raster and not has_vector:
        # Shouldn't happen often...
        return make_error(_("Source has no raster or vector content"), req)

    if has_raster and has_vector:
        # Could happen for example for a GeoPackage.
        # Currently we will process it as vector-only or raster-only depending
        # on the presence of layer.
        if 'layer' in req:
            logging.info("Source has both raster and vector. Processing it as vector due to 'layer' being specified")
        else:
            logging.info("Source has both raster and vector. Processing it as vector due to 'layer' being absent")
    is_raster = not has_vector or (has_raster and 'layer' not in req)

    # Check dst_format
    dst_format = req['dst_format']
    dst_format_name = None
    if isinstance(dst_format, dict):
        if 'gdal_driver' not in dst_format:
            return missing_parameter_error('dst_format.gdal_driver', req)
        dst_format_name = dst_format['gdal_driver']
        if not isinstance(dst_format_name, str):
            return invalid_parameter_type_error('dst_format.gdal_driver', req)
    else:
        dst_format_name = dst_format
        # Normalize for worker
        req['dst_format'] = {}
        req['dst_format']['gdal_driver'] = dst_format_name
        dst_format = req['dst_format']

    # Check that extension is present for MapInfo File'
    if dst_format_name.upper() == 'MapInfo File'.upper():
        if 'extension' not in req['dst_format']:
            return missing_parameter_error('dst_format.extension', req)
        extension = req['dst_format']['extension']
        if not isinstance(dst_format_name, str):
            return invalid_parameter_type_error('dst_format.extension', req)
        if extension.lower() not in ['tab', 'mif']:
            return make_error(_("dst_format.extension should be either 'mif' or 'tab'"))

    # Check driver existence and capabilities
    drv = gdal.GetDriverByName(dst_format_name)
    if drv is None:
        return make_error(_("Driver '%s' is unknown") % dst_format_name)
    drv_md = drv.GetMetadata()
    if is_raster:
        if 'DCAP_RASTER' not in drv_md:
            return make_error(_("Driver '%s' has no raster capabilities") % dst_format_name)
        if 'DCAP_CREATE' not in drv_md and 'DCAP_CREATECOPY' not in drv_md:
            return make_error(_("Driver '%s' has no creation capabilities") % dst_format_name)
    else:
        if drv.GetMetadataItem('DCAP_VECTOR') is None:
            return make_error(_("Driver '%s' has no vector capabilities") % dst_format_name)
        if 'DCAP_CREATE' not in drv_md:
            return make_error(_("Driver '%s' has no creation capabilities") % dst_format_name)

    if 'options' in dst_format:
        options = dst_format['options']
        if not isinstance(options, dict):
            return invalid_parameter_type_error('dst_format.options', req)
        creation_options = drv.GetMetadataItem('DMD_CREATIONOPTIONLIST')
        for k in options:
            if k.upper() not in creation_options:
                return make_error(_('Option %s is unknown') % k)

    # Check if layer is required and correct when present
    if is_raster:
        if 'layer' in req:
            return make_error(_("'%s' is present, but unexpected for a raster source") % 'layer')
        ds_wkt = ds.GetProjectionRef()
        if len(ds_wkt) == 0:
            return make_error(_('%s is not georeferenced') % source)
    else:
        if 'layer' not in req:
            accept_several_out_layers = upper(dst_format_name) in \
                    (upper('ESRI Shapefile'), upper('MapInfo File'), upper('GPKG'))
            if ds.GetLayerCount() > 1 and not accept_several_out_layers:
                return missing_parameter_error('layer', req)
        else:
            layer = req['layer']
            if ds.GetLayerByName(layer) is None:
                return make_error(_("Layer '%s' does not exist") % layer, req)
        for k in req:
            if k.startswith('img_'):
                return make_error(_("'%s' is present, but unexpected for a raster source") % k)

        if 'layer_options' in dst_format:
            layer_options = dst_format['layer_options']
            if not isinstance(layer_options, dict):
                return invalid_parameter_type_error('dst_format.layer_options', req)
            layer_creation_options = drv.GetMetadataItem('DMD_LAYERCREATIONOPTIONLIST')
            for k in layer_options:
                if k.upper() not in layer_creation_options:
                    return make_error(_('Option %s is unknown') % k)

    # Validate dst_srs
    if 'dst_srs' in req:
        dst_srs = req['dst_srs']
        sr = osr.SpatialReference()
        if sr.SetFromUserInput(dst_srs) != 0:
            return make_error(_('%s is not a valid SRS') % dst_srs, req)

    # Validate footprint and footprint_srs
    if 'footprint' in req:
        footprint = req['footprint']
        footprint_json = False
        if isinstance(footprint, dict):
            g = ogr.CreateGeometryFromJson(json.dumps(footprint))
            footprint_json = True
        else:
            g = ogr.CreateGeometryFromWkt(footprint)

        if g is None:
            return make_error(_('%s is not a valid footprint') % str(footprint), req)
        gdal.PushErrorHandler()
        is_valid = g.IsValid()
        gdal.PopErrorHandler()
        if not is_valid:
            validity_error = gdal.GetLastErrorMsg()
            return make_error(_('%s is not a valid footprint: %s') %
                              (str(footprint), validity_error), req)

        # Normalize footprint to WKT
        footprint = g.ExportToWkt()
        req['footprint'] = footprint

        if 'footprint_srs' not in req:
            return missing_parameter_error('footprint_srs', req)
        footprint_srs = req['footprint_srs']
        sr = osr.SpatialReference()
        if sr.SetFromUserInput(footprint_srs) != 0:
            return make_error(_('%s is not a valid SRS') % footprint_srs, req)
        if footprint_json and sr.GetAuthorityCode(None) != '4326':
            return make_error(_('As footprint is a JSon geometry, footprint_srs should be EPSG:4326'))

    if 'img_resampling_method' in req:
        img_resampling_method = req['img_resampling_method'].lower()
        if img_resampling_method not in ('nearest', 'bilinear', 'cubic',
                                         'cubicspline', 'lanczos', 'average'):
            return make_error(_("img_resampling_method value should be one "
                                "of 'nearest', 'bilinear', 'cubic', "
                                "'cubicspline', 'lanczos' or 'average'"))

    known_parameters = [
        'user_id',
        'user_name',
        'user_first_name',
        'user_company',
        'user_email_address',
        'user_address',
        'source',
        'layer',
        'dst_format',
        'dst_srs',
        'footprint',
        'footprint_srs',
        'img_overviewed',
        'img_overview_min_size',
        'img_res',
        'img_resampling_method',
        'extracts_volume'
    ]
    for k in req:
        if k not in known_parameters:
            logging.info('Key %s found in request will be ignored' % k)

    dt = get_current_datetime()

    if 'fake_processing' in req:
        task_result = taskmanager.send_task('extraction.fake_processing',
                                            args=[req, dt, is_raster])
    else:
        task_result = taskmanager.send_task('extraction.do',
                                            args=[req, dt, is_raster])
    task_id = task_result.id
    task_result.backend.store_result(task_id=task_id, result={'request': req}, state='SUBMITTED')

    resp = {
        "status": "SUBMITTED",
        "submission_datetime": dt,
        "submitted_request": req,
        "possible_requests": {
            "status": {
                "url": request.url_root + "jobs/" + str(task_id),
                "verb": "GET"
            },
            "abort": {
                "url": request.url_root + "jobs/" + str(task_id),
                "verb": "PUT",
                "payload": {"status": "STOP_REQUESTED"}
            }
        },
        "task_id": str(task_id)
    }

    return make_json_response(resp, 201)


def is_valid_task(result):
    # PENDING is normally returned by Celery for submitted as well as
    # non existing tasks. But as we explicitely changed the state as
    # SUBMITTED at submit time, PENDING means non existing here
    return result.state != 'PENDING'


@app.route('/jobs/<string:task_id>', methods=['GET'])
def jobs_get(task_id):
    """Get the status of a task"""

    res = taskmanager.AsyncResult(task_id)

    if not is_valid_task(res):
        return make_error(_('Unknown task_id'), http_status=404)

    possible_requests = {
        "status": {
            "url": request.url_root + "jobs/" + str(task_id),
            "verb": "GET"
        }
    }
    if res.state in ('SUBMITTED', 'STARTED', 'PROGRESS'):
        possible_requests['abort'] = {
            "url": request.url_root + "jobs/" + str(task_id),
            "verb": "PUT",
            "payload": {"status": "STOP_REQUESTED"}
        }

    if res.state == 'SUCCESS':
        possible_requests['download'] = {
            "url": request.url_root + "jobs/" + str(task_id) + "/download",
            "verb": "GET"
        }

    resp = {
        "task_id": str(task_id),
        "possible_requests": possible_requests,
        "status": str(res.state)
    }

    info = res.info
    if not isinstance(info, dict):
        logging.error('res.info is not a dict')
    else:
        resp.update(info)
    return make_json_response(resp, 200)


@app.route('/jobs/<string:task_id>', methods=['PUT'])
def jobs_put(task_id):
    """Cancel a task (if {"status": "STOP_REQUESTED"} payload provided)"""

    try:
        req = json.loads(request.data.decode('UTF-8'))
    except Exception as e:
        return make_json_load_error(e)
    if type(req) != dict:
        return make_error(_("Payload should be a JSON dictionary"))

    if 'status' not in req:
        return missing_parameter_error('status', req)
    if req['status'] != 'STOP_REQUESTED':
        return make_error(_('Expecting {"status": "STOP_REQUESTED"}'), req)

    res = taskmanager.AsyncResult(task_id)
    if not is_valid_task(res):
        return make_error(_('Unknown task_id'), http_status=404)
    elif res.state == 'STOPPED':
        return make_error(_('Task already aborted'), http_status=409)
    elif res.state == 'STOP_REQUESTED':
        return make_error(_('Task already in abortion'), http_status=409)
    elif res.state == 'SUCCESS':
        return make_error(_('Task already finished successfully'),
                          http_status=409)
    elif res.state == 'FAILED':
        return make_error(_('Task already finished in failure'),
                          http_status=409)

    terminate = False
    if 'hard_kill' in req and req['hard_kill']:
        terminate = True
    taskmanager.control.revoke(task_id, terminate=terminate)
    res.backend.store_result(res.id, res.info, 'STOP_REQUESTED')

    possible_requests = {
        "status": {
            "url": request.url_root + "jobs/" + str(task_id),
            "verb": "GET"
        }
    }

    resp = {
        "task_id": str(task_id),
        "possible_requests": possible_requests,
        "status": str(res.state)
    }
    info = res.info
    resp.update(info)
    return make_json_response(resp, 201)


@app.route('/jobs/<string:task_id>/download', methods=['GET'])
def jobs_download_result(task_id):
    """Return the result of a successful task as a .zip attachement"""

    res = taskmanager.AsyncResult(task_id)

    if not is_valid_task(res):
        return make_error(_('Unknown task_id'), http_status=404)
    elif res.state != 'SUCCESS':
        return make_error(_('Task is not in SUCCESS state'), http_status=409)

    file_path = res.info['zip_name']

    def generate(fp):
        with open(fp, 'rb') as f:
            while True:
                data = f.read(4096)
                if not data:
                    break
                yield data

    headers = Headers()
    headers.add('Content-Type', 'application/zip')
    headers.add('Content-Disposition', 'attachment', filename='%s.zip' % task_id)
    headers.add('Content-Length', str(os.path.getsize(file_path)))
    return Response(generate(file_path), headers=headers)


if __name__ == '__main__':
    app.run(debug=DEBUG == "True")
