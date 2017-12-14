# -*- coding: utf-8 -*-

import math
import os
import os.path
import tempfile
import shutil
import logging
import sys
import time
from osgeo import gdal, ogr, osr
from zipfile import ZipFile
from celery import Task
from celery.exceptions import Ignore
from functools import wraps
# use billiard instead of standard multiprocessing to avoid
# 'daemonic processes are not allowed to have children' exception
import billiard as mp

if os.path.exists('../common'):
    sys.path.append('../common')
elif os.path.exists('/common'):
    sys.path.append('/common')
from common import taskmanager, service_conf

logger = logging.getLogger('worker')

env = os.environ

BASE_URL = env.get('BASE_URL', 'http://localhost:8080')

IDGO_EXTRACT_EXTRACTS_DIR = env.get('IDGO_EXTRACT_EXTRACTS_DIR', '/tmp')
IDGO_EXTRACT_EXTRACTS_RETENTION_DAYS = int(env.get('IDGO_EXTRACT_EXTRACTS_RETENTION_DAYS', 1))

PG_CONNECT_STRING = env.get("PG_CONNECT_STRING")

PROCESS_TIMEOUT = env.get("PROCESS_TIMEOUT", 3600)


def get_current_datetime():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


class MyTask(Task):
    # Do that so Celery doesn't set automatically the FAILURE state in
    # case of exceptions (the override to FAILED done in on_failure() can
    # arrive a bit too late if a get status is done in between), hence this
    # disabling. The consequence is that it will not set the SUCCESS
    # state automatically, but we can set it ourselves.
    ignore_result = True

    last_time_state_checked = None
    last_state = None

    start_datetime = None
    end_datetime = None

    def mark_has_stopped_and_raise_ignore(self):
        self.end_datetime = get_current_datetime()
        self.update_state(state='STOPPED',
                          meta={'pid': os.getpid(),
                                'hostname': self.request.hostname,
                                "start_datetime": self.start_datetime,
                                "end_datetime": self.end_datetime,
                                'request': self.req})
        logging.info('Task has been stopped')
        raise Ignore()

    def check_if_stop_requested_and_report_progress(self, progress_pct=None):
        if self.last_state is None or time.time() - self.last_time_state_checked > 1:
            result = self.AsyncResult(self.request.id)
            new_state = result.state
            self.last_state = new_state
            self.last_time_state_checked = time.time()
            if new_state != 'STOP_REQUESTED' and progress_pct is not None:
                self.update_state(state='PROGRESS',
                                  meta={'pid': os.getpid(),
                                        'hostname': self.request.hostname,
                                        "progress_pct": progress_pct,
                                        "start_datetime": self.start_datetime,
                                        'request': self.req})

        return self.last_state == 'STOP_REQUESTED'

    # Overriden method from Task. Called when an exception occurs
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        self.end_datetime = get_current_datetime()
        meta = {'pid': os.getpid(),
                'hostname': self.request.hostname,
                "exception": str(einfo.exception),
                "start_datetime": self.start_datetime,
                "end_datetime": self.end_datetime,
                'request': args[0]}
        logging.error('Failure occured: ' + str(meta))
        # Change state to FAILED instead of FAILURE, because there are issues
        # on the frontend side with the deserialization of exception. And
        # we want to embed more state in the meta.
        self.update_state(state='FAILED', meta=meta)


class OperationalError(Exception):
    pass


def task_decorator(f):
    """This decorator wraps a task main function so as to report its STARTED
        state before beginning and its SUCCESS state afterwards.
    """

    @wraps(f)
    def decorated_function(self, req, datetime, is_raster, **kwargs):

        logging.info('Receiving task_id %s, pid %d: %s, created at %s' %
                     (str(self.request.id), os.getpid(), str(req), datetime))

        self.req = req
        if self.check_if_stop_requested_and_report_progress():
            self.mark_has_stopped_and_raise_ignore()

        self.update_state(state='STARTED',
                          meta={'pid': os.getpid(),
                                'hostname': self.request.hostname,
                                'request': req
                                })
        self.start_datetime = get_current_datetime()

        res = f(self, req, datetime, is_raster, **kwargs)
        if res is None:
            res = {}

        logging.info('Finished task_id %s, pid %d: %s, created at %s, result %s' %
                     (str(self.request.id), os.getpid(), str(req), datetime, str(res)))

        res['pid'] = os.getpid()
        res['hostname'] = self.request.hostname
        res['request'] = req
        if self.start_datetime:
            res['start_datetime'] = self.start_datetime
        if self.end_datetime:
            res['end_datetime'] = self.end_datetime
        else:
            res['end_datetime'] = get_current_datetime()

        # Would be normally done automatically if ignore_result wasn't set
        self.update_state(state='SUCCESS', meta=res)

    return decorated_function


# Instanciate a forked process into which process_func is run
# We do that so that native crash in GDAL is caught up properly
# If we didn't do that, Celery would not properly record the state as no
# Python exception would be raised.
def do_process_in_forked_process(task, process_func, process_func_args):
    # GDAL compatible progress callback that assumes to communicate with
    # do_process_in_forked_process() through socket
    def _forked_process_gdal_callback(pct, msg, callback_data):

        socket = callback_data[0]
        socket.send({'progress_pct': pct * 100.0})
        msg = socket.recv()
        assert 'continue_process' in msg
        continue_process = msg['continue_process']
        assert continue_process in (True, False)
        if not continue_process:
            callback_data[1] = True
            logging.info('Abortion requested')
            return 0
        return 1

    def _forked_process_decorator(f):
        """This decorator wraps a task main function dedicated to be run under
            do_process_in_forked_process
        """

        @wraps(f)
        def decorated_function(process_func_args, callback, callback_data):

            socket = callback_data[0]
            logging.info('Forked process PID %d' % os.getpid())
            try:
                res = f(process_func_args, callback, callback_data)
                stop_requested = callback_data[1]
                if stop_requested:
                    res = {'stopped': True}
                else:
                    assert 'error' or 'success' in res
                socket.send(res)
                socket.close()
                if stop_requested:
                    logging.info('End of child on task cancellation')
                else:
                    if 'error' in res:
                        logging.info('End of child on handled error')
                    else:
                        logging.info('End of child on success')
            except Exception as e:
                res = {'error': str(e)}
                socket.send(res)
                socket.close()
                logging.info('End of child on error')
                raise

        return decorated_function

    parent_conn, child_conn = mp.Pipe()
    callback_data = [child_conn, False]
    p = mp.Process(target=_forked_process_decorator(process_func),
                   args=(process_func_args, _forked_process_gdal_callback, callback_data))
    p.start()
    logging.info('Processing of task forked as PID %d from %d' % (p.pid, os.getpid()))
    child_conn.close()
    last_time = time.time()

    try:
        # Listening loop : we listen for progress_pct or error from the child
        # and send it back if it must go on or cancel.
        while True:

            # Listen for the child. If we don't get any feedback within 5 seconds
            # check for the task status. If STOP_REQUESTED, then force kill the
            # child process
            while not parent_conn.poll(5):
                logging.info('Did not receive information from child in the last 5 seconds')
                result = task.AsyncResult(task.request.id)
                if result.state == 'STOP_REQUESTED':
                    logging.error('STOP_REQUESTED: Hard kill child!')
                    p.terminate()
                    task.mark_has_stopped_and_raise_ignore()

            msg = parent_conn.recv()
            if 'success' in msg:
                task.check_if_stop_requested_and_report_progress(progress_pct=100.0)
                break
            elif 'stopped' in msg:
                break
            elif 'progress_pct' in msg:
                progress_pct = msg['progress_pct']

                cur_time = time.time()
                if cur_time - last_time > 5.0:
                    logging.info('Progress %f' % progress_pct)
                    last_time = cur_time

                if task.check_if_stop_requested_and_report_progress(
                        progress_pct=progress_pct):
                    parent_conn.send({'continue_process': False})
                else:
                    parent_conn.send({'continue_process': True})
            elif 'error' in msg:
                raise OperationalError(msg['error'])
    finally:
        time.sleep(0.1)
        while True:
            if not mp.active_children():
                break
            logging.info('Joining child...')
            time.sleep(1)

        parent_conn.close()

    if task.check_if_stop_requested_and_report_progress():
        task.mark_has_stopped_and_raise_ignore()


# Return the input dict with all keys put in upper case.
def upper_dict(d):
    new_d = {}
    for k in d:
        new_d[k.upper()] = d[k]
    return new_d


def create_scaled_progress(pct_min, pct_max, gdal_callback):
    def scaled_progress_cbk(pct, msg, user_data):
        return gdal_callback(pct_min + (pct_max - pct_min) * pct, msg, user_data)

    return scaled_progress_cbk


def is_geom_rectangle(geom):
    minx, maxx, miny, maxy = geom.GetEnvelope()
    geom_area = geom.GetArea()
    bbox_area = (maxx - minx) * (maxy - miny)
    return abs(geom_area - bbox_area) < 1e-3 * bbox_area


def normalize_resampling(method):
    if method.upper().startswith('NEAR'):
        return 'NEAR'
    return method


# Aimed at being run under do_process_in_forked_process()
def process_raster(process_func_args, gdal_callback, gdal_callback_data):
    (req, tmpdir) = process_func_args

    if 'simulate_stuck_process' in req:
        logging.info('Simulating stucked proccess')
        time.sleep(100)

    src_filename = req['source']
    src_ds = gdal.Open(src_filename)
    src_gt = src_ds.GetGeoTransform()

    dst_format = req['dst_format']
    driver_name = dst_format['gdal_driver'].upper()
    target_ext = gdal.GetDriverByName(driver_name).GetMetadataItem('DMD_EXTENSION')

    parts = os.path.splitext(os.path.basename(src_filename))
    out_filename = os.path.join(tmpdir, parts[0] + '_extract.' + target_ext)

    can_warp_directly = False
    if 'options' in dst_format:
        driver_options = upper_dict(dst_format['options'])
    else:
        driver_options = {}

    # We can directly wrap in very few cases
    # Let's restrict that to uncompressed GeoTIFF.
    # In other cases warp into a VRT (very fast) and translate the result
    # afterwards.
    if driver_name == 'GTIFF':
        if 'COMPRESS' not in driver_options or \
                        driver_options['COMPRESS'].upper() != 'NONE':
            can_warp_directly = False

    warp_options = ''
    if can_warp_directly:
        warp_options += ' -of ' + driver_name
        for option in driver_options:
            val = driver_options[option]
            if val == True:
                val = 'YES'
            elif val == False:
                val = 'NO'
            warp_options += ' -co "%s=%s"' % (option, val)
    else:
        warp_options += ' -of VRT'
    if 'dst_srs' in req:
        warp_options += ' -t_srs "' + req['dst_srs'] + '"'
    if 'img_res' in req:
        res = float(req['img_res'])
        warp_options += ' -tr %.15f %.15f' % (res, res)
    if 'img_resampling_method' in req:
        warp_options += ' -r ' + normalize_resampling(req['img_resampling_method'])

    src_srs_wkt = src_ds.GetProjectionRef()
    src_srs = osr.SpatialReference()
    src_srs.SetFromUserInput(src_srs_wkt)

    if 'dst_srs' in req:
        dst_srs = osr.SpatialReference()
        dst_srs.SetFromUserInput(req['dst_srs'])
    else:
        dst_srs = src_srs

    footprint_geom = None
    cutline_filename = None
    if 'footprint' in req:
        footprint_geom = ogr.CreateGeometryFromWkt(req['footprint'])
        footprint_srs_wkt = req['footprint_srs']
        footprint_srs = osr.SpatialReference()
        footprint_srs.SetFromUserInput(footprint_srs_wkt)
        footprint_geom.AssignSpatialReference(footprint_srs)
        # Project footprint to target SRS
        footprint_geom.TransformTo(dst_srs)

        debug_cutline = False
        if debug_cutline:
            cutline_filename = "/tmp/cutline.json"
        else:
            cutline_filename = "/vsimem/cutline.json"

        cutline_ds = ogr.GetDriverByName('GeoJSON').CreateDataSource(cutline_filename)
        cutline_lyr = cutline_ds.CreateLayer('cutline', srs=dst_srs)
        f = ogr.Feature(cutline_lyr.GetLayerDefn())
        f.SetGeometry(footprint_geom)
        cutline_lyr.CreateFeature(f)
        cutline_lyr = None
        cutline_ds = None

        warp_options += ' -cutline ' + cutline_filename

        # Check if the footprint is a rectangle in the target SRS
        if not is_geom_rectangle(footprint_geom):
            # No it is not a rectangle
            # Then check if we must and can add an alpha channel
            src_has_alpha_or_nodata = False
            if src_ds.GetRasterBand(1).GetNoDataValue() is not None:
                src_has_alpha_or_nodata = True
            elif src_ds.GetRasterBand(src_ds.RasterCount).GetColorInterpretation() == gdal.GCI_AlphaBand:
                src_has_alpha_or_nodata = True

            add_alpha_channel = False
            if not src_has_alpha_or_nodata:
                if driver_name == 'GTIFF':
                    add_alpha_channel = True
                    if driver_options.get('COMPRESS', 'NONE') == 'JPEG':
                        add_alpha_channel = False
                elif driver_name.startswith('JP2'):
                    add_alpha_channel = True
                elif driver_name == 'PNG':
                    add_alpha_channel = True
            if add_alpha_channel:
                warp_options += ' -dstalpha'

    # In the situation of a north-up image where no explicit resolution
    # has been asked, align on source pixel boundaries;
    if 'img_res' not in req and src_srs.IsSame(dst_srs) and src_gt[2] == 0.0 and src_gt[4] == 0.0 and src_gt[5] < 0.0:
        raster_minx = src_gt[0]
        raster_miny = src_gt[3] + src_ds.RasterYSize * src_gt[5]
        if footprint_geom is not None:
            minx, maxx, miny, maxy = footprint_geom.GetEnvelope()
            te_minx = raster_minx + max(0, int((minx - raster_minx) / src_gt[1])) * src_gt[1]
            te_miny = raster_miny + max(0, int((miny - raster_miny) / abs(src_gt[5]))) * abs(src_gt[5])
            te_maxx = raster_minx + min(src_ds.RasterXSize, int(math.ceil((maxx - raster_minx) / src_gt[1]))) * src_gt[
                1]
            te_maxy = raster_miny + min(src_ds.RasterYSize,
                                        int(math.ceil((maxy - raster_miny) / abs(src_gt[5])))) * abs(src_gt[5])
        else:
            # TODO: fix minx and miny undefinied
            te_minx = raster_minx
            te_miny = raster_miny
            te_maxx = minx + src_ds.RasterXSize * src_gt[1]
            te_maxy = miny + src_ds.RasterYSize * abs(src_gt[5])
        warp_options += " -te %.18g %.18g %.18g %.18g" % (te_minx, te_miny, te_maxx, te_maxy)
        warp_options += " -tr %.18g %.18g" % (src_gt[1], abs(src_gt[5]))
    elif footprint_geom is not None:
        minx, maxx, miny, maxy = footprint_geom.GetEnvelope()
        warp_options += " -te %.18g %.18g %.18g %.18g" % (minx, miny, maxx, maxy)

    # In theory, building all power of twos overviews takes 1/3 of the
    # full resolution image ( 1/2^2 + 1/4^2 + ... = 1 /3 )
    if 'img_overviewed' in req and req['img_overviewed']:
        pct_max = 0.75
    else:
        pct_max = 1.0

    if can_warp_directly:
        logging.info('Invoking gdalwarp %s %s %s' % (src_filename, out_filename, warp_options))
        ret_ds = gdal.Warp(out_filename, src_ds, options=warp_options,
                           callback=gdal_callback,
                           callback_data=gdal_callback_data)
        success = ret_ds is not None
        ret_ds = None
    else:
        tmp_vrt = out_filename + '.vrt'
        logging.info('Invoking gdalwarp %s %s %s' % (src_filename, tmp_vrt, warp_options))
        tmp_ds = gdal.Warp(tmp_vrt, src_ds, options=warp_options)
        if tmp_ds is None:
            return {'error': gdal.GetLastErrorMsg()}
        translate_options = '-of ' + driver_name
        for option in driver_options:
            translate_options += ' -co %s=%s' % (option, driver_options[option])
        logging.info('Invoking gdal_translate %s %s %s' % (tmp_vrt, out_filename, translate_options))
        ret_ds = gdal.Translate(out_filename, tmp_ds,
                                options=translate_options,
                                callback=create_scaled_progress(0, pct_max, gdal_callback),
                                callback_data=gdal_callback_data)
        success = ret_ds is not None
        gdal.Unlink(tmp_vrt)

    if cutline_filename is not None and cutline_filename.startswith("/vsimem/"):
        gdal.Unlink(cutline_filename)

    # Build overviews if requested
    if 'img_overviewed' in req and req['img_overviewed'] and success:
        method = 'AVERAGE'
        if 'img_resampling_method' in req:
            method = normalize_resampling(req['img_resampling_method'])

        ds = gdal.Open(out_filename, gdal.GA_Update)

        img_overview_min_size = int(req.get('img_overview_min_size', 256))
        xsize = ds.RasterXSize
        ysize = ds.RasterYSize
        ratio = 1
        ratios = []
        while xsize / ratio >= img_overview_min_size or ysize / ratio >= img_overview_min_size:
            ratio *= 2
            ratios.append(ratio)
        if len(ratios) > 0:
            logging.info('Invoking gdaladdo -r %s %s %s' % (method, out_filename, ' '.join(str(r) for r in ratios)))
            ret = ds.BuildOverviews(method, ratios,
                                    callback=create_scaled_progress(pct_max, 1.0, gdal_callback),
                                    callback_data=gdal_callback_data)
            success = ret == 0
        ds = None

    if not success:
        return {'error': gdal.GetLastErrorMsg()}
    else:
        return {'success': True}


# Aimed at being run under do_process_in_forked_process()
def process_vector(process_func_args, gdal_callback, gdal_callback_data):
    (req, tmpdir) = process_func_args

    if 'simulate_stuck_process' in req:
        logging.info('Simulating stucked proccess')
        time.sleep(100)

    source = req['source']
    flags = gdal.OF_VERBOSE_ERROR
    # We don't want to hit the PostGIS Raster driver11
    if source.upper().startswith('PG:'):
        flags += gdal.OF_VECTOR
    src_ds = gdal.OpenEx(source, flags)

    dst_format = req['dst_format']
    driver_name = dst_format['gdal_driver'].upper()
    if 'extension' in dst_format:
        target_ext = '.' + dst_format['extension']
    else:
        target_ext = gdal.GetDriverByName(driver_name).GetMetadataItem('DMD_EXTENSION')
        if driver_name == 'GEOJSON':
            target_ext = '.geojson'
        elif target_ext is not None:
            target_ext = '.' + target_ext
        else:
            target_ext = ''

    layer_name_component = ''
    if 'layer' in req:
        layer_name_component = req['layer'] + '_'
    elif src_ds.GetLayerCount() == 1:
        layer_name_component = src_ds.GetLayer(0).GetName() + '_'

    if os.path.exists(source):
        parts = os.path.splitext(os.path.basename(source))
        out_filename = os.path.join(tmpdir, parts[0] + '_' + layer_name_component + 'extract' + target_ext)
    else:
        out_filename = os.path.join(tmpdir, layer_name_component + 'extract' + target_ext)

    dataset_creation_options = upper_dict(dst_format.get('options', {}))
    layer_creation_options = upper_dict(dst_format.get('layer_options', {}))

    translate_options = ' -progress '
    translate_options += ' -f "' + driver_name + '"'

    for option in dataset_creation_options:
        val = dataset_creation_options[option]
        if val == True:
            val = 'YES'
        elif val == False:
            val = 'NO'
        translate_options += ' -dsco "%s=%s"' % (option, val)

    for option in layer_creation_options:
        val = layer_creation_options[option]
        if val == True:
            val = 'YES'
        elif val == False:
            val = 'NO'
        translate_options += ' -lco "%s=%s"' % (option, val)

    if 'dst_srs' in req:
        translate_options += ' -t_srs ' + req['dst_srs']

    if 'layer' in req:
        layers = [src_ds.GetLayerByName(req['layer'])]
    else:
        layers = [src_ds.GetLayer(i) for i in range(src_ds.GetLayerCount())]

    base_translate_options = translate_options

    out_filename_or_ds = out_filename
    for idx, layer in enumerate(layers):

        translate_options = base_translate_options
        add_layer_name = True

        if 'footprint' in req:
            footprint_geom = ogr.CreateGeometryFromWkt(req['footprint'])
            footprint_srs_wkt = req['footprint_srs']
            footprint_srs = osr.SpatialReference()
            footprint_srs.SetFromUserInput(footprint_srs_wkt)
            footprint_geom.AssignSpatialReference(footprint_srs)
            # Project footprint to source SRS
            footprint_geom.TransformTo(layer.GetSpatialRef())

            if not is_geom_rectangle(footprint_geom):
                if src_ds.GetDriver().ShortName == 'PostgreSQL':
                    parts = layer.GetName().split('.')
                    if len(parts) == 2:
                        schema_name = parts[0]
                        table_name = parts[1]
                        sql = "SELECT srid FROM geometry_columns WHERE f_table_name = '%s' AND f_schema_name = '%s'" % (
                            table_name, schema_name)
                    else:
                        sql = "SELECT srid FROM geometry_columns WHERE f_table_name = '%s'" % layer.GetName()
                    sql_lyr = src_ds.ExecuteSQL(sql)
                    srid = None
                    for f in sql_lyr:
                        srid = int(f.GetField(0))
                        break
                    src_ds.ReleaseResultSet(sql_lyr)
                    if srid is None:
                        raise OperationalError('Cannot find PostGIS SRID matching layer %s' % layer.GetName())
                    translate_options += ' -where "ST_Intersects(%s, ST_GeomFromEWKT(\'SRID=%d;%s\'))"' % (
                        layer.GetGeometryColumn(), srid, footprint_geom.ExportToWkt())
                else:
                    geom_col = layer.GetGeometryColumn()
                    if geom_col == '':
                        geom_col = 'geometry'
                    translate_options += ' -dialect SQLite -sql "SELECT * FROM \"%s\" WHERE ST_Intersects(%s, ST_GeomFromText(\'%s\'))"' % (
                    layer.GetName(), geom_col, footprint_geom.ExportToWkt())
                    add_layer_name = False
            else:
                minx, maxx, miny, maxy = footprint_geom.GetEnvelope()
                translate_options += ' -spat %.18g %.18g %.18g %.18g' % (minx, miny, maxx, maxy)

        if add_layer_name:
            translate_options += ' "' + layer.GetName() + '"'

        cbk = create_scaled_progress(float(idx) / len(layers),
                                     float(idx + 1) / len(layers),
                                     gdal_callback)
        logging.info('Invoking ogr2ogr %s %s %s' % (out_filename, source, translate_options))
        out_ds = gdal.VectorTranslate(out_filename_or_ds, src_ds,
                                      options=translate_options,
                                      callback=cbk,
                                      callback_data=gdal_callback_data)
        if out_ds is None or out_ds != 0:
            break
        out_filename_or_ds = out_ds

    success = out_ds is not None
    out_ds = None

    if not success:
        return {'error': gdal.GetLastErrorMsg()}
    else:
        return {'success': True}


@taskmanager.task(name='extraction.do', bind=True, base=MyTask,
                  throws=(OperationalError))
@task_decorator
def do(self, req, datetime, is_raster):
    extracts_volume = IDGO_EXTRACT_EXTRACTS_DIR
    if service_conf is not None:
        extracts_volume = service_conf.get('extracts_volume', extracts_volume)
    extracts_volume = req.get('extracts_volume', extracts_volume)

    # Disabled
    if False:
        # clean older files
        now = time.time()
        for f in os.listdir(extracts_volume):
            file = os.path.join(extracts_volume, f)
            if not f.startswith('IDGO_EXTRACT_') or not os.path.isfile(file):
                continue
            try:
                creation = os.path.getctime(file)
            except Exception:
                pass
            if (now - creation) // (24 * 3600) >= IDGO_EXTRACT_EXTRACTS_RETENTION_DAYS:
                try:
                    os.unlink(file)
                    logger.info('Removed file %s because it was older than %s day(s)' % (
                        f, IDGO_EXTRACT_EXTRACTS_RETENTION_DAYS))
                except Exception:
                    pass

    extraction_id = 'IDGO_EXTRACT_{0}'.format(self.request.id)
    tmpdir = tempfile.mkdtemp(dir=extracts_volume, prefix="%s-" % extraction_id)
    logger.info('Created temp dir %s' % tmpdir)

    try:
        if is_raster:
            do_process_in_forked_process(self,
                                         process_raster,
                                         (req, tmpdir))
        else:
            do_process_in_forked_process(self,
                                         process_vector,
                                         (req, tmpdir))

        # create ZIP archive
        try:
            zip_name = os.path.join(extracts_volume, "%s.zip" % extraction_id)
            with ZipFile(zip_name, 'w') as myzip:
                for root, dirs, files in os.walk(tmpdir):
                    for file in files:
                        myzip.write(os.path.join(root, file), file)

        except IOError as e:
            logger.error('IOError while zipping %s' % tmpdir)
            raise e

    finally:
        # delete directory after zipping or exception
        shutil.rmtree(tmpdir)
        logger.info('Removed dir %s' % tmpdir)

    return {'zip_name': zip_name}


@taskmanager.task(name='extraction.fake_processing', bind=True, base=MyTask,
                  throws=(OperationalError))
@task_decorator
def fake_processing(self, req, datetime, is_raster):
    total_iters = 20
    for i in range(total_iters):
        logging.info('Step %d' % i)
        if self.check_if_stop_requested_and_report_progress(
                progress_pct=100.0 * i / total_iters):
            self.mark_has_stopped_and_raise_ignore()

        time.sleep(1)
        if 'simulate_failure_at_step' in req and req['simulate_failure_at_step'] == i:
            raise OperationalError('Simulate failure')
