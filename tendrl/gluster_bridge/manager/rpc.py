import json
import logging
import re
import traceback
import uuid

import etcd
import gevent.event

from tendrl.bridge_common.JobValidator import api
from tendrl.gluster_bridge.config import TendrlConfig
from tendrl.gluster_bridge.persistence.sds_operation import SDSOperation


config = TendrlConfig()
LOG = logging.getLogger(__name__)


class EtcdRPC(object):

    def __init__(self, Etcdthread):
        etcd_kwargs = {'port': int(config.get("bridge_common", "etcd_port")),
                       'host': config.get("bridge_common", "etcd_connection")}

        self.client = etcd.Client(**etcd_kwargs)
        self.bridge_id = str(uuid.uuid4())
        self.Etcdthread = Etcdthread

    def _acceptor(self):
        while not self.Etcdthread._complete.is_set():
            jobs = self.client.read("/api_job_queue")
            for job in jobs.children:
                raw_job = json.loads(job.value.decode('utf-8'))
                # Pick up the "new" job that is not locked by any other bridge
                if raw_job['status'] == "new" and \
                    raw_job['sds_name'] == "gluster":
                    try:
                        raw_job['status'] = "processing"
                        # Generate a request ID for tracking this job
                        # further by tendrl-api
                        req_id = str(uuid.uuid4())
                        raw_job['request_id'] = "%s/flow_%s" % (
                            self.bridge_id, req_id)
                        self.client.write(job.key, json.dumps(raw_job))
                        gevent.sleep(2)
                        LOG.info("Processing API-JOB %s" % raw_job[
                            'request_id'])
                        self.invoke_flow(raw_job['flow'], raw_job)
                        break
                    except Exception as ex:
                        self.api_job['status'] = 'failed'
                        self.api_job['response'] = ex.message
                        self.client.write(
                            self.api_job['request_id'],
                            json.dumps(self.api_job)
                        )
                        LOG.error(ex)

    def run(self):
        self._acceptor()

    def stop(self):
        pass

    def validate_api_job(self, api_job):
        sds_ops_def_obj = SDSOperation(
            cluster_id=api_job['cluster_id']
        )
        self.client.read(sds_ops_def_obj)
        api_validator = api.JobValidator(sds_ops_def_obj)
        ret_val, msg = api_validator.validateApi(api_job)
        if not ret_val:
            api_job['status'] = "failed"
            api_job['response'] = msg
            self.client.write(
                api_job['request_id'],
                json.dumps(api_job)
            )
            raise Exception("Error validating api job: %s" % msg)

    def invoke_flow(self, flow_name, api_job):
        self.validate_api_job(api_job)

        flow_module = 'tendrl.gluster_bridge.flows.%s' %\
                      self.convert_flow_name(flow_name)
        mod = __import__(flow_module, fromlist=[
            flow_name])
        getattr(mod, flow_name)(api_job).start()

    def convert_flow_name(self, flow_name):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', flow_name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


class EtcdThread(gevent.greenlet.Greenlet):
    """Present a ZeroRPC API for users

    to request state changes.

    """

    # In case server.run throws an exception, prevent
    # really aggressive spinning
    EXCEPTION_BACKOFF = 5

    def __init__(self, manager):
        super(EtcdThread, self).__init__()
        self._manager = manager
        self._complete = gevent.event.Event()
        self._server = EtcdRPC(self)

    def stop(self):
        LOG.info("%s stopping" % self.__class__.__name__)
        self._complete.set()
        if self._server:
            self._server.stop()

    def _run(self):

        while not self._complete.is_set():
            try:
                LOG.info("%s run..." % self.__class__.__name__)
                self._server.run()
            except Exception:
                LOG.error(traceback.format_exc())
                self._complete.wait(self.EXCEPTION_BACKOFF)

        LOG.info("%s complete..." % self.__class__.__name__)
