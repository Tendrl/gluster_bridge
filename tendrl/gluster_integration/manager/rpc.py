import json
import logging
import re
import traceback
import uuid

import etcd
import gevent.event

from tendrl.gluster_integration.config import TendrlConfig


config = TendrlConfig()
LOG = logging.getLogger(__name__)


class EtcdRPC(object):

    def __init__(self, Etcdthread):
        etcd_kwargs = {'port': int(config.get("common", "etcd_port")),
                       'host': config.get("common", "etcd_connection")}

        self.client = etcd.Client(**etcd_kwargs)
        self.integration_id = str(uuid.uuid4())
        self.Etcdthread = Etcdthread

    def _acceptor(self):
        while not self.Etcdthread._complete.is_set():
            jobs = self.client.read("/api_job_queue")
            for job in jobs.children:
                raw_job = json.loads(job.value.decode('utf-8'))
        # Pick up the "new" job that is not locked by any other integration
                if raw_job['status'] == "new":
                    try:
                        raw_job['status'] = "processing"
                        # Generate a request ID for tracking this job
                        # further by tendrl-api
                        req_id = str(uuid.uuid4())
                        raw_job['request_id'] = "%s/flow_%s" % (
                            self.integration_id, req_id)
                        self.client.write(job.key, json.dumps(raw_job))
                        gevent.sleep(2)
                        LOG.info("Processing API-JOB %s" % raw_job[
                            'request_id'])
                        self.invoke_flow(raw_job['flow'], raw_job)
                        break
                    except Exception as ex:
                        LOG.error(ex)
            gevent.sleep(2)

    def run(self):
        self._acceptor()

    def stop(self):
        pass

    def invoke_flow(self, flow_name, api_job):
        # TODO(rohan) parse sds_operations_gluster.yaml and correlate here
        flow_module = 'tendrl.gluster_integration.flows.%s' %\
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
