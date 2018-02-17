import json

from tendrl.commons import objects
from tendrl.commons.objects.job import Job
from tendrl.commons.utils import log_utils as logger


class GenerateBrickMapping(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(GenerateBrickMapping, self).__init__(*args, **kwargs)

    def run(self):
        logger.log(
            "info",
            NS.publisher_id,
            {"message": "Generating brick mapping for gluster volume"},
            job_id=self.parameters["job_id"],
            flow_id=self.parameters["flow_id"],
            integration_id=NS.tendrl_context.integration_id
        )
        brick_count = self.parameters.get('Volume.brick_count')
        subvol_size = self.parameters.get('Volume.subvol_size')
        message = ""
        # get brick_count number of bricks from all the selected nodes

        nodes = {}
        for node in self.parameters.get('Cluster.node_configuration'):
            key = "nodes/%s/NodeContext/fqdn" % node
            host = NS._int.client.read(key).value
            nodes[host] = []

        hosts = NS._int.client.read(
            '/clusters/%s/Bricks/free/' % NS.tendrl_context.integration_id
        )
        for host in hosts.leaves:
            host = host.key.split("/")[-1]
            bricks = NS._int.client.read(
                '/clusters/%s/Bricks/free/%s' % (
                    NS.tendrl_context.integration_id,
                    host
                )
            )
            for brick in bricks.leaves:
                brick = brick.key.split("/")[-1]
                if host in nodes:
                    if len(nodes[host]) < brick_count:
                        nodes[host].append(brick)

        # Form a brick list such that when you fill sub volumes with
        # bricks from this list, it should honour the failure domains

        brick_list = []
        total_bricks = len(nodes) * brick_count
        for iterator in range(total_bricks):
            brick_list.append("")

        counter = 0
        node_count = len(nodes)
        for key, value in nodes.iteritems():
            if len(value) < brick_count:
                message = "Host %s has %s bricks which is less than" + \
                          " bricks per host %s" % (
                              key,
                              len(value),
                              brick_count
                          )
                job = Job(job_id=self.parameters["job_id"]).load()
                res = {"message": message, "result": [[]], "optimal": False}
                job.output["GenerateBrickMapping"] = json.dumps(res)
                job.save()
                return False

            for i in range(brick_count):
                brick_list[node_count * i + counter] = value[i]
            counter += 1

        # Check if total number of bricks available is less than the
        # sub volume size. If its less, then return accordingly

        if len(brick_list) < subvol_size:
            message = "Total bricks available %s less than subvol_size %s" % (
                len(brick_list), subvol_size
            )
            job = Job(job_id=self.parameters["job_id"]).load()
            res = {"message": message, "result": [[]], "optimal": False}
            job.output["GenerateBrickMapping"] = json.dumps(res)
            job.save()
            return False

        # Fill the result list with bricks from the brick_list,
        # try to fill untill you exhaust the brick list and
        # also the number of sub volumes is maximum for the
        # available list

        result = []
        lower_bound = 0
        upper_bound = subvol_size
        while True:
            if upper_bound > len(brick_list):
                break
            subvol = brick_list[lower_bound:upper_bound]
            result.append(subvol)
            lower_bound = upper_bound
            upper_bound += subvol_size

        # check if the mapping provided is optimal as per expected
        # failure domain or not

        optimal = True
        if node_count < subvol_size:
            optimal = False

        # Write the result back to the job

        job = Job(job_id=self.parameters["job_id"]).load()
        res = {"message": message, "result": result, "optimal": optimal}
        job.output["GenerateBrickMapping"] = json.dumps(res)
        job.save()

        return True
