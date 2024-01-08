import datetime
import json
import logging
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Optional, List

import pandas as pd
from faas.context import NodeService, FunctionDeploymentService, InMemoryNodeService, InMemoryDeploymentService
from faas.system import NodeState, Function, FunctionImage, FunctionContainer, FunctionDeployment, FunctionNode
from galileodb.factory import create_mysql_from_env, create_influxdb_from_env
from galileodb.influx.db import InfluxExperimentDatabase
from galileodb.sql.adapter import ExperimentSQLDatabase
from galileofaas.constants import zone_label, function_label, pod_type_label
from galileofaas.context.platform.replica.model import parse_function_replica, KubernetesFunctionReplica, Pod
from galileofaas.system.core import KubernetesFunctionNode, KubernetesFunctionDeployment, \
    KubernetesResourceConfiguration
from galileofaas.util.storage import parse_size_string_to_bytes
from skippy.core.model import ResourceRequirements
from faas.system.core import FunctionReplicaState
from galileojp import env
from galileojp.frames import MixedExperimentFrameGateway

logger = logging.getLogger()

# Defaults taken from:
# https://github.com/kubernetes/kubernetes/blob/4c659c5342797c9a1f2859f42b2077859c4ba621/pkg/scheduler/util/pod_resources.go#L25
default_milli_cpu_request = '100m'  # 0.1 core
default_mem_request = f'{200}Mi'  # 200 MB


def parse_cpu_millis(cpu: str) -> int:
    cpu = int(cpu.replace('m', '').replace('"', ''))
    if cpu <= 10:
        # in case resource request is 1 (== 1 core) == 1000m
        return cpu * 1000
    return cpu


def get_zone_from_client(client: str) -> str:
    galileo_worker_zone_pattern = "zone-.{1}"
    return re.search(galileo_worker_zone_pattern, client).group(0)


class K3SGateway(MixedExperimentFrameGateway):

    def __init__(self, inflxudb: InfluxExperimentDatabase, sqldb: ExperimentSQLDatabase,
                 deployment_pattern='-deployment'):
        super().__init__(inflxudb, sqldb)
        self.deployment_pattern = deployment_pattern

    def build_node_service(self, exp_id) -> NodeService[KubernetesFunctionNode]:
        nodes = list(self.get_nodes_by_name(exp_id).values())
        zones = set()
        for node in nodes:
            if node.cluster is not None:
                zones.add(node.cluster)
        zones = list(zones)
        return InMemoryNodeService[KubernetesFunctionNode](zones, nodes)

    def export(self, exp_id: str, folder_path: str):
        exp_folder = f'{folder_path}/{exp_id}'
        exp: pd.DataFrame = self.get_experiment(exp_id)
        if len(exp) == 0:
            logger.info(f'No experiment found with ID {exp_id} - aborting export.')
            return False

        try:
            Path(exp_folder).mkdir(parents=True, exist_ok=False)
        except Exception as e:
            logger.error('Error, abort export', e)
            return

        logger.info(f'Saving experiment {exp_id} under {exp_folder}')
        use_index = True

        # save experiment from sql db including metadata
        exp_csv_file = f'{exp_folder}/experiment.csv'
        exp.to_csv(exp_csv_file, index=use_index)

        # save telemetry
        telemetry_df = self.telemetry(exp_id)
        telemetry_csv_file = f'{exp_folder}/telemetry.csv'
        logger.info(f'Save {len(telemetry_df)} telemetry in {telemetry_csv_file}')
        telemetry_df.to_csv(telemetry_csv_file, index=use_index)

        # save traces
        traces_df = self.traces(exp_id)
        traces_csv_file = f'{exp_folder}/traces.csv'
        logger.info(f'Save {len(traces_df)} traces in {traces_csv_file}')
        traces_df.to_csv(traces_csv_file, index=use_index)

        # save raw traces
        raw_traces_df = super().traces(exp_id)
        raw_traces_csv_file = f'{exp_folder}/raw_traces.csv'
        logger.info(f'Save {len(raw_traces_df)} raw traces in {raw_traces_csv_file}')
        raw_traces_df.to_csv(raw_traces_csv_file, index=use_index)

        # save events
        events_df = self.events(exp_id)
        events_csv_file = f'{exp_folder}/events.csv'
        logger.info(f'Save {len(events_df)} events in {events_csv_file}')
        events_df.to_csv(events_csv_file, index=use_index)

        # save nodeinfos
        node_info_df = self.nodeinfo(exp_id)
        nodeinfo_csv_file = f'{exp_folder}/nodeinfo.csv'
        logger.info(f'Save {len(node_info_df)} nodeinfos in {nodeinfo_csv_file}')
        node_info_df.to_csv(nodeinfo_csv_file, index=use_index)

        logger.info(f'Successfully saved experiment with ID {exp_id}')

    def parse_container_request(self, container_requests):
        for k, v in container_requests.items():
            container_requests[k] = v.replace('"', '')

        memory_request = container_requests.get('memory', default_mem_request)
        parsed_memory_request = parse_size_string_to_bytes(memory_request)
        cpu_request = container_requests.get('cpu', default_milli_cpu_request)
        parsed_cpu_request = parse_cpu_millis(cpu_request)

        container_requests['cpu'] = cpu_request
        container_requests['memory'] = memory_request

        parsed_request = {
            'cpu': parsed_cpu_request,
            'memory': parsed_memory_request
        }
        return parsed_request

    def build_deployment_service(self, exp_id) -> FunctionDeploymentService:
        query_result = self.get_raw_replicas(exp_id)
        deployments = {}
        for _, row in query_result.iterrows():
            replica: Pod = Pod.from_json(row['_value'])
            name = replica.name

            index = name.find(self.deployment_pattern)
            if index == -1:
                continue

            function_name = name[:index]
            if deployments.get(function_name, None) is not None:
                continue

            container = list(replica.containers.values())[0]
            image = FunctionImage(container.image)
            namespace = replica.namespace
            original_name = f"{function_name}{self.deployment_pattern}"

            fn = Function(
                name=function_name,
                fn_images=[image],
                labels=replica.labels
            )

            fn_containers = [
                FunctionContainer(
                    image,
                    KubernetesResourceConfiguration(requests=ResourceRequirements(
                        self.parse_container_request(container.resource_requests))
                    ),
                    labels=replica.labels
                )
            ]

            fn_deployment = FunctionDeployment(
                fn=fn,
                fn_containers=fn_containers,
                scaling_configuration=None,
                deployment_ranking=None
            )

            deployments[original_name] = (KubernetesFunctionDeployment(
                original_name=original_name,
                namespace=namespace,
                deployment=fn_deployment
            ))

        return InMemoryDeploymentService[KubernetesFunctionDeployment](list(deployments.values()))

    def exps_by_example(self, example: Dict, experiments: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for idx, exp in experiments.iterrows():
            j = json.loads(exp['metadata'])
            include = self.item_equal(example, j)
            if include:
                rows.append(exp)
        return pd.DataFrame(rows, columns=experiments.columns)

    def item_equal(self, example, item):
        if type(item) is list:
            return set(example) == set(item)
        elif type(item) is dict:
            if len(item) == 0:
                return False
            for k, v in example.items():
                j = item.get(k, None)
                if j is not None:
                    if self.item_equal(v, j) is False:
                        return False
        elif item != example:
            return False
        return True

    def get_exp_params(self, exp_id: str) -> Dict:
        exps = self.experiments()
        exps = exps[exps['EXP_ID'] == exp_id]
        if len(exps) == 0:
            raise ValueError(f'No experiment found with id: {exp_id}')

        for _, exp in exps.iterrows():
            j = json.loads(exp['metadata'])
            return j

    def get_nodes_by_name(self, exp_id) -> Dict[str, KubernetesFunctionNode]:
        nodeinfos = self.nodeinfo(exp_id)
        data = {}

        def get_attribute(node, name):
            nodeinfo = nodeinfos[nodeinfos['NODE'] == node]
            nodeinfo = nodeinfo[nodeinfo['INFO_KEY'] == name]
            return nodeinfo['INFO_VALUE'].iloc[0]

        for node in nodeinfos['NODE'].unique():
            name = node
            arch = get_attribute(node, 'arch')
            cpus = get_attribute(node, 'cpus')
            ram = get_attribute(node, 'ram')
            boot = get_attribute(node, 'boot')
            disk = get_attribute(node, 'disk')
            net = get_attribute(node, 'net')
            netspeed = get_attribute(node, 'netspeed')
            labels = json.loads(get_attribute(node, 'labels'))
            zone = labels.get(zone_label, None)
            cluster = self.convert_cluster(zone)
            allocatable = json.loads(get_attribute(node, 'allocatable'))
            cpu = allocatable.get('cpu', None)
            if cpu is not None:
                if cpu == '1':
                    allocatable['cpu'] = '1000m'

            fn_node = FunctionNode(
                name=name,
                arch=arch,
                cpus=cpus,
                ram=ram,
                netspeed=netspeed,
                labels=labels,
                allocatable=allocatable,
                cluster=cluster,
                state=NodeState.READY
            )
            data[name] = KubernetesFunctionNode(
                fn_node=fn_node,
                boot=boot,
                disk=disk,
                net=net,
            )

        return data

    def get_replicas(self, exp_id, state: Optional[str] = "running", deployment_service=None,
                     node_service=None) -> pd.DataFrame:
        query_result = self.get_raw_replicas(exp_id, state)
        data = defaultdict(list)
        exp = self.get_experiment(exp_id)
        start_trace = exp.START.iloc[0]
        if deployment_service is None:
            deployment_service = self.build_deployment_service(exp_id)

        if node_service is None:
            node_service = self.build_node_service(exp_id)

        for _, row in query_result.iterrows():
            value = row['_value']
            state = row['state']
            replica = parse_function_replica(value, deployment_service, node_service)
            if replica is None:
                continue
            if replica.state == FunctionReplicaState.PENDING:
                continue

            data['ts'].append(float(row['ts']) - start_trace)
            data['podUid'].append(replica.replica_id)
            data['replica_id'].append(replica.replica_id)
            data['name'].append(replica.pod_name)
            data['hostIP'].append(replica.host_ip)
            data['podIP'].append(replica.ip)
            data['startTime'].append(replica.start_time)
            data['image'].append(replica.image if replica.container is not None else 'NA')
            data['container_id'].append(replica.container_id)
            data['namespace'].append(replica.namespace)
            data['nodeName'].append(replica.node.name)
            data['cpu_request'].append(
                replica.container.get_resource_requirements().get('cpu') if replica.container is not None else 'NA')
            data['mem_request'].append(
                replica.container.get_resource_requirements().get('memory') if replica.container is not None else 'NA')
            data['state'].append(state)
            node_labels = replica.node.labels
            zone = replica.labels.get(zone_label, node_labels.get(zone_label, 'N/A'))  if replica.container is not None else 'N/A'
            data['zone'].append(zone)
            cluster = self.convert_cluster(zone)
            data['cluster'].append(cluster)
            data['fn'].append(replica.labels.get(function_label, 'N/A') if replica.container is not None else 'N/A')
            data['pod_type'].append(replica.labels.get(pod_type_label, 'N/A') if replica.container is not None else 'N/A')
            data['replica_type'].append(replica.labels.get(pod_type_label, 'N/A') if replica.container is not None else 'N/A')
        try:
            return pd.DataFrame(data=data).sort_values(by='ts')
        except KeyError:
            print('Probably no scale down happened!')
            return pd.DataFrame(data=data)

    def get_replica_schedule_statistics(self, exp_id, fn: str, clusters: List[str] = None, per_second: bool = True):
        if clusters is None:
            clusters = ['Cloud', 'IoT-Box', 'Cloudlet']
        sc_df_running = self.get_replicas(exp_id, state='running')
        sc_df_delete = self.get_replicas(exp_id, state='delete')
        sc_df = pd.concat([sc_df_running, sc_df_delete])
        sc_df = sc_df[sc_df['image'].str.contains(fn)].sort_values(by='ts')

        def rindex(mylist, myvalue):
            return len(mylist) - mylist[::-1].index(myvalue) - 1

        data = defaultdict(list)

        cluster_total = {}

        exp = self.get_experiment(exp_id)
        end = exp.END.iloc[0]
        start = exp.START.iloc[0]
        end_scaled = end - start

        for cluster in clusters:
            cluster_total[cluster] = 0
            data['ts'].append(0)
            data['total'].append(0)
            data['cluster'].append(cluster)
            data['cluster_total'].append(0)

        for sc_df_idx, row in sc_df.iterrows():
            if row['state'] == 'pending' or row['state'] == 'create' or row['state'] == 'shutdown':
                continue

            add = row['state'] == 'running'
            ts = row['ts']
            last_ts = data['ts'][-1]
            diff = ts - last_ts
            cluster = row['cluster']

            if per_second:
                for i in range(0, math.floor(diff), 1):
                    for other_cluster in ['Cloud', 'IoT-Box', 'Cloudlet']:
                        if other_cluster != cluster:
                            data['ts'].append(last_ts + i)
                            data['total'].append(data['total'][-1])
                            idx = rindex(data['cluster'], other_cluster)
                            data['cluster'].append(other_cluster)
                            data['cluster_total'].append(data['cluster_total'][idx])

            data['cluster'].append(cluster)
            data['ts'].append(ts)
            total = data['total'][-1]
            if add:
                data['total'].append(total + 1)
                cluster_total[cluster] += 1

            else:
                data['total'].append(total - 1)
                cluster_total[cluster] -= 1

            data['cluster_total'].append(cluster_total[cluster])

        for cluster in ['Cloud', 'IoT-Box', 'Cloudlet']:
            data['ts'].append(end_scaled)
            data['total'].append(data['total'][-1])
            idx = rindex(data['cluster'], cluster)
            data['cluster'].append(cluster)
            data['cluster_total'].append(data['cluster_total'][idx])

        df = pd.DataFrame(data=data)
        df['exp_id'] = exp_id
        df['ts'] = df['ts'].apply(lambda x: datetime.datetime.utcfromtimestamp(x))
        return df

    def get_raw_replicas(self, exp_id, state: Optional[str] = "running"):
        stop = datetime.datetime.utcnow() + datetime.timedelta(days=1)
        stop = stop.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        if state is not None:
            states = [state]
        else:
            states = ['create', 'pending', 'running', 'shutdown', 'delete']
        dfs = []
        for _state in states:
            query = f"""
            from(bucket: "{exp_id}")
              |> range(start: 1970-01-01, stop: {stop})
              |> filter(fn: (r) => r["_measurement"] == "events")
              |> filter(fn: (r) => r["name"] == "pod/{_state}")
            """
            query_result = self.raw_influxdb_query(query)
            query_result['state'] = _state
            dfs.append(query_result)
        return pd.concat(dfs)

    def get_replica_by_ip(self, exp_id: str) -> Dict[str, KubernetesFunctionReplica]:
        query_result = self.get_raw_replicas(exp_id)
        data = {}
        deployment_service = self.build_deployment_service(exp_id)
        node_service = self.build_node_service(exp_id)
        for value in query_result['_value']:
            replica = parse_function_replica(value, deployment_service, node_service)
            if replica is not None:
                data[replica.ip] = replica
        return data

    def get_replica_by_container_id(self, exp_id: str) -> Dict[str, KubernetesFunctionReplica]:
        query_result = self.get_raw_replicas(exp_id)
        data = {}
        deployment_service = self.build_deployment_service(exp_id)
        node_service = self.build_node_service(exp_id)
        for value in query_result['_value']:
            replica = parse_function_replica(value, deployment_service, node_service)

            if replica is None:
                continue
            if replica is not None:
                data[replica.container_id] = replica
        return data

    def preprocessed_traces(self, exp_id):
        traces = self.traces(exp_id)
        replicas = self.get_replica_by_ip(exp_id)
        nodes = self.get_nodes_by_name(exp_id)
        for index, row in traces.iterrows():
            split = row['final_server'].split(',')
            final_url = split[-1].split(':')
            final_server_ip = final_url[0].replace(' ', '')
            replica = replicas.get(final_server_ip)
            status = row['status']
            if replica is None or status != 200:
                traces.loc[index, 'final_ip'] = 'N/A'
                traces.loc[index, 'final_port'] = 'N/A'
                traces.loc[index, 'final_server'] = 'N/A'
                dest_zone = 'N/A'
                traces.loc[index, 'pod_name'] = 'N/A'
                traces.loc[index, 'pod_image'] = 'N/A'
                traces.loc[index, 'function'] = 'N/A'
            else:
                traces.loc[index, 'final_ip'] = final_server_ip
                traces.loc[index, 'final_port'] = final_url[1]
                traces.loc[index, 'final_server'] = replica.node.name
                traces.loc[index, 'pod_name'] = replica.pod_name
                traces.loc[index, 'pod_image'] = replica.image
                traces.loc[index, 'function'] = replica.labels.get(function_label, 'N/A')
                dest_zone = replica.labels.get(zone_label, None)
                if dest_zone is None:
                    dest_zone = nodes[replica.node.name].cluster
            traces.loc[index, 'dest_zone'] = dest_zone
            cluster = self.convert_cluster(dest_zone)
            traces.loc[index, 'dest_cluster'] = cluster

        return traces

    def traces(self, *exp_ids) -> pd.DataFrame:
        """
        This method intentionally preserves the structure of the original message.
        For example, 'final_server' will contain all hops in the chain.
        Call preprocessed_traces to retrieve a DataFrame that extracts all details.
        """
        traces = super().traces(*exp_ids)
        print(f'original traces size: {len(traces)}')
        traces['status'] = traces['status'].astype(int)
        traces = traces[traces['status'] != -1]
        print(f'after -1 filter traces size: {len(traces)}')
        traces['sent'] = traces['sent'].astype(float)
        traces['done'] = traces['done'].astype(float)
        traces['created'] = traces['created'].astype(float)
        exp = self.get_experiment(exp_ids[0])
        start_trace = exp.START.iloc[0]
        traces['sent'] -= start_trace
        traces['created'] -= start_trace
        traces['done'] -= start_trace
        traces['rtt'] = traces['done'] - traces['sent']
        for index, row in traces.iterrows():
            headers = json.loads(row['headers'])

            traces.loc[index, 'final_server'] = headers['X-Final-Host']
            origin_zone = get_zone_from_client(row['client'])
            traces.loc[index, 'origin_zone'] = origin_zone
            origin_cluster = self.convert_cluster(origin_zone)
            traces.loc[index, 'origin_cluster'] = origin_cluster
            gateways = headers['X-Forwarded-For'].split(',')
            last_gateway = gateways[len(gateways) - 1].replace(' ', '')
            lower_case_dict = {}
            for key, value in headers.items():
                lower_case_dict[key.lower()] = value
            last_forward = lower_case_dict[f'x-forwarded-host-{last_gateway}']
            last_forward = last_forward.split(',')[-1].replace(' ', '')
            last_forward_at = float(last_forward) - start_trace
            traces.loc[index, 'latency_gateway'] = ((last_forward_at - row['sent']) * 2) * 1000
            if int(row['status']) == 200:
                start = float(headers['X-Start']) - start_trace
                end = float(headers['X-End']) - start_trace
                traces.loc[index, 'start'] = start
                traces.loc[index, 'end'] = end
                traces.loc[index, 'exec'] = end - start
                traces.loc[index, 'latency'] = ((start - row['sent']) + (row['done'] - end)) * 1000
            else:
                traces.loc[index, 'latency'] = traces.loc[index, 'latency_gateway']

        traces.index = self.normalize_index(traces.index, exp_ids[0])
        return traces

    def get_experiment(self, exp_id):
        exps = self.experiments()
        return exps[exps['EXP_ID'] == exp_id]

    def find_zone_for_client(self, client: str) -> Optional[str]:
        # specific to galileo
        return client[len('gateway-'):]

    def convert_cluster(self, zone: str) -> str:
        # specific to testbed
        if 'zone-a' == zone:
            return 'IoT-Box'
        elif 'zone-b' == zone:
            return 'Cloudlet'
        elif 'zone-c' == zone:
            return 'Cloud'
        else:
            return 'N/A'

    def get_weight_updates(self, exp_id):
        """
        Looks up the weight updates the go load balancer has received
        :param exp_id:
        :return:
        """
        stop = datetime.datetime.utcnow() + datetime.timedelta(days=1)
        stop = stop.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        query = f"""
                from(bucket: "{exp_id}")
                  |> range(start: 1970-01-01, stop: {stop})
                  |> filter(fn: (r) => r["_measurement"] == "events")
                  |> filter(fn: (r) => r["name"] == "weight_update")
                """
        data = defaultdict(list)
        result = self.raw_influxdb_query(query)
        for value in result['_value']:
            obj = json.loads(value)
            ts = obj['ts']
            fn = obj['fn']
            zone = obj['zone']
            cluster = self.convert_cluster(zone)
            for i in range(len(obj['weights']['weights'])):
                weight = obj['weights']['weights'][i]
                ip = obj['weights']['ips'][i]
                data['ts'].append(ts)
                data['fn'].append(fn)
                data['zone'].append(zone)
                data['cluster'].append(cluster)
                data['ip'].append(ip)
                data['weight'].append(weight)
        df = pd.DataFrame(data=data)
        df.index = pd.DatetimeIndex(pd.to_datetime(df['ts'].astype(float), unit='s'))
        df.index = self.normalize_index(df.index, exp_id)
        return df

    @staticmethod
    def from_env() -> 'K3SGateway':
        env.load()
        influxdb = create_influxdb_from_env()
        influxdb.open()
        sqldb = create_mysql_from_env()
        sqldb.open()
        exp_db = ExperimentSQLDatabase(sqldb)
        return K3SGateway(influxdb, exp_db)

    def _get_influxdb_df_metric_subsystem(self, metric: str, subsystem: str,
                                          exp_id: str) -> Optional[pd.DataFrame]:

        stop = datetime.datetime.utcnow() + datetime.timedelta(days=1)
        stop = stop.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        query = f"""
                 from(bucket: "{exp_id}")
                     |> range(start: 1970-01-01, stop: {stop})
                     |> filter(fn: (r) => r["_measurement"] == "telemetry")
                     |> filter(fn: (r) => r["metric"] == "{metric}")
                     |> filter(fn: (r) => r["subsystem"] == "{subsystem}")
                """
        df = self.raw_influxdb_query(query)
        if len(df) == 0:
            return None
        df.rename(columns={'_value': 'value'}, inplace=True)
        df.index = pd.DatetimeIndex(pd.to_datetime(df['ts'].astype(float), unit='s'))
        # scale such that every index starts at 1970-01-01
        df.index = self.normalize_index(df.index, exp_id)
        return df

    def get_cpu_containers_by_name(self, exp_id: str, container_pattern: str, absolute: bool = True) -> pd.DataFrame:
        exp_replicas = self.get_replicas(exp_id)
        containers_df = exp_replicas[exp_replicas['name'].str.contains(container_pattern)]
        dfs = []
        for container in containers_df.itertuples():
            df = self.get_cpu_container(exp_id, container.container_id, absolute=absolute)

            if df is not None:
                df['zone'] = container.cluster
                cluster = self.convert_cluster(container.cluster)
                df['cluster'] = cluster
                df['name'] = container.name
                df['id'] = container.container_id
                df['image'] = container.image
                df['cpu_request'] = container.cpu_request
                df['mem_request_mb'] = container.mem_request / 1_000_000
                dfs.append(df)

        return pd.concat(dfs)

    def get_replicas_with_shutdown(self, exp_id) -> pd.DataFrame:
        exp = self.get_experiment(exp_id)
        end_ts = exp.iloc[0]['END'] - exp.iloc[0]['START']
        running_replicas = self.get_replicas(exp_id, 'running')
        shutdown_replicas = self.get_replicas(exp_id, 'shutdown')

        def apply(row):
            replica_uid = row['podUid']
            shutdown_replica = shutdown_replicas[shutdown_replicas['podUid'] == replica_uid]
            if len(shutdown_replica) == 0:
                return end_ts
            else:
                return shutdown_replica.iloc[0]['ts']

        running_replicas['shutdown_ts'] = running_replicas.apply(apply, axis=1)
        return running_replicas

    def get_cpu(self, exp_id, replicas: pd.DataFrame):
        dfs = []
        for _, row in replicas.iterrows():
            df = self.get_cpu_container(exp_id, row['container_id'])
            if df is not None:
                df.index = self.normalize_index(df.index, exp_id)
                exp = self.get_experiment(exp_id)
                df['ts'] = df['ts'].astype(float)
                df['ts'] -= exp.START.iloc[0]
                dfs.append(df)
        return pd.concat(dfs)

    def get_gateways_replicas(self, exp_id):
        replicas = self.get_replicas(exp_id)
        return replicas[replicas['pod_type'] == 'api-gateway']

    def get_running_replicas(self, exp_id, deployments: List[FunctionDeployment], now: float):
        replicas = self.get_replicas_with_shutdown(exp_id)

        replicas_by_function_name = {}
        for deployment in deployments:
            replicas_of_function = replicas[replicas['fn'] == deployment.fn.name]
            replicas_of_function = replicas_of_function[replicas_of_function['ts'] < now]
            replicas_of_function = replicas_of_function[replicas_of_function['shutdown_ts'] > now]
            replicas_by_function_name[deployment.fn.name] = replicas_of_function
        return replicas_by_function_name

    def get_cpu_container(self, exp_id: str, container_id: str, absolute: bool = True) -> Optional[
        pd.DataFrame]:
        df = self._get_influxdb_df_metric_subsystem('kubernetes_cgrp_cpu', container_id, exp_id)
        nodes = self.get_nodes_by_name(exp_id)
        raw_replicas = self.get_raw_replicas(exp_id)
        node = None
        for idx, row in raw_replicas.iterrows():
            obj = json.loads(row['_value'])
            for v in obj['containers'].values():
                parsed_container_id = v['id'].replace('containerd://', '')
                if container_id == parsed_container_id:
                    node = obj['nodeName']
                break
            if node is not None:
                break

        node = nodes.get(node, None)
        if node is None:
            return None
        cores = node.cpus
        if df is None:
            return None
        if len(df) >= 2:
            d = (float(df['ts'].iloc[1]) - float(df['ts'].iloc[0]))
            df['value_ms'] = df['value'] / 1e6
            df['milli_cores'] = (df['value_ms'].diff() / d)
            df['percentage'] = df['milli_cores'] / 10
            # at the end * 100 to get percentage between [0,100]
            df['percentage_relative'] = (df['milli_cores'] / (10 * (cores * 100))) * 100

        return df

    def get_blkio_rate_container(self, exp_id: str, container_id: str) -> Optional[pd.DataFrame]:
        """
        Calculate the blkio rate for the given container.
        The rate depends on the interval of the corresponding telemd instrument.
        Assuming that it's set to 1s, the resulting data rate is kbyte/s.
        The time component varies depending on the interval.
        """
        df = self._get_influxdb_df_metric_subsystem('kubernetes_cgrp_blkio', container_id, exp_id)
        if len(df) >= 2:
            d = (float(df['ts'].iloc[1]) - float(df['ts'].iloc[0]))
            # diff and map to kbyte
            df['blkio_rate'] = (df['value'].diff() / d) / 1_000
        exp = self.get_experiment(exp_id)
        df['ts'] = df['ts'].astype(float)
        df['ts'] -= exp.START.iloc[0]
        return df

    def get_network_rate_container(self, exp_id: str, container_id: str) -> Optional[pd.DataFrame]:
        """
        Calculate the  network rate for the given container. This combines read + writes.
        The rate depends on the interval of the corresponding telemd instrument.
        Assuming that it's set to 1s, the resulting data rate is kbyte/s.
        The time component varies depending on the interval.
        """
        df = self._get_influxdb_df_metric_subsystem('kubernetes_cgrp_net', container_id, exp_id)
        if df is None:
            return None
        if len(df) >= 2:
            d = (float(df['ts'].iloc[1]) - float(df['ts'].iloc[0]))
            # diff and map to kbyte
            df['net_rate'] = (df['value'].diff() / d) / 1_000

        exp = self.get_experiment(exp_id)
        df['ts'] = df['ts'].astype(float)
        df['ts'] -= exp.START.iloc[0]
        return df

    def preprocessed_telemetry(self, exp_id):
        """
        Fetches all telemetry and does the following things:
        * Normalizes the date time index (i.e., maps the start of the beginning to 0)
        * Converts kubernetes_cgrp_memory and docker_cgrp_memory to Megabyte from Byte
        :param exp_id:
        :return:
        """
        telemetry = self.telemetry(exp_id)
        telemetry.index = self.normalize_index(telemetry.index, exp_id)
        telemetry['ts'] = telemetry.index.to_series().apply(lambda x: x.timestamp())

        mask = telemetry['metric'] == 'kubernetes_cgrp_memory'
        memory = telemetry[mask]
        telemetry.loc[mask, 'value'] = (memory['value'] / 1e6)

        mask = telemetry['metric'] == 'docker_cgrp_memory'
        memory = telemetry[mask]
        telemetry.loc[mask, 'value'] = (memory['value'] / 1e6)

        return telemetry

    def get_replicas_by_deployments(self, exp_id, deployments: List[KubernetesFunctionDeployment]) -> Dict[
        str, pd.DataFrame]:
        replicas = self.get_replicas(exp_id)
        data = {}
        for deployment in deployments:
            replicas_of_function = replicas[replicas['fn'] == deployment.fn.name]
            data[deployment.fn.name] = replicas_of_function
        return data

    def normalize_index(self, idx, exp_id):
        exp = self.get_experiment(exp_id)
        start = exp.START.iloc[0]
        a = datetime.datetime.utcfromtimestamp(start)
        b = datetime.datetime.fromisoformat('1970-01-01')
        c = a - b
        return idx - c

def main():
    gw = K3SGateway.from_env()
    traces = gw.traces('202401080108-ac98')
    pass

if __name__ == '__main__':
    main()
