import json
import logging
import math
from collections import defaultdict
from datetime import datetime
from typing import Optional, Union, Dict, List, Tuple

import pandas as pd
from faas.system import FunctionNode, FunctionReplica, FunctionDeployment, Function, FunctionImage, FunctionContainer, \
    ResourceConfiguration, FunctionReplicaState, ScalingConfiguration
from faas.util.constant import pod_type_label, api_gateway_type_label
from galileojp.frames import ExperimentFrameGateway

logger = logging.getLogger()


class InMemoryFaasSimGateway(ExperimentFrameGateway):

    def __init__(self, exp_dfs: Dict[str, Dict[str, pd.DataFrame]]):
        self.all_exp_dfs = exp_dfs

    def experiments(self) -> pd.DataFrame:
        dfs = []
        for exp_id, exp_dfs in self.all_exp_dfs.items():
            dfs.append(exp_dfs['experiment_df'])
        return pd.concat(dfs)

    def metadata(self, exp_id: str = None) -> Optional[Union[pd.DataFrame, Dict]]:
        return None

    def nodeinfo(self, *exp_ids):
        dfs = []
        for exp_id in exp_ids:
            df = self.all_exp_dfs[exp_id]['nodes']
            df['exp_id'] = exp_id
            dfs.append(df)
        return pd.concat(dfs)

    def events(self, *exp_ids):
        return None

    def _get_exp_folder(self, exp_id):
        return f'{self.root_dir}/{exp_id}'

    def telemetry(self, *exp_ids) -> pd.DataFrame:
        dfs = []
        for exp_id in exp_ids:
            node_utilization_df = self.all_exp_dfs[exp_id]['node_utilization_df']
            replica_utilization_df = self.all_exp_dfs[exp_id]['function_utilization_df']
            node_utilization_df['exp_id'] = exp_id
            replica_utilization_df['exp_id'] = exp_id
            dfs.append(node_utilization_df)
            dfs.append(replica_utilization_df)
        return pd.concat(dfs)

    def get_nodes_by_name(self, exp_id) -> Dict[str, FunctionNode]:
        nodeinfos = self.nodeinfo(exp_id)
        data = {}

        for _, nodeinfo in nodeinfos.iterrows():
            name = nodeinfo['name']
            arch = nodeinfo['arch']
            cpus = nodeinfo['cpus']
            ram = nodeinfo['ram']
            netspeed = nodeinfo['netspeed']
            if type(nodeinfo['labels']) is str:
                labels = json.loads(nodeinfo['labels'].replace("'", '"'))
            else:
                labels = nodeinfo['labels']
            allocatable = nodeinfo['allocatable']
            cluster = nodeinfo['cluster']
            state = nodeinfo['state']
            node = FunctionNode(
                name=name,
                arch=arch,
                cpus=cpus,
                ram=ram,
                netspeed=netspeed,
                labels=labels,
                allocatable=allocatable,
                cluster=cluster,
                state=state
            )
            data[name] = node
        return data

    def map_sim_replica_lifecycle_to_kubernetes(self, sim_state: str) -> str:
        sim_state_map = {
            'deploy': 'pending',
            'startup': 'pending',
            'setup': 'pending',
            'finish': 'running',
            'teardown': 'shutdown',
            'delete': 'delete'
        }
        return sim_state_map[sim_state]

    def map_kubernetes_lifecyle_to_sim(self, kubernetes_state: str) -> Union[str, List[str]]:
        kubernetes_state_map = {
            'pending': ['deploy', 'startup', 'setup'],
            'running': 'finish',
            'shutdown': 'teardown',
            'delete': 'delete'
        }
        return kubernetes_state_map[kubernetes_state]

    def get_raw_replicas(self, exp_id: str, state: Optional[str] = "running"):
        if state is not None:
            states = [state]
        else:
            states = ['create', 'pending', 'running', 'shutdown', 'delete']

        replica_deployment_df = self._get_replica_deployment_df(exp_id)
        schedule_df = self._get_schedule_df(exp_id)
        schedule_df = schedule_df[schedule_df['value'] == 'queue']
        schedule_df['value'] = 'pending'
        running_df = replica_deployment_df[replica_deployment_df['value'] == 'finish']
        running_df['value'] = 'running'
        teardown_df = replica_deployment_df[replica_deployment_df['value'] == 'teardown']
        teardown_df['value'] = 'shutdown'
        delete_df = replica_deployment_df[replica_deployment_df['value'] == 'delete']

        all_states = pd.concat([schedule_df, running_df, teardown_df, delete_df])
        filtered_states = all_states[all_states['value'].isin(states)]
        return filtered_states

    def _get_replica_deployment_df(self, exp_id: str) -> pd.DataFrame:
        return self.all_exp_dfs[exp_id]['replica_deployment_df']

    def _get_schedule_df(self, exp_id: str) -> pd.DataFrame:
        return self.all_exp_dfs[exp_id]['schedule_df']

    def _convert_time_to_datetime(self, time: str):
        return datetime.strptime(time, '%Y-%m-%d %H:%M:%S.%f')

    def get_raw_deployments(self, exp_id: str) -> pd.DataFrame:
        deployments = self.all_exp_dfs[exp_id]['function_deployments_df']
        return deployments

    def get_functions(self, exp_id: str) -> Dict[str, Function]:
        functions = self.all_exp_dfs[exp_id]['functions_df']
        data = {}
        function_images = self.get_function_images(exp_id)

        def find_images_for_function(name: str) -> List[FunctionImage]:
            fn_images = []
            for fn, image in function_images.keys():
                if fn == name:
                    fn_images.append(function_images[(fn, image)])
            return fn_images

        for _, row in functions.iterrows():
            name = row['name']
            if type(row['labels']) is str:
                labels = json.loads(row['labels'].replace("'", '"'))
            else:
                labels = row['labels']
            data[name] = Function(
                name=name,
                fn_images=find_images_for_function(name),
                labels=labels
            )
        return data

    def get_deployments(self, exp_id: str) -> Dict[str, FunctionDeployment]:
        data = {}
        functions = self.get_functions(exp_id)
        fn_containers = self.get_function_containers(exp_id)

        def find_containers_for_function(name: str) -> List[FunctionContainer]:
            containers = []
            for fn_name, image in fn_containers.keys():
                if fn_name == name:
                    containers.append(fn_containers[(fn_name, image)])
            return containers

        raw_deployments = self.get_raw_deployments(exp_id)
        for _, row in raw_deployments.iterrows():
            fn = functions[row['name']]
            scale_min = row['scale_min']
            scale_max = row['scale_max']
            scale_factor = row['scale_factor']
            scale_zero = row['scale_zero']
            containers = find_containers_for_function(fn.name)
            scaling_configuration = ScalingConfiguration(scale_min, scale_max, scale_factor, scale_zero)
            deployment = FunctionDeployment(
                fn=fn,
                fn_containers=containers,
                scaling_configuration=scaling_configuration,
                deployment_ranking=None
            )
            data[fn.name] = deployment
        return data

    def get_conceived_replicas_by_replica_id(self, exp_id: str) -> Dict[str, FunctionReplica]:
        replicas = self.all_exp_dfs[exp_id]['function_replicas_df']
        deployments = self.get_deployments(exp_id)
        containers_by_name = self.get_function_containers(exp_id)
        data = {}
        for _, row in replicas.iterrows():
            replica_id = row['replica_id']
            fn_name = row['name']
            if fn_name != 'pythonpi':
                continue
            fn_image = row['image']
            deployment = deployments[fn_name]
            container = containers_by_name[(fn_name, fn_image)]
            labels = {}
            if row.get('labels') is not None:
                if type(row['labels']) is str:
                    labels = json.loads(row['labels'].replace("'", '"'))
                else:
                    labels = row['labels']
            data[replica_id] = FunctionReplica(
                replica_id=replica_id,
                labels=labels,
                function=deployment,
                container=container,
                node=None,
                state=FunctionReplicaState.CONCEIVED
            )
        return data

    def get_function_images(self, exp_id: str) -> Dict[Tuple[str, str], FunctionImage]:
        fn_images = self.all_exp_dfs[exp_id]['function_images_df']
        data = {}
        for _, row in fn_images.iterrows():
            fn_image = FunctionImage(row['image'])
            data[(row['function_name'], row['image'])] = fn_image
        return data

    def get_function_containers(self, exp_id: str) -> Optional[Dict[Tuple[str, str], FunctionContainer]]:
        fn_containers = self.all_exp_dfs[exp_id]['function_containers_df']
        fn_images = self.get_function_images(exp_id)
        data = {}
        for _, row in fn_containers.iterrows():
            fn_name = row['name']
            if type(row['resource_requirements']) is str:
                requirements = json.loads(row['resource_requirements'].replace("'", '"'))
            else:
                requirements = row['resource_requirements']
            if type(row['labels']) is str:
                labels = json.loads(row['labels'].replace("'", '"'))
            else:
                labels = row['labels']
            image_name = row['image']
            fn_container = FunctionContainer(
                fn_image=fn_images[(fn_name, image_name)],
                resource_config=ResourceConfiguration(requirements),
                labels=labels
            )
            data[(fn_name, image_name)] = fn_container
        return data

    def exps_by_example(self, example: Dict, experiments: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for idx, exp in experiments.iterrows():
            j = json.loads(exp['metadata'].replace("'", '"'))
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

    def get_replicas(self, exp_id, state: Optional[str] = "running"):
        raw_replicas = self.get_raw_replicas(exp_id, state).reset_index()
        data = defaultdict(list)
        nodes_by_name = self.get_nodes_by_name(exp_id)
        conceived_replicas = self.get_conceived_replicas_by_replica_id(exp_id)
        exp = self.get_experiment(exp_id)
        start_ts = exp.START.iloc[0].timestamp()
        for _, row in raw_replicas.iterrows():
            replica_id = row['replica_id']
            conceived_replica = conceived_replicas.get(replica_id)
            if conceived_replica is None:
                continue
            state = row['value']
            ts = row['time'].timestamp()
            data['ts'].append(ts - start_ts)
            data['replica_id'].append(replica_id)
            start_time = None
            data['state'].append(state.lower())
            if state == 'RUNNING' or state == 'SHUTDOWN' or state == 'DELETE':
                start_time = row['time']
            data['startTime'].append(start_time)
            data['image'].append(row['image'])
            node_name = row['node_name']
            if nodes_by_name.get(node_name) is None:
                pass
            else:
                data['nodeName'].append(node_name)
                node = nodes_by_name[node_name]
                cluster = node.cluster
                data['cluster'].append(cluster)
            data['fn'].append(row['function_name'])
            data['replica_type'].append(conceived_replica.labels.get(pod_type_label, 'N/A'))

        if len(data) == 0:
            return None
        else:
            return pd.DataFrame(data=data).sort_values(by='ts')

    def get_exp_params(self, exp_id: str) -> Dict:
        exps = self.experiments()
        exps = exps[exps['EXP_ID'] == exp_id]
        if len(exps) == 0:
            raise ValueError(f'No experiment found with id: {exp_id}')

        for _, exp in exps.iterrows():
            j = json.loads(exp['metadata'].replace("'", '"'))
            return j

    def get_experiment(self, exp_id: str):
        exps = self.experiments()
        return exps[exps['EXP_ID'] == exp_id]

    def get_clusters(self, exp_id: str) -> List[str]:
        nodes = self.get_nodes_by_name(exp_id)
        clusters = set()
        for node in nodes.values():
            if node.cluster is not None:
                clusters.add(node.cluster)
        return list(clusters)

    def get_replica_schedule_statistics(self, exp_id, fn: str, clusters: List[str] = None, per_second: bool = True):
        all_clusters = self.get_clusters(exp_id)
        if clusters is None:
            clusters = all_clusters
        dfs = []
        sc_df_running = self.get_replicas(exp_id, state='running')
        dfs.append(sc_df_running)
        if sc_df_running is None:
            raise ValueError('No running containers found')
        sc_df_delete = self.get_replicas(exp_id, state='shutdown')
        if sc_df_delete is not None:
            dfs.append(sc_df_delete)
        sc_df = pd.concat(dfs)
        sc_df = sc_df[sc_df['image'].str.contains(fn)].sort_values(by='ts')

        def rindex(mylist, myvalue):
            return len(mylist) - mylist[::-1].index(myvalue) - 1

        data = defaultdict(list)

        cluster_total = {}

        exp = self.get_experiment(exp_id)
        end = exp.END.iloc[0]
        start = exp.START.iloc[0]
        end_scaled = (end.timestamp() - start.timestamp())

        for cluster in clusters:
            cluster_total[cluster] = 0
            data['ts'].append(0)
            data['total'].append(0)
            data['cluster'].append(cluster)
            data['cluster_total'].append(0)

        for _, row in sc_df.iterrows():
            if row['state'] == 'pending' or row['state'] == 'shutdown':
                continue

            add = row['state'] == 'running'
            ts = row['ts']
            last_ts = data['ts'][-1]
            diff = ts - last_ts
            cluster = row['cluster']

            if per_second:
                for i in range(0, math.ceil(diff), 1):
                    for other_cluster in all_clusters:
                        # if other_cluster != cluster:
                        data['ts'].append(last_ts + i)
                        data['total'].append(data['total'][-1])
                        idx = rindex(data['cluster'], other_cluster)
                        data['cluster'].append(other_cluster)
                        data['cluster_total'].append(data['cluster_total'][idx])
                if math.floor(diff) == 0:
                    for other_cluster in all_clusters:
                        if other_cluster != cluster:
                            data['ts'].append(ts)
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

        for cluster in all_clusters:
            data['ts'].append(end_scaled)
            data['total'].append(data['total'][-1])
            idx = rindex(data['cluster'], cluster)
            data['cluster'].append(cluster)
            data['cluster_total'].append(data['cluster_total'][idx])

        df = pd.DataFrame(data=data)
        df['exp_id'] = exp_id
        # df['ts'] = df['ts'].apply(lambda x: datetime.utcfromtimestamp(x))
        df['ts'] = df['ts'].apply(lambda x: math.ceil(x))
        return df.groupby(['ts', 'cluster']).max().reset_index()

    def get_replicas_with_shutdown(self, exp_id) -> Optional[pd.DataFrame]:
        exp = self.get_experiment(exp_id)
        end_ts = exp.iloc[0]['END'] - exp.iloc[0]['START']
        running_replicas = self.get_replicas(exp_id, 'running')
        shutdown_replicas = self.get_replicas(exp_id, 'shutdown')

        def apply(row):
            replica_id = row['replica_id']
            if shutdown_replicas is None:
                return end_ts
            shutdown_replica = shutdown_replicas[shutdown_replicas['replica_id'] == replica_id]
            if len(shutdown_replica) == 0:
                return end_ts
            else:
                return shutdown_replica.iloc[0]['ts']

        running_replicas['shutdown_ts'] = running_replicas.apply(apply, axis=1)
        return running_replicas

    def get_gateways_replicas(self, exp_id):
        replicas = self.get_replicas(exp_id)
        return replicas[replicas['replica_type'] == api_gateway_type_label]

    def get_running_replicas(self, exp_id, deployments: List[FunctionDeployment], now: float):
        replicas = self.get_replicas_with_shutdown(exp_id)

        replicas_by_function_name = {}
        for deployment in deployments:
            replicas_of_function = replicas[replicas['fn'] == deployment.fn.name]
            replicas_of_function = replicas_of_function[replicas_of_function['ts'] < now]
            replicas_of_function = replicas_of_function[replicas_of_function['shutdown_ts'] > now]
            replicas_by_function_name[deployment.fn.name] = replicas_of_function
        return replicas_by_function_name

    def traces(self, *exp_ids) -> pd.DataFrame:
        dfs = []
        for exp_id in exp_ids:
            exp = self.get_experiment(exp_id)
            start = exp.START.iloc[0]
            traces_df = pd.read_csv(f'{self._get_exp_folder(exp_id)}/traces_df.csv')
            time_format = '%Y-%m-%d %H:%M:%S.%f'
            traces_df['time'] = traces_df['time'].apply(lambda x: datetime.strptime(x, time_format))
            traces_df['ts'] = traces_df['time'].apply(lambda x: x.timestamp())
            traces_df['ts'] -= start
            traces_df['time'] = traces_df['ts'].apply(lambda x: datetime.utcfromtimestamp(x))
            traces_df['exp_id'] = exp_id
            dfs.append(traces_df)
        return pd.concat(dfs)

    def normalize_index(self, idx, exp_id):
        exp = self.get_experiment(exp_id)
        start = exp.START.iloc[0]
        a = datetime.utcfromtimestamp(start)
        b = datetime.fromisoformat('1970-01-01')
        c = a - b
        return idx - c
