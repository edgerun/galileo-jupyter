import unittest

from galileojp.faassim import FaasSimGateway


class TestFaasSim(unittest.TestCase):

    def setUp(self) -> None:
        root_dir = f'./results/'
        self.sim_gateway = FaasSimGateway(root_dir)
        self.exp_1 = '20220906152903-6627'
        self.exp_2 = '20220906152858-fe81'
        self.resnet_inference_fn = 'resnet50-inference'

    def test_get_experiments(self):
        experiment_df = self.sim_gateway.experiments()
        self.assertEqual(2, len(experiment_df))
        exp_1_df = experiment_df.iloc[0]['EXP_ID']
        exp_2_df = experiment_df.iloc[1]['EXP_ID']
        self.assertEqual(self.exp_1, exp_1_df)
        self.assertEqual(self.exp_2, exp_2_df)

    def test_get_telemetry_for_one_experiment(self):
        telemetry_df = self.sim_gateway.telemetry(self.exp_1)
        self.assertTrue(len(telemetry_df) > 0)

    def test_get_telemetry_for_two_experiment(self):
        telemetry_df = self.sim_gateway.telemetry(self.exp_1, self.exp_2)
        self.assertTrue(len(telemetry_df) > 0)

    def test_get_traces_for_one_experiment(self):
        traces_df = self.sim_gateway.traces(self.exp_1)
        self.assertTrue(len(traces_df) > 0)

    def test_get_nodeinfos_for_one_experiment(self):
        nodes_df = self.sim_gateway.nodeinfo(self.exp_1)
        self.assertTrue(len(nodes_df) > 0)

    def test_get_nodes_by_name(self):
        nodes_by_name = self.sim_gateway.get_nodes_by_name(self.exp_1)
        self.assertTrue(len(nodes_by_name) > 0)

    def test_get_raw_replicas(self):
        raw_replicas = self.sim_gateway.get_raw_replicas(self.exp_1)
        self.assertTrue(len(raw_replicas) > 0)

    def test_get_replicas(self):
        replicas = self.sim_gateway.get_replicas(self.exp_1)
        self.assertTrue(len(replicas) > 0)

    def test_get_function_containers(self):
        containers = self.sim_gateway.get_function_containers(self.exp_1)
        self.assertTrue(len(containers) > 0)

    def test_get_deployments(self):
        deployments = self.sim_gateway.get_deployments(self.exp_1)
        self.assertTrue(len(deployments) > 0)

    def test_get_conceived_replicas(self):
        replicas = self.sim_gateway.get_conceived_replicas_by_replica_id(self.exp_1)
        self.assertTrue(len(replicas) > 0)

    def test_get_functions(self):
        functions = self.sim_gateway.get_functions(self.exp_1)
        self.assertTrue(len(functions) > 0)

    def test_get_function_images(self):
        function_images = self.sim_gateway.get_function_images(self.exp_1)
        self.assertTrue(len(function_images) > 0)

    def test_get_replica_schedule_statistics(self):
        schedule = self.sim_gateway.get_replica_schedule_statistics(self.exp_1, self.resnet_inference_fn,
                                                                    per_second=False)
        self.assertTrue(len(schedule) > 0)

    def test_get_exp_params(self):
        params = self.sim_gateway.get_exp_params(self.exp_1)
        self.assertTrue(len(params) > 0)

    def test_get_api_gateway_replicas(self):
        replicas = self.sim_gateway.get_gateways_replicas(self.exp_1)
        self.assertTrue(len(replicas) > 0)

    def test_get_replicas_with_shutdown(self):
        replica_df = self.sim_gateway.get_replicas_with_shutdown(self.exp_1)
        self.assertTrue(len(replica_df) > 0)

    def test_get_running_replicas(self):
        deployments = list(self.sim_gateway.get_deployments(self.exp_1).values())
        replica_df = self.sim_gateway.get_running_replicas(self.exp_1, deployments, 3)

        self.assertTrue(len(replica_df) > 0)
