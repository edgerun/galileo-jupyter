import os
import unittest

from galileojp.faassim import FaasSimGateway


class TestFaasSim(unittest.TestCase):

    def setUp(self) -> None:
        root_dir = f'./results/'
        self.sim_gateway = FaasSimGateway(root_dir)
        self.exp_1 = '2022-08-27-11-00-45'
        self.exp_2 = '2022-08-27-11-00-46'

    def test_get_experiments(self):
        experiment_df = self.sim_gateway.experiments()
        self.assertEqual(2,len(experiment_df))
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
