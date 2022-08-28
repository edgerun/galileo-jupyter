import logging
import os
from typing import Optional, Union, Dict

import pandas as pd

from galileojp import env
from galileojp.frames import ExperimentFrameGateway

logger = logging.getLogger()


class FaasSimGateway(ExperimentFrameGateway):

    def __init__(self, root_dir: str):
        self.root_dir = root_dir

    def experiments(self) -> pd.DataFrame:
        # TODO do we need more metadata?
        exp_ids = [f.path for f in os.scandir(self.root_dir) if f.is_dir()]
        exp_ids = [os.path.basename(p) for p in exp_ids]
        data = {
            'EXP_ID': exp_ids
        }
        return pd.DataFrame(data)

    def metadata(self, exp_id: str = None) -> Optional[Union[pd.DataFrame, Dict]]:
        return None

    def nodeinfo(self, *exp_ids):
        # TODO save this from simulation
        return None

    def events(self, *exp_ids):
        return None

    def telemetry(self, *exp_ids) -> pd.DataFrame:
        dfs = []
        for exp_id in exp_ids:
            node_utilization_df = pd.read_csv(f'{self.root_dir}/{exp_id}/node_utilization_df.csv')
            replica_utilization_df = pd.read_csv(f'{self.root_dir}/{exp_id}/function_utilization_df.csv')
            node_utilization_df['exp_id'] = exp_id
            replica_utilization_df['exp_id'] = exp_id
            dfs.append(node_utilization_df)
            dfs.append(replica_utilization_df)
        return pd.concat(dfs)

    def traces(self, *exp_ids) -> pd.DataFrame:
        dfs = []
        for exp_id in exp_ids:
            traces_df = pd.read_csv(f'{self.root_dir}/{exp_id}/traces_df.csv')
            traces_df['exp_id'] = exp_id
            dfs.append(traces_df)
        return pd.concat(dfs)

    @staticmethod
    def from_env() -> 'ExperimentFrameGateway':
        env.load()
        root_directory = os.environ.get('galileo_expdb_faas_sim_results_folder', os.getcwd())
        return FaasSimGateway(root_directory)
