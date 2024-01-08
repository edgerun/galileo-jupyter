import abc
import datetime
import json
from typing import List, Dict, Union, Optional

import pandas as pd
from galileodb.factory import create_experiment_database_from_env, create_influxdb_from_env, create_mysql_from_env
from galileodb.influx.db import InfluxExperimentDatabase
from galileodb.sql.adapter import ExperimentSQLDatabase

from galileojp import env


def to_idlist(exp_ids):
    idlist = ', '.join([f'"{i}"' for i in exp_ids])
    return idlist


class ExperimentFrameGateway(abc.ABC):

    def experiments(self) -> pd.DataFrame:
        raise NotImplementedError()

    def metadata(self, exp_id: str = None) -> Optional[Union[pd.DataFrame, Dict]]:
        """
        :param exp_id: optional
        :return: in case no exp_id is passed, returns a DataFrame containing all items from metadata, otherwise
        returns the parsed JSON of metadata of this experiment as Dict
        """
        raise NotImplementedError()

    def nodeinfo(self, *exp_ids):
        raise NotImplementedError()

    def events(self, *exp_ids):
        raise NotImplementedError()

    def telemetry(self, *exp_ids) -> pd.DataFrame:
        raise NotImplementedError()

    def traces(self, *exp_ids) -> pd.DataFrame:
        raise NotImplementedError()

    @staticmethod
    def from_env() -> 'ExperimentFrameGateway':
        raise NotImplementedError()

    def delete(self, exp_id: str):
        raise NotImplementedError()

    def export(self, exp_id: str, folder_path: str):
        raise NotImplementedError()


class MixedExperimentFrameGateway(ExperimentFrameGateway):

    def __init__(self, inflxudb: InfluxExperimentDatabase, sqldb: ExperimentSQLDatabase):
        self.influxdb = inflxudb
        self.sqldb = sqldb

    @property
    def _sql_con(self):
        user = self.sqldb.db.connect_kwargs['user']
        password = self.sqldb.db.connect_kwargs['password']
        host = self.sqldb.db.connect_kwargs['host']
        port = self.sqldb.db.connect_kwargs['port']
        db = self.sqldb.db.connect_kwargs['db']
        sql_connect_string = f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}'
        return sql_connect_string

    @property
    def _influxdb_client(self):
        return self.influxdb.client

    def experiments(self) -> pd.DataFrame:
        experiments = pd.read_sql(f'SELECT * FROM experiments', con=self.sqldb.db.connection)
        experiments['metadata'] = '{}'
        metadata_df = self.metadata()
        for exp in experiments.iterrows():
            exp_id = exp[1]['EXP_ID']
            index = exp[0]
            metadata = metadata_df[metadata_df['EXP_ID'] == exp_id]['DATA']
            if len(metadata) > 0:
                metadata = metadata.iloc[0]
                experiments.loc[index, 'metadata'] = metadata
        return experiments

    def metadata(self, exp_id: str = None) -> Optional[Union[pd.DataFrame, Dict]]:
        if exp_id is None:
            return pd.read_sql('SELECT * FROM metadata', con=self.sqldb.db.connection)
        else:
            sql = f"SELECT * FROM metadata WHERE EXP_ID = '{exp_id}'"
            metadata = pd.read_sql(sql, con=self._sql_con)
            if len(metadata) > 0:
                metadata = metadata.iloc[0]['DATA']
                return json.loads(metadata)
            else:
                return None

    def nodeinfo(self, *exp_ids):
        if not exp_ids:
            raise ValueError

        idlist = to_idlist(exp_ids)

        df = pd.read_sql(f'SELECT * FROM nodeinfo WHERE exp_id in ({idlist})', con=self.sqldb.db.connection)
        return df

    def raw_influxdb_query(self, query: str) -> pd.DataFrame:
        return self.influxdb.client.query_api().query_data_frame(org=self.influxdb.org_name, query=query)

    def events(self, *exp_ids):
        return self._get_influxdb_df('events', 'ts', *exp_ids)

    def telemetry(self, *exp_ids) -> pd.DataFrame:
        return self._get_influxdb_df('telemetry', 'ts', *exp_ids)

    def traces(self, *exp_ids) -> pd.DataFrame:
        return self._get_influxdb_df('traces', 'sent', *exp_ids)

    def _get_influxdb_df(self, measurement: str, time_col: str, *exp_ids) -> pd.DataFrame:
        if not exp_ids:
            raise ValueError()

        telemetry_dfs = []
        stop = datetime.datetime.utcnow() + datetime.timedelta(days=1)
        stop = stop.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        for exp_id in exp_ids:
            query = f"""
                     from(bucket: "{exp_id}")
                         |> range(start: 1970-01-01, stop: {stop})
                         |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                    """
            influxdb_query_result = self.raw_influxdb_query(query)
            if isinstance(influxdb_query_result, List):
                influxdb_query_result = pd.concat(influxdb_query_result)
            telemetry_dfs.append(influxdb_query_result)

        df = pd.concat(telemetry_dfs)
        if len(df) == 0:
            raise ValueError("Empty dataframes")
        df.rename(columns={'_value': 'value'}, inplace=True)
        df.index = pd.DatetimeIndex(pd.to_datetime(df[time_col].astype(float), unit='s'))
        influxdb_drop_columns = ['result', 'table', '_start', '_stop', '_time', '_field', '_measurement']
        df = df.drop(influxdb_drop_columns, axis=1)
        return df

    @staticmethod
    def from_env() -> 'ExperimentFrameGateway':
        env.load()
        influxdb = create_influxdb_from_env()
        influxdb.open()
        sqldb = create_mysql_from_env()
        sqldb.open()
        return MixedExperimentFrameGateway(influxdb, sqldb)

    def delete(self, exp_id: str):
        self.influxdb.delete_experiment(exp_id)
        self.sqldb.delete_experiment(exp_id)


class SqlExperimentFrameGateway(ExperimentFrameGateway):

    def __init__(self, edb: ExperimentSQLDatabase) -> None:
        super().__init__()
        self.edb = edb

    @property
    def _con(self):
        return self.edb.db.connection

    def experiments(self) -> pd.DataFrame:
        experiments = pd.read_sql(f'SELECT * FROM experiments', con=self._con)
        experiments['metadata'] = '{}'
        metadata_df = self.metadata()
        for exp in experiments.iterrows():
            exp_id = exp[1]['EXP_ID']
            index = exp[0]
            metadata = metadata_df[metadata_df['EXP_ID'] == exp_id]['DATA']
            if len(metadata) > 0:
                metadata = metadata.iloc[0]
                experiments.loc[index, 'metadata'] = metadata
        return experiments

    def metadata(self, exp_id: str = None) -> Optional[Union[pd.DataFrame, Dict]]:
        if exp_id is None:
            return pd.read_sql('SELECT * FROM metadata', con=self._con)
        else:
            sql = f"SELECT * FROM metadata WHERE EXP_ID = '{exp_id}'"
            metadata = pd.read_sql(sql, con=self._con)
            if len(metadata) > 0:
                metadata = metadata.iloc[0]['DATA']
                return json.loads(metadata)
            else:
                return None

    def nodeinfo(self, *exp_ids):
        if not exp_ids:
            raise ValueError

        idlist = to_idlist(exp_ids)

        df = pd.read_sql(f'SELECT * FROM nodeinfo WHERE exp_id in ({idlist})', con=self._con)
        return df

    def events(self, *exp_ids):
        if not exp_ids:
            raise ValueError

        idlist = to_idlist(exp_ids)

        df = pd.read_sql(f'SELECT * FROM events WHERE exp_id in ({idlist})', con=self._con)
        df.index = pd.DatetimeIndex(pd.to_datetime(df['TIMESTAMP'].astype(float), unit='s'))
        return df

    def telemetry(self, *exp_ids) -> pd.DataFrame:
        if not exp_ids:
            raise ValueError

        idlist = to_idlist(exp_ids)

        df = pd.read_sql(f'SELECT * FROM telemetry WHERE exp_id in ({idlist})', con=self._con)
        df.index = pd.DatetimeIndex(pd.to_datetime(df['TIMESTAMP'].astype(float), unit='s'))
        return df

    @staticmethod
    def from_env() -> 'ExperimentFrameGateway':
        env.load()
        return ExperimentFrameGateway(create_experiment_database_from_env())


if __name__ == '__main__':
    gw = MixedExperimentFrameGateway.from_env()
    d = gw.metadata('202111131530-b84a')
    pass
