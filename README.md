galileo-jupyter
===============

Tools for analyzing galileo experiments.

Configuration
-------------

Create a `$HOME/.galileojp` and fill with environment variables that configure the database access to galileo-db.
For example:

```
galileo_expdb_driver=mixed

galileo_expdb_mysql_host=localhost
galileo_expdb_mysql_port=3307
galileo_expdb_mysql_db=galileo
galileo_expdb_mysql_user=galileo
galileo_expdb_mysql_password=mypassword
galileo_expdb_influxdb_url=http://localhost:8086
galileo_expdb_influxdb_token=...
galileo_expdb_influxdb_timeout=10000
galileo_expdb_influxdb_org=galileo
galileo_expdb_influxdb_org_id=...
galileo_expdb_faas_sim_results_folder=...
```

Usage
-----

Then you can run

```python
from galileojp.frames import MixedExperimentFrameGateway

efg = MixedExperimentFrameGateway.from_env()
efg.telemetry('my-exp-id') # returns a dataframe containing the telemetry for the given experiment
```
