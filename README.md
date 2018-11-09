# Intermix.io Airflow Plugin

## Introduction

The [intermix.io](http://intermix.io) Airflow plugin surfaces important information about the performance of Airflow DAGs and Tasks.

The plugin will automatically annotate SQL queries executed by Airflow via **_PostgresOperator_**.
The queries are annotated with the [intermix.io - Tagging Specification](https://docs.intermix.io/hc/en-us/articles/360003340453-intermix-io-Tagging-Specification). Tagging your SQL allows you to leverage
the intermix.io App Tracing suite of tools.

The plugin works by prepending the query with a SQL comment containing metadata about
the query itself (Airflow DAG, task, user, etc). This does not slow down query execution or affect
the logical execution of the code. It is used to provide data inside our analytics service.


[Read more about App Tracing here.
](https://docs.intermix.io/hc/en-us/articles/360004361073-intermix-io-App-Tracing-Guide)





## Installation

Set the AIRFLOW_HOME environment variable to point to your Airflow data directory.
Copy this folder to all machines and environments that will be executing Airflow.
Run `python setup.py install` inside this folder on all those environments.

## Support for PostgresHook

The plugin will automatically annotate SQL queries executed by Airflow via **_PostgresOperator_**.

In addition, there is lightweight support for PostgresHook methods _**get_first, get_records,**_ and _**run**_.  If you are using these methods,
the plugin will retrieve the file name, line of code, and class name. An attempt will be made to retrieve the DAG and
Task name as well.

However, if you are using **_PostgresHook_**, we recommend using the [intermix.io Python Plugin](https://docs.intermix.io/hc/en-us/articles/360004408853-intermix-io-Python-Plugin) to explicitly pass in
the DAG and Task name.


[See this link for intermix.io Python Plugin installation and use.](https://docs.intermix.io/hc/en-us/articles/360004408853-intermix-io-Python-Plugin)


## Compatibility

This plugin has been tested on:

Airflow versions > 1.1.0

Python version 2.7.x.


## Questions & Support

For questions and support please contact support@intermix.io.

## Contributing

If you're looking to contribute, please contact us at support@intermix.io.

## About intermix.io

[intermix.io](http://intermix.io) is a single monitoring dashboard for data engineers to keep an eye on
their mission-critical data flows.


## License

This software is published under the MIT license.  For full license see the [LICENSE file](https://github.com/intermix/airflow-plugin/master/LICENSE).
