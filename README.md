# Intermix.io Airflow Plugin

## Introduction

The [intermix.io](http://intermix.io) Airflow plugin surfaces important information about the performance Airflow DAGs and Tasks.

The plugin will automatically annotate SQL queries executed by Airflow via the Postgres or Redshift operator. 
The queries are annotated with the intermix.io - Tagging Specification. Tagging your SQL allows you to leverage 
the intermix.io App Tracing suite of tools.

The plugin works by  prepending the query with a SQL comment containing metadata about 
the query itself (Airflow DAG, task, user, etc). This does not slow down query execution or affect 
the logical execution of the code. It is used to provide data inside our analytics service. 


[Read more about App Tracing here.
](https://docs.intermix.io/hc/en-us/articles/360004361073-intermix-io-App-Tracing-Guide)





## Installation

Set the AIRFLOW_HOME environment variable to point to your Airflow data directory.
Copy this folder to all machines and environments that will be executing Airflow.
Run `python setup.py install` inside this folder on all those environments.

## Questions

For questions and support please contact support@intermix.io.

## Contributing

If you're looking to contribute, please contact us at support@intermix.io.

## About intermix.io

[intermix.io](http://intermix.io) is a intermix.io is a single monitoring dashboard for data engineers to keep an eye on 
their monitor mission-critical data flows.


## License

This software is published under the MIT license.  For full license see the [LICENSE file](https://github.com/intermix/airflow-plugin/master/LICENSE).

