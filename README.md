# Intermix.io Airflow Plugin

## Introduction

The Intermix.io Airflow plugin is used to annotate Postgres and Redshift queries being executed with a comment
that contains metadata about the query itself. This does not slow down query execution and is used to provide richer
data inside our analytics service.


## Installation

Set the AIRFLOW_HOME environment variable to point to your Airflow data directory.
Copy this folder to all machines and environments that will be executing Airflow.
Run `python setup.py install` inside this folder on all those environments.

## Questions

For questions and support please contact support@intermix.io.

## Contributing

If you're looking to contribute, please contact us at support@intermix.io.

## About Intermix

intermix.io ([http://intermix.io](http://intermix.io)) is a product that instruments Amazon Redshift to provide 
performance analytics. It helps Redshift adminstrators to:

- optimize WLM to maximize throughput and memory utilization
- monitor user behavior (ad hoc queries, batch jobs, etc.)
- root cause analysis of issues impacting your cluster
- predict storage growth and deep visilibty into storage utilization
- optimize dist and sort keys

We offer a free trial and procurement via [AWS Marketplace](https://aws.amazon.com/marketplace/pp/B0764JGX86?qid=1513291438437&sr=0-2&ref_=srh_res_product_title).

## License

This software is published under the MIT license.  For full license see the [LICENSE file](https://github.com/intermix/airflow-plugin/master/LICENSE).

