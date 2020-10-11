# FTRLIM

## Introduction

FTRLIM: A Distributed Instance Matching Framework

## Env

- Hadoop
- Spark
- Hive
- Python

## Interactive Python Shell

Alternatively, if you prefer Python, you can use the Python shell:

```
./bin/pyspark
```

And run the following command, which should also return 1,000,000,000:

```
>>> spark.range(1000 * 1000 * 1000).count()
```

## Start up

You can set the MASTER environment variable when running examples to submit examples to a cluster. This can be a mesos:// or spark:// URL, "yarn" to run on YARN, and "local" to run locally with one thread, or "local[N]" to run locally with N threads. You can also use an abbreviated class name if the class is in the `examples` package. For instance:

```
MASTER=spark://host:7077 ./bin/run-example SparkPi
```

Many of the example programs print usage help if no params are given.

Testing first requires [building Spark](https://github.com/apache/spark/blob/master/README.md#building-spark). Once Spark is built, tests can be run using:

```
./dev/run-tests
```

Please see the guidance on how to [run tests for a module, or individual tests](https://spark.apache.org/developer-tools.html#individual-tests).

There is also a Kubernetes integration test, see resource-managers/kubernetes/integration-tests/README.md

## Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported storage systems. Because the protocols have changed in different versions of Hadoop, you must build Spark against the same version that your cluster runs.

Please refer to the build documentation at ["Specifying the Hadoop Version and Enabling YARN"](https://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version-and-enabling-yarn) for detailed guidance on building for a particular distribution of Hadoop, including building for particular Hive and Hive Thriftserver distributions.

## Configuration

Please refer to the [Configuration Guide](https://spark.apache.org/docs/latest/configuration.html) in the online documentation for an overview on how to configure Spark.