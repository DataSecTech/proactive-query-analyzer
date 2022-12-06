# Schemas

There are two ways to analyze and run queries.

1. Build calcite-supported schema with actual data
2. Build an analyzer schema with only statistics

## Calcite schema

Input Query file

```yaml
db:
  calciteModel: model-sales.yml
  schema: SALES
  planDir: plans
  sensitiveColumns:
    - emps.age
    - emps.gender

queries:
  plain-select-1:
    SELECT * FROM emps

  # ... More queries ...
```

A **standard** Apache Calcite model file referred to in the input `db.calciteModel` (`model-sales.yml`)

```yaml
version: 1.0
defaultSchema: SALES
schemas:
  - name: SALES
    type: custom
    factory: org.apache.calcite.adapter.csv.CsvSchemaFactory
    operand:
      directory: data
```

Finally, put some csv files following  <https://calcite.apache.org/docs/tutorial.html> inside the `data` directory, as defined in `schemas[0].operand.directory`. We provide an example in `samples/db/sales`.

## Analyzer Schema

Analyzer schemas are schema **only** with metadata, no actual data. We use this type of schema to perform the analysis.
We implement different factory classes of various analyzer schemas `com.datasectech.queryanalyzer.core.db` package.
We provide an example in `samples/stats/sales`.

## Running analyzer from CLI

Two ways to run the analyzer in CLI, with and without docker container.
Without docker, we run the jar file with the java command

```shell
java -jar query-analyzer/build/libs/proactive-query-analyzer-<version>.jar
```

We added a shortcut

```shell
./dev.sh run-jar
```

With docker

```shell
docker run --rm -v "$(pwd)/samples/db:/db" secure-dl/query-analyzer-cli
```

Shortcut

```shell
./dev.sh run-docker
```

### Commands

Now to list the queries in the input file

```shell
./dev.sh run-jar list -i <input-file.yml>
```

Analyze queries

```shell
./dev.sh run-jar analyze -i <input-file.yml> [--overwrite] [--disable-planner]
```

Execute queries __\[doesn't work on analyzer schema]__

```shell
./dev.sh run-jar exec -i <input-file.yml> [--overwrite] [--disable-planner]
```

Save statistics from real database __\[doesn't work on analyzer schema]__

```shell
./dev.sh run-jar save-stats -i <input-file.yml> [--overwrite] [--redis-command] [--out-dir]
```

This command generates statistics that are compatible with the web service upload.

To run with docker, replace `run-jar` with `run-docker` and update the paths to reflect the path inside the container.
