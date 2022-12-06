# SecureDL - Proactive Query Analyzer

SecureDL Proactive Query Analyzer helps you to analyze queries with unexpected data exposure.

## Build docker containers

Build the gradle project with

```shell
./gradlew build
```

Next, build the docker containers

```shell
./dev.sh build --docker
```

Please analyze the build function in `dev.sh` to understand the docker container building process.

### Run the REST API webservice

We provide convenient `docker-compose.yml` for building a two container deployment.

This requires few configurations in `.env` file.
Either you can run `./dev.sh init` to generate one or manually create `.env` file with following variables.

```
MYSQL_ROOT_PASSWORD=
MYSQL_PROACTIVE_QUERY_ANALYZER_PASSWORD=
VOLUME_BASE=
SDL_QUERY_ANALYZER_VERSION=
SDL_QUERY_ANALYZER_PORT=
```

Now start the containers

```shell
docker compose up
```

## Upload sample statistics data

Create a schema

```shell
http POST ':8080/schema' 'name=sales'
```

Upload statistics to the schema

```shell
http PUT ':8080/schema/sales/table/DEPTS' < samples/stats/sales/stats/depts.json
http PUT ':8080/schema/sales/table/EMPS' < samples/stats/sales/stats/emps.json
http PUT ':8080/schema/sales/table/LOGIN' < samples/stats/sales/stats/login.json
http PUT ':8080/schema/sales/table/PERSONS' < samples/stats/sales/stats/persons.json
http PUT ':8080/schema/sales/table/SDEPTS' < samples/stats/sales/stats/sdepts.json
```

Run batch of queries query

```shell
http POST :8080/query/batch/sales < samples/stats/sales/query-batch.json
```

Please refer to [Schemas](Schemas.md) for more information about the statistics files.

## Run the webservice locally

Create database and user, copy the `config/application.properties-sample` to `config/application.properties`,
and fill up with database information. Run

```shell
./gradlew bootRun
```

Refer to the `bootRun` task in `webservice/build.gradle` and
<https://docs.spring.io/spring-boot/docs/1.5.6.RELEASE/reference/html/boot-features-external-config.html>

## Run the CLI for analysis

Please refer to [Schemas](Schemas.md) for more information about running the analyzer in the command line.
