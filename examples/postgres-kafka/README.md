# Stream data changes from postgres to Kafka

This example demonstrates how to stream changes from a Postgres database to a Kafka topic using the `go-db-stream`.
Instructions on how to run this example:
- clone the repository `git clone git@github.com:dtunikov/go-db-stream.git`
- navigate to the example directory `cd go-db-stream/examples/postgres-kafka`
- run `docker compose -f docker-compose.yml -f ../kafka-compose.yml up -d --build` to start the services (tested with Docker Compose version v2.30.3-desktop.1)
- wait until the services are up and running (go-db-stream, postgres, kafka, zookeeper)
- create users table in postgres database by running  
`docker exec postgres-db psql -U postgres -d postgres -c 'CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name VARCHAR(100), created_at TIMESTAMP DEFAULT NOW());'`
- create 'users' topic in kafka by running  
`docker exec kafka kafka-topics --create --topic users --bootstrap-server localhost:9092`

Now we're ready to test our go-db-stream. Let's create a new user in the users table and see if the changes are streamed to the Kafka topic.
- start listening to kafka topic by running
`docker exec kafka kafka-console-consumer --topic users --from-beginning --bootstrap-server localhost:9092 --property print.headers=true`
- open another terminal and insert a new user in the users table by running
`docker exec postgres-db psql -U postgres -d postgres -c "INSERT INTO users (name) VALUES ('Alice');"`
- check the terminal where you're listening to the kafka topic. You should see the new user data being streamed!

To stop the services, run `docker compose -f docker-compose.yml -f ../kafka-compose.yml down`.

# How to use the go-db-stream with existing postgres database (in production)

To use the go-db-stream with an existing Postgres database, you need to configure following postgres properties (postgresql.conf):
- wal_level = logical
- max_wal_senders = 10 (or any number of senders you want to use)
- max_replication_slots = 10 (or any number of replication slots you want to use)
You also need to allow replication connections from desired IP address (pg_hba.conf):
```shell
host replication postgres 0.0.0.0/0 md5
host all all 0.0.0.0/0 md5
```
Restart the postgres service after making these changes.

In this example we create temporary replication slot and publication when go-db-stream starts. This is not recommended in production environment. Instead, you should create replication slot and publication as a part of your database migration process.
Without persistent replication slot, you may lose data if the go-db-stream service is restarted.

- connect to your database
- create a replication slot
```shell
SELECT pg_create_logical_replication_slot('example_slot', 'pgoutput');
```
- create a new user with replication privileges
```shell
CREATE USER <username> WITH REPLICATION PASSWORD '<password>';
```
- grant permissions to desired schema
```shell
grant all on schema public to <username>;
```
- allow replication in pg_hba.conf (you can limit the IP address to your desired IP address, where go-db-stream is running)
```shell
host replication pglogrepl 0.0.0.0/0 md5
```

In this example we also create a publication for all tables (see config.yaml postgres configuration):
```shell
CREATE PUBLICATION example_publication FOR ALL TABLES;
```
This is not recommended in production environment. You should [create a publication](https://www.postgresql.org/docs/current/sql-createpublication.html) for each table you want to stream changes from separately. Also, if you're not using super user to connect to the database, you need to create publications before running the go-db-stream (as a part of your database migration process).