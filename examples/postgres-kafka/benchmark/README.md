# This folder contains scripts to run benchmark tests for postgres-kafka example

- Run the example deployment from the folder above
- Run the http server which will simply expose an endpoint to insert a new user in the users table
```shell
DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres go run main.go
```
- Run kafka listener to consume messages from the 'users' topic in a separate terminal (it should write the messages to the benchmark.log file)
```shell
docker exec kafka kafka-console-consumer --topic users --bootstrap-server localhost:9092 --property print.headers=true --property print.timestamp=true > benchmark.log
```
- Run the load test with 100 RPS (you can adjust the load test parameters in the script.js)
```shell
k6 run script.js
```
- Stop your kafka listener and http server. Check if benchmark.log contains the messages from the load test.
- Run the benchmark script parser to calculate the average latency
```shell
BENCHMARK=true go run main.go
```
- The script will output the average latency, which shows how long it takes for the message to be streamed from postgres to kafka (kafka message timestamp - postgres created_at row field).

On my machine the average latency was around 500ms.