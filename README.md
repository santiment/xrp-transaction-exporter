# XRP Transactions Exporter

This code allows you to fetch all transactions from the XRP ledger and dump them into kafka.

## Run

Example:

```bash
$ ./bin/run.sh
```

## Configure

You can configure the service with the following ENV variables:

* `LEDGER` - block numer from which to start the data extraction. Default: `32570`
* `XRP_NODE_URL` - XRP node url. Default: `wss://s2.ripple.com`
* `MAX_CONNECTION_CONCURRENCY` - **DESCRIBE** Default: `10`
* `CONNECTIONS_COUNT` - **DESCRIBE** Default: `1`
* `DEFAULT_WS_TIMEOUT` - **DESCRIBE** Default: `10000`
* `SEND_BATCH_SIZE` - **DESCRIBE** Default: `30`
* `EXPORT_TIMEOUT_MLS` - max time interval between successful data pushing to kafka to treat the service as healthy. Default: `1000 * 60 * 5, 5 minutes`


#### Health checks

You can make health check GET requests to the service. The health check makes a request to Kafka to make sure the connection is not lost and also checks time from the previous pushing data to kafka (or time of the service start if no data pushed yet):

```bash
curl http://localhost:3000/healthcheck
```

If the health check passes you get response code 200 and response message `ok`.
If the health check does not pass you get response code 500 and a message describing what failed.

## Running the tests

Tests are not imlemented yet :(
