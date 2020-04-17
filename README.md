# XRP Transactions Exporter

This code allows you to fetc all transactions from the XRP ledger and dump it into kafka.

## Running the service

The easiest way to run the service is using `docker-compose`:

Example:

```bash
$ docker-compose up --build
```

## Configure

You can configure the service with the following ENV variables:

* LEDGER - block numer from which to start the data extraction. Default: `32570`
* XRP\_NODE\_URL - XRP node url. Default: `wss://s2.ripple.com`
* MAX\_CONNECTION\_CONCURRENCY - **DESCRIBE** Default: `10`
* CONNECTIONS\_COUNT - **DESCRIBE** Default: `1`
* DEFAULT\_WS\_TIMEOUT - **DESCRIBE** Default: `10000`
* SEND\_BATCH\_SIZE - **DESCRIBE** Default: `30`
* EXPORT\_TIMEOUT\_MLS - max time interval between successful data pushing to kafka to treat the service as healthy. Default: `1000 * 60 * 5, 5 minutes`


#### Health checks

You can make health check GET requests to the service. The health check makes a request to Kafka to make sure the connection is not lost and also checks time from the previous pushing data to kafka (or time of the service start if no data pushed yet):

```bash
curl http://localhost:3000/healthcheck
```

If the health check passes you get response code 200 and response message `ok`.
If the health check does not pass you get response code 500 and a message describing what failed.

## Running the tests

Tests are not imlemented yet :(
