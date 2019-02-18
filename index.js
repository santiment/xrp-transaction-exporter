const pkg = require('./package.json');
const { send } = require('micro')
const url = require('url')
const { Exporter } = require('san-exporter')
const Client = require('rippled-ws-client')

const exporter = new Exporter(pkg.name)

const XRPLNodeUrl = process.env.XRP_NODE_URL || 'wss://s2.ripple.com'
let lastProcessedPosition = {
  blockNumber: parseInt(process.env.LEDGER || "32570"),
}

const SEND_BATCH_SIZE = parseInt(process.env.SEND_BATCH_SIZE || "30")
const DEFAULT_WS_TIMEOUT = 500

console.log('Fetch XRPL transactions')
  
const fetchLedgerTransactions = (connection, ledger_index) => {
  return new Promise((resolve, reject) => {
    return connection.send({
      command: 'ledger',
      ledger_index: parseInt(ledger_index),
      transactions: true,
      expand: false
    }, DEFAULT_WS_TIMEOUT).then(({ledger}) => {
      if (typeof ledger.transactions === 'undefined' || ledger.transactions.length === 0) {
        // Do nothing
        resolve({ ledger: ledger, transactions: [] })
        return
      } else {
        if (ledger.transactions.length > 200) {
          // Lots of data. Per TX
          console.log(`<<< MANY TXS at ledger ${ledger_index}: [[ ${ledger.transactions.length} ]], processing per-tx...`)
          let transactions = ledger.transactions.map(Tx => {
            return connection.send({
              command: 'tx',
              transaction: Tx
            }, DEFAULT_WS_TIMEOUT)
          })
          Promise.all(transactions).then(r => {
            let allTxs = r.filter(t => {
              return typeof t.error === 'undefined' && typeof t.meta !== 'undefined' && typeof t.meta.TransactionResult !== 'undefined'
            })
            console.log(`>>> ALL SUCCESSFUL TXS FETCHED for ${ledger_index}: ${allTxs.length}`, )
            resolve({ ledger: ledger, transactions: allTxs.map(t => {
              return Object.assign(t, {
                metaData: t.meta
              })
            }) })
            return
          })
          .catch(reject)
        } else {
          // Fetch at once.
          resolve(new Promise((resolve, reject) => {
            connection.send({
              command: 'ledger',
              ledger_index: parseInt(ledger_index),
              transactions: true,
              expand: true
            }, DEFAULT_WS_TIMEOUT).then(Result => {
              resolve({ ledger: ledger, transactions: Result.ledger.transactions })
              return
            }).catch(reject)
          }))
        }
      }
      return
    }).catch(reject)
  })
}

async function work(connection) {
  const currentLedger = await connection.send({
    command: 'ledger',
    ledger_index: 'validated',
    transactions: true,
    expand: false
  }, DEFAULT_WS_TIMEOUT)

  const currentBlock = parseInt(currentLedger.ledger.ledger_index)
  const requests = []

  console.info(`Fetching transfers for interval ${lastProcessedPosition.blockNumber}:${currentBlock}`)

  while (lastProcessedPosition.blockNumber + requests.length < currentBlock) {
    requests.push(fetchLedgerTransactions(connection, lastProcessedPosition.blockNumber + requests.length))

    if (requests.length >= SEND_BATCH_SIZE || lastProcessedPosition.blockNumber + requests.length == currentBlock) {
      const ledgers = await Promise.all(requests).map(async ({ledger, transactions}) => {
        console.log(`Transactions in ${ledger.ledger_index}: ${transactions.length}`)
        return { ledger, transactions, primaryKey: ledger.ledger_index }
      })

      console.log(`Flushing ledgers ${ledgers[0].primaryKey}:${ledgers[ledgers.length - 1].primaryKey}`)
      await exporter.sendDataWithKey(ledgers, "primaryKey")

      lastProcessedPosition.blockNumber += ledgers.length
      await exporter.savePosition(lastProcessedPosition)

      requests.length = 0
    }
  }
}

async function initLastProcessedLedger() {
  const lastPosition = await exporter.getLastPosition()

  if (lastPosition) {
    lastProcessedPosition = lastPosition
    console.info(`Resuming export from position ${JSON.stringify(lastPosition)}`)
  } else {
    await exporter.savePosition(lastProcessedPosition)
    console.info(`Initialized exporter with initial position ${JSON.stringify(lastProcessedPosition)}`)
  }
}

const fetchEvents = (connection) => {
  return work(connection)
    .then(() => {
      console.log(`Progressed to position ${JSON.stringify(lastProcessedPosition)}`)

      // Look for new events every 1 sec
      setTimeout(fetchEvents, 1000)
    })
}

const init = async () => {
  const connection = await new Client(XRPLNodeUrl)
  await exporter.connect()
  await initLastProcessedLedger()
  await fetchEvents(connection)
}

init()

const healthcheckKafka = () => {
  return new Promise((resolve, reject) => {
    if (exporter.producer.isConnected()) {
      resolve()
    } else {
      reject("Kafka client is not connected to any brokers")
    }
  })
}

module.exports = async (request, response) => {
  const req = url.parse(request.url, true);

  switch (req.pathname) {
    case '/healthcheck':
      return healthcheckKafka()
        .then(() => send(response, 200, "ok"))
        .catch((err) => send(response, 500, `Connection to kafka failed: ${err}`))

    default:
      return send(response, 404, 'Not found');
  }
}
