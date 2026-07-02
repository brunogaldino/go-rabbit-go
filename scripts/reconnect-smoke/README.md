# reconnect-smoke

A manual smoke test for the client's automatic reconnection. It runs a
publisher and a consumer against a local RabbitMQ, publishes a message
every second, and prints the client's health each tick so you can watch
it survive a forced connection drop or a broker restart.

## 1. Start a local RabbitMQ

```bash
docker run -d --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

The management UI is at http://localhost:15672 (guest / guest).

## 2. Run the smoke test

```bash
go run ./scripts/reconnect-smoke
```

Override the broker URL if needed:

```bash
RABBITMQ_URI="amqp://user:pass@host:5672/" go run ./scripts/reconnect-smoke
```

You should see ticks like:

```
tick=1    connected=true  pub=true  con=true  blocked=false reconnecting=false publishErr=<nil>
  <- received on "smoke.process": {"tick":1}
```

## 3. Force a disconnect and watch it recover

In another terminal, drop the client's connections:

```bash
# Kill all connections (server-initiated close):
docker exec rabbit rabbitmqctl close_all_connections "smoke test"
```

The ticks should briefly show `connected=false` / `reconnecting=true`,
then return to `connected=true` once the client redials — exercising the
`reconnect*` path in `client/client.go`.

Other ways to force a drop:

```bash
# Full broker outage across the reconnect window:
docker restart rabbit

# Kill a single connection from the management UI:
#   Connections tab -> pick a connection -> Close
```

### Terminal vs. recoverable closes

The client reconnects on any unexpected drop but gives up on a defined
set of terminal errors (see `isTerminalClose` in `client/client.go`):
codes `channel-error` (504), `precondition-failed` (406), `not-allowed`
(530), `access-refused` (403), and closes whose reason contains
`closed via management plugin`.

To see the terminal path, close the connection through the management
plugin so the reason carries `closed via management plugin`: the client
logs a permanent shutdown and stops reconnecting.

## 4. Clean up

```bash
docker rm -f rabbit
```
