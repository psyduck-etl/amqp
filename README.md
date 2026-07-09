# psyduck amqp plugin

An [AMQP 0.9.1](https://www.rabbitmq.com/) (RabbitMQ) plugin for
[Psyduck](https://github.com/psyduck-etl/sdk). It exposes a single resource,
`amqp.queue`, that can act as either end of a pipeline:

- as a **producer** it consumes messages off a queue and emits their bodies;
- as a **consumer** it publishes incoming data to a queue or exchange.

Built against `github.com/psyduck-etl/sdk` **v0.5.0**.

```sh
go build -buildmode=plugin -o amqp.so .
```

---

## Writing Psyduck pipelines

Psyduck pipelines are written in HCL (`.psy` files). A file is made of two
kinds of things: **mover** blocks that declare a configured resource, and a
**pipeline** block that wires movers together.

### Mover blocks

Every mover is a block whose type is its role — `produce`, `consume`, or
`transform` — with two labels and a body:

```hcl
<role> "<plugin>.<resource>" "<instance>" {
  # host-owned block metadata (optional, same on every mover)
  per-minute = 180   # rate limit; 0 = unrestricted
  stop-after = 0     # stop after n items; 0 = unbounded

  # resource-specific configuration
  <field> = <value>
  ...
}
```

- The **first label** `"<plugin>.<resource>"` selects the resource to bind —
  here always `"amqp.queue"`.
- The **second label** `"<instance>"` is a name you choose. You refer to the
  mover elsewhere as `<resource>.<instance>` — so `consume "amqp.queue"
  "orders-out" { … }` is referenced as `queue.orders-out`.

`per-minute` and `stop-after` are read by the host, not the plugin; they work
on any mover regardless of plugin.

### The pipeline block

A `pipeline` block connects movers by reference. Each attribute is a **list**,
so a single pipeline can fan several producers into several consumers
(n-to-n), optionally through a chain of transforms:

```hcl
pipeline "<name>" {
  produce   = [ queue.orders-in ]
  transform = [ filter.dedup ]
  consume   = [ table.load ]
}
```

---

## Resource: `amqp.queue`

Kinds: **producer** and **consumer**.

### Common / topology options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection` | string | *(required)* | Broker URL, `amqp://user:pass@host:port` |
| `queue` | string | *(required)* | Queue name to declare and use |
| `exchange` | string | `""` | Exchange to publish to / bind against. Empty ⇒ default exchange, routed by queue name |
| `exchange-type` | string | `direct` | `direct`, `fanout`, `topic`, or `headers` (when declaring one) |
| `routing-key` | string | `""` | Routing key for publishing to and binding a named exchange |
| `declare-exchange` | bool | `false` | Declare the exchange before use (the queue is always declared) |
| `durable` | bool | `false` | Declare queue/exchange as durable (survive broker restart) |
| `auto-delete` | bool | `false` | Delete when the last consumer/binding goes away |
| `exclusive` | bool | `false` | Declare and consume exclusively for this connection |
| `no-wait` | bool | `false` | Issue declarations without waiting for broker confirmation |
| `content-type` | string | `text/plain` | Content type set on published messages |
| `message-ttl` | int | `0` | Queue arg `x-message-ttl` (ms); 0 = unset |
| `max-length` | int | `0` | Queue arg `x-max-length`; 0 = unset |
| `dead-letter-exchange` | string | `""` | Queue arg `x-dead-letter-exchange` |
| `queue-type` | string | `""` | Queue arg `x-queue-type`, e.g. `quorum` |

### Producer-only (consume side)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `prefetch` | int | `0` | QoS: max unacknowledged deliveries in flight (0 = unlimited). Provides backpressure and batching |
| `auto-ack` | bool | `false` | Let the broker consider messages acked on delivery instead of after they are forwarded downstream |

Messages are acknowledged **after** they have been handed downstream (unless
`auto-ack`), so a crash mid-pipeline re-delivers rather than drops.

### Consumer-only (publish side)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `confirm` | bool | `false` | Publisher-confirm mode: wait for a broker ack per message and surface nacks as errors |
| `mandatory` | bool | `false` | Publish `mandatory`, so unroutable messages are returned rather than dropped |
| `persistent` | bool | `false` | Mark messages persistent so a durable queue retains them across restarts |

---

## Examples

### Drain a queue into a pipeline

```hcl
produce "amqp.queue" "orders-in" {
  stop-after = 0        # run until the pipeline is torn down
  connection = "amqp://guest:guest@localhost:5672"
  queue      = "orders.in"
  prefetch   = 100      # up to 100 unacked deliveries in flight
  durable    = true
}

pipeline "drain-orders" {
  produce = [ queue.orders-in ]
  consume = [ /* some sink */ ]
}
```

### Reliable publishing to a durable queue

```hcl
consume "amqp.queue" "orders-out" {
  connection = "amqp://guest:guest@localhost:5672"
  queue      = "orders.done"
  durable    = true
  persistent = true     # messages survive a broker restart
  confirm    = true     # block for a broker ack per publish
  mandatory  = true     # error instead of silently dropping unroutable messages
}
```

### Topic exchange with a dead-letter queue

```hcl
consume "amqp.queue" "audit" {
  connection       = "amqp://guest:guest@localhost:5672"
  queue            = "audit.events"
  exchange         = "events"
  exchange-type    = "topic"
  declare-exchange = true
  routing-key      = "order.#"
  durable          = true

  dead-letter-exchange = "events.dlx"
  message-ttl          = 86400000   # 24h
  queue-type           = "quorum"
}
```

### End-to-end: AMQP → dedup → MySQL

A realistic ingest that pairs this plugin with
[`psyduck-etl/mysql`](https://github.com/psyduck-etl/mysql): read order events
off a queue, drop ones already loaded, batch-insert the rest.

```hcl
produce "amqp.queue" "orders-in" {
  connection = "amqp://guest:guest@localhost:5672"
  queue      = "orders.in"
  prefetch   = 200
  durable    = true
}

transform "mysql.filter" "dedup" {
  connection = "etl:etl@tcp(localhost:3306)/warehouse"
  query      = "SELECT EXISTS(SELECT 1 FROM orders WHERE order_id = :order_id)"
  pass-when  = "0"           # 0 = order not yet stored -> keep it
}

consume "mysql.table" "load" {
  connection        = "etl:etl@tcp(localhost:3306)/warehouse"
  table             = "orders"
  fields            = ["order_id", "customer", "total"]
  insert-chunk-size = 500
  write-mode        = "upsert"
}

pipeline "ingest-orders" {
  produce   = [ queue.orders-in ]
  transform = [ filter.dedup ]
  consume   = [ table.load ]
}
```
