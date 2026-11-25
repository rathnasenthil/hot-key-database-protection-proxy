# Hot-Key Database Protection Proxy

A middleware proxy that sits between clients and PostgreSQL and stops a hot key from taking down the database. Identical concurrent reads are collapsed into one origin query, the result is cached in Redis with stale-while-revalidate, and excess origin work is rejected when the database is saturated.

The behavior follows `docs/SRS-Hot-Key-Database-Protection-Proxy.pdf`.

## Layout

```text
hot-key-proxy/
├── docs/                 SRS
├── origin-service/       Spring Boot catalog API on PostgreSQL
├── proxy-service/        Cache, singleflight, hot-key detection, admission control
├── gateway/              Consistent-hash routing and the simulation dashboard
├── benchmarks/           k6 workloads W1–W5
├── monitoring/           Prometheus, Grafana, postgres exporter
├── docker-compose.yml
└── README.md
```

## What runs

| Path | Address | Role |
| --- | --- | --- |
| Protected | http://localhost:8080 | Gateway, hashes the product id onto one of three proxy instances |
| Baseline | http://localhost:8081 | Origin service only. No cache, coalescing, or admission control |
| Proxies | http://localhost:8091–8093 | Direct access to each proxy instance |
| Grafana | http://localhost:3001 | Comparison dashboard (`admin` / `admin`) |
| Prometheus | http://localhost:9090 | Scrapes origin, proxies, gateway, and PostgreSQL |

Redis is three standalone nodes (`localhost:6379`, `6380`, `6381`). The proxy places keys with its own consistent-hash ring (150 virtual nodes). It does not use Redis Cluster.

Both paths use the same PostgreSQL database, the same seed, and the same container CPU and memory limits. Compare a baseline run with a protected run. Do not send both at once, or the shared database mixes the two results.

## Start

Docker Desktop needs to be running.

```powershell
cd hot-key-proxy
docker compose up -d --build
```

The origin seeds 100,000 products and 1,000,000 reviews on first boot. That can take a few minutes. The origin health check stays down until the seed finishes, and the proxies wait for it.

```powershell
curl http://localhost:8081/actuator/health
curl http://localhost:8080/api/products/1
curl http://localhost:8081/api/products/1
```

A protected response includes `X-Cache` (`HIT`, `MISS`, `STALE`, `COALESCED`, or `BYPASS`), `X-Hot-Key`, and `X-Proxy-Instance`. Repeating the same product should move from `MISS` to `HIT`. Concurrent misses share one origin read and the waiters are marked `COALESCED`.

A smaller catalog is enough for a first look:

```powershell
$env:SEED_PRODUCTS = "10000"
$env:SEED_REVIEWS = "50000"
docker compose up -d --build
```

`docker compose down -v` drops the database volume so the next start seeds again. Keep `GATEWAY_PRODUCT_COUNT` aligned with `SEED_PRODUCTS`; Compose already passes the same value through.

## Read path

1. The gateway hashes the product id and forwards the request to the owning proxy. If that proxy is down, it tries the next instance on the ring and, if none are up, the origin.
2. A fresh cache hit returns immediately. Promoted hot keys can also be served from a small in-process cache.
3. A stale hit returns immediately and one background refresh runs for that key.
4. A miss joins the in-flight map. One leader calls the origin. Waiters share that result, or after 50 ms they use a stale value or their own origin call.
5. The leader stores the body in Redis with jittered TTL. Failures are not cached. A 404 is negative-cached for 5 seconds.
6. The hot-key detector counts requests in a 5 second sliding window. Crossing the threshold (default 500 rps) promotes the key and extends its TTL. It is demoted only after the rate falls below half the threshold.
7. Origin calls take a permit from a bulkhead of 20, with a queue of 100. Beyond that the proxy returns HTTP 429 and `Retry-After`. If a stale value exists, it is served instead.
8. If Redis is unreachable, the proxy skips the cache and still coalesces origin reads. `X-Cache` is `BYPASS`. Clients do not see a Redis error.

Writes are forwarded and then invalidate the affected key: `PUT /api/products/{id}` drops `product:{id}`, and `POST /api/products/{id}/reviews` drops `reviews:{id}`.

## API

The gateway and the proxies expose the same catalog API as the origin.

```http
GET  /api/products/{id}
GET  /api/products/{id}/reviews
PUT  /api/products/{id}
POST /api/products/{id}/reviews
POST /api/config/hot-key-threshold
GET  /api/admin/hot-keys
GET  /api/metrics
```

`POST /api/config/hot-key-threshold` with `{"threshold": 200}` updates every proxy without a restart. `POST /api/config` can also change coalesce wait and per-type TTLs. The gateway fans both calls out to all instances. Values reset to `application.yml` on process restart.

```yaml
proxy:
  policies:
    product: { ttl: 60s, stale-window: 30s, hot-ttl: 300s }
    reviews: { ttl: 30s, stale-window: 15s, hot-ttl: 120s }
  coalescing: { max-wait: 50ms }
  hot-key: { threshold: 500, window: 5s }
  admission: { max-origin-concurrency: 20, queue-limit: 100 }
  hashing: { virtual-nodes-per-node: 150 }
```

Reviews returned by the origin are the 20 most recent rows for that product.

`ORIGIN_EXTRA_DELAY_MS` adds a fixed pause to origin reads when a machine's primary-key lookups are too cheap to saturate the pool of 20 connections. Leave it at 0 unless the baseline and protected runs look the same.

## Dashboard and benchmarks

http://localhost:8080 is the simulation page. It can run five workloads against either path and keeps the last baseline snapshot next to the last protected snapshot:

| Workload | What it does |
| --- | --- |
| W1 Uniform | Reads and writes spread across the catalog (about 95% reads) |
| W2 Hot-key spike | Most traffic hits product `1`, with background traffic on other ids |
| W3 Moving hotspot | The hot id changes every 30 seconds. Use a duration of at least 90 seconds |
| W4 Redis failure | The dashboard tells the proxies to bypass Redis, then turns caching back on |
| W5 Overload | Unique keys, so requests miss the cache and exercise the bulkhead |

W4 on the dashboard does not kill the Redis containers. The container test is:

```powershell
.\benchmarks\w4-redis-failure.ps1
```

k6 is the measurement harness. With Docker available and k6 optional:

```powershell
.\benchmarks\run-suite.ps1 -Duration 30s -SpikeVus 1000
```

The SRS hot-key target is 10,000 concurrent requests for one product. That is a large run:

```powershell
.\benchmarks\run-suite.ps1 -Duration 45s -SpikeVus 10000
```

Or directly:

```powershell
k6 run benchmarks/k6/w2-hotkey.js -e BASE_URL=http://localhost:8080 -e SPIKE_VUS=10000 -e PRODUCTS=100000
k6 run benchmarks/k6/w2-hotkey.js -e BASE_URL=http://localhost:8081 -e SPIKE_VUS=10000 -e PRODUCTS=100000
```

Grafana's **Hot-Key Proxy Comparison** dashboard plots origin query rate, cache hit ratio, p95/p99 latency, database connections, coalesced requests, 429s, and Redis bypass. Run baseline and protected as separate windows and compare those ranges. Summaries from the suite land in `benchmarks/results/`.

Useful Prometheus series:

- `proxy_cache_requests_total{result="hit|miss|stale|coalesced|bypass"}`
- `proxy_origin_queries_total`
- `proxy_coalesced_requests_total`
- `proxy_admission_rejections_total`
- `proxy_origin_concurrency`
- `hikaricp_connections_active`
- `pg_stat_activity_count`

Hit ratio is `(hit + stale + coalesced) / (hit + stale + coalesced + miss + bypass)`.

## Success criteria

1. A hot-key burst of 10,000 in-flight reads for one product should produce fewer than 5 origin queries. Check `proxy_origin_queries_total` and `GET /api/admin/hot-keys`.
2. After warmup, a uniform workload should keep the cache hit ratio above 85%.
3. A cache hit should add less than 2 ms at p99 versus the time spent outside the proxy. Promoted keys are served from the local cache specifically so this stays small.
4. With Redis stopped, product reads still return 200 and `X-Cache: BYPASS`. Restoring Redis resumes caching within a few seconds.
5. The same hot-key script against port 8081 should show higher origin qps, higher p99, and a full connection pool compared with port 8080.
6. During that spike, background product ids should keep a p99 close to their no-spike p99. The k6 background scenario and the dashboard's other-key p99 are there for that comparison.

## Tests

Unit tests cover the hash ring, singleflight, hot-key hysteresis, the origin bulkhead, cache hit/stale/bypass/429 behavior, and write invalidation. From each service directory, with Maven installed:

```powershell
mvn test
```

The services are built inside their Docker images with Maven 3.9 and Java 21, so a local Maven install is only needed to run the tests on the host.
