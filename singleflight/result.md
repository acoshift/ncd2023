# singleflight benchmark

## control

```shell
$ wrk -c100 -t10 http://localhost:8080/f0
Running 10s test @ http://localhost:8080/f0
  10 threads and 100 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.51ms    2.78ms  59.39ms   91.02%
    Req/Sec    16.10k     4.67k   25.51k    85.80%
  1602486 requests in 10.02s, 180.33MB read
Requests/sec: 159948.87
Transfer/sec:     18.00MB
```

## no cache

```shell
$ wrk -c100 -t10 http://localhost:8080/f1
Running 10s test @ http://localhost:8080/f1
  10 threads and 100 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     5.22ms    8.02ms 203.13ms   93.02%
    Req/Sec     2.73k   673.90     3.97k    88.60%
  271869 requests in 10.02s, 30.59MB read
Requests/sec:  27143.19
Transfer/sec:      3.05MB
```

## singleflight

```shell
$ wrk -c100 -t10 http://localhost:8080/f2
Running 10s test @ http://localhost:8080/f2
  10 threads and 100 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.84ms    2.59ms  50.65ms   96.83%
    Req/Sec     6.49k     1.20k    8.57k    87.10%
  646032 requests in 10.01s, 72.70MB read
Requests/sec:  64556.20
Transfer/sec:      7.26MB
```

## cache

```shell
$ wrk -c100 -t10 http://localhost:8080/f3
Running 10s test @ http://localhost:8080/f3
  10 threads and 100 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.27ms    2.57ms  49.50ms   92.54%
    Req/Sec    18.75k     3.46k   25.46k    91.90%
  1866114 requests in 10.01s, 210.00MB read
Requests/sec: 186422.39
Transfer/sec:     20.98MB
```
