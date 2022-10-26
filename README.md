Awair Local to InfluxDB
=====================

This script pulls data from a local Awair Omni device and pushes it to an
InfluxDB 1.0 database.

## Configuration

This requires two configurations: one for the Awair device(s) and one for the
InfluxDB database.

### APsystems Configuration

`/etc/swarm-gateway/awairlocal.conf`:

```
ipaddress=192.168.1.1,192.168.1.2
```