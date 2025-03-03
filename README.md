# Wherobots Adapter for Harlequin

This repository provides the Harlequin adapter for WherobotsDB, using
the Wherobots Spatial SQL API and its
[wherobots-python-dbapi-driver](https://github.com/wherobots/wherobots-python-dbapi-driver).

## Installation

```
$ pip install harlequin-wherobots
```

## Usage

Procure an API key from Wherobots, and start Harlequin with the
required parameters:

```
$ harlequin -a wherobots --api-key <key>
```

Alternatively, you can use your session token:

```
$ harlequin -a wherobots --token <token>
```

The Harlequin adapter for Wherobots will automatically start a Wherobots
SQL session with the default runtime (Tiny, 4 executors) in the
default Wherobots public compute region (AWS `us-west-2`). You can
override those defaults with the `--runtime` and `--region` options,
respectively:

```
$ harlequin -a wherobots --api-key <key> --runtime MEDIUM --region AWS_US_WEST_2
```

> [!NOTE]
> Community Edition users of Wherobots Cloud are restricted to the
> "Tiny" runtime size. See our [Pricing](https://www.wherobots.com/pricing)
> for more information.

> [!WARNING]
> To prepare for the expansion of Wherobots Cloud to new regions and cloud providers, the `--region` parameter will become mandatory in a future SDK version.
> Before this support for new regions is added, we will release an updated version of the SDK.
> If you continue using an older SDK version, your existing commands for creating connections will still work.
> However, any new or existing SQL session you initialize without specifying the `--region` parameter will be hosted in the `aws-us-west-2` region.

## Advanced options

If your SQL session is already provisioned and running, you can force
the driver to directly connect to it via its WebSocket URL (without
protocol version):

```
$ harlequin -a wherobots --api-key <key> --ws-url <session-url>
```

You can also specify the base hostname of the Wherobots stack to
interact with as the first positional parameter. By default, the driver
connects to `cloud.wherobots.com`, the official public Wherobots
service.

```
$ harlequin -a wherobots --api-key <key> [host]
```
