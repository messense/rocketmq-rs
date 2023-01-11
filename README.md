# rocketmq-rs

[![GitHub Actions](https://github.com/messense/rocketmq-rs/workflows/CI/badge.svg)](https://github.com/messense/rocketmq-rs/actions?query=workflow%3ACI)
[![codecov](https://codecov.io/gh/messense/rocketmq-rs/branch/master/graph/badge.svg)](https://codecov.io/gh/messense/rocketmq-rs)
[![Crates.io](https://img.shields.io/crates/v/rocketmq.svg)](https://crates.io/crates/rocketmq)
[![docs.rs](https://docs.rs/rocketmq/badge.svg)](https://docs.rs/rocketmq)

Rust client for Apache RocketMQ.

**Please use the official RocketMQ Rust client in [rocketmq-clients](https://github.com/apache/rocketmq-clients/tree/master/rust)**.

## Features

`rocketmq-rs` is currently **working in progress**, it supports:

* Send message in asynchronous/oneway mode
* Send batch messages in asynchronous/oneway mode
* ACL

features to be implemented:

* [ ] Send orderly messages
* [ ] Consume messages using push model
* [ ] Consume messages using pull model
* [ ] Message tracing
* [ ] ...

## License

This work is released under the Apache-2.0 license. A copy of the license is provided in the [LICENSE](./LICENSE) file.
