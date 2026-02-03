# Scope

## Language

Native rust
A high volume HTTP receiver designed to receive data sent from the following Vector Sinks to Kafka or directly to a loader process

## Sources

Data is received from one of these sources
https://vector.dev/docs/reference/configuration/sinks/http/
https://vector.dev/docs/reference/configuration/sinks/vector/ (gRPC)
But technically it can be any source (does not need to be Vector but that is the initial use case)

HTTP Auth can be any or all of

- client cert (local file or secret manager AWS or Hashicorp/OpenBAO)
- http header
- none

Data can also optionally be rejected or sent to a dlq if a configured JSON path Key or KVP is not present

Acceptable data is ony JSON. Non JSON is rejected

## Scale

This is CRTIICAL. This is for PB/s a day scale. So the hot path and resource use efficiency is Critical.

## Destinations

For production data is sent to a Kafka topic in highly optimised batches (our defaults are 10K)
Data is routed to a topic based on matching keys or a key being present. Or ideally (now its native) additional smart HOT PATH OPTIMISED (important!) language expressions. 
Direct to dfe-loader (see that project for )

## Libraries

Use https://github.com/hypersec-io/hs-rustlib

## Replaces

A https://vector.dev/ implementation based on the following:
/projects/dfe-receiver/reference/vector-receiver
Not a direct copy implementation  - thi sone is better

## Best practice Pattern to use and copy

https://github.com/hypersec-io/dfe-loader
Locally /project/dfe-loader (so you don't need to clone it)
This includes:

- how it builds a compound single scaling metric for keda
- use the same hot path optimised expression language if at all possible
- same memory capping
- same config cascade WITH auto polled update from config source
- And discuss other features of this project we should bring across

## Local spool

Use the hs-rustlib local spool if it is promoted in that repo yet

## Deployment

Production is Keda K8S and -> kafka (AutoHQ or Strimzi)
Test can be K8S or docker -> Kafka
Dev is Docker (receiver) -> loader -> Clickhouse

## Key design decisions and bake offs

- The http server we use
- Multi threading approach


## Refactor

- we are rebranding from HyperSec to HyperI, which will mean refactoring, doco changes and the common libs move from hs- to hyperi- (e.g. hyperi-rustlib)