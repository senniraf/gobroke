# gobroke

An unfinished
[MQTT 5](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html) broker
written in go. This is only a hobby side project. It is far from being usable
and only a very limited functionality of MQTT is implemented yet.

## Completed MQTT functionality

* Application message to subscriber matching, based on a tree structure
* Topic filter wildcards
* CONNECT packet parsing
* PUBLISH packet parsing
* CONNACK packet writing
