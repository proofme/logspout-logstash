# logspout-logstash

Modified by me in this fork to passthrough log messages already in JSON format and add the docker specific fields added. 
Instead of creating a new JSON object and included the JSON-log message as escaped JSON in the 'message' field.

A minimalistic adapter for github.com/gliderlabs/logspout to write to Logstash UDP

Follow the instructions in https://github.com/gliderlabs/logspout/tree/master/custom on how to build your own Logspout container with custom modules. Basically just copy the contents of the custom folder and include:

```
import (
  _ "github.com/looplab/logspout-logstash"
  _ "github.com/gliderlabs/logspout/transports/udp"
)
```

in modules.go.

Use by setting `ROUTE_URIS=logstash://host:port` to the Logstash host and port for UDP.

In your logstash config, set the input codec to `json` e.g:

input {
  udp {
    port => 5000
    codec => json
  }
}

