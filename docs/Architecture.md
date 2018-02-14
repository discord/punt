# Architecture

## Basic Messaging Flow

1. Messages get recieved by the syslog server over UDP or TCP
2. The syslog server parses the message line to extract the syslog fields / parts
  Success; message is placed into the provided message queue (blocking channel send)
  Failure; invalid message struct is placed into the error queue (nonblocking channel send)
3. Each ClusterWorker pulls a syslog message from the global queue and starts processing;
  1. It extracts the syslog tag and maps it to a type based on the configuration
  2. It passes the message to the configured Transformer, which returns a modified payload based on the transformation rules.
  3. It type-casts any fields based on the typecasting configuration
  4. It applies any configured mutators
  5. It adds a `@timestamp` field
  6. It adds a `punt-server` field
  7. It creates a `DatastorePayload` containing the message, and calls the `Write` method on all configured datastores
  8. It dispatches the payload to all subscribers of the type (used by the control master for "tailing"). This is a nonblocking channel send.
  9. It runs a goroutine which checks any alerts on the payload
