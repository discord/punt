# Architecture

## Basic Messaging Flow

1. Messages get recieved by the syslog server over UDP or TCP
2. Syslog server parses message.
  Success; message is placed into the provided message queue (blocking channel send)
  Failure; invalid message struct is placed into the error queue (nonblocking channel send)
3. ClusterWorker pulls messages from the message queue
