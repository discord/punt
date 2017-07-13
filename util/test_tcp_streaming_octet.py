import socket

messages = [
    '98 <133>Mar 14 04:20:29 example-host-prod-1-1 audit type=SYSCALL msg=audit(1489465219.995:1699): test',
]

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('localhost', 5678))

for msg in messages:
    s.send(msg)

s.close()
