import socket

messages = [
        '393 <133>Mar 14 04:20:29 example-host-prod-1-1 audit type=SYSCALL msg=audit(1489465219.995:1699): arch=c000003e syscall=42 success=no exit=-2 a0=6 a1=7ffcf6c928a0 a2=6e a3=7ffcf6c92af0',
        ' items=0 ppid=12377 pid=14427 auid=4294967295 uid=107 gid=114 euid=107 suid=107 fsuid=107 egid=114 sgid=114 fsgid=114 tty=(none) ses=4294967295 comm="ps" exe="/opt/datadog-agent/embedded/bin/ps" key=(null)',
        '329 <133>Mar 14 04:20:29 example-host-prod-1-2 audit type=SOCKADDR msg=audit(1489465219.995:1699): saddr=01002F7661722F72756E2F6E7363642F736F636B65740000630B0B604A7F0000E02CC9F6FC7F0000060000000000000000000000000000000000000000000000002CC9F6FC7F0000CE120B604A7F000000000000FC7F00005B2BC9F6FC7F0000407D41604A7F00006C4E85604A',
        '7F'
]

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('localhost', 5678))

for msg in messages:
    s.send(msg)

s.close()
