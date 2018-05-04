from datetime import datetime
import socket


def syslog_fmt(host, prio, tag, procid, contents):
    msg = '<{prio}>{date} {host} {tag}[{procid}]: {contents}'.format(
        prio=prio,
        date=datetime.utcnow().strftime('%b %d %H:%M:%S'),
        host=host,
        tag=tag,
        procid=procid,
        contents=contents,
    )

    return '{} {}'.format(len(msg), msg)


def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 5678))
    s.send(syslog_fmt(
        'example-host-prd-1-1',
        133,
        'json',
        1234,
        '{"field1": 123.456, "field2": "test"}',
    ))
    s.close()


main()
