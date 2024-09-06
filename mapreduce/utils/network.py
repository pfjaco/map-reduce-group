"""File to handle TCP and UDP messages."""

import socket
import json
import logging

LOGGER = logging.getLogger(__name__)


def tcp_send(message, host, port):
    """Both the manager and workers use this function to send messages."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        sock.sendall(message.encode("utf-8"))


def tcp_receive(callback_function, host, port, shutdown):
    """Both the manager and worker use this function to receive messages."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # Bind the socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen()

        # Socket accept() will block for a maximum of 1 second.  If you
        # omit this, it blocks indefinitely, waiting for a connection.
        sock.settimeout(1)

        while not shutdown["shutdown"]:
            # Wait for a connection for 1s.
            # The socket library avoids consuming
            # CPU while waiting for a connection.
            try:
                clientsocket, address = sock.accept()
            except socket.timeout:
                continue

            # Socket recv() will block for a maximum of 1 second.  If you omit
            # this, it blocks indefinitely, waiting for packets.
            clientsocket.settimeout(1)

            # Receive data, one chunk at a time.  If recv() times out before we
            # can read a chunk, then go back to the top of the loop and try
            # again.  When the client closes the connection, recv() returns
            # empty data, which breaks out of the loop.  We make a simplifying
            # assumption that the client will always cleanly close the
            # connection.
            with clientsocket:
                message = []
                while True:
                    try:
                        data = clientsocket.recv(4096)
                    except socket.timeout:
                        continue
                    if not data:
                        break
                    message.append(data)

            # Decode list-of-byte-strings to UTF8 and parse JSON data
            message_bytes = b''.join(message)
            message_str = message_bytes.decode("utf-8")

            try:
                message = json.loads(message_str)
                # return message
                # LOGGER.info(message)
                callback_function(message)

            except json.JSONDecodeError:
                continue


def udp_send(message, host, port):
    """Worker sends its heartbeat to the manager."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect((host, port))
        sock.sendall(message.encode("utf-8"))


def udp_receive(callback_function, host, port, shutdown):
    """Manage receiving the heartbeat message from a worker."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

        # Bind the UDP socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.settimeout(1)

        # No sock.listen() since UDP doesn't establish connections like TCP

        # Receive incoming UDP messages
        while not shutdown["shutdown"]:
            try:
                message_bytes = sock.recv(4096)
            except socket.timeout:
                continue
            message_str = message_bytes.decode("utf-8")
            message = json.loads(message_str)
            # return message
            callback_function(message)
            # LOGGER.info(message)

            # LOGGER.info(message)
