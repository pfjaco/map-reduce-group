"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import mapreduce.utils
import threading
import subprocess
import tempfile
import hashlib
import shutil
import pathlib
import heapq
import contextlib
# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )

        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.shutdown = {"shutdown": False}
        self.messages = []
        current_message = {"message_type": ""}
        # self.message = {"message_type": ""}
        self.task_running = {"running": False}

        self.send_heartbeat_thread = threading.Thread(
            target=self.send_heartbeat_message)
        self.worker_tcp_thread = threading.Thread(
            target=mapreduce.utils.tcp_receive,
            args=(self.handle_worker_message, self.host, self.port,
                  self.shutdown))
        self.worker_tcp_thread.start()
        # include listening thread for worker

        register_message = json.JSONEncoder().encode(
            {"message_type": "register", "worker_host": self.host,
             "worker_port": self.port})
        mapreduce.utils.tcp_send(register_message, self.manager_host,
                                 self.manager_port)

        while current_message["message_type"] != "shutdown":
            if len(self.messages) > 0:
                current_message = self.messages[0]
                self.messages.pop(0)

                if current_message["message_type"] == "new_map_task":
                    self.map(current_message)

                elif current_message["message_type"] == "new_reduce_task":
                    self.reduce(current_message)

            time.sleep(0.1)

        if self.worker_tcp_thread.is_alive():
            self.worker_tcp_thread.join()

        if self.send_heartbeat_thread.is_alive():
            self.send_heartbeat_thread.join()

        return

    def handle_worker_message(self, message):
        """Handle TCP messages."""
        if message["message_type"] != "register_ack":
            self.messages.append(message)

        """
        if message["message_type"] == "register_ack":
            # self.send_heartbeat_thread.start()
            pass

        elif message["message_type"] == "new_map_task":
            self.map()

        elif message["message_type"] == "new_reduce_task":
            self.reduce()
        """

        if message["message_type"] == "shutdown":
            self.shutdown["shutdown"] = True

        if message["message_type"] == "register_ack":
            self.send_heartbeat_thread.start()

    def send_heartbeat_message(self):
        """Send heartbeat messages to the manager."""
        while not self.shutdown["shutdown"]:
            heartbeat_message = json.JSONEncoder().encode(
                {"message_type": "heartbeat", "worker_host": self.host,
                 "worker_port": self.port})
            mapreduce.utils.udp_send(heartbeat_message, self.manager_host,
                                     self.manager_port)
            time.sleep(2)

    def map(self, message):
        """Complete a map task."""
        # add a check to see if the shutdown signal was received
        # if not self.shutdown["shutdown"]:
        files = message["input_paths"]
        tempfiles = []

        prefix = f"mapreduce-local-task{message['task_id']:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            for i in range(0, message["num_partitions"]):
                t = f"{tmpdir}/maptask{message['task_id']:05d}-part{i:05d}"
                tempfiles.append(t)

            for file in files:
                with open(file, 'r') as infile:
                    with subprocess.Popen(
                        [message["executable"]],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as map_process:
                        with contextlib.ExitStack() as stack:
                            tempfilesx = [stack.enter_context(open(name, "a"))
                                          for name in tempfiles]
                            for line in map_process.stdout:
                                key = line.split("\t")
                                hexdigest = hashlib.md5(key[0].encode(
                                    "utf-8")).hexdigest()
                                keyhash = int(hexdigest, base=16)
                                partition_number = keyhash % message[
                                    "num_partitions"]
                                tempfilesx[partition_number].write(line)

            for f in tempfiles:
                path = pathlib.Path(tmpdir, f)
                path2 = pathlib.Path(message["output_directory"])
                subprocess.run(["sort", "-o", f, f], check=True)
                # new_path = pathlib.Path(message["output_directory"], f)
                shutil.move(f, message["output_directory"])

        if not self.shutdown["shutdown"]:
            tcp_message = json.JSONEncoder().encode(
                {"message_type": "finished", "task_id": message["task_id"],
                 "worker_host": self.host, "worker_port": self.port})
            mapreduce.utils.tcp_send(tcp_message, self.manager_host,
                                     self.manager_port)

    def reduce(self, message):
        """Complete a reduce task."""
        # add a check to see if the shutdown signal was received
        # if not self.shutdown["shutdown"]:
        files = []
        for file in message["input_paths"]:
            files.append(open(file))

        merged = heapq.merge(*files)

        prefix = f"mapreduce-local-task{message['task_id']:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            outfile = open(f"{tmpdir}/part-{message['task_id']:05d}", "a")
            path = pathlib.Path(outfile.name)
            with subprocess.Popen(
                [message["executable"]],
                text=True,
                stdin=subprocess.PIPE,
                stdout=outfile,
            ) as reduce_process:
                for line in merged:
                    reduce_process.stdin.write(line)

            path = pathlib.Path(tmpdir, outfile.name)
            path2 = pathlib.Path(message["output_directory"])
            shutil.move(path, path2)

        if not self.shutdown["shutdown"]:
            tcp_message = json.JSONEncoder().encode(
                {"message_type": "finished", "task_id": message["task_id"],
                    "worker_host": self.host, "worker_port": self.port})
            mapreduce.utils.tcp_send(tcp_message, self.manager_host,
                                     self.manager_port)


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
