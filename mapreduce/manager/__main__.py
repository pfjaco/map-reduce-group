"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import click
import mapreduce.utils
import threading
import shutil
import pathlib

# Configure logging
LOGGER = logging.getLogger(__name__)


class Job:
    """Store a job."""

    def __init__(self, job_id, input_directory, output_directory,
                 mapper_executable, reducer_executable,
                 num_mappers, num_reducers):
        """Initialize values for a job."""
        self.status = "not started"
        self.job_id = job_id
        self.input_directory = input_directory
        self.output_directory = output_directory
        self.mapper_executable = mapper_executable
        self.reducer_executable = reducer_executable
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.num_workers = 0
        self.assigned_workers = []
        self.tasks = []
        self.stage = "map"
        self.num_tasks_completed = 0
        # add a variable stating whether the job is in the map or reduce stage

    def add_task(self, new_task):
        """Add a task to the current job."""
        self.tasks.append(new_task)

    def next_task(self):
        """Get the next task to be completed."""
        for task in self.tasks:
            if task["status"] == "incomplete":
                return task

        return "all tasks are either in progress or are completed"

    def task_reset(self, task):
        """Change a task to incomplete."""
        for i in range(0, len(self.tasks)):
            if self.tasks[i]["task_id"] == task["task_id"]:
                self.tasks[i]["status"] = "incomplete"
                break

    def task_finished(self, task_id):
        """Mark a task as completed."""
        for i in range(0, len(self.tasks)):
            if self.tasks[i]["task_id"] == task_id:
                self.tasks[i]["status"] = "complete"
                break

    def switch_stage(self):
        """Check if manager can change status."""
        num_complete = 0
        for task in self.tasks:
            if task["status"] == "complete":
                num_complete += 1

        if num_complete == len(self.tasks):
            """
            if self.stage == "map":
                self.stage = "reduce"
            elif self.stage == "reduce":
                self.stage = "complete"
            """
            return True

        return False

    def get_status(self):
        """Return current status of a worker."""
        return self.status

    def set_status(self, new_status):
        """Set current status of a worker."""
        self.status = new_status

    def get_job_id(self):
        """Return the job id."""
        return self.job_id

    def get_assigned_workers(self):
        """Return workers assigned to a job."""
        return self.assigned_workers

    def get_tasks(self):
        """Return tasks for a job."""
        return self.tasks

    def get_mapper_executable(self):
        """Return mapper executable for a job."""
        return self.mapper_executable

    def get_input_directory(self):
        """Return input directory for a job."""
        return self.input_directory

    def get_output_directory(self):
        """Return output directory for a job."""
        return self.output_directory

    def get_reducer_executable(self):
        """Return reducer executable for a job."""
        return self.reducer_executable

    def get_num_mappers(self):
        """Return number of mappers for a job."""
        return self.num_mappers

    def get_num_reducers(self):
        """Return number of reducers for a job."""
        return self.num_reducers

    def get_stage(self):
        """Return the current stage of a job."""
        return self.stage

    def set_stage(self, new_stage):
        """Change the stage for a job."""
        self.stage = new_stage

    def set_num_tasks_completed(self):
        """Increase the number of completed tasks by 1."""
        self.num_tasks_completed += 1

    def get_num_completed_tasks(self):
        """Return the number of completed tasks."""
        return self.num_tasks_completed


class RemoteWorker:
    """Class to store information about a worker."""

    def __init__(self, host, port, status):
        """Initialize worker's values."""
        self.host = host
        self.port = port
        self.status = status
        self.task_info = {"partitions": [], "task_id": -1,
                          "status": "incomplete", "task_type": "map"}
        self.last_heartbeat = time.time()

    def get_host(self):
        """Return worker's host."""
        return self.host

    def get_port(self):
        """Return worker's port."""
        return self.port

    def get_status(self):
        """Return worker's status."""
        return self.status

    def set_status(self, new_status):
        """Change worker's status."""
        self.status = new_status

    def unassign_task(self):
        """Unassign a worker's task."""
        self.task_info = {"partitions": [], "task_id": -1,
                          "status": "incomplete", "task_type": "map"}

    def get_task_info(self):
        """Return the task a worker does."""
        return self.task_info

    def assign_task(self, new_task):
        """Assign a worker a task."""
        self.task_info = new_task

    def set_last_heartbeat(self, new_heartbeat):
        """Set the last heartbeat of a worker to a given value."""
        self.last_heartbeat = new_heartbeat

    def get_last_heartbeat(self):
        """Return the worker's last heartbeat."""
        return self.last_heartbeat

    def check_worker_status(self):
        """Check if the worker is still alive."""
        now = time.time()
        return now - self.last_heartbeat <= 10


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        self.port = port
        self.host = host
        self.workers = []
        self.jobs = []
        self.messages = []
        self.shutdown = {"shutdown": False}
        self.task_complete = {"complete": False, "message": ""}
        # self.job_finished: {"job_finished": False}
        self.partition_info = []
        current_message = {"message_type": ""}
        # self.tmpdir = ""
        self.dead = {"dead": False}
        self.completed_tasks = []
        self.dead_workers = []
        self.added_workers = []
        self.receive_heartbeat_thread = threading.Thread(
            target=mapreduce.utils.udp_receive,
            args=(self.handle_udp_messages, self.host,
                  self.port, self.shutdown))
        self.fault_tolerance_thread = threading.Thread(
            target=self.handle_fault_tolerance)
        self.manager_tcp_thread = threading.Thread(
            target=mapreduce.utils.tcp_receive,
            args=(self.handle_manager_message, self.host,
                  self.port, self.shutdown))
        self.manager_tcp_thread.start()
        self.job_number = 0

        while current_message["message_type"] != "shutdown":
            if len(self.messages) > 0:
                current_message = self.messages[0]
                self.messages.pop(0)

                """
                if current_message["message_type"] == "register":
                    self.add_worker(current_message)
                """

                if current_message["message_type"] == "new_manager_job":
                    self.jobs.append(Job(self.job_number,
                                         current_message["input_directory"],
                                         current_message["output_directory"],
                                         current_message["mapper_executable"],
                                         current_message["reducer_executable"],
                                         current_message["num_mappers"],
                                         current_message["num_reducers"]))
                    current_job = self.jobs[0]
                    self.job_number += 1
                    self.run_job(current_job, current_message)
                    self.jobs.pop(0)
                    self.partition_info = []

                    for i in range(0, len(self.workers)):
                        self.workers[i].unassign_task()

                """
                elif current_message["message_type"] == "finished":
                    current_job = self.jobs[0]
                    self.handle_finished_task(current_job, current_message)
                """

            time.sleep(0.1)

        self.handle_shutdown()

        if self.manager_tcp_thread.is_alive():
            self.manager_tcp_thread.join()

        if self.receive_heartbeat_thread.is_alive():
            self.receive_heartbeat_thread.join()

        if self.fault_tolerance_thread.is_alive():
            self.fault_tolerance_thread.join()

        return

    def handle_manager_message(self, message):
        """Handle TCP messages."""
        temp = "shutdown"
        temp2 = "new_manager_job"
        if message["message_type"] == temp or message["message_type"] == temp2:
            self.messages.append(message)

        if message["message_type"] == "shutdown":
            self.shutdown["shutdown"] = True

        elif message["message_type"] == "register":
            self.add_worker(message)

        elif message["message_type"] == "finished":
            self.completed_tasks.append(message)

    def handle_udp_messages(self, message):
        """Handle UDP heartbeat messages."""
        if message["message_type"] == "heartbeat":
            for i in range(0, len(self.workers)):
                temp_host = self.workers[i].get_host()
                temp_port = self.workers[i].get_port()
                s1 = "worker_host"
                s2 = "worker_port"
                if temp_host == message[s1] and temp_port == message[s2]:
                    self.workers[i].set_last_heartbeat(time.time())
                    break

    def add_worker(self, worker_message):
        """Add worker to list of workers that the manager keeps track of."""
        worker_host = worker_message["worker_host"]
        worker_port = worker_message["worker_port"]
        # self.workers.append(RemoteWorker(worker_host, worker_port, "ready"))

        if len(self.workers) == 0:
            self.workers.append(RemoteWorker(worker_host, worker_port,
                                             "ready"))

        else:
            worker_found = False
            for i in range(0, len(self.workers)):
                bool1 = self.workers[i].get_host() == worker_host
                bool2 = self.workers[i].get_port() == worker_port
                if bool1 and bool2:
                    self.workers[i].set_status("ready")
                    self.workers[i].set_last_heartbeat(time.time())
                    worker_found = True
                    break

            if not worker_found:
                self.workers.append(RemoteWorker(worker_host, worker_port,
                                                 "ready"))

        tcp_message = json.JSONEncoder().encode(
            {"message_type": "register_ack"})
        mapreduce.utils.tcp_send(tcp_message, worker_host, worker_port)

        bool1 = self.receive_heartbeat_thread.is_alive()
        bool2 = self.fault_tolerance_thread.is_alive()
        if not bool1 and not bool2:
            self.receive_heartbeat_thread.start()
            self.fault_tolerance_thread.start()

        if len(self.jobs) > 0 and self.jobs[0].get_status() == "job running":
            self.added_workers.append(True)

    def run_job(self, current_job, message):
        """Run a job to completion or until a shutdown message occurs."""
        current_job.set_status("job running")
        prefix = f"mapreduce-shared-job{current_job.get_job_id():05d}-"
        input_path = pathlib.Path(message["input_directory"])

        # if os.path.exists(message["output_directory"]):
        if pathlib.Path(message["output_directory"]).exists():
            shutil.rmtree(message["output_directory"])

        os.mkdir(message["output_directory"])
        # self.tmpdir = tempfile.TemporaryDirectory(prefix=prefix)
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            files = []

            for i in range(0, message["num_mappers"]):
                self.partition_info.append(
                    {"task_id": i, "files": [],
                     "status": "incomplete", "stage": current_job.get_stage()})

            for file in os.listdir(message["input_directory"]):
                files.append(file)

            files = list(sorted(files))

            """splitting the files up into partitions"""
            while len(files) > 0:
                for i in range(0, len(self.partition_info)):
                    if len(files) == 0:
                        break

                    json_style_path = mapreduce.utils.PathJSONEncoder.default(
                        json.JSONEncoder, input_path/files[0])
                    self.partition_info[i]["files"].append(json_style_path)
                    # current_job.add_task(self.partition_info[i])
                    # # possibly prone to bugs, address this later
                    files.pop(0)

            for partition in self.partition_info:
                current_job.add_task(partition)

            self.assign_tasks(current_job, tmpdir)
            """assigning each worker a task if it's incomplete"""

            # consider if a new worker registers
            while not self.shutdown["shutdown"] and (
                    current_job.get_stage() != "complete"):
                if len(self.added_workers) > 0:
                    self.assign_tasks(current_job, tmpdir)
                    self.added_workers.pop(0)

                if len(self.completed_tasks) > 0:
                    # if self.task_complete["complete"]
                    # and self.task_complete["message"] != "":
                    # completed_message = self.task_complete["message"]
                    completed_message = self.completed_tasks[0]

                    # current_job.task_finished(completed_message["task_id"])
                    # current_job.set_num_tasks_completed()
                    t = "task_id"
                    s = "status"
                    # self.partition_info[completed_message[t]][s] = "complete"

                    for i in range(0, len(self.partition_info)):
                        if self.partition_info[i][t] == completed_message[t]:
                            self.partition_info[i][s] = "complete"
                            current_job.task_finished(completed_message[t])

                    for i in range(0, len(self.workers)):
                        s = self.workers[i].get_host()
                        c = completed_message["worker_host"]
                        c2 = completed_message["worker_port"]
                        if s == c and self.workers[i].get_port() == c2:
                            self.workers[i].set_status("ready")
                            break

                    # length = len(current_job.get_tasks())
                    if current_job.switch_stage():
                        if current_job.get_stage() == "map":
                            current_job.set_stage("reduce")
                        elif current_job.get_stage() == "reduce":
                            current_job.set_stage("complete")

                        if current_job.get_stage() == "reduce":
                            self.partition_info = []
                            self.assign_reduce_tasks(current_job, tmpdir)

                        """
                        elif current_job.get_stage() == "complete":
                            # self.tmpdir.cleanup()
                            # self.tmpdir = ""
                            # self.jobs.pop(0)
                            # self.partition_info = []
                            break
                        """

                    self.assign_tasks(current_job, tmpdir)
                    self.completed_tasks.pop(0)

                # elif self.dead["dead"]:
                if (len(self.dead_workers)) > 0:
                    self.assign_tasks(current_job, tmpdir)
                    self.dead_workers.pop(0)

                time.sleep(0.1)

    def assign_tasks(self, current_job, tmpdir):
        """Give workers map or reduce tasks."""
        for i in range(0, len(self.partition_info)):
            for x in range(0, len(self.workers)):
                bool1 = self.partition_info[i]["status"] == "incomplete"
                bool2 = self.workers[x].get_status() == "ready"
                if bool1 and bool2:
                    tcp_message = ""

                    if current_job.get_stage() == "map":
                        tcp_message = json.JSONEncoder().encode(
                            {"message_type": "new_map_task",
                             "task_id": self.partition_info[i]["task_id"],
                             "executable": current_job.get_mapper_executable(),
                             "input_paths": self.partition_info[i]["files"],
                             "output_directory": tmpdir,
                             "num_partitions": current_job.get_num_reducers()})
                        self.partition_info[i]["status"] = "working on task"

                    elif current_job.get_stage() == "reduce":
                        o = "output_directory"
                        e = "executable"
                        tcp_message = json.JSONEncoder().encode(
                            {"message_type": "new_reduce_task",
                             "task_id": self.partition_info[i]["task_id"],
                             e: current_job.get_reducer_executable(),
                             "input_paths": self.partition_info[i]["files"],
                             o: current_job.get_output_directory()})
                        self.partition_info[i]["status"] = "working on task"

                    try:
                        mapreduce.utils.tcp_send(tcp_message,
                                                 self.workers[x].get_host(),
                                                 self.workers[x].get_port())
                        self.workers[x].set_status("busy")
                        self.workers[x].assign_task(self.partition_info[i])
                        # current_job.add_task(self.partition_info[i])
                        # # keep track of which tasks pertain to a given job

                    except ConnectionRefusedError:
                        self.workers[x].set_status("dead")
                        self.partition_info[i]["status"] = "incomplete"
                        self.dead_workers.append(True)

    def assign_reduce_tasks(self, current_job, tmpdir):
        """Determine files for reduce tasks."""
        for i in range(0, current_job.get_num_reducers()):
            self.partition_info.append(
                {"task_id": i,
                 "files": list(pathlib.Path(tmpdir).glob(f"*-part{i:05d}")),
                 "status": "incomplete", "stage": current_job.get_stage()})

        for i in range(0, len(self.partition_info)):
            for j in range(0, len(self.partition_info[i]["files"])):
                string = str(self.partition_info[i]["files"][j])
                self.partition_info[i]["files"][j] = string

            self.partition_info[i]["files"] = sorted(
                self.partition_info[i]["files"])
            current_job.add_task(self.partition_info[i])

    def handle_shutdown(self):
        """Send shutdown messages to non dead workers."""
        message = json.JSONEncoder().encode({"message_type": "shutdown"})

        for worker in self.workers:
            if worker.get_status() != "dead":
                mapreduce.utils.tcp_send(message, worker.get_host(),
                                         worker.get_port())

    def handle_fault_tolerance(self):
        """Deal with dead workers."""
        while not self.shutdown["shutdown"]:
            for i in range(0, len(self.workers)):
                bool1 = self.workers[i].get_status() != "dead"
                if not self.workers[i].check_worker_status() and bool1:
                    self.workers[i].set_status("dead")
                    task = self.workers[i].get_task_info()

                    for j in range(0, len(self.partition_info)):
                        temp1 = self.partition_info[j]["task_id"]
                        temp2 = task["task_id"]
                        temp3 = self.partition_info[j]["status"]
                        if temp1 == temp2 and temp3 != "complete":
                            self.partition_info[j]["status"] = "incomplete"
                            # self.reassign_task(self.jobs[0])
                            break
                    s = "worker %s at port %d died"
                    LOGGER.info(s % (self.workers[i].get_host(),
                                     self.workers[i].get_port()))
                    self.workers[i].unassign_task()
                    # self.dead["dead"] = True
                    self.dead_workers.append(True)


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
