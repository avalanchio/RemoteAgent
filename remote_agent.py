import http.client
import inspect
import json
import os
import platform
import shlex
import shutil
import ssl
import subprocess
import sys
import urllib.parse
from time import time, sleep, tzname
import socket
import pathlib
import configparser
import threading

help_text = """
This is simple avalanchio agent. 
It must always be dependent on the core libraries only without any external dependencies.
It polls task queue and executes it. 

topic: Optional field to pull data from a specific queue. 
       Valid topic: user, rule. Set the topic to rule to enable rule playbook execution
       If you do not specify any topic, any type of task would be executed
       You can run multiple remote_agent for task parallelism

---------------------------------------------------------------------------------
Property                  | Description                                         |
---------------------------------------------------------------------------------
token                     | Avalanchio Jwot token. Agent will use this token    |
                            for communication with avalanchio servers. If you 
                            do not set this value and base_url, the script will
                            try to check if environment variable (AIO_TOKEN)
                            and $HOME/aio/avalanchio.ini file for token as fall
                            back mechanism.  
--------------------------|-----------------------------------------------------|
base_url                  | Avalanchio endpoint, e.g. https://api.avalanchio.com
                            Like the token, if the value is not provided, the 
                            script searches the environment variable 
                            (AIO_BASE_URL) and $HOME/aio/avalanchio.ini. 
--------------------------|-----------------------------------------------------|
topic                     | [Optional] topic to subscribe to, e.g. user, user/1,|
                            rule, rule/1 etc. Essentially it is scan prefix to 
                            find the next task in the queue. 
--------------------------|-----------------------------------------------------|
disable_ssl_verification  | Disable SSL verification during the http client     |
                            communication
--------------------------|-----------------------------------------------------|
num_workers               | Maximum number of process workers to run at a given |
                            time. Default: 1
--------------------------|-----------------------------------------------------|
task_timeout_second       | [Optional] Timeout for the tasks. Set it to None    |
                            to disable the timeout check. Default: None
--------------------------|-----------------------------------------------------|
remote_agent_id           | [Optional] Id of the remote agent. Used for         |
                            tracking or blocking from the server side. Default
                            value is the host name. Default: <hostname>
--------------------------|-----------------------------------------------------|
"""
############## Update the settings in section based on your requirement ##########
token = None
base_url = None
topic = None
disable_ssl_verification = True
delete_working_directory = True
num_workers = 4
task_timeout_second = None
remote_agent_id = None
###################################################################################

current_directory = os.getcwd()
tmp_directory = os.getenv('AIO_TMP_DIRECTORY', os.path.join(current_directory, "tmp"))


def connection_info(profile = None):
    """
    This function is used to read base_url and token
    from ~/aio/avalanchio.ini file if they are not provided
    in the above global variables.
    """
    token_ = os.getenv("AIO_TOKEN")
    base_url_ = os.getenv("AIO_BASE_URL")
    ini_file = os.path.join(pathlib.Path.home(), "aio", "avalanchio.ini")
    if os.path.exists(ini_file):
        config = configparser.ConfigParser()
        config.read(ini_file)
        section = config.defaults() if profile is None else config[profile]
        if token_ is None and "authorization" in section:
            token_ = section["authorization"]
        if base_url_ is None and "base_url" in section:
            base_url_ = section["base_url"]
    return token_, base_url_



def to_str(s):
    if isinstance(s,str):
        return s
    elif isinstance(s, bytes):
        return s.decode()
    return str(s)


def agent_info():
    return {
        "os": platform.system(),
        "os_version": platform.release(),
        "timezone": str(tzname[0]),
        "name": socket.gethostname(),
    }

def execute_cmd(cmd, timeout = None, env = None):
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    response = dict()
    sp_start = time()
    try:
        sp = subprocess.run(cmd, text=True, timeout=timeout, env=env, capture_output=True)
        sp_duration = time() - sp_start
        response['duration'] = int(sp_duration * 1000)
        response["stdout"] = to_str(sp.stdout)
        response["stderr"] = to_str(sp.stderr)
        response["return_code"] = sp.returncode
        response["status"] = "Success"
    except Exception as e:
        response["status"] = "Failed"
        response["message"] = str(e)
    return response

def execute_python_script(working_directory:str, input_data:dict, code:str, timeout:int = None):

    input_s = json.dumps(input_data)

    error_file = os.path.join(working_directory, "error_file.json")
    output_file = os.path.join(working_directory, "output.txt")
    task_script_file = os.path.join(working_directory, 'task_script.py')
    executor_file = os.path.join(working_directory, 'executor.py')

    with open(task_script_file, 'w') as f:
        f.write(code)

    code = f"""
               import json
               import traceback
               import types
               import sys
               import os.path
               import base64
               
               input_s = '''{input_s}'''
               
               try:
                   from task_script import process
                   input_data = json.loads(input_s)
                   output = process(input_data)
                   if isinstance(output, dict):
                       with open('''{output_file}''', "w") as f:
                           json.dump(output, f, default=str)
                   elif output is not None:
                       with open('''{output_file}''', "w") as f:
                           f.write(str(output))
               except Exception as e:
                   stack = traceback.format_exc()
                   error_dict = dict(status = 'Failed', stderr = stack, error_type=str(type(e)))
                   with open('''{error_file}''', 'w') as f:
                       json.dump(error_dict, f)
           """
    clean_code = inspect.cleandoc(code)
    with open(executor_file, "w") as f:
        f.write(clean_code + "\n")
    os.chdir(working_directory)
    executable = os.getenv("PYTHON_EXECUTABLE", sys.executable)
    cmd = [executable, executor_file]
    try:
        res = execute_cmd(cmd, timeout=timeout)
        if os.path.isfile(output_file):
            with open(output_file) as f:
                res['response'] = f.read()
        elif os.path.isfile(error_file):
            with open(error_file) as f:
                res.update(json.load(f))
        return res
    finally:
        os.chdir(current_directory)


def execute_task(task_data:dict):
    """
    Execute a task based on received data.

    :param task_data: Dictionary containing task details
    :return: Execution result
    """
    # Extract task type and input
    task_type = (task_data.get('type') or '').lower()
    task_id = task_data.get('id')

    code = task_data.get('code')
    assert isinstance(code, str) and len(code)>0, 'code body must be non-empty'

    if task_type == 'shell_command':
        # Execute shell command safely
        print(f"Executing shell script: {task_id}")
        result = subprocess.run(
            code.split(),
            capture_output=True,
            text=True
        )
        return {
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode
        }

    elif task_type == 'python':
        # Execute Python script
        working_directory = os.path.join(tmp_directory, str(task_id))
        print(f"Executing python task: {task_id}, working directory: {working_directory}")
        os.makedirs(working_directory)
        try:
            params = task_data.get('data')
            if params is None:
                params = {}
            timeout = task_data.get("timeout", task_timeout_second)
            result = execute_python_script(working_directory=working_directory,
                                           input_data=params,
                                           code=code,
                                           timeout=timeout)
            return result
        finally:
            # Optional: Clean up output directory
            if delete_working_directory:
                shutil.rmtree(working_directory)
    else:
        raise RuntimeError(f'Unsupported task type: {task_type}')


class BackgroundTaskProcessor:

    def __init__(self, server_url, poll_interval:float, max_retries = 100):
        """
        Initialize the background task processor.

        :param server_url: Full URL of the server endpoint (e.g., 'example.com/tasks')
        :param poll_interval: Time between server checks (in seconds)
        :param max_retries: Maximum number of connection retry attempts
        """
        self.server_url = server_url
        self.poll_interval = poll_interval
        self.last_task_time = None
        self.max_retries = max_retries
        self.topic = topic
        self.conn = None
        self.token = token
        self.host = None
        self.scheme = None
        self.parse_url(server_url)
        self.running_processes = list()
        self.last_poll_ts = 0.0
        self.stop = threading.Event()
        self.shared_agent_info = False

    def parse_url(self, url):
        """
        Parse the server URL into host and path components.

        :param url: Full URL string
        :return: Dictionary with host and path
        """
        # Remove protocol if present
        if url.startswith('http://'):
            self.scheme = "http"
            url = url[7:]
        elif url.startswith('https://'):
            self.scheme = "https"
            url = url[8:]
        else:
            raise RuntimeError(f"Invalid url: {url}")

        # Split host and path
        parts = url.split('/', 1)
        self.host = parts[0]

    def make_conn(self):
        # Choose connection based on URL
        if self.scheme == 'https':
            ctx = None
            if disable_ssl_verification:
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            conn = http.client.HTTPSConnection(self.host, context=ctx)
        else:
            conn = http.client.HTTPConnection(self.host)
        return conn

    def _make_request_raw(self, method:str, path:str, params:dict = None, body:dict = None)->dict:
        conn = self.make_conn()
        try:
            headers = {'Accept': 'application/json',
                       'Content-Type': 'application/json',
                       'Authorization': f'Bearer {self.token}'}
            if params:
                query_string = urllib.parse.urlencode(params)
                path = f"{path}?{query_string}"
            if isinstance(body, dict):
                body = json.dumps(body)
            assert body is None or isinstance(body, str)
            conn.request(method, path, headers=headers, body = body)
            response = conn.getresponse()
            # Check successful response
            if response.status == 200:
                content = response.read()
                if content:
                    return json.loads(content.decode())
            else:
                content = response.read().decode()
                raise RuntimeError(str(response.status) + " " + content)
        finally:
            conn.close()

    def find_tasks(self, topic:str = None, size: int = 1):
        """
        Make HTTP request to server to fetch tasks.

        :return: Parsed JSON response or None
        """
        self.last_poll_ts = time()
        running_task_ids = [str(r[0].get('id')) for r in self.running_processes]
        params = dict()
        if topic:
            params['topic'] = topic
        params['size'] = size
        params['agent-id'] = remote_agent_id
        if running_task_ids:
            params['running'] = "|".join(running_task_ids)
        if not self.shared_agent_info:
            params['agent-info'] = json.dumps(agent_info())
            self.shared_agent_info = True
        path = "/api/v1/remote-agent/poll"
        # print("Polling for new tasks")
        for attempt in range(self.max_retries):
            try:
                return self._make_request_raw('GET', path, params)
            except Exception as e:
                print(f"Request attempt {attempt + 1} failed: {e}")
                sleep(5)  # Wait before retry

    def acknowledge_task(self, task_id:int):
        """
        Before processing the task, send an ack to the server
        The server changes the status of the task from Queue to Processing
        """
        return self._make_request_raw("PATCH", f"/api/v1/remote-agent/{task_id}/ack")

    def save_result(self, task_id:int, result:dict):
        """
        Send the task output upon completion.
        Upon received this request, the server marks the task as Complete
        and delete from the queue (default).
        """
        return self._make_request_raw("POST", f"/api/v1/remote-agent/{task_id}/response", body = result)

    # def is_task_stopped(self, task_id):
    #     res = self._make_request_raw("GET", f"/api/v1/remote-agent/{task_id}/is-stopped")
    #     return res and res.get('value')
    #
    # def check_status_loop(self, task_id:int, stop: threading.Event):
    #     while not stop.is_set():
    #         sleep(3.0)
    #         self.is_task_stopped(task_id)
    #     stop.set()


    def loop_task(self, task:dict):
        task_id = task.get('id')
        start_time = time()
        result = self.acknowledge_task(task_id)
        assert result.get('status') == 'Success', f'Failed to acknowledge task: {task}'
        result = execute_task(task)
        self.save_result(task_id, result)
        duration = time()-start_time
        print(f"Completed task {task_id}, duration: {duration:.2f}sec")


    def run(self):
        """
        Main processing loop to continuously check for tasks.
        """
        print(f"Background Task Processor started. Base URL: {self.server_url}, "
              f"working directory: {tmp_directory}, "
              f"num_workers: {num_workers}, "
              f"remote_agent_id: {remote_agent_id}")
        os.makedirs(tmp_directory, exist_ok=True)
        while not self.stop.is_set():
            try:
                self.running_processes = [r for r in self.running_processes if r[1].is_alive()]
                if len(self.running_processes) >= num_workers:
                    sleep(0.1)
                    continue
                tasks = self.find_tasks(self.topic, num_workers - len(self.running_processes))
                if isinstance(tasks, list) and tasks:
                    for task in tasks:
                        proc = threading.Thread(target=self.loop_task, args=(task,))
                        self.running_processes.append((task, proc, time()))
                        proc.start()
                elif time()-self.last_poll_ts < 1.0:
                    sleep(1.0)
            except KeyboardInterrupt:
                self.stop.set()
            except Exception as e:
                print(f"Unexpected error: {e}")
                sleep(3.0)
        print("Exiting agent")


def main():
    global token, base_url, topic, remote_agent_id
    token_, base_url_ = connection_info()
    if not token:
        token = token_
    if not base_url:
        base_url = base_url_
    if remote_agent_id is None:
        remote_agent_id = socket.gethostname()
    poll_interval = float(os.getenv("AGENT_POLL_INTERVAL", "1.0"))
    max_retries = int(os.getenv("AGENT_MAX_RETRIES", "100"))
    if not token:
        print("AIO_TOKEN is not found. Set the environment variable with the token.")
        exit(1)
    processor = BackgroundTaskProcessor(base_url, poll_interval=poll_interval, max_retries=max_retries)
    processor.run()


if __name__ == '__main__':
    main()