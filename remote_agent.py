import http.client
import inspect
import json
import os
import shlex
import shutil
import subprocess
import sys
import traceback
import urllib.parse
from time import time, sleep

"""
This is simple avalanchio agent. 
It must always be dependent on the core libraries only without any external dependencies.
It polls task queue and executes it. 
"""

current_directory = os.getcwd()
tmp_directory = os.getenv('AIO_TMP_DIRECTORY', f"{current_directory}/tmp")

def to_str(s):
    if isinstance(s,str):
        return s
    elif isinstance(s, bytes):
        return s.decode()
    return str(s)

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
    except subprocess.TimeoutExpired as e:
        response["status"] = "TimedOut"
        response["stdout"] = to_str(e.stdout)
        response["stderr"] = to_str(e.stderr)
    except Exception as e:
        response["status"] = "Failed"
        response["message"] = str(e)
        stack = traceback.format_exc()
        response.update({'stack': stack, 'message': str(e), "status": "Failed", 'error_type': str(type(e))})
    return response

def execute_python_script(working_directory:str, input_data:dict, code:str):

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

               
               def main():    
                   from task_script import process
                   input_data = json.loads('''{input_s}''')
                   output = process(input_data)
                   if isinstance(output, dict):
                       with open('''{output_file}''', "w") as f:
                           json.dump(output, f, default=str)
                   elif isinstance(output, str):
                       with open('''{output_file}''', "w") as f:
                           f.write(output)


               if __name__ == "__main__":
                   try:
                       main()
                   except Exception as e:
                       stack = traceback.format_exc()
                       error_dict = dict(status = 'Failed', message = str(e) or str(type(e)), stack = stack, error_type=str(type(e)))
                       with open('''{error_file}''', 'w') as f:
                           json.dump(error_dict, f)
                       traceback.print_stack()
           """
    clean_code = inspect.cleandoc(code)
    with open(executor_file, "w") as f:
        f.write(clean_code + "\n")
    os.chdir(working_directory)
    executable = os.getenv("PYTHON_EXECUTABLE", sys.executable)
    cmd = [executable, executor_file]
    try:
        res = execute_cmd(cmd)
        if os.path.isfile(output_file):
            with open(output_file) as f:
                res['response'] = f.read()
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
    task_type = task_data.get('type')
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
            result = execute_python_script(working_directory=working_directory,
                                           input_data=params,
                                           code=code)
            return result
        finally:
            # Optional: Clean up output directory
            shutil.rmtree(working_directory)
    else:
        raise RuntimeError('Unsupported task type: {task_type}')


class BackgroundTaskProcessor:

    def __init__(self, server_url, token:str, topic:str, poll_interval:float, max_retries = 100):
        """
        Initialize the background task processor.

        :param server_url: Full URL of the server endpoint (e.g., 'example.com/tasks')
        :param poll_interval: Time between server checks (in seconds)
        :param max_retries: Maximum number of connection retry attempts
        """
        self.server_url = server_url
        self.poll_interval = poll_interval
        self.max_retries = max_retries
        self.topic = topic
        self.conn = None
        self.token = token
        self.host = None
        self.scheme = None
        self.parse_url(server_url)

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
            conn = http.client.HTTPSConnection(self.host)
        else:
            conn = http.client.HTTPConnection(self.host)
        return conn

    def _make_request_raw(self, method:str, path:str, params:dict = None, body:dict = None)->dict:
        self.conn = self.make_conn()
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
            self.conn.request(method, path, headers=headers, body = body)
            response = self.conn.getresponse()
            # Check successful response
            if response.status == 200:
                content = response.read()
                if content:
                    return json.loads(content.decode())
            else:
                raise RuntimeError(str(response.status) + " " + str(response.read()))
        finally:
            self.conn.close()

    def find_task(self, topic:str)->dict:
        """
        Make HTTP request to server to fetch tasks.

        :return: Parsed JSON response or None
        """
        path = "/api/v1/remote-agent/poll"
        for attempt in range(self.max_retries):
            try:
                return self._make_request_raw('GET', path, dict(topic = topic))
            except Exception as e:
                print(f"Request attempt {attempt + 1} failed: {e}")
                sleep(5)  # Wait before retry

    def acknowledge_task(self, task_id:int):
        return self._make_request_raw("PATCH", f"/api/v1/remote-agent/ack/{task_id}")

    def save_result(self, task_id:int, result:dict):
        return self._make_request_raw("POST", f"/api/v1/remote-agent/{task_id}/response", body = result)

    def loop_task(self):

        # Fetch tasks from server
        task = self.find_task(self.topic)
        if task :
            task_id = task.get('id')
            print(f"Processing task: {task.get('type')}, id: {task_id}")
            result = self.acknowledge_task(task_id)
            assert result.get('status') == 'Success', f'Failed to acknowledge task: {task}'
            result = execute_task(task)
            self.save_result(task_id, result)
            print(f"Completed task {task_id}")

    def run(self):
        """
        Main processing loop to continuously check for tasks.
        """
        assert isinstance(self.topic, str), "Task topic is not set"
        print(f"Background Task Processor started. Base URL: {self.server_url}")

        while True:
            try:
                self.loop_task()
                sleep(self.poll_interval)
            except KeyboardInterrupt:
                print("\nTask processor stopped by user.")
                break
            except Exception as e:
                print(f"Unexpected error: {e}")
                sleep(self.poll_interval)


def main():
    server_url = os.getenv("AIO_BASE_URL", "http://localhost:8080")
    token = os.getenv("AIO_TOKEN")
    topic = os.getenv("AIO_AGENT_TOPIC", "user")
    if not token:
        print("AIO_TOKEN is not found. Set the environment variable with the token.")
        exit(1)
    processor = BackgroundTaskProcessor(server_url, token, topic, poll_interval=1.0, max_retries=100)
    processor.run()


if __name__ == '__main__':
    main()