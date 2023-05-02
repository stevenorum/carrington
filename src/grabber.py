import io
import logging
import sys
import time
import traceback

class Tee(object):
    def __init__(self, *args):
        self.destinations = args

    def write(self, *args, **kwargs):
        for destination in self.destinations:
            destination.write(*args, **kwargs)

    def flush(self, *args, **kwargs):
        pass

class Grabber(object):
    def __init__(self):
        self.stdout = None
        self.stderr = None
        self.logs = None
        self.duration = 0

    def __enter__(self):
        self._stdout_original = sys.stdout
        self._stderr_original = sys.stderr
        self._stdout_capture = io.StringIO()
        self._stderr_capture = io.StringIO()
        self._stdout_tee = Tee(self._stdout_original, self._stdout_capture)
        self._stderr_tee = Tee(self._stderr_original, self._stderr_capture)
        sys.stdout = self._stdout_tee
        sys.stderr = self._stderr_tee
        self._logging_capture = io.StringIO()
        self._logging_handler = logging.StreamHandler(self._logging_capture)
        self._logging_handler.setLevel(logging.DEBUG)
        self._logging_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(self._logging_handler)
        self.start_time = time.perf_counter()

    def __exit__(self, *args):
        self.end_time = time.perf_counter()
        self.stdout = self._stdout_capture.getvalue()
        self.stderr = self._stderr_capture.getvalue()
        self.logs = self._logging_capture.getvalue()
        sys.stdout = self._stdout_original
        sys.stderr = self._stderr_original
        logging.getLogger().removeHandler(self._logging_handler)
        self._stdout_capture.close()
        self._stderr_capture.close()
        self._logging_capture.close()
        self.duration = self.end_time - self.start_time

    def info(self):
        return {
            "stdout": self.stdout,
            "stderr": self.stderr,
            "logs": self.logs,
            "duration": self.duration
        }
