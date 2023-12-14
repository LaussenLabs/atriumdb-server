import threading  # don't think its Java Threading


class WALManager:
    threads = {}
    files = {}

    def __init__(self, num_threads, idle_timeout: int):
        self.num_threads = num_threads
        self.idle_timeout = idle_timeout

    def spawn_thread(self, thread_id, directory):
        pass

    def stop_thread(self, thread_id):
        pass
