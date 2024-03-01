class ErrorLogger:
    def __init__(self, thread_lock, errors_q, purgatory_q=None, quarantine_q=None):
        self.thread_lock = thread_lock
        self.purgatory_q = purgatory_q
        self.errors_q = errors_q
        self.quarantine_q = quarantine_q

    def log_to_purgatory(self, msg, error_string):
        try:
            with self.thread_lock:
                msg.update({"reason_for_failure": error_string})
                self.purgatory_q.put(msg)
        except Exception:
            raise

    def log_to_errors(self, entity_identifier, error_str, stack_trace, entity_type):
        try:
            with self.thread_lock:
                # print(error_str, stack_trace)
                self.errors_q.put({"entity_identifier": entity_identifier,
                                   "entity_type": entity_type,
                                   "error": error_str,
                                   "stack_trace": stack_trace.replace("\x00", "\uFFFD")})
        except Exception:
            raise

    def log_to_quarantine(self, quarantine_obj):
        with self.thread_lock:
            self.quarantine_q.put(quarantine_obj)