from threading import Lock
from queue import Queue
from typing import Dict, Any, Optional


class ErrorLogger:
    """
    A thread-safe error logging utility that logs messages to different queues
    based on the type of error encountered.

    Attributes:
        thread_lock (Lock): A threading lock to ensure safe concurrent access.
        errors_q (Queue): Queue for storing general error logs.
        purgatory_q (Optional[Queue]): Queue for messages awaiting further processing.
        quarantine_q (Optional[Queue]): Queue for quarantined messages due to errors.
    """

    def __init__(
            self,
            thread_lock: Lock,
            errors_q: Queue,
            purgatory_q: Optional[Queue] = None,
            quarantine_q: Optional[Queue] = None
    ) -> None:
        """
        Initializes the ErrorLogger with thread synchronization and message queues.

        Args:
            thread_lock (Lock): A threading lock for safe concurrent modifications.
            errors_q (Queue): Queue to store error logs.
            purgatory_q (Optional[Queue]): Queue for temporarily held messages.
            quarantine_q (Optional[Queue]): Queue for quarantined messages.
        """
        self.thread_lock = thread_lock
        self.purgatory_q = purgatory_q
        self.errors_q = errors_q
        self.quarantine_q = quarantine_q

    def log_to_purgatory(self, msg: Dict[str, Any], error_string: str) -> None:
        """
        Logs a message to the purgatory queue, adding a reason for failure.

        Args:
            msg (Dict[str, Any]): The message to be logged.
            error_string (str): The reason why the message is being sent to purgatory.
        """
        try:
            if self.purgatory_q is not None:
                with self.thread_lock:
                    msg.update({"reason_for_failure": error_string})
                    self.purgatory_q.put(msg)
        except Exception as e:
            raise RuntimeError("Failed to log message to purgatory queue.") from e

    def log_to_errors(self, entity_identifier: str, error_str: str, stack_trace: str, entity_type: str) -> None:
        """
        Logs error details into the error queue.

        Args:
            entity_identifier (str): Unique identifier of the entity that caused the error.
            error_str (str): Description of the error.
            stack_trace (str): Stack trace of the error occurrence.
            entity_type (str): The type/category of the entity.
        """
        try:
            with self.thread_lock:
                self.errors_q.put({
                    "entity_identifier": entity_identifier,
                    "entity_type": entity_type,
                    "error": error_str,
                    "stack_trace": stack_trace.replace("\x00", "\uFFFD")
                    # Replaces null characters with Unicode replacement char
                })
        except Exception as e:
            raise RuntimeError("Failed to log error message.") from e

    def log_to_quarantine(self, quarantine_obj: Dict[str, Any]) -> None:
        """
        Logs an object to the quarantine queue for further inspection.

        Args:
            quarantine_obj (Dict[str, Any]): The object to be placed in quarantine.
        """
        if self.quarantine_q is not None:
            with self.thread_lock:
                self.quarantine_q.put(quarantine_obj)
