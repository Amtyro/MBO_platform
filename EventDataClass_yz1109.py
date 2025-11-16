"""
Event-driven framework of YQ 
author : Yvonne Zhou
time : 11/9/2025
"""

from collections import defaultdict
from collections.abc import Callable
from queue import Empty, Queue
from threading import Thread
from time import sleep
from typing import Any
import struct

# Timer event type
EVENT_TIMER = "eTimer" #?
MBO_STRUCT = struct.Struct("!Q I c B d d Q Q")
SIDE_ENCODE = {
    "":0,
    "B" : 1, # bid
    "A" : 2, # ask
    "S" : 3, # sell 
}

class Event:
    """
    Event object has:
    - type: a string used for routing
    - data: arbitrary payload (dict, dataclass, etc.)
    """

    def __init__(self, type: str, data: Any = None) -> None:
        self.type: str = type
        self.data: Any = data


# Handler function type
HandlerType = Callable[[Event], None]


class EventEngine:
    """
    Main functions:
    1. Distribute Event objects based on their type.
    2. Optionally generate periodic timer events.
    """

    def __init__(self, interval: int = 1) -> None:
        self._interval: int = interval               # seconds between timer events
        self._queue: Queue[Event] = Queue()
        self._active: bool = False
        self._thread: Thread = Thread(target=self._run, daemon=True)
        self._timer: Thread = Thread(target=self._run_timer, daemon=True)
        self._handlers: defaultdict[str, list[HandlerType]] = defaultdict(list)
        self._general_handlers: list[HandlerType] = []

    # ---------- internal loops ----------

    def _run(self) -> None:
        """Main loop: fetch events from queue and process them."""
        while self._active:
            try:
                event: Event = self._queue.get(block=True, timeout=1)
                self._process(event)
            except Empty:
                pass

    def _process(self, event: Event) -> None:
        """Dispatch event to specific-type handlers, then general handlers."""
        if event.type in self._handlers:
            for handler in self._handlers[event.type]:
                handler(event)

        for handler in self._general_handlers:
            handler(event)

    def _run_timer(self) -> None:
        """Generate timer events periodically."""
        while self._active:
            sleep(self._interval)
            event: Event = Event(EVENT_TIMER)
            self.put(event)

    # ---------- public API ----------

    def start(self) -> None:
        """Start the engine threads."""
        if self._active:
            return
        self._active = True
        self._thread.start()
        self._timer.start()

    def stop(self) -> None:
        """Stop the engine and wait for threads to finish."""
        if not self._active:
            return
        self._active = False
        self._timer.join()
        self._thread.join()

    def put(self, event: Event) -> None:
        """Put an event into the queue."""
        self._queue.put(event)

    def register(self, type: str, handler: HandlerType) -> None:
        """
        Register a handler for a specific event type.
        Each handler is added at most once per type.
        """
        handler_list = self._handlers[type]
        if handler not in handler_list:
            handler_list.append(handler)

    def unregister(self, type: str, handler: HandlerType) -> None:
        """Unregister a handler for a specific event type."""
        handler_list = self._handlers[type]
        if handler in handler_list:
            handler_list.remove(handler)
        if not handler_list:
            self._handlers.pop(type, None)

    def register_general(self, handler: HandlerType) -> None:
        """Register a handler for all events."""
        if handler not in self._general_handlers:
            self._general_handlers.append(handler)

    def unregister_general(self, handler: HandlerType) -> None:
        """Unregister a general handler."""
        if handler in self._general_handlers:
            self._general_handlers.remove(handler)
