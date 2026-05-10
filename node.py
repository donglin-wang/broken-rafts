#!/usr/bin/env -S uv run --python 3.14t
import sys
import json
from typing import TypedDict, Optional, Callable, Any, Literal
from enum import StrEnum, IntEnum, auto
import threading

class ReadOperation(TypedDict):
    type: Literal["read"]
    key: str

class WriteOperation(TypedDict):
    type: Literal["write"]
    key: str
    value: Any

class CompareAndSetOperation(TypedDict):
    type: Literal["cas"]
    key: str
    value_from: Any
    value_to: Any

class LogEntry(TypedDict):
    term: int
    op: WriteOperation | CompareAndSetOperation | ReadOperation | None


class ErrorCode(IntEnum):
    TIMEOUT = 0
    NODE_NOT_FOUND = 1
    NOT_SUPPORTED = 10
    TEMPORARILY_UNAVAILABLE = 11
    MALFORMED_REQUEST = 12
    CRASH = 13
    ABORT = 14
    KEY_DOES_NOT_EXIST = 20
    KEY_ALREADY_EXISTS = 21
    PRECONDITION_FAILED = 22
    TXN_CONFLICT = 30


class MessageType(StrEnum):
    INIT = auto()
    INIT_OK = auto()
    TOPOLOGY = auto()
    TOPOLOGY_OK = auto()
    READ = auto()
    READ_OK = auto()
    BROADCAST = auto()
    BROADCAST_OK = auto()
    REPLICATE = auto()
    REPLICATE_OK = auto()
    ADD = auto()
    ADD_OK = auto()
    TXN = auto()
    TXN_OK = auto()
    WRITE = auto()
    WRITE_OK = auto()
    CAS = auto()
    CAS_OK = auto()
    ERROR = auto()
    REQUEST_VOTE = auto()
    REQUEST_VOTE_OK = auto()
    APPEND_ENTRIES = auto()
    APPEND_ENTRIES_OK = auto()
    HEARTBEAT = auto()
    HEARTBEAT_OK = auto()


MessageBody = TypedDict(
    "MessageBody",
    {
        "msg_id": int,
        "type": MessageType,
        "in_reply_to": Optional[int],
        "node_id": Optional[str],
        "node_ids": Optional[list[str]],
        "topology": Optional[dict[str, list[str]]],
        "message": Optional[int],
        "value": Optional[set[int]],
        "element": Optional[int],
        "delta": Optional[int],
        "txn": Optional[list[tuple[int, int, Any]]],
        "code": Optional[int],
        "text": Optional[str],
        "term": Optional[str],
        "candidate_id": Optional[str],
        "prev_log_index": Optional[int],
        "prev_log_term": Optional[int],
        "vote_granted": Optional[bool],
        "entries": Optional[list[LogEntry]],
        "success": Optional[bool],
        "leader_id": Optional[str],
        "leader_commit": Optional[int],
        "key": Optional[str],
        "from": Optional[str],
        "to": Optional[str],
        "last_log_index": Optional[int],
        "last_log_term": Optional[int],
        "match_index": Optional[int],
    },
)


class Message(TypedDict):
    src: int
    dest: int
    body: MessageBody


class RPCException(Exception):
    pass


class Node:
    def __init__(self):
        self.node_id: str | None = None
        self.neighbors: list[str] = []
        self.next_msg_id: int = 0
        self.handlers: dict[MessageType, Callable[[Message], None]] = {}
        self.lock = threading.Lock()
        self.background_tasks: list[threading.Thread] = []

    def send(self, dest: str, body: MessageBody, ignore_log: bool = False):
        message = json.dumps(
            {
                "src": self.node_id,
                "dest": dest,
                "body": body,
            }
        )
        if not ignore_log:
            self.log(f"Sending {message}")
        print(message, file=sys.stdout, flush=True)
        self.next_msg_id += 1

    def log(self, msg: str):
        print(msg, file=sys.stderr, flush=True)

    def main(self):
        for thread in self.background_tasks:
            thread.start()

        for line in sys.stdin:
            self.process_line(line)

    def process_line(self, line: str):
        try:
            message: Message = json.loads(line)
            message_type = message["body"]["type"]
            if message_type not in self.handlers:
                self.log(f"Handler missing for message {line.strip()}")
                return
            threading.Thread(
                target=self.handlers[message_type], args=(message,)
            ).start()
        except json.JSONDecodeError as json_error:
            self.log(f"JSON decode failed for {line}; {json_error}")
        except RPCException as rpc_exception:
            error_summary = f"RPC failed: {rpc_exception}"
            self.log(error_summary)
            self.send(
                message["src"],
                {
                    "type": MessageType.ERROR,
                    "code": ErrorCode.ABORT,
                    "in_reply_to": message["body"]["msg_id"],
                    "text": error_summary,
                },
            )
        except Exception as e:
            error_summary = f"Unknown error occurred"
            self.log(error_summary)
            self.send(
                message["src"],
                {
                    "type": MessageType.ERROR,
                    "code": ErrorCode.ABORT,
                    "in_reply_to": message["body"]["msg_id"],
                    "text": error_summary,
                },
            )
            raise e
