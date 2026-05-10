#!/usr/bin/env -S uv run --python 3.14t
import sys
import json
import threading
from typing import TypedDict, NotRequired, Callable, Any, Literal
from enum import StrEnum, IntEnum, auto


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
    TEMPORARILY_UNAVAILABLE = 11
    ABORT = 14
    KEY_DOES_NOT_EXIST = 20
    PRECONDITION_FAILED = 22


class MessageType(StrEnum):
    INIT = auto()
    INIT_OK = auto()
    READ = auto()
    READ_OK = auto()
    WRITE = auto()
    WRITE_OK = auto()
    CAS = auto()
    CAS_OK = auto()
    ERROR = auto()
    REQUEST_VOTE = auto()
    REQUEST_VOTE_OK = auto()
    APPEND_ENTRIES = auto()
    APPEND_ENTRIES_OK = auto()


class InitBody(TypedDict):
    type: Literal[MessageType.INIT]
    msg_id: NotRequired[int]
    node_id: str
    node_ids: list[str]


class InitOkBody(TypedDict):
    type: Literal[MessageType.INIT_OK]
    msg_id: NotRequired[int]
    in_reply_to: int


class RequestVoteBody(TypedDict):
    type: Literal[MessageType.REQUEST_VOTE]
    msg_id: NotRequired[int]
    term: int
    candidate_id: str
    leader_id: str | None
    last_log_index: int
    last_log_term: int


class RequestVoteOkBody(TypedDict):
    type: Literal[MessageType.REQUEST_VOTE_OK]
    msg_id: NotRequired[int]
    in_reply_to: int
    term: int
    vote_granted: bool


class AppendEntriesBody(TypedDict):
    type: Literal[MessageType.APPEND_ENTRIES]
    msg_id: NotRequired[int]
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit: int


class AppendEntriesOkBody(TypedDict):
    type: Literal[MessageType.APPEND_ENTRIES_OK]
    msg_id: NotRequired[int]
    in_reply_to: int
    term: int
    success: bool
    leader_id: NotRequired[str | None]
    match_index: NotRequired[int]


class ReadBody(TypedDict):
    type: Literal[MessageType.READ]
    msg_id: NotRequired[int]
    key: str


class ReadOkBody(TypedDict):
    type: Literal[MessageType.READ_OK]
    msg_id: NotRequired[int]
    in_reply_to: NotRequired[int]
    value: Any


class WriteBody(TypedDict):
    type: Literal[MessageType.WRITE]
    msg_id: NotRequired[int]
    key: str
    value: Any


class WriteOkBody(TypedDict):
    type: Literal[MessageType.WRITE_OK]
    msg_id: NotRequired[int]
    in_reply_to: NotRequired[int]


CasBody = TypedDict(
    "CasBody",
    {
        "type": Literal[MessageType.CAS],
        "msg_id": NotRequired[int],
        "key": str,
        "from": Any,
        "to": Any,
    },
)


class CasOkBody(TypedDict):
    type: Literal[MessageType.CAS_OK]
    msg_id: NotRequired[int]
    in_reply_to: NotRequired[int]


class ErrorBody(TypedDict):
    type: Literal[MessageType.ERROR]
    msg_id: NotRequired[int]
    in_reply_to: NotRequired[int]
    code: int
    text: str


MessageBody = (
    InitBody
    | InitOkBody
    | RequestVoteBody
    | RequestVoteOkBody
    | AppendEntriesBody
    | AppendEntriesOkBody
    | ReadBody
    | ReadOkBody
    | WriteBody
    | WriteOkBody
    | CasBody
    | CasOkBody
    | ErrorBody
)


class Message[B: MessageBody](TypedDict):
    src: str
    dest: str
    body: B


class Node:
    def __init__(self):
        self.node_id: str | None = None
        self.neighbors: list[str] = []
        self.next_msg_id: int = 0
        self.handlers: dict[MessageType, Callable[[Message[Any]], None]] = {}
        self.lock = threading.Lock()
        self.background_tasks: list[threading.Thread] = []

    def send(self, dest: str, body: MessageBody, ignore_log: bool = False):
        body["msg_id"] = self.next_msg_id
        self.next_msg_id += 1
        line = json.dumps({"src": self.node_id, "dest": dest, "body": body})
        if not ignore_log:
            self.log(f"Sending {line}")
        print(line, file=sys.stdout, flush=True)

    def forward(self, dest: str, message: Message[Any]):
        forwarded = dict(message)
        forwarded["dest"] = dest
        self.log(f"Forwarding {forwarded}")
        print(json.dumps(forwarded), file=sys.stdout, flush=True)

    def log(self, msg: str):
        print(msg, file=sys.stderr, flush=True)

    def main(self):
        for thread in self.background_tasks:
            thread.start()
        for line in sys.stdin:
            self.process_line(line)

    def process_line(self, line: str):
        try:
            message: Message[Any] = json.loads(line)
        except json.JSONDecodeError as e:
            self.log(f"JSON decode failed for {line.strip()}: {e}")
            return
        message_type = message["body"].get("type")
        handler = self.handlers.get(message_type)
        if handler is None:
            self.log(f"No handler for {message_type}: {line.strip()}")
            return
        threading.Thread(target=handler, args=(message,), daemon=True).start()
