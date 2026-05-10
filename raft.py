from node import (
    Node,
    MessageType,
    Message,
    LogEntry,
    ErrorCode,
    WriteOperation,
    CompareAndSetOperation,
    ReadOperation,
    InitBody,
    RequestVoteBody,
    RequestVoteOkBody,
    AppendEntriesBody,
    AppendEntriesOkBody,
    ReadBody,
    ReadOkBody,
    WriteBody,
    WriteOkBody,
    CasBody,
    CasOkBody,
    ErrorBody,
)
from typing import Any, Iterable
from enum import StrEnum, auto
from datetime import timedelta, datetime
from random import randint
import threading
import math
import time


ELECTION_TIMEOUT_MIN_MS = 500
ELECTION_TIMEOUT_MAX_MS = 1000
ELECTION_TICK_S = ELECTION_TIMEOUT_MIN_MS / 10 / 1000


class Record:
    def __init__(self):
        self.__entries: list[LogEntry] = []

    def at(self, index: int) -> LogEntry:
        if index < 0:
            raise IndexError(f"Negative index {index} is illegal")
        if index == 0:
            return {"op": None, "term": 0}
        if index - 1 >= len(self.__entries):
            raise IndexError(
                f"Index {index} out of bound; there are only {len(self.__entries)} entries"
            )
        return self.__entries[index - 1]

    def slice_from(self, index: int) -> list[LogEntry]:
        if index < 0:
            raise IndexError(f"Negative index {index} is illegal")
        if index == 0:
            return self.__entries
        return self.__entries[index - 1 :]

    def apply_entries(self, incoming_entries: list[LogEntry], starting_index: int):
        if starting_index < 0:
            raise IndexError(f"Negative index {starting_index} is illegal")
        if starting_index == 0:
            self.__entries = incoming_entries
            return

        incoming_pointer = 0
        local_pointer = starting_index - 1
        while local_pointer < len(self.__entries) and incoming_pointer < len(
            incoming_entries
        ):
            self.__entries[local_pointer] = incoming_entries[incoming_pointer]
            local_pointer += 1
            incoming_pointer += 1
        while incoming_pointer < len(incoming_entries):
            self.__entries.append(incoming_entries[incoming_pointer])
            incoming_pointer += 1

    def next_index(self) -> int:
        return len(self.__entries) + 1

    def last_index(self) -> int:
        return len(self.__entries)

    def last_term(self) -> int:
        return self.at(self.last_index())["term"]

    def has_entry_at(self, index: int) -> bool:
        if index < 0:
            return False
        return self.last_index() >= index

    def append(self, entry: LogEntry):
        self.__entries.append(entry)


class State(StrEnum):
    LEADER = auto()
    CANDIDATE = auto()
    FOLLOWER = auto()


def majority(n: int) -> int:
    return math.floor((n / 2.0) + 1)


def median(xs: Iterable[int]) -> int:
    xs = sorted(xs)
    return xs[len(xs) - majority(len(xs))]


class RaftNode(Node):
    def __init__(self):
        super().__init__()
        self.state: State = State.FOLLOWER
        self.term: int = 0
        self.voted_for: str | None = None
        self.votes: set[str] = set()
        self.record = Record()
        self.election_deadline = datetime.now() + self.generate_election_timeout()
        self.leader: str | None = None

        self.commit_index: int = 0

        self.follower_next_indexes: dict[str, int] = {}
        self.follower_match_indexes: dict[str, int] = {}

        self.handlers = {
            MessageType.INIT: self.handle_init,
            MessageType.REQUEST_VOTE: self.handle_request_vote,
            MessageType.REQUEST_VOTE_OK: self.handle_request_vote_ok,
            MessageType.APPEND_ENTRIES: self.handle_append_entries,
            MessageType.APPEND_ENTRIES_OK: self.handle_append_entries_ok,
            MessageType.READ: self.handle_read,
            MessageType.WRITE: self.handle_write,
            MessageType.CAS: self.handle_cas,
        }

        self.replication_signal = threading.Event()
        self.background_tasks = [
            threading.Thread(target=self.election_loop, daemon=True),
            threading.Thread(target=self.replication_loop, daemon=True),
        ]

        self.snapshot: dict[str, Any] = {}
        self.pending_replies: dict[int, Message[Any]] = {}

    def handle_init(self, message: Message[InitBody]):
        with self.lock:
            self.log(f"Initialization: {message}")
            self.node_id = message["body"]["node_id"]
            self.neighbors = message["body"]["node_ids"]
            self.send(
                message["src"],
                {
                    "type": MessageType.INIT_OK,
                    "in_reply_to": message["body"]["msg_id"],
                },
            )

    def handle_append_entries(self, message: Message[AppendEntriesBody]):
        with self.lock:
            self.log(f"Appending entries: {message}")
            self.election_deadline += self.generate_election_timeout()

            incoming_term = message["body"]["term"]
            self.become_follower_if_applicable(message)

            prev_log_index = message["body"]["prev_log_index"]
            prev_log_term = message["body"]["prev_log_term"]

            if (
                incoming_term < self.term
                or not self.record.has_entry_at(prev_log_index)
                or self.record.at(prev_log_index)["term"] != prev_log_term
            ):
                self.send(
                    message["src"],
                    {
                        "type": MessageType.APPEND_ENTRIES_OK,
                        "in_reply_to": message["body"]["msg_id"],
                        "term": self.term,
                        "success": False,
                        "leader_id": self.leader,
                    },
                )
                return

            self.record.apply_entries(message["body"]["entries"], prev_log_index + 1)

            leader_commit = message["body"]["leader_commit"]
            if leader_commit > self.commit_index:
                self.commit_at(min(leader_commit, self.record.last_index()))

            self.send(
                message["src"],
                {
                    "type": MessageType.APPEND_ENTRIES_OK,
                    "in_reply_to": message["body"]["msg_id"],
                    "term": self.term,
                    "success": True,
                    "match_index": self.record.last_index(),
                },
            )

    def handle_append_entries_ok(self, message: Message[AppendEntriesOkBody]):
        with self.lock:
            self.log(f"Append entries OK: {message}")
            if (
                self.become_follower_if_applicable(message)
                or self.state != State.LEADER
                or message["body"]["term"] != self.term
            ):
                return

            src = message["src"]
            if message["body"]["success"]:
                self.follower_match_indexes[src] = max(
                    message["body"]["match_index"], self.follower_match_indexes[src]
                )
                self.follower_next_indexes[src] = max(
                    message["body"]["match_index"] + 1,
                    self.follower_next_indexes[src],
                )
            else:
                self.follower_next_indexes[src] -= 1

            self.commit_and_reply_if_applicable()

    def handle_request_vote(self, message: Message[RequestVoteBody]):
        with self.lock:
            self.become_follower_if_applicable(message)

            incoming_term = message["body"]["term"]
            last_log_index = message["body"]["last_log_index"]
            last_log_term = message["body"]["last_log_term"]

            candidate_up_to_date = last_log_term > self.record.last_term() or (
                last_log_term == self.record.last_term()
                and last_log_index >= self.record.last_index()
            )

            vote_granted = (
                self.voted_for is None
                and incoming_term >= self.term
                and candidate_up_to_date
            )
            if vote_granted:
                self.leader = None
                self.voted_for = message["body"]["candidate_id"]
                self.term = incoming_term
                self.election_deadline += self.generate_election_timeout()

            self.log(
                f"Vote {'granted' if vote_granted else 'denied'} for {message['src']}"
            )
            self.send(
                message["src"],
                {
                    "type": MessageType.REQUEST_VOTE_OK,
                    "in_reply_to": message["body"]["msg_id"],
                    "term": self.term,
                    "vote_granted": vote_granted,
                },
            )

    def handle_request_vote_ok(self, message: Message[RequestVoteOkBody]):
        with self.lock:
            if self.become_follower_if_applicable(message):
                return
            if self.state != State.CANDIDATE or message["body"]["term"] != self.term:
                return
            if message["body"]["vote_granted"]:
                self.votes.add(message["src"])
            if len(self.votes) >= majority(len(self.neighbors)):
                self.become_leader()

    def handle_read(self, message: Message[ReadBody]):
        with self.lock:
            self.log(f"Reading: {message}")
            self.try_persist_or_forward_entry(
                {
                    "term": self.term,
                    "op": {"type": "read", "key": message["body"]["key"]},
                },
                message,
            )

    def handle_write(self, message: Message[WriteBody]):
        with self.lock:
            self.log(f"Writing: {message}")
            self.try_persist_or_forward_entry(
                {
                    "term": self.term,
                    "op": {
                        "type": "write",
                        "key": message["body"]["key"],
                        "value": message["body"]["value"],
                    },
                },
                message,
            )

    def handle_cas(self, message: Message[CasBody]):
        with self.lock:
            self.log(f"CAS: {message}")
            self.try_persist_or_forward_entry(
                {
                    "term": self.term,
                    "op": {
                        "type": "cas",
                        "key": message["body"]["key"],
                        "value_from": message["body"]["from"],
                        "value_to": message["body"]["to"],
                    },
                },
                message,
            )

    def election_loop(self):
        while True:
            with self.lock:
                if (
                    datetime.now() > self.election_deadline
                    and self.state != State.LEADER
                ):
                    self.trigger_election()
            time.sleep(ELECTION_TICK_S)

    def replication_loop(self):
        while True:
            with self.lock:
                self.replicate_if_applicable()
            self.replication_signal.wait(timeout=0.1)
            self.replication_signal.clear()

    def try_persist_or_forward_entry(self, entry: LogEntry, message: Message[Any]):
        if self.state == State.LEADER:
            self.record.append(entry)
            self.pending_replies[self.record.last_index()] = message
            self.replication_signal.set()
            self.log(f"Pending reply at index {self.record.last_index()} for {message}")
        elif self.leader is not None:
            self.forward(self.leader, message)
        else:
            self.send(
                message["src"],
                {
                    "type": MessageType.ERROR,
                    "code": ErrorCode.TEMPORARILY_UNAVAILABLE,
                    "in_reply_to": message["body"]["msg_id"],
                    "text": "No leader elected",
                },
            )

    def commit_and_reply_if_applicable(self):
        if self.state != State.LEADER:
            return
        index = median(
            [self.record.last_index(), *self.follower_match_indexes.values()]
        )
        if self.commit_index < index and self.record.at(index)["term"] == self.term:
            self.commit_at(index, send_reply=True)

    def commit_at(self, index: int, send_reply: bool = False):
        self.log(f"Committing up to and including index {index}")
        while self.commit_index < index:
            next_index = self.commit_index + 1
            entry = self.record.at(next_index)
            op = entry["op"]
            reply_body = self.apply(op) if op is not None else None
            self.commit_index = next_index
            if send_reply and reply_body is not None:
                self.reply_to_client(next_index, reply_body)

    def apply(
        self, op: WriteOperation | CompareAndSetOperation | ReadOperation
    ) -> ReadOkBody | WriteOkBody | CasOkBody | ErrorBody:
        if op["type"] == "read":
            return {
                "type": MessageType.READ_OK,
                "value": self.snapshot.get(op["key"]),
            }
        if op["type"] == "write":
            self.snapshot[op["key"]] = op["value"]
            return {"type": MessageType.WRITE_OK}
        if op["type"] == "cas":
            key = op["key"]
            if key not in self.snapshot:
                return {
                    "type": MessageType.ERROR,
                    "code": ErrorCode.KEY_DOES_NOT_EXIST,
                    "text": f"Key {key} does not exist",
                }
            if self.snapshot[key] != op["value_from"]:
                return {
                    "type": MessageType.ERROR,
                    "code": ErrorCode.PRECONDITION_FAILED,
                    "text": f"Key {key} has value {self.snapshot[key]}, not {op['value_from']}",
                }
            self.snapshot[key] = op["value_to"]
            return {"type": MessageType.CAS_OK}
        raise ValueError(f"Unknown op type: {op['type']}")

    def reply_to_client(
        self, index: int, body: ReadOkBody | WriteOkBody | CasOkBody | ErrorBody
    ):
        message = self.pending_replies.pop(index, None)
        if message is None:
            return
        body["in_reply_to"] = message["body"]["msg_id"]
        self.send(message["src"], body)

    def replicate_if_applicable(self):
        if self.state != State.LEADER:
            return
        assert self.node_id is not None
        for neighbor in self.neighbors:
            if neighbor == self.node_id:
                continue
            follower_next_index = self.follower_next_indexes[neighbor]
            prev_log_index = follower_next_index - 1
            prev_entry = self.record.at(prev_log_index)
            payload: AppendEntriesBody = {
                "type": MessageType.APPEND_ENTRIES,
                "term": self.term,
                "leader_id": self.node_id,
                "prev_log_term": prev_entry["term"],
                "prev_log_index": prev_log_index,
                "entries": self.record.slice_from(follower_next_index),
                "leader_commit": self.commit_index,
            }
            self.send(neighbor, payload, ignore_log=True)

    def become_follower_if_applicable(self, message: Message[Any]) -> bool:
        incoming_term = message["body"]["term"]
        if "leader_id" in message["body"] and message["body"]["leader_id"] is not None:
            self.leader = message["body"]["leader_id"]

        if incoming_term > self.term:
            self.log(f"Became follower {self.node_id}")
            self.state = State.FOLLOWER
            self.voted_for = None
            self.term = incoming_term
            self.votes.clear()
            return True
        if (
            incoming_term == self.term
            and self.state == State.CANDIDATE
            and message["body"]["type"] == MessageType.APPEND_ENTRIES
        ):
            self.log(f"Became follower {self.node_id}")
            self.state = State.FOLLOWER
            return True
        return False

    def become_leader(self):
        self.log(f"Leader is now {self.node_id}")
        self.state = State.LEADER
        self.leader = self.node_id
        self.follower_next_indexes = {
            neighbor: self.record.next_index()
            for neighbor in self.neighbors
            if neighbor != self.node_id
        }
        self.follower_match_indexes = {
            neighbor: 0 for neighbor in self.neighbors if neighbor != self.node_id
        }
        self.votes = set()
        self.pending_replies = {}
        self.replication_signal.set()

    def request_vote(self):
        assert self.node_id is not None
        for node_id in self.neighbors:
            if node_id == self.node_id:
                continue
            payload: RequestVoteBody = {
                "type": MessageType.REQUEST_VOTE,
                "term": self.term,
                "candidate_id": self.node_id,
                "leader_id": self.leader,
                "last_log_index": self.record.last_index(),
                "last_log_term": self.record.last_term(),
            }
            self.send(node_id, payload)

    def trigger_election(self):
        self.log("Triggering election")
        assert self.node_id is not None
        self.term += 1
        self.state = State.CANDIDATE
        self.leader = None
        self.election_deadline = datetime.now() + self.generate_election_timeout()
        self.voted_for = self.node_id
        self.votes = {self.node_id}
        self.request_vote()

    def generate_election_timeout(self) -> timedelta:
        return timedelta(
            milliseconds=randint(ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS)
        )
