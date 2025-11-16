import time as pytime             # time module, has perf_counter, sleep, etc.
from datetime import time as dt_time
from typing import Optional
import json
import socket
import struct
import traceback
from pathlib import Path
from typing import Dict, Set, List, Type, Optional
from collections import defaultdict
from glob import glob
import os
from datetime import datetime, time, timezone
from threading import Thread, Lock
import databento as db


from EventDataClass_yz1109 import EventEngine, Event
# from trader.engine import BaseEngine, MainEngine
# from trader.utility import save_json
# from trader.event import(
#     EVENT_TICK,
#     EVENT_ORDER,
#     EVENT_TRADE,
#     EVENT_POSITION,
#     EVENT_TIMER
#)
from ObjectClass_yz1109 import MBOData, EVENT_MBO, MBO_STRUCT, SIDE_ENCODE,ACTION_TO_BYTE
# from trader.constant import(
#     Direction, Offset, OrderType
# )

def _b2s(x):
    return x.decode() if isinstance(x, (bytes, bytearray)) else x

def _to_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default

def _to_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def _read_mbo_stream(store, want_schema="MBO"):
    """
    Iterate MBO records from a Databento DBNStore.
    DBNStore is iterable; no .records() or call support.
    """
    # Optional safety: make sure the file actually is MBO
    # (DBN files are typically single-schema)
    try:
        if hasattr(store, "schema"):
            schema = store.schema  # property
            if schema and want_schema and str(schema).upper() != want_schema:
                # Either raise, or just log and continue.
                print(f"[WARN] DBN file schema is {schema}, expected {want_schema}. Proceeding anyway…")
    except Exception:
        pass

    # DBNStore itself is the iterator
    for rec in store:
        yield rec
def _encode_mbo_json(mbo: MBOData) -> bytes:
    # Pre-normalize side/action once when you create MBOData if possible.
    action = mbo.action if isinstance(mbo.action, str) else getattr(mbo.action, "name", str(mbo.action))
    side   = mbo.side   if isinstance(mbo.side,   str) else getattr(mbo.side,   "name", str(mbo.side))

    msg = {
        "ts_event": mbo.ts_event,
        "rtype": int(mbo.rtype),
        "publisher_id": mbo.publisher_id,
        "instrument_id": mbo.instrument_id,
        "action": action,
        "side": side,
        "price": mbo.price,
        "size": mbo.size,
        "channel_id": mbo.channel_id,
        "order_id": mbo.order_id,
        "flags": mbo.flags,
        "ts_in_delta": mbo.ts_in_delta,
        "sequence": mbo.sequence,
    }

    return json.dumps(
        msg,
        # you probably don't need unicode here; these are mostly numeric / ascii:
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii") + b"\n"

def _encode_mbo_text(mbo: MBOData) -> bytes:
    # Assume side/action are already strings like "B"/"S"/"A"/"C"
    return (
        f"{mbo.ts_event}|"
        f"{mbo.rtype}|"
        f"{mbo.publisher_id}|"
        f"{mbo.instrument_id}|"
        f"{mbo.action}|"
        f"{mbo.side}|"
        f"{mbo.price}|"
        f"{mbo.size}|"
        f"{mbo.channel_id}|"
        f"{mbo.order_id}|"
        f"{mbo.flags}|"
        f"{mbo.ts_in_delta}|"
        f"{mbo.sequence}\n"
    ).encode("ascii")

def encode_action_byte(action) -> bytes:
    if action is None:
        return b""
    return ACTION_TO_BYTE.get(action,b"")

def _encode_mbo_bin(mbo: MBOData) -> bytes:
        """
        Encode one MBOData into a fixed-size binary record:

        !Q I B c d d Q Q
        ts_event, instrument_id, side_code, price, size, order_id, sequence
        """
        # Normalize side to a simple string
        side = mbo.side
        if isinstance(side, bytes):
            side = side.decode(errors="ignore")
        elif isinstance(side, str):
            side_str = side
        else:
            # Enum-like object: try .value (Side.ASK.value == "A")
            value = getattr(side, "value", None)
            if isinstance(value, str):
                side_str = value
            else:
                # Fallback: str(enum) -> "Side.ASK", take last char if needed
                side_str = str(side)
        side = side_str.strip().upper()
        side_code = SIDE_ENCODE.get(side, 0)
        action_byte = encode_action_byte(mbo.action)

        return MBO_STRUCT.pack(
            int(mbo.ts_event),
            int(mbo.instrument_id),
            action_byte,
            int(side_code),
            float(mbo.price),
            float(mbo.size),
            int(mbo.order_id),
            int(mbo.sequence),
        )


# class StrategyEngine(BaseEngine):
#     """
#     strategy engine
#     """
#     def __init__(self,main_engine: MainEngine, event_engine: EventEngine):
#         super().__init__(main_engine, event_engine)
#         # load configuration for the strategy engine
#         config_path = 'cfgs\\delta_one\\config.json'
#         #config = load_json(config_path) # tbw
#         #self.setting_filename = config['setting_filename']

# EVENT_MBO = "eMBO"

class MBOFileReader:
    """
    Continuously read the file with MBO dataand push each row 
    into the EventEngine as an EVENT_MBO
    It runs on its own thread so the engine can keep processing
    """

    def __init__(self, engine: EventEngine, path: str, throttle_sleep : float = 0.0)->None:
        """
        :param engine: send events into
        :param path: path to orginal file
        :param throttle_sleep: optional sleep in seconds after each row
        """
        self._engine = engine
        self._path = Path(path)
        self._throttle_sleep = throttle_sleep
        self._thread = Thread(target = self._run, daemon = True)

    def start(self) -> None:
        self._thread.start()
    
    def join(self) -> None:
        self._thread.join()

    def _run(self) -> None:
        # Load the DBN store from file
        store = db.DBNStore.from_file(self._path)

        total_recs = 0
        skipped_ts_none = 0
        pushed_event = 0

        # Iterate over raw records in MBO schema
        # Each 'rec' is a typed object from databento (MBOMsg)
        for rec in _read_mbo_stream(store, want_schema="MBO"):
            total_recs += 1 #detect 1115

            # ts_event is ns since epoch
            ts_ns = rec.ts_event
            if ts_ns is None:
                skipped_ts_none += 1
                continue
            ts_event = ts_ns
            # try:
            #     ts_event = datetime.fromtimestamp(int(ts_ns) / 1e9, tz=timezone.utc)
            # except Exception:
            #     continue

            MBO = MBOData(
                ts_event=ts_event,                 # raw int ns
                rtype=rec.rtype,
                publisher_id=rec.publisher_id,
                instrument_id=rec.instrument_id,
                action=rec.action.decode() if isinstance(rec.action, bytes) else rec.action,
                side=rec.side.decode() if isinstance(rec.side, bytes) else rec.side,
                price=rec.price,                   # or scaled if needed
                size=rec.size,
                channel_id=rec.channel_id,
                order_id=rec.order_id,
                flags=rec.flags,
                ts_in_delta=rec.ts_in_delta,
                sequence=rec.sequence,
            )

            self._engine.put(Event(EVENT_MBO, MBO))
            pushed_event += 1

            if self._throttle_sleep > 0:
                pytime.sleep(self._throttle_sleep)
        
        print(f"[MBOFileReader] total_recs={total_recs}, "
          f"skipped_ts_none={skipped_ts_none}, pushed_events={pushed_event}")

class TcpPublisher: 
    def __init__(self, host: str, port: int, 
                 byte_threshold:int = 64*1024,
                 msg_threshold : int = 50,
                 time_threshold_ms : int = 10,
                 send_buf_bytes : int = 4*1024*1024)->None:
        # low-latency and bigger OS buffer
        #
        self.sock = socket.create_connection((host,port))
        self.sock.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
        self.sock.setsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF,send_buf_bytes)

        self.buffer = bytearray()
        self.total_msgs = 0
        self.total_bytes = 0

        self.byte_threshold = byte_threshold
        self.msg_threshold = msg_threshold
        self.time_threshold_ms = time_threshold_ms

        self._msgs_since_flush = 0
        self._last_flush_ns = pytime.perf_counter_ns()
        self._start_ns = pytime.perf_counter_ns()

        self._last_log_ns = self._start_ns
        self._log_every_ms = 1000

        ## optimize 1112
        self._chunk = []
        self._chunk_msg_cnt = 0
        self._chunk_bytes = 0
        # tiny optimization: only check wall time every N messages
        self._check_time_every = 512
        self._since_time_check = 0

    def handle_mbo(self,event:Event)->None:
        mbo : MBOData = event.data
        msg = {
            "ts_event": mbo.ts_event,  # int
            "rtype":  int(mbo.rtype),
            "publisher_id":  int(mbo.publisher_id),
            "instrument_id": int(mbo.instrument_id),
            "action": mbo.action if isinstance(mbo.action, str) else getattr(mbo.action, "name", str(mbo.action)),
            "side":   mbo.side   if isinstance(mbo.side,   str) else getattr(mbo.side,   "name", str(mbo.side)),
            "price":  float(mbo.price),
            "size": float(mbo.size),
            "channel_id": int(mbo.channel_id),
            "order_id": int(mbo.order_id),
            "flags": int(mbo.flags),
            "ts_in_delta": int(mbo.ts_in_delta),
            "sequence": int(mbo.sequence),
        }

        line = json.dumps(msg, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
        self.buffer.extend(line)

        # add counter to measure client side efficiency
        self.total_bytes += len(line)
        self.total_msgs += 1
        self._msgs_since_flush += 1

        # optimize 1112
        # self._chunk.append(line)
        # self._chunk_msg_cnt += 1
        # self._chunk_bytes += len(line)
        # self._since_time_check += 1

        #if len(self.buffer)>2660:#65536:
        if len(self.buffer) >= self.byte_threshold or self._msgs_since_flush >= self.msg_threshold:
            self.flush()
        # optimize 1112
        # if self._chunk_msg_cnt >= self.msg_threshold or self._chunk_bytes >= self.byte_threshold:
        #     self._flush_chunk()
        # elif self._since_time_check>= self._check_time_every:
        #     now_ns = pytime.perf_counter_ns()
        #     if (now_ns - self._last_flush_ns) // 1_000_000 >= self.time_threshold_ms:
        #         self._flush_chunk(now_ns)
        #     self._since_time_check = 0

        now_ns = pytime.perf_counter_ns()
        if (now_ns - self._last_log_ns) // 1_000_000 >= self._log_every_ms:
            elapsed_s = max((now_ns - self._start_ns) / 1e9, 1e-9)
            rate = self.total_msgs / elapsed_s
            mbps = (self.total_bytes / 1_000_000) / elapsed_s
            # print(f"[client] sent={self.total_msgs:,} bytes={self.total_bytes:,} "
            #       f"rate={rate:,.0f} msg/s, {mbps:.2f} MB/s")
            self._last_log_ns = now_ns

    def handle_mbo_chunk(self,event:Event)->None:
        mbo : MBOData = event.data
        # msg = {
        #     "ts_event": mbo.ts_event,  # int
        #     "rtype": int(mbo.rtype),
        #     "publisher_id": mbo.publisher_id,
        #     "instrument_id": mbo.instrument_id,
        #     "action": mbo.action if isinstance(mbo.action, str) else getattr(mbo.action, "name", str(mbo.action)),
        #     "side":   mbo.side   if isinstance(mbo.side,   str) else getattr(mbo.side,   "name", str(mbo.side)),
        #     "price": mbo.price,
        #     "size": mbo.size,
        #     "channel_id":  mbo.channel_id,
        #     "order_id":  mbo.order_id,
        #     "flags":  mbo.flags,
        #     "ts_in_delta":  mbo.ts_in_delta,
        #     "sequence":  mbo.sequence,
        # }

        #line = _encode_mbo_text(mbo)
        line = _encode_mbo_bin(mbo)
        
        self._chunk.append(line)
        self._chunk_msg_cnt += 1
        self._chunk_bytes += len(line)

        self.total_bytes += len(line)

        if self._chunk_msg_cnt >= self.msg_threshold or self._chunk_bytes >= self.byte_threshold :
            self._flush_chunk()

    def maybe_time_flush(self) -> None:
        """Call this from a timer event to enforce time-based flushing."""
        if not self._chunk:
            return
        now_ns = pytime.perf_counter_ns()
        delta_ms = (now_ns - self._last_flush_ns) // 1_000_000
        if delta_ms >= self.time_threshold_ms:
            self._flush_chunk()

    def _flush_chunk(self, now_ns:int | None = None) -> None:
        if not self._chunk or self.sock is None:
            return
        # single join → one big allocation
        payload = b"".join(self._chunk)
        self._chunk.clear()
        self._chunk_bytes = 0
        self._chunk_msg_cnt = 0
        self._since_time_check = 0

        try:
            self.sock.sendall(payload)
        except(BrokenPipeError, OSError) as e:
            print(f"[TcpPublisher] Broken pipe, stopping sender: {e}")
            self.sock = None
        self.total_bytes += len(payload)
        # NDJSON: fastest count is by newlines in the payload we just sent
        # self.total_msgs += payload.count(b"\n")
        self.total_msgs += len(payload)//MBO_STRUCT.size

        self._last_flush_ns = pytime.perf_counter_ns() if now_ns is None else now_ns


    def flush(self) -> None:
        if not self.buffer or self.sock is None:
            return
        
        payload = bytes(self.buffer)
        self.buffer = bytearray()
        self._msgs_since_flush = 0

        try:
            self.sock.sendall(payload)
        except (BrokenPipeError, OSError) as e:
            print(f"[TcpPublisher] Broken pipe in flush, stopping sender: {e}")
            self.sock = None
            return

        self.total_msgs += payload.count(b"\n")
        self.total_bytes += len(payload)
        self._last_flush_ns = pytime.perf_counter_ns()

    def close(self)->None:
        self.flush()

        if self.sock is None:
            return
        # take a local snapshot to avoid races
        sock = self.sock
        try:
            sock.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        
        elapsed_s = max((pytime.perf_counter_ns() - self._start_ns) / 1e9, 1e-9)
        print(f"[client] total_sent={self.total_msgs:,} in {elapsed_s:.3f}s "
              f"avg_rate={self.total_msgs/elapsed_s:,.0f} msg/s, "
              f"{(self.total_bytes/1_000_000)/elapsed_s:.2f} MB/s")
        try:
            sock.close()
        except Exception:
            pass
        self.sock = None

    def close_chunk(self) -> None:
        # ensure final time-based flush
        self.maybe_time_flush()
        self._flush_chunk()
        if self.sock is None:
            return
        try:
            self.sock.shutdown(socket.SHUT_WR)
        except OSError:
            pass

        elapsed = max((pytime.perf_counter_ns() - self._start_ns) / 1e9, 1e-9)
        print(f"[client] total_sent={self.total_msgs:,} in {elapsed:.3f}s "
              f"avg_rate={self.total_msgs/elapsed:,.0f} msg/s, "
              f"{(self.total_bytes/1_000_000)/elapsed:.2f} MB/s")
        self.sock.close()
