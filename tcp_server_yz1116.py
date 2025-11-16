# tcp_server.py
import socket
from collections import defaultdict
import time
import json
import struct

from ObjectClass_yz1109 import OrderBook,MBOData, EVENT_MBO
from EventDataClass_yz1109 import Event

MBO_STRUCT = struct.Struct("!Q I c B d d Q Q")
RECORD_SIZE = MBO_STRUCT.size
latencies = [] # for p99 latencies

books_by_id:dict[int,OrderBook] = defaultdict(OrderBook)

def on_mbo_event(event: Event) -> None:
    """Example handler that just prints the first few events."""
    mbo: MBOData = event.data
    order_book = books_by_id[mbo.publisher_id]

    order_book.last_ts = mbo.ts_event
    
    if mbo.flags & 0x80:
        order_book.clear()

    act = (mbo.action or "").upper()
    side = mbo.side.upper() if mbo.side else "N"

    # print(
    #     f"[{mbo.ts_event}] {mbo.instrument_id} "
    #     f"{'BID' if mbo.side == 'B' else 'ASK'} "
    #     f"action={mbo.action} price={mbo.price} size={mbo.size} order_id={mbo.order_id}"
    # )

    if act == b"A":
        # Add a new resting order
        if side in ("B", "A"):
            order_book.add_order(mbo)
        return

    if act == b"C":
        # First version: treat as full cancel of that order id.
        # (Later you can use mbo.size for partial cancels if needed.)
        order_book.cancel(order_id=mbo.order_id,size = mbo.size)
        return

    if act == b"F":
        # Passive order got filled partially or fully
        fill_sz = int(mbo.size) if mbo.size else 0
        if fill_sz > 0:
            order_book.trade_fill(order_id=mbo.order_id, filled_size=fill_sz)
        return

    if act == b"M":
        # Modify existing order in place
        new_price = float(mbo.price) if mbo.price is not None else None
        new_size = int(mbo.size) if mbo.size is not None else None
        order_book.modify(
            order_id=mbo.order_id,
            new_price=new_price,
            new_size=new_size,
            ts_event=mbo.ts_event,
        )
        return

    if act == b"T":
        # Aggressor trade record – book already updated via `F`.
        # You can keep a separate trade tape here if you want.
        # e.g. trades.append((mbo.ts_event, mbo.symbol, mbo.price, mbo.size))
        return

    # Anything else: ignore safely
    return

def run_server(host: str = "127.0.0.1", port: int = 9000) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        server.bind((host, port))
        server.listen(1)
        print(f"Server listening on {host}:{port} ...")

        conn, addr = server.accept()
        with conn:
            print(f"Connected by {addr}")
            # TCP_NODELAY not required on receive side, but harmless:
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            buf = bytearray()
            total_msgs = 0
            total_bytes = 0
            start = time.perf_counter()
            last_log = start

            # JSON Snapshot
            last_snapshot =  time.perf_counter()

            while True:
                data = conn.recv(65536)
                if not data:
                    # client closed connection
                    break

                total_bytes += len(data)
                # buf += data #opti 1116
                buf.extend(data)

            # opmize 1116
             # Process as many whole records as we have
            while len(buf) >= RECORD_SIZE:
                record_bytes = buf[:RECORD_SIZE]
                del buf[:RECORD_SIZE]  # shift buffer

                # Unpack binary record
                (
                    ts_event,
                    instrument_id,
                    action_b,
                    side_code,
                    price,
                    size,
                    order_id,
                    sequence,
                ) = MBO_STRUCT.unpack(record_bytes)

                mbo = MBOData(
                    ts_event= ts_event,
                    rtype= 160,
                    instrument_id=instrument_id,
                    publisher_id=1,
                    action = action_b,
                    side="B" if side_code == 1 else "A" if side_code == 2 else "N",
                    price=price,
                    size=size,
                    order_id=order_id,
                    channel_id= 26,
                    flags=1,
                    ts_in_delta= 1,
                    sequence=sequence,
                )
                event = Event(EVENT_MBO,mbo)
                on_mbo_event(event)

                total_msgs += 1

                now = time.perf_counter()
                if now - last_log >= 1.0:
                    elapsed = max(now - start, 1e-9)
                    msg_rate = total_msgs / elapsed
                    mb_rate = (total_bytes / 1_000_000) / elapsed
                    print(f"[server] recv={total_msgs:,} bytes={total_bytes:,} "
                        f"rate={msg_rate:,.0f} msg/s, {mb_rate:.2f} MB/s")
                    last_log = now

                # for p99 latency
                lat = now - (mbo.ts_event or now)
                latencies.append(lat)

                # for snapshot
                book = books_by_id[mbo.publisher_id]
                #print(mbo.publisher_id)
                snap = book.snapshot_json(depth=10)
                # print(snap)

            # After loop exits (client closed)
            elapsed = max(time.perf_counter() - start, 1e-9)
            msg_rate = total_msgs / elapsed
            mb_rate = (total_bytes / 1_000_000) / elapsed
            print(f"[server] DONE recv={total_msgs:,} bytes={total_bytes:,} "
                f"in {elapsed:.3f}s => {msg_rate:,.0f} msg/s, {mb_rate:.2f} MB/s")
            
            print(books_by_id)

            p99 = sorted(latencies)[int(0.9*len(latencies))]
            print("p99 latency(ms) = ", p99 * 1000)

if __name__ == "__main__":
    run_server()
