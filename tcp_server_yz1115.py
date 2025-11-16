# tcp_server.py
import socket
import time
import json
import struct

MBO_STRUCT = struct.Struct("!Q I c B d d Q Q")
RECORD_SIZE = MBO_STRUCT.size


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

            while True:
                data = conn.recv(65536)
                if not data:
                    # client closed connection
                    break

                total_bytes += len(data)
                # buf += data #opti 1116
                buf.extend(data)

            #     # Process complete lines
            #     while True:
            #         pos = buf.find(b"\n")
            #         if pos < 0:
            #             break
            #         line = buf[:pos]
            #         buf = buf[pos+1:]
            #         if line:
            #             total_msgs += 1
            #             # validate JSON occasionally:
            #             if (total_msgs & 0xFFFF) == 0 : json.loads(line)

            # elapsed = time.perf_counter() - start
            # if elapsed >= 1.0:
            #     elapsed = time.perf_counter() - start
            #     msg_rate = total_msgs / max(elapsed, 1e-9)
            #     mb_rate  = (total_bytes / 1_000_000) / max(elapsed, 1e-9)
            #     print(f"[server] recv={total_msgs:,} bytes={total_bytes:,} "
            #             f"rate={msg_rate:,.0f} msg/s, {mb_rate:.2f} MB/s")
            #     last_log = time.perf_counter()

            # elapsed = max(time.perf_counter()-start,1e-9)
            # msg_rate = total_msgs / elapsed
            # mb_rate = (total_bytes / 1_000_000) / elapsed

            # print(f"[server] DONE recv={total_msgs:,} bytes={total_bytes:,} "
            #       f"in {elapsed:.3f}s => {msg_rate:,.0f} msg/s, {mb_rate:.2f} MB/s")

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

                total_msgs += 1

                now = time.perf_counter()
                if now - last_log >= 1.0:
                    elapsed = max(now - start, 1e-9)
                    msg_rate = total_msgs / elapsed
                    mb_rate = (total_bytes / 1_000_000) / elapsed
                    print(f"[server] recv={total_msgs:,} bytes={total_bytes:,} "
                        f"rate={msg_rate:,.0f} msg/s, {mb_rate:.2f} MB/s")
                    last_log = now

            # After loop exits (client closed)
            elapsed = max(time.perf_counter() - start, 1e-9)
            msg_rate = total_msgs / elapsed
            mb_rate = (total_bytes / 1_000_000) / elapsed
            print(f"[server] DONE recv={total_msgs:,} bytes={total_bytes:,} "
                f"in {elapsed:.3f}s => {msg_rate:,.0f} msg/s, {mb_rate:.2f} MB/s")

if __name__ == "__main__":
    run_server()
