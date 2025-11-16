# MBO Streaming & Limit Order Book Reconstruction

A multithreaded, event-driven Python pipeline that ingests Market-By-Order (.dbn) data, streams it over TCP, reconstructs a time-ordered limit order book (LOB), and exposes a plug-in interface for quantitative trading strategies.

---

## Overview

This project is a personal quantitative engineering project I built to practice:

- Event-driven system design
- High-throughput message streaming
- Limit order book reconstruction from MBO data
- Store a time-series database
- Connecting trading strategies to a live data pipeline

The system currently achieves a baseline of **~200k messages/second** on my local machine, with room for further optimization.

---

## Main Features

- **Event Engine**
  - Central hub with thread-safe queues
  - Per-symbol event ordering
  - Simple subscription API for custom handlers (e.g., strategies, loggers)

- **MBO File Reader**
  - Reads `.dbn` Market-By-Order data
  - Converts raw records into typed events

- **Limit Order Book (LOB) Reconstruction**
  - Maintains per-symbol order books
  - Handles add/modify/cancel/trade actions
  - Easily extendable for extra analytics (e.g., best bid/ask, depth snapshots)

- **TCP Publisher & Server**
  - Streams events over TCP to downstream consumers
  - Chunked sending with configurable thresholds
  - Designed with scalability in mind (target: 50k–500k msg/s)

- **Strategy Plug-in Interface**
  - Draft `StrategyEngine` that allows strategies to subscribe to LOB updates
  - Decouples infrastructure from quantitative logic

---

## Project Architecture

High-level flow:

1. **MBOFileReader**
   - Reads `.dbn` file
   - Creates `Event(type=EVENT_MBO, data=MBOData, symbol=...)`
   - Pushes events into the `EventEngine`

2. **EventEngine**
   - Maintains queues for incoming events
   - Dispatches events to registered handlers (e.g., `on_mbo_event`, `TcpPublisher.handle_mbo`)

3. **OrderBook / LOB**
   - `on_mbo_event` updates the corresponding order book:
     - `A` – add order
     - `M` – modify order
     - `C` – cancel order
     - `T` – trade / execution
     - `R` – reset book

4. **TcpPublisher**
   - Batches messages
   - Sends serialized events to a TCP server

5. **Strategy / Analytics**
   - Downstream components subscribe to LOB updates
   - Can implement trading signals, monitoring, or logging

You can find the core logic in:

- `EventEngine_*.py` – event loop and subscriptions
- `EventDataClass_*.py` / `ObjectClass_*.py` – data containers
- `TcpPublisher_*.py` – TCP streaming
- `ObjectClass_yz1109.py` – order book implementation

---

## File Variants and Tasks

This repository contains several versions of the core Python scripts.  
Each version corresponds to a different stage of the project and solves a specific task:

- `tcp_server_1110.py` + change part of `run_yz_1109.py` codes into the following form: `engine.register(EVENT_MBO,publisher.handle_mbo)` and `publisher.close_chunk()` 
  Minimal working version of the pipeline.  
  Implements the basic event engine, MBO file reading, and simple streaming logic.  
  Use this if you want to understand the core idea with the least complexity.
  This version achieve a **~50kmsg/s** result.

- `tcp_server_1115.py`  + `run_yz_1109.py`
  Solution for achieving **~200kmsg/s**.
  The main improvemnt here is applying `_encode_mbo_bin(mbo: MBOData)` function, using fixed-size records and send only the essential fields and use `struct.Struct`.
  Before achieving this result, I had made several attempts for that purpose. For instance, use chunk and clean up current JSON path, or, avoid json.jumps per message.

- `tcp_server_1116.py`
  Solution for constructing limit order book with p99 latency less than **50 ms**.
  For latencies, the calculation logic is the time between the msg arrives and order books been built.
  Notice that in current logics, under the action M, F and C, if there are not exist previous orders, then the action will be simply continue. Thus, it seems that these msgs were in vain, but in terms of order book correctness, it's actually the safest thing to do. Because I do not know what that order's orignial price/size or queue position was, I can not update the true book correctly. I've thought about mantaining two order books, which means add a new one for records the whole process. But then I realized for trading or realistic simulation, that's actually harmful. 

Keeping these versions separate makes it easier to:

- Compare the evolution from a simple baseline to more advanced solutions  
- Reuse specific stages for teaching, debugging, or experimentation  
- Showcase how the system was gradually improved across tasks

---
## Planned
- Strategy Engine
As far as I'm concerned, the platform will ultimately be used to support certain strategies. Thus, I wrote the `EventEngine_yz1109.py` file seperately. In the future, if needed, the strategies logics can be added in this part.
- Data Storage function is essential because this is exactly the symbol for the pipeline turning into a research platform instead just a streamer. Although I do not have enough time for that part, but the core ideas are :
  1. raw feed for reproducibility
  2. store a top-N snapshot
  3. derived features for ML&analytics
  4. performance & pipeline metrics
- Installation
  so that user can run this project either **locally** or using **Docker**

### Prerequisites

- Python 3.10+
- `pip` (Python package manager)
- Git

