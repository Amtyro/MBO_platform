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
- `OrderBook_*.py` – order book implementation

*(Adapt the file names to your actual ones.)*

---

## Installation

### Prerequisites

- Python 3.10+
- `pip` (Python package manager)
- Git

### Clone the repository

```bash
git clone https://github.com/yourname/your-repo.git
cd your-repo
