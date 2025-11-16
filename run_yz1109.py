from EventEngine_yz1109 import MBOFileReader, TcpPublisher
from EventDataClass_yz1109 import Event,EventEngine
from ObjectClass_yz1109 import MBOData,OrderBook, EVENT_MBO,EVENT_TIMER

if __name__ == "__main__":
    # 1. Create engine and register a handler for market-by-order events
    engine = EventEngine()
    publisher = TcpPublisher(
                            "127.0.0.1", 9000,
                            byte_threshold=256*1024,   # 256 KiB (bigger chunk)
                            msg_threshold=10000,      # batch more messages
                            time_threshold_ms=5        # but flush every ~5ms to keep output lively
                        )
    engine.register(EVENT_MBO,publisher.handle_mbo_chunk)
    # comment out the line below for max throughput, console IO is slow
    #engine.register(EVENT_MBO, on_mbo_event)

    # time-based flush hook optize 1112
    #engine.register(EVENT_TIMER, lambda e: publisher.maybe_time_flush())

    # 2. Create reader pointing to your CSV file
    reader = MBOFileReader(
        engine,
        path=r"C:\Users\30704\Desktop\OA-HFT\CLX5_mbo (2).dbn",
        throttle_sleep=0.0,       # set >0.0 if want to slow it down
    )

    # 3. Start engine and reader
    engine.start()
    reader.start()

    try:
        # Keep the main thread alive while data is being read
        reader.join()
    except KeyboardInterrupt:
        pass
    finally:
        publisher.close_chunk()
        # optimize 1112
        #publisher.close_chunk()
        engine.stop()
        