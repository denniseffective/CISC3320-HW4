
import multiprocessing as mp
import time

PRODUCERS = 3
CONSUMERS = 2
ITEMS_PER_PRODUCER = 10
BUFFER_SIZE = 5


def producer(number, buffer):
    for i in range(ITEMS_PER_PRODUCER):
        item = (number, i)
        buffer.put(item)  # Waits if full
        print(f"Producer {number} produced {item}", flush=True)
    print(f"Producer {number} finished", flush=True)


def consumer(number, buffer, results):
    while True:
        item = buffer.get()  # Waits if empty

        if item is None:
            buffer.task_done()
            print(f"Consumer {number} stopped", flush=True)
            break

        print(f"Consumer {number} consumed {item}", flush=True)
        results.put(item)
        buffer.task_done()


def main():
    start = time.perf_counter()

    buffer = mp.JoinableQueue(maxsize=BUFFER_SIZE)
    results = mp.Queue()

    consumers = [
        mp.Process(target=consumer, args=(i, buffer, results))
        for i in range(1, CONSUMERS + 1)
    ]

    producers = [
        mp.Process(target=producer, args=(i, buffer))
        for i in range(1, PRODUCERS + 1)
    ]

    for worker in consumers + producers:
        worker.start()

    for worker in producers:
        worker.join()

    # Send one stop signal to each consumer
    for _ in consumers:
        buffer.put(None)

    buffer.join()

    # Collect all 30 results
    received = [
        results.get()
        for _ in range(PRODUCERS * ITEMS_PER_PRODUCER)
    ]

    for worker in consumers:
        worker.join()

    expected = {
        (p, i)
        for p in range(1, PRODUCERS + 1)
        for i in range(ITEMS_PER_PRODUCER)
    }

    assert len(received) == len(expected)
    assert set(received) == expected

    print(f"Produced: {len(expected)}")
    print(f"Consumed: {len(received)}")
    print("Items left: 0")
    print(f"Multiprocessing time: {time.perf_counter() - start:.4f} seconds")

    buffer.close()
    results.close()


if __name__ == "__main__":
    mp.freeze_support()
    main()
