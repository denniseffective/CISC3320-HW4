
import threading
import queue
import time

PRODUCERS = 3
CONSUMERS = 2
ITEMS_PER_PRODUCER = 10
BUFFER_SIZE = 5


def producer(number, buffer):
    for i in range(ITEMS_PER_PRODUCER):
        item = (number, i)
        buffer.put(item)  # Waits if buffer is full
        print(f"Producer {number} produced {item}", flush=True)
    print(f"Producer {number} finished", flush=True)


def consumer(number, buffer, results, result_lock):
    while True:
        item = buffer.get()  # Waits if buffer is empty

        if item is None:
            buffer.task_done()
            print(f"Consumer {number} stopped", flush=True)
            break

        print(f"Consumer {number} consumed {item}", flush=True)

        with result_lock:
            results.append(item)

        buffer.task_done()


def main():
    start = time.perf_counter()

    buffer = queue.Queue(maxsize=BUFFER_SIZE)
    results = []
    result_lock = threading.Lock()

    consumers = [
        threading.Thread(
            target=consumer,
            args=(i, buffer, results, result_lock)
        )
        for i in range(1, CONSUMERS + 1)
    ]

    producers = [
        threading.Thread(target=producer, args=(i, buffer))
        for i in range(1, PRODUCERS + 1)
    ]

    for worker in consumers + producers:
        worker.start()

    for worker in producers:
        worker.join()

    # Stop consumers only after producers finish
    for _ in consumers:
        buffer.put(None)

    buffer.join()

    for worker in consumers:
        worker.join()

    expected = {
        (p, i)
        for p in range(1, PRODUCERS + 1)
        for i in range(ITEMS_PER_PRODUCER)
    }

    assert len(results) == len(expected)
    assert set(results) == expected

    print(f"Produced: {len(expected)}")
    print(f"Consumed: {len(results)}")
    print(f"Items left: {buffer.qsize()}")
    print(f"Threading time: {time.perf_counter() - start:.4f} seconds")


if __name__ == "__main__":
    main()
