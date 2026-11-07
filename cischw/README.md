# CISC 3320 - Homework 4

## Producer-Consumer Problem

This assignment implements the producer-consumer problem using threading and multiprocessing in Python.

## Project Files

- `threading_version.py` - Threading implementation
- `multiprocessing_version.py` - Multiprocessing implementation
- `comparison.txt` - Written comparison and test results

## Program Setup

Both versions use:
- 3 producers
- 2 consumers
- Buffer size of 5
- 10 items per producer
- 30 total items

## How to Run

Python 3 is required.

Run the threading version:

`python threading_version.py`

Run the multiprocessing version:

`python multiprocessing_version.py`

## Testing Results

Threading: 0.0087 seconds

Multiprocessing: 0.5677 seconds

Both programs produced and consumed 30 items successfully, with 0 items remaining.

The programs use queues to synchronize producers and consumers and stop signals to shut down cleanly.