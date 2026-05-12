<div align="center">

# 📡 Network Error Resilience & FEC Engine

**A high-performance Go implementation of a lossy asynchronous network channel utilizing XOR-based Forward Error Correction (FEC).**

[![Go](https://img.shields.io/badge/Go-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://go.dev/)
[![Concurrency](https://img.shields.io/badge/Goroutines_%26_Channels-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://go.dev/doc/effective_go#concurrency)
[![Domain](https://img.shields.io/badge/Networking-FEC-ED8B00?style=for-the-badge)](https://en.wikipedia.org/wiki/Forward_error_correction)

---

*Ensuring reliable data transmission over unpredictable networks through intelligent redundancy and concurrent architecture.*

</div>

## 📑 Overview

This repository provides a robust **Forward Error Correction (FEC)** engine and network simulation tool designed to safeguard multimedia streams and critical data payloads against transmission loss. 

By utilizing an optimized XOR-based parity generation mechanism, the system chunks raw byte streams into configurable blocks, achieving zero-cost perfect reconstruction for single-packet losses within any given block. The built-in network simulator leverages Go's powerful concurrency model to mimic real-world, asynchronous, and lossy network behaviors natively.

## ✨ Key Capabilities

- 🚀 **Asynchronous Architecture:** Built natively on Goroutines and buffered channels to accurately model non-blocking network I/O and concurrent data flow.
- 🛡️ **Mathematical Recovery Engine:** Features a highly efficient XOR-based parity algorithm capable of instantly reconstructing dropped packets without retransmission overhead.
- 📉 **Deterministic Loss Simulation:** Includes a configurable lossy channel simulator that deterministically applies packet drop rates (e.g., 10%) while maintaining uniform loss distribution.
- 📊 **Comprehensive Telemetry:** Outputs real-time ANSI-colored telemetry, detailed drop logs, and an end-to-end statistics dashboard for throughput and overhead analysis.
- 🔒 **CSPRNG Integration:** Employs cryptographically secure pseudo-random number generation to mock realistic, high-entropy multimedia byte streams.

## 🛠️ Technical Stack

- **Language:** Go (Golang)
- **Core Concepts:** Concurrency, Channels, Goroutines, Byte Manipulation
- **Domain:** Network Protocol Engineering, Error Control Coding, Streaming

## 📂 System Architecture

```text
==============================================================================
                            [ Pipeline Architecture ]
==============================================================================

  [Encoder/Packetizer]         [Lossy Channel]          [Decoder/Reconstructor]
  +------------------+        +---------------+        +----------------------+
  | Raw Byte Stream  |--TX--> |  Goroutine    |--RX--> |  FEC Recovery Engine |
  | + FEC Parity Gen |  chan  |  Network Sim  |  chan  |  + Integrity Check   |
  +------------------+        +---------------+        +----------------------+
