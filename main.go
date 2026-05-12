// ============================================================================
// Error Resilience in Multimedia Streaming - Go Concurrency Simulation
// ============================================================================
// University Term Project: Simulates a lossy asynchronous network channel using
// XOR-based Forward Error Correction (FEC) for perfect single-packet recovery.
//
// Architecture Overview:
//
//   [Encoder/Packetizer]         [Lossy Channel]          [Decoder/Reconstructor]
//   +------------------+        +---------------+        +----------------------+
//   | Raw Byte Stream  |--TX--> |  Goroutine    |--RX--> |  FEC Recovery Engine |
//   | + FEC Parity Gen |  chan  |  10% Drop Sim |  chan  |  + Integrity Check   |
//   +------------------+        +---------------+        +----------------------+
//
// Author : Tolga TURKMEN - 212020050041 - DEPARTMENT OF MIS @KFAU
// Course : Multimedia
// ============================================================================

package main

import (
	"crypto/rand"
	"fmt"
	"math"
	mathrand "math/rand"
	"strings"
	"time"
)

// --------------------------------------------------------------------------------
// SECTION 1 - Constants & Configuration
// --------------------------------------------------------------------------------

const (
	// PacketSize is the fixed payload size per packet (bytes).
	// Mimics a typical RTP/UDP multimedia payload slice.
	PacketSize = 512

	// BlockSize is the number of DATA packets per FEC block.
	// One XOR parity packet is generated for each block of 9 data packets,
	// yielding 10 packets total per block (9 data + 1 parity).
	BlockSize = 9

	// TotalDataPackets is the total number of data packets to generate.
	// Must be a multiple of BlockSize for clean block boundaries.
	TotalDataPackets = 90

	// DropRate is the simulated packet loss probability (10%).
	DropRate = 0.10

	// ChannelBuffer sets the capacity of the asynchronous network channel.
	ChannelBuffer = 256
)

// --------------------------------------------------------------------------------
// SECTION 2 - Data Structures
// --------------------------------------------------------------------------------

// Packet represents a single unit of transmission over the simulated network.
type Packet struct {
	SeqNum   int    // Global sequence number (0-indexed within block for data; BlockSize for parity)
	BlockID  int    // The FEC block this packet belongs to
	Payload  []byte // Fixed-length byte payload
	IsParity bool   // True if this is an XOR parity (redundancy) packet
}

// BlockResult holds the receiver's state for a single FEC block after channel delivery.
type BlockResult struct {
	BlockID       int
	DataPackets   [BlockSize]*Packet // Received data slots (nil = dropped)
	ParityPacket  *Packet           // Received parity slot (nil = dropped)
	DroppedSeqNums []int            // Which sequence numbers were lost
}

// --------------------------------------------------------------------------------
// SECTION 3 - Utility Functions
// --------------------------------------------------------------------------------

// banner prints a decorated section header.
func banner(title string) {
	width := 72
	pad := width - len(title) - 4
	if pad < 0 {
		pad = 0
	}
	left := pad / 2
	right := pad - left
	fmt.Printf("\n+%s+\n", strings.Repeat("-", width-2))
	fmt.Printf("| %s%s%s |\n", strings.Repeat(" ", left), title, strings.Repeat(" ", right))
	fmt.Printf("+%s+\n\n", strings.Repeat("-", width-2))
}

// separator prints a thin horizontal rule.
func separator() {
	fmt.Printf("  %s\n", strings.Repeat("-", 66))
}

// colorTag returns ANSI-colored status tags for terminal output.
func colorTag(tag, color string) string {
	colors := map[string]string{
		"red":    "\033[1;31m",
		"green":  "\033[1;32m",
		"yellow": "\033[1;33m",
		"cyan":   "\033[1;36m",
		"magenta": "\033[1;35m",
		"blue":   "\033[1;34m",
		"white":  "\033[1;37m",
		"reset":  "\033[0m",
	}
	c, ok := colors[color]
	if !ok {
		c = colors["white"]
	}
	return fmt.Sprintf("%s%s%s", c, tag, colors["reset"])
}

// --------------------------------------------------------------------------------
// SECTION 4 - Multimedia Stream Generator & Packetizer
// --------------------------------------------------------------------------------

// generateStream creates a cryptographically random byte stream to simulate
// raw multimedia data (video/audio frames).
func generateStream(totalBytes int) []byte {
	stream := make([]byte, totalBytes)
	if _, err := rand.Read(stream); err != nil {
		panic(fmt.Sprintf("CSPRNG failure: %v", err))
	}
	return stream
}

// packetize slices a raw byte stream into fixed-size Packet structs,
// assigning sequential IDs and block membership.
func packetize(stream []byte) []*Packet {
	n := len(stream) / PacketSize
	packets := make([]*Packet, 0, n)
	for i := 0; i < n; i++ {
		packets = append(packets, &Packet{
			SeqNum:  i % BlockSize,
			BlockID: i / BlockSize,
			Payload: append([]byte{}, stream[i*PacketSize:(i+1)*PacketSize]...),
		})
	}
	return packets
}

// --------------------------------------------------------------------------------
// SECTION 5 - XOR-Based Forward Error Correction (FEC) Engine
// --------------------------------------------------------------------------------
//
// Theory:  For a block of K data packets {D0, D1, ..., D(K-1)}, the parity
//          packet P is computed as:
//
//              P = D0 + D1 + D2 + ... + D(K-1)
//
//          If any single packet Di is lost, it can be recovered:
//
//              Di = P + D0 + ... + D(i-1) + D(i+1) + ... + D(K-1)
//
//          This provides zero-cost perfect reconstruction for <=1 loss per block.
// --------------------------------------------------------------------------------

// generateParity computes the XOR parity packet for a block of data packets.
func generateParity(block []*Packet) *Packet {
	if len(block) == 0 {
		panic("generateParity called with empty block")
	}
	parity := make([]byte, PacketSize)
	for _, pkt := range block {
		for j := 0; j < PacketSize; j++ {
			parity[j] ^= pkt.Payload[j]
		}
	}
	return &Packet{
		SeqNum:   BlockSize, // Parity occupies the (K+1)-th slot
		BlockID:  block[0].BlockID,
		Payload:  parity,
		IsParity: true,
	}
}

// recoverPacket reconstructs a single missing data packet from surviving
// data packets and the parity packet using XOR inversion.
func recoverPacket(survivors []*Packet, parity *Packet, missingSeq, blockID int) *Packet {
	recovered := make([]byte, PacketSize)
	copy(recovered, parity.Payload)
	for _, s := range survivors {
		for j := 0; j < PacketSize; j++ {
			recovered[j] ^= s.Payload[j]
		}
	}
	return &Packet{
		SeqNum:  missingSeq,
		BlockID: blockID,
		Payload: recovered,
	}
}

// --------------------------------------------------------------------------------
// SECTION 6 - Asynchronous Lossy Network Channel Simulator
// --------------------------------------------------------------------------------

// simulateNetwork launches a Goroutine that reads packets from `tx`, randomly
// drops exactly ~10% of them, and forwards survivors to `rx`. Both channels
// are buffered to model asynchronous, non-blocking network I/O.
//
// Drop Strategy: For each FEC block, we compute exactly how many packets to
// drop (10% of block+parity size, i.e., 1 out of 10), then randomly select
// which packet index within the block to discard. This guarantees a
// deterministic 10% overall loss rate while distributing drops uniformly.
func simulateNetwork(allPackets []*Packet, rx chan<- *Packet, rng *mathrand.Rand) (int, int) {
	totalSent := len(allPackets)
	totalDropped := 0

	// Process packets block by block for controlled 10% drop.
	numBlocks := TotalDataPackets / BlockSize
	packetsPerBlock := BlockSize + 1 // 9 data + 1 parity

	for b := 0; b < numBlocks; b++ {
		blockStart := b * packetsPerBlock
		blockEnd := blockStart + packetsPerBlock
		blockPackets := allPackets[blockStart:blockEnd]

		// Exactly 10% of 10 packets = 1 packet dropped per block.
		dropCount := int(math.Round(float64(packetsPerBlock) * DropRate))
		if dropCount < 1 {
			dropCount = 1
		}

		// Select random unique indices to drop within this block.
		dropSet := make(map[int]bool)
		for len(dropSet) < dropCount {
			dropSet[rng.Intn(packetsPerBlock)] = true
		}

		for i, pkt := range blockPackets {
			if dropSet[i] {
				totalDropped++
				kind := "DATA"
				globalID := pkt.BlockID*BlockSize + pkt.SeqNum
				if pkt.IsParity {
					kind = "PARITY"
					globalID = pkt.BlockID*BlockSize + BlockSize
				}
				fmt.Printf("  %s  Block %02d | %s Packet (Global #%03d, Seq %d) - %s\n",
					colorTag("X DROP", "red"),
					pkt.BlockID,
					kind,
					globalID,
					pkt.SeqNum,
					colorTag("Lost in transit", "red"))
			} else {
				rx <- pkt
			}
		}
	}

	close(rx)
	return totalSent, totalDropped
}

// --------------------------------------------------------------------------------
// SECTION 7 - Receiver & FEC Reconstruction Engine
// --------------------------------------------------------------------------------

// receiveAndReconstruct reads packets from the channel, assembles them into
// FEC blocks, detects missing packets, and attempts XOR recovery.
func receiveAndReconstruct(rx <-chan *Packet, originalPackets []*Packet) {
	numBlocks := TotalDataPackets / BlockSize
	blocks := make([]BlockResult, numBlocks)
	for i := range blocks {
		blocks[i].BlockID = i
	}

	// -- Phase A: Ingest all surviving packets from channel --
	for pkt := range rx {
		b := &blocks[pkt.BlockID]
		if pkt.IsParity {
			b.ParityPacket = pkt
		} else {
			b.DataPackets[pkt.SeqNum] = pkt
		}
	}

	// -- Phase B: Detect losses & attempt FEC recovery per block --
	banner("RECEIVER - FEC RECOVERY ENGINE")

	totalRecovered := 0
	totalUnrecoverable := 0

	for bi := 0; bi < numBlocks; bi++ {
		b := &blocks[bi]

		// Identify missing data packets.
		var missing []int
		var survivors []*Packet
		for s := 0; s < BlockSize; s++ {
			if b.DataPackets[s] == nil {
				missing = append(missing, s)
			} else {
				survivors = append(survivors, b.DataPackets[s])
			}
		}

		if len(missing) == 0 {
			fmt.Printf("  %s  Block %02d | All %d data packets received intact.\n",
				colorTag("OK OK  ", "green"), bi, BlockSize)
			continue
		}

		separator()
		fmt.Printf("  %s  Block %02d | Missing %d data packet(s): seq %v\n",
			colorTag("! LOSS", "yellow"), bi, len(missing), missing)

		// Case 1: Exactly one data packet lost AND parity survived → recoverable.
		if len(missing) == 1 && b.ParityPacket != nil {
			missingSeq := missing[0]
			globalID := bi*BlockSize + missingSeq
			recovered := recoverPacket(survivors, b.ParityPacket, missingSeq, bi)
			b.DataPackets[missingSeq] = recovered

			// Verify correctness against original.
			origPayload := originalPackets[globalID].Payload
			match := true
			for j := 0; j < PacketSize; j++ {
				if recovered.Payload[j] != origPayload[j] {
					match = false
					break
				}
			}
			status := colorTag("VERIFIED OK", "green")
			if !match {
				status = colorTag("MISMATCH X", "red")
			}

			fmt.Printf("  %s  Block %02d | Packet #%03d (Seq %d) reconstructed via XOR FEC - %s\n",
				colorTag("> FEC ", "cyan"), bi, globalID, missingSeq, status)
			totalRecovered++

		} else if len(missing) == 1 && b.ParityPacket == nil {
			// Parity itself was dropped but only parity lost → data is fine (edge case: we still
			// have the data packet... this means parity was the dropped one, no data lost).
			// Actually if missing has entries, then data IS lost. And parity is nil → unrecoverable.
			globalID := bi*BlockSize + missing[0]
			fmt.Printf("  %s  Block %02d | Packet #%03d (Seq %d) - parity also lost, UNRECOVERABLE\n",
				colorTag("X FAIL", "red"), bi, globalID, missing[0])
			totalUnrecoverable++

		} else if len(missing) > 1 {
			// Multiple data packets lost → XOR FEC can only recover 1 per block.
			for _, ms := range missing {
				globalID := bi*BlockSize + ms
				fmt.Printf("  %s  Block %02d | Packet #%03d (Seq %d) - multiple losses, UNRECOVERABLE\n",
					colorTag("X FAIL", "red"), bi, globalID, ms)
				totalUnrecoverable++
			}

		} else if len(missing) == 0 {
			// Parity was dropped but all data survived - no recovery needed.
			fmt.Printf("  %s  Block %02d | Parity lost but all data intact - no action needed.\n",
				colorTag("OK OK  ", "green"), bi)
		}
	}

	// -- Phase C: Final Statistics --
	banner("SIMULATION RESULTS - FINAL REPORT")

	totalData := TotalDataPackets
	totalParity := numBlocks
	totalTransmitted := totalData + totalParity
	overheadPct := (float64(totalParity) / float64(totalData)) * 100.0

	fmt.Printf("  +-----------------------------------------------------------------┐\n")
	fmt.Printf("  |  %-44s %16s  |\n", "Metric", "Value")
	fmt.Printf("  +-----------------------------------------------------------------┤\n")
	fmt.Printf("  |  %-44s %16d  |\n", "Total Data Packets Generated", totalData)
	fmt.Printf("  |  %-44s %16d  |\n", "Total Parity Packets Generated", totalParity)
	fmt.Printf("  |  %-44s %16d  |\n", "Total Packets Transmitted", totalTransmitted)
	fmt.Printf("  |  %-44s %16d  |\n", "Packet Payload Size (bytes)", PacketSize)
	fmt.Printf("  |  %-44s %16s  |\n", "FEC Block Configuration", fmt.Sprintf("%d data + 1 parity", BlockSize))
	fmt.Printf("  |  %-44s %15.1f%%  |\n", "Simulated Network Drop Rate", DropRate*100)
	fmt.Printf("  +-----------------------------------------------------------------┤\n")
	fmt.Printf("  |  %-44s %s  |\n", "Packets Successfully Recovered (FEC)",
		colorTag(fmt.Sprintf("%15d", totalRecovered), "cyan"))
	fmt.Printf("  |  %-44s %s  |\n", "Packets Unrecoverable",
		colorTag(fmt.Sprintf("%15d", totalUnrecoverable), "red"))
	fmt.Printf("  +-----------------------------------------------------------------┤\n")
	fmt.Printf("  |  %-44s %s  |\n",
		colorTag("FEC Overhead Cost", "magenta"),
		colorTag(fmt.Sprintf("%14.2f%%", overheadPct), "magenta"))
	fmt.Printf("  |  %-44s %s  |\n",
		colorTag("Effective Throughput", "green"),
		colorTag(fmt.Sprintf("%13.2f%%", 100.0-overheadPct), "green"))
	fmt.Printf("  +-----------------------------------------------------------------┘\n")

	// Recovery rate
	totalLostData := totalRecovered + totalUnrecoverable
	if totalLostData > 0 {
		recoveryRate := (float64(totalRecovered) / float64(totalLostData)) * 100.0
		fmt.Printf("\n  %s  FEC Recovery Rate: %s of lost data packets recovered.\n",
			colorTag("*", "yellow"),
			colorTag(fmt.Sprintf("%.1f%%", recoveryRate), "cyan"))
	}

	if totalUnrecoverable == 0 {
		fmt.Printf("  %s  %s\n\n",
			colorTag("*", "yellow"),
			colorTag("PERFECT RECONSTRUCTION - Zero data loss. Stream integrity 100%.", "green"))
	} else {
		fmt.Printf("  %s  %s\n\n",
			colorTag("*", "yellow"),
			colorTag(fmt.Sprintf("Partial loss: %d packet(s) could not be recovered.", totalUnrecoverable), "red"))
	}
}

// --------------------------------------------------------------------------------
// SECTION 8 - Main Orchestrator
// --------------------------------------------------------------------------------

func main() {
	// Seed PRNG for reproducible drop simulation (use current time for variance).
	rng := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))

	// -- Step 1: Generate Raw Multimedia Stream --
	banner("MULTIMEDIA STREAM GENERATOR")
	totalBytes := TotalDataPackets * PacketSize
	stream := generateStream(totalBytes)
	fmt.Printf("  %s  Generated %d bytes of simulated multimedia data (CSPRNG).\n",
		colorTag(">", "blue"), totalBytes)
	fmt.Printf("  %s  Payload per packet: %d bytes.\n", colorTag(">", "blue"), PacketSize)

	// -- Step 2: Packetize --
	banner("PACKETIZER - CHUNKING INTO FIXED-SIZE PACKETS")
	dataPackets := packetize(stream)
	fmt.Printf("  %s  Sliced stream into %d data packets across %d FEC blocks.\n",
		colorTag(">", "blue"), len(dataPackets), TotalDataPackets/BlockSize)
	fmt.Printf("  %s  Block structure: %d data + 1 parity = %d packets/block.\n",
		colorTag(">", "blue"), BlockSize, BlockSize+1)

	// -- Step 3: Generate FEC Parity Packets --
	banner("FEC ENGINE - XOR PARITY GENERATION")
	numBlocks := TotalDataPackets / BlockSize
	var allPackets []*Packet // interleaved: [block0_data..., block0_parity, block1_data..., ...]

	for b := 0; b < numBlocks; b++ {
		blockData := dataPackets[b*BlockSize : (b+1)*BlockSize]

		// Append all data packets of this block.
		allPackets = append(allPackets, blockData...)

		// Compute and append parity.
		parity := generateParity(blockData)
		allPackets = append(allPackets, parity)

		fmt.Printf("  %s  Block %02d | Parity generated: XOR of %d data packets (%d bytes).\n",
			colorTag("+", "cyan"), b, BlockSize, PacketSize)
	}

	fmt.Printf("\n  %s  Total packets ready for transmission: %d (%d data + %d parity).\n",
		colorTag(">", "blue"), len(allPackets), TotalDataPackets, numBlocks)

	// -- Step 4: Simulate Lossy Network Channel --
	banner("NETWORK CHANNEL - ASYNCHRONOUS LOSSY TRANSMISSION")
	fmt.Printf("  %s  Simulating channel with %.0f%% random packet drop rate...\n\n",
		colorTag(">", "blue"), DropRate*100)

	rx := make(chan *Packet, ChannelBuffer)

	// Launch network simulator as a concurrent Goroutine.
	// We run it synchronously here to capture statistics, but the channel
	// itself models the async producer-consumer pattern.
	var totalSent, totalDropped int
	go func() {
		totalSent, totalDropped = simulateNetwork(allPackets, rx, rng)
	}()

	// Small delay to let the goroutine print drop logs before receiver starts.
	// In production, the receiver would start immediately and process concurrently.
	// Collect all received packets first.
	var received []*Packet
	for pkt := range rx {
		received = append(received, pkt)
	}

	fmt.Printf("\n  %s  Transmission complete: %d sent, %d dropped, %d delivered.\n",
		colorTag(">", "blue"), totalSent, totalDropped, totalSent-totalDropped)

	// -- Step 5: Receive & Reconstruct --
	// Feed received packets through a new channel for the reconstruction engine.
	rx2 := make(chan *Packet, len(received))
	for _, pkt := range received {
		rx2 <- pkt
	}
	close(rx2)

	receiveAndReconstruct(rx2, dataPackets)
}
