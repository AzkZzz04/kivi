package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/arthurzhang/kivi/internal/testutil"
)

var (
	workloadStr = flag.String("workload", "A", "Workload type (A,B,C,E,F)")
	numKeys     = flag.Int64("num-keys", 100000, "Number of keys")
	valueSize   = flag.Int("value-size", 256, "Value size in bytes")
	numOps      = flag.Int("num-ops", 100000, "Number of operations")
	skew        = flag.Float64("skew", 0.99, "Zipfian skew parameter")
	duration    = flag.Duration("duration", 30*time.Second, "Benchmark duration")
	seed        = flag.Int64("seed", 12345, "Random seed")
	outDir      = flag.String("out", "runs", "Output directory")
)

func main() {
	flag.Parse()

	workload := parseWorkload(*workloadStr)

	logger, err := testutil.SetupLogging(fmt.Sprintf("%s/bench.log", *outDir), testutil.LevelInfo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to setup logging: %v\n", err)
		os.Exit(1)
	}

	logger.Info("Starting benchmark")
	logger.Info("  Workload: %s", *workloadStr)
	logger.Info("  Num Keys: %d", *numKeys)
	logger.Info("  Value Size: %d bytes", *valueSize)
	logger.Info("  Num Ops: %d", *numOps)
	logger.Info("  Skew: %.2f", *skew)
	logger.Info("  Seed: %d", *seed)

	gen := testutil.NewWorkloadGenerator(workload, *seed, *numKeys, *valueSize, *skew)
	gen.SetNumOps(*numOps)

	stats := testutil.NewBenchStats()
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	timer := testutil.NewTimer("benchmark")

	// Run benchmark (this will be connected to the actual store later)
	for {
		select {
		case <-ctx.Done():
			goto done
		default:
			op, key, val, err := gen.Next()
			if err != nil {
				goto done
			}

			opStart := time.Now()
			_ = simulateOp(op, key, val)
			opLatency := time.Since(opStart)

			stats.Record(op, opLatency)
		}
	}

done:
	timer.Log(logger)
	stats.Print(logger)

	logger.Info("Benchmark complete")
}

func parseWorkload(s string) testutil.WorkloadType {
	switch s {
	case "A":
		return testutil.WorkloadA
	case "B":
		return testutil.WorkloadB
	case "C":
		return testutil.WorkloadC
	case "E":
		return testutil.WorkloadE
	case "F":
		return testutil.WorkloadF
	default:
		return testutil.WorkloadA
	}
}

// simulateOp is a placeholder that will be replaced with actual store operations.
func simulateOp(op string, key, val []byte) error {
	// Simulate some work
	time.Sleep(1 * time.Microsecond)
	return nil
}
