package main

import (
	"fmt"
	"math/rand"
	"strconv"
)

type pendingInput struct {
	attempts         int
	minPublishHeight int
	nr               int
	createdHeight    int
	confirmHeight    int
	published        bool
}

func (p *pendingInput) String() string {
	return strconv.Itoa(p.nr)
}

var (
	maxPerTx = 10
)

func main() {
	inputNr := 0
	block := 0
	pi := make(map[int]*pendingInput)

	totalSweepTime := 0
	totalSweeps := 0
	totalFails := 0
	totalTxCount := 0

	for {
		fmt.Printf("Height: %v\n", block)

		// Confirm txes.
		var confInputs []int
		for _, input := range pi {
			if !input.published || input.confirmHeight > block {
				continue
			}

			confInputs = append(confInputs, input.nr)
			sweepTime := block - input.createdHeight

			totalSweepTime += sweepTime
			totalSweeps++

			delete(pi, input.nr)
		}
		fmt.Printf("Confirming inputs: %v\n", confInputs)
		if totalSweeps > 0 {
			fmt.Printf("Avg sweep time: %v, avg inputs per tx: %v, fail percentage: %v\n",
				float32(totalSweepTime)/float32(totalSweeps),
				float32(totalSweeps)/float32(totalTxCount),
				100*totalFails/totalSweeps,
			)
		}

		// Add between 0 and 9 new inputs.
		newInputCount := int(rand.Int31n(10))
		for i := 0; i < newInputCount; i++ {
			pi[inputNr] = &pendingInput{
				nr:            inputNr,
				createdHeight: block,
			}
			inputNr++
		}

		// Filter on inputs that reached their publish height. Split in
		// two sets to give every new input a fresh tx without retried
		// inputs.
		var newInputs, allInputs []*pendingInput
		for _, i := range pi {
			if i.minPublishHeight > block {
				continue
			}
			if i.attempts == 0 && !i.published {
				newInputs = append(newInputs, i)
			}
			allInputs = append(allInputs, i)
		}

		// Construct txes.
		publishTxes := func(set []*pendingInput) {
			var inputs []*pendingInput
			for len(set) > 0 {
				inputs = append(inputs, set[0])
				set = set[1:]

				if len(inputs) >= maxPerTx || len(set) == 0 {
					// Check for double spend. Assume no RBF
					// (btcd).
					doubleSpend := false
					for _, i := range inputs {
						if i.published {
							doubleSpend = true
							break
						}
					}
					if doubleSpend {
						fmt.Printf("Publish tx double spend for inputs: %v \n", inputs)
						for _, i := range inputs {
							delta := 1

							// Exponential back-off
							// delta := 1 << uint(i.attempts)

							// Exponential back-off window
							// delta += int(rand.Int31n(int32(delta)))

							i.minPublishHeight = block + delta
							i.attempts++
							if i.attempts < 144 {
								fmt.Printf("   Reschedule %v for block %v\n", i.nr, i.minPublishHeight)
							} else {
								fmt.Printf("   Sweep %v failed\n", i.nr)
								delete(pi, i.nr)
								totalFails++
							}
						}

					} else {
						totalTxCount++

						// Confirm within 1 to 6 blocks from now.
						delta := 1 + int(rand.Int31n(5))

						// Sometimes the tx gets stuck
						// because too low fee.
						if rand.Int31n(5) == 0 {
							delta = 100000
						}

						for _, i := range inputs {
							// Determine confirmation height already now.
							i.confirmHeight = block + delta
							i.published = true
						}
						fmt.Printf("Publish tx for inputs: %v\n", inputs)
					}
					inputs = nil
				}
			}
		}

		fmt.Printf("New inputs:\n")
		publishTxes(newInputs)

		fmt.Printf("Retry inputs:\n")
		publishTxes(allInputs)

		// Increase block height.
		block++
	}

}
