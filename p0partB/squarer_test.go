// Tests for SquarerImpl. Students should write their code in this file.

package p0partB

import (
	"fmt"
	"testing"
	"time"
)

const (
	timeoutMillis = 5000
)

// A basic test for example
func TestBasicCorrectness(t *testing.T) {
	fmt.Println("Running TestBasicCorrectness.")
	input := make(chan int)
	sq := SquarerImpl{}
	squares := sq.Initialize(input)
	go func() {
		input <- 2
	}()
	timeoutChan := time.After(time.Duration(timeoutMillis) * time.Millisecond)
	select {
	case <-timeoutChan:
		t.Error("Test timed out.")
	case result := <-squares:
		if result != 4 {
			t.Error("Error, got result", result, ", expected 4 (=2^2).")
		}
	}
}

func TestSquarerImplWithNegativeValue(t *testing.T) {
	fmt.Println("Running TestSquarerImplWithNegativeValue.")
	helperFuncForTest(t, -10, 100)
}

func TestSquarerImplWithZeroValue(t *testing.T) {
	fmt.Println("Running TestSquarerImplWithZeroValue.")
	helperFuncForTest(t, 0, 0)
}

func helperFuncForTest(t *testing.T, inputValue int, expectedValue int) {
	input := make(chan int)
	sq := SquarerImpl{}
	squares := sq.Initialize(input)
	go func() {
		input <- inputValue
	}()
	timeoutChan := time.After(time.Duration(timeoutMillis) * time.Millisecond)
	select {
	case <-timeoutChan:
		t.Error("Test timed out.")
	case result := <-squares:
		if result != expectedValue {
			t.Error("Error, got result", result, ", expected ", expectedValue, "(", inputValue, "*", inputValue, ")")
		}
	}
}
