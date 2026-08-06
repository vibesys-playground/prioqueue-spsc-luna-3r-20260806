package main

import "testing"

func enqueueRecord(client int, value uint64, ok bool, call int64) recordedOperation {
	return priorityEnqueueRecord(client, value, 0, ok, call)
}

func priorityEnqueueRecord(
	client int,
	value uint64,
	priority uint64,
	ok bool,
	call int64,
) recordedOperation {
	return recordedOperation{
		ClientID: client,
		Input:    queueInput{Kind: "enqueue", Value: &value, Priority: &priority},
		Call:     call,
		Output:   queueOutput{EnqueueOK: &ok},
		Return:   call + 1,
	}
}

func dequeueRecord(client int, value *uint64, call int64) recordedOperation {
	output := queueOutput{DequeueNone: value == nil}
	if value != nil {
		copy := *value
		output.DequeueVal = &copy
	}
	return recordedOperation{
		ClientID: client,
		Input:    queueInput{Kind: "dequeue"},
		Call:     call,
		Output:   output,
		Return:   call + 1,
	}
}

func value(value uint64) *uint64 {
	return &value
}

func TestScenarioModelAllowsEitherOrderWithinPriority(t *testing.T) {
	history := []recordedOperation{
		priorityEnqueueRecord(0, 10, 9, true, 1),
		priorityEnqueueRecord(0, 20, 2, true, 3),
		priorityEnqueueRecord(0, 30, 2, true, 5),
		dequeueRecord(0, value(30), 7),
		dequeueRecord(0, value(20), 9),
		dequeueRecord(0, value(10), 11),
	}
	if !checkScenarioHistory(scenarioSPSC, 3, history) {
		t.Fatal("equal-priority reordering was rejected")
	}
}

func TestModelsAgreeOnSequentialHistories(t *testing.T) {
	tests := map[string][]recordedOperation{
		"valid bounded queue": {
			enqueueRecord(0, 10, true, 1),
			enqueueRecord(0, 20, false, 3),
			dequeueRecord(0, value(10), 5),
			dequeueRecord(0, nil, 7),
		},
		"duplicate payloads": {
			enqueueRecord(0, 10, true, 1),
			dequeueRecord(0, value(10), 3),
			enqueueRecord(0, 10, true, 5),
			dequeueRecord(0, value(10), 7),
		},
		"false full": {
			enqueueRecord(0, 10, false, 1),
		},
		"false empty": {
			enqueueRecord(0, 10, true, 1),
			dequeueRecord(0, nil, 3),
		},
	}

	for name, history := range tests {
		t.Run(name, func(t *testing.T) {
			exact := checkPriorityHistory(1, history)
			scenario := checkScenarioHistory(scenarioSPSC, 1, history)
			if exact != scenario {
				t.Fatalf("model verdicts differ: exact=%t scenario=%t", exact, scenario)
			}
		})
	}
}
