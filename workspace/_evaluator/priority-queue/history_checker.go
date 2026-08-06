package main

func checkScenarioHistory(s scenario, capacity int, history []recordedOperation) bool {
	switch s {
	case scenarioSPSC, scenarioMPSC, scenarioMPMC:
		return checkPriorityHistory(capacity, history)
	default:
		return false
	}
}

func correctnessContract(s scenario) string {
	return "linearizable bounded priority queue"
}
