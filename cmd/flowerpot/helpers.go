package main

import (
	"fmt"
	"os"
	"strconv"
)

// readPIDFile returns (pid, port) from a Flowerpot PID file.
func readPIDFile(path string) (int, int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}
	var pid, port int
	n, err := fmt.Sscanf(string(data), "%d\n%d", &pid, &port)
	if err != nil || n < 2 {
		lines := splitLines(string(data))
		if len(lines) >= 1 {
			pid, _ = strconv.Atoi(lines[0])
		}
		if len(lines) >= 2 {
			port, _ = strconv.Atoi(lines[1])
		}
	}
	return pid, port, nil
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
