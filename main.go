package main

import (
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"

	"github.com/gocql/gocql"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
)

func main() {
	// Installed Datstax recent version locally and created keyspace go and table stats
	// Connect to Cassandra cluster
	cluster := gocql.NewCluster("127.0.0.1") // Cassandra cluster address
	cluster.Keyspace = "go"                  // Keyspace name
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal("Failed to connect to Cassandra:", err)
	}
	defer session.Close()

	// Set path for nodetool commands
	nodetoolPath := "/Downloads/dse/bin/"

	// Get CPU usage
	cpuPercent, err := cpu.Percent(0, false)
	if err != nil {
		log.Fatal("Failed to get CPU usage:", err)
	}
	cpuUsage := fmt.Sprintf("%.2f%%", cpuPercent[0])

	// Get memory usage
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		log.Fatal("Failed to get memory usage:", err)
	}
	memUsage := fmt.Sprintf("%.2f%%", memInfo.UsedPercent)

	// Run nodetool tablehistograms for keyspace "go" and table "stats"
	readLatency, writeLatency, err := runNodetoolTablehistograms(nodetoolPath, "go", "stats")
	if err != nil {
		log.Fatal("Failed to get tablehistograms:", err)
	}

	// Run nodetool compactionstats
	pendingCompactions, err := runNodetoolCompactionstats(nodetoolPath)
	if err != nil {
		log.Fatal("Failed to get compactionstats:", err)
	}

	// Get count of total number of rows for table "dse_perf.user_io"
	activeConnections, err := getRowCount(session, "dse_perf", "user_io")
	if err != nil {
		log.Fatal("Failed to get row count:", err)
	}

	// Get storage space utilization
	// Can also calculate size of dse-data directory  to get data size
	storageUtilization, err := getStorageSpaceUtilization()
	if err != nil {
		log.Fatal("Failed to get storage space utilization:", err)
	}

	// Printing extracted values in key-value format
	fmt.Println("cpu:", cpuUsage)
	fmt.Println("memory:", memUsage)
	fmt.Println("read_latency:", readLatency)
	fmt.Println("write_latency:", writeLatency)
	fmt.Println("pending_compactions:", pendingCompactions)
	fmt.Println("active_connections:", activeConnections)
	fmt.Println("storage_utilization:", storageUtilization)
}

func runNodetoolTablehistograms(nodetoolPath, keyspace, table string) (string, string, error) {
	cmd := exec.Command(nodetoolPath+"nodetool", "tablehistograms", keyspace, table)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", "", fmt.Errorf("failed to run nodetool tablehistograms: %s", err)
	}

	outputStr := string(output)
	lines := strings.Split(outputStr, "\n")
	readLatency := ""
	writeLatency := ""

	for _, line := range lines {
		if strings.Contains(line, "Read latency histogram") {
			readLatency = strings.TrimSpace(strings.TrimPrefix(line, "Read latency histogram:"))
		} else if strings.Contains(line, "Write latency histogram") {
			writeLatency = strings.TrimSpace(strings.TrimPrefix(line, "Write latency histogram:"))
		}
	}

	return readLatency, writeLatency, nil
}

func runNodetoolCompactionstats(nodetoolPath string) (int, error) {
	cmd := exec.Command(nodetoolPath + "nodetool compactionstats")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("failed to run nodetool compactionstats: %s", err)
	}

	outputStr := string(output)
	lines := strings.Split(outputStr, "\n")
	pendingCompactions := 0

	for _, line := range lines {
		if strings.HasPrefix(line, "pending tasks:") {
			pendingCompactionsStr := strings.TrimSpace(strings.TrimPrefix(line, "pending tasks:"))
			pendingCompactions, _ = strconv.Atoi(pendingCompactionsStr)
			break
		}
	}

	return pendingCompactions, nil
}

func getRowCount(session *gocql.Session, keyspace, table string) (int, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", keyspace, table)
	iter := session.Query(query).Iter()
	var rowCount int
	if iter.Scan(&rowCount) {
		return rowCount, nil
	}
	return 0, iter.Close()
}

func getStorageSpaceUtilization() (string, error) {
	cmd := exec.Command("df", "-h")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to run df command: %s", err)
	}

	outputStr := string(output)
	lines := strings.Split(outputStr, "\n")
	storageUtilization := ""

	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 5 && fields[5] == "/" {
			storageUtilization = fields[4]
			break
		}
	}

	return storageUtilization, nil
}
