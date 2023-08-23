package main

import (
	//"net/http"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin" //rest server library
)

type worker struct {
	State     string `json:"State"`           // claimed | unclaimed
	Arch      string `json:"Arch"`            // x86_64 | arm64
	Hostname  string `json:"UtsnameNodename"` // hostname
	TotalCpus int    `json:"TotalCpus"`       // number of cpus on the machine running the worker pod
	Name      string `json:"Name"`            // slot name
}

type job struct {
	DAG_JobsIdle   string `json:"DAG_JobsIdle"`    // number of idle jobs (unused so far)
	DAGManJobId    string `json:"DAGManJobId"`     // job id
	DAGNodeName    string `json:"DAGNodeName"`     // job name
	JobStatus      int    `json:"JobStatus"`       // 1 = idle, 2 = running, 5 = held, 9 = completed
	WFName         string `json:"pegasus_wf_name"` // workflow name
	Requirements   string `json:"Requirements"`    //raw expression containing arch
	Arch           string // extracted arch
	DAG_NodesReady string `json:"DAG_NodesReady"`       // number of ready jobs
	DAG_NodesTotal string `json:"DAG_NodesTotal"`       // total number of jobs
	Notes          string `json:"SubmitEventUserNotes"` // notes
	Site           string `json:"pegasus_site"`         // site: local = executing on schedd (not involved in autoscaling), other = executing on worker
}

/*
*
HTCondor allows us to partition a worker into slots, each slot has a number of cpus.
in our setup each slot has 1 cpu.
HTCondor will use the number of cpus to schedule jobs in a ONE to ONE fashion.
when we condor_status the cluster we get a list of workers, in this setup the main worker machine is always unclaimed.
when htcondor schedule a job on a worker it will create a new slot that will appear in the condor_status result as a new machine:
Example 2 jobs running on a worker with 2 slots:

		slot1@htcondor-worker-xxxx-yyyy 	(WORKER MAIN MACHINE) - unclaimed
	  slot1_1@htcondor-worker-xxxx-yyyy (Same machine as above) - claimed (running JOB A)
		slot1_2@htcondor-worker-xxxx-yyyy (Same machine as above) - claimed (running JOB B)
		slot1@htcondor-worker-xxxx-zzzz 	(DIFFERENT WORKER MAIN MACHINE) - unclaimed

We want to know the ratio between the number of running jobs and the number of slots in order to implement autoscaling of worker nodes.
We cannot count the length of the list of slots because it will include the main worker machine that is always unclaimed.
We need to count the number of slots that are claimed and group them by hostname (the related worker machine name).
This defined type worker_map has this purpose:
the keys are the hostnames of the worker machines and the values are list of slots claimed by that worker.
Following the previous example the worker_map will look like this:

	{
		htcondor-worker-xxxx-yyyy: [slot1_1@htcondor-worker-xxxx-yyyy, slot1_2@htcondor-worker-xxxx-yyyy]
		htcondor-worker-xxxx-zzzz: [slot1@htcondor-worker-xxxx-zzzz]
	}
*/
type worker_map map[string][]worker

// typings of supported worker architectures
type arch string

const (
	amd64 arch = "x86_64"
	arm64 arch = "aarch64"
)

func getWorkers(arch arch) ([]worker, worker_map) {
	cmd := "condor_status -json |  jq '[.[] | {State,Arch,UtsnameNodename,TotalCpus,Name}]'"
	all_workers := []worker{}
	out, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		log.Fatal(err)
	}
	json.Unmarshal(out, &all_workers)

	var m = map[string][]worker{}
	for _, worker := range all_workers {
		if strings.ToLower(worker.Arch) == string(arch) {
			m[worker.Hostname] = append(m[worker.Hostname], worker)
		}
	}
	pretty_m, _ := json.MarshalIndent(m, "", "    ")
	fmt.Print("[WORKER MAP]: ", string(pretty_m), "\n")
	return all_workers, m
}

// check if element is in array
func contains_worker(workers []worker, hostname string) bool {
	for _, w := range workers {
		if w.Hostname == hostname {
			return true
		}
	}
	return false
}

func selectWorkersByArch(workers []worker, arch string) []worker {
	selected_workers := []worker{}

	for _, worker := range workers {
		if worker.Arch == arch && !contains_worker(selected_workers, "Hostanme") {
			selected_workers = append(selected_workers, worker)
		}
	}
	return selected_workers
}

func selectWorkersByState(workers []worker, state string) []worker {
	selected_workers := []worker{}

	for _, worker := range workers {
		if worker.State == state {
			selected_workers = append(selected_workers, worker)
		}
	}
	return selected_workers
}

func getJobs(arch arch) []job {
	cmd := "condor_q -json | " +
		" jq '[.[] | select(.pegasus_site != \"local\") " +
		"| {DAG_JobsIdle,DAGManJobId,DAGNodeName,JobStatus,pegasus_wf_name,Requirements,DAG_NodesReady,DAG_NodesTotal,SubmitEventUserNotes,pegasus_site}]'"
	all_jobs := []job{}
	arch_jobs := []job{}
	out, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		log.Fatal(err)
	}
	json.Unmarshal(out, &all_jobs)
	archRegex := regexp.MustCompile(`Arch == "(.+?)"`)
	for i, job := range all_jobs {
		match := archRegex.FindStringSubmatch(job.Requirements)
		all_jobs[i].Arch = match[1]
		if strings.ToLower(match[1]) == string(arch) {
			arch_jobs = append(arch_jobs, job)
		}
	}
	return arch_jobs
}
func getJobsAMD64(c *gin.Context) {
	cmd := "condor_q -json | " +
		" jq '[.[] | select(.pegasus_site != \"local\") " +
		"| {DAG_JobsIdle,DAGManJobId,DAGNodeName,JobStatus,pegasus_wf_name,Requirements,DAG_NodesReady,DAG_NodesTotal,SubmitEventUserNotes,pegasus_site}]'"
	all_jobs := []job{}
	arch_jobs := make([]job, 0)
	out, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		log.Fatal(err)
	}
	json.Unmarshal(out, &all_jobs)
	archRegex := regexp.MustCompile(`Arch == "(.+?)"`)
	for i, job := range all_jobs {
		match := archRegex.FindStringSubmatch(job.Requirements)
		all_jobs[i].Arch = match[1]
		if strings.ToLower(match[1]) == string(amd64) {
			arch_jobs = append(arch_jobs, all_jobs[i])
		}
	}

	fmt.Println(len(arch_jobs))
	c.JSONP(http.StatusOK, arch_jobs)
}
func getJobsARM64(c *gin.Context) {
	cmd := "condor_q -json | " +
		" jq '[.[] | select(.pegasus_site != \"local\") " +
		"| {DAG_JobsIdle,DAGManJobId,DAGNodeName,JobStatus,pegasus_wf_name,Requirements,DAG_NodesReady,DAG_NodesTotal,SubmitEventUserNotes,pegasus_site}]'"
	all_jobs := []job{}
	arch_jobs := []job{}
	out, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		log.Fatal(err)
	}
	json.Unmarshal(out, &all_jobs)
	archRegex := regexp.MustCompile(`Arch == "(.+?)"`)
	for i, job := range all_jobs {
		match := archRegex.FindStringSubmatch(job.Requirements)
		all_jobs[i].Arch = match[1]
		if strings.ToLower(match[1]) == string(arm64) {
			arch_jobs = append(arch_jobs, all_jobs[i])
		}
	}
	fmt.Println(len(arch_jobs))
	c.JSONP(http.StatusOK, arch_jobs)
}

func get_amd64_metrics(c *gin.Context) {

	file, err := os.OpenFile("workers.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Create a logger that writes to the file
	//logger := log.New(file, "", log.LstdFlags)
	_, worker_map := getWorkers(amd64)
	all_jobs := getJobs(amd64)

	//fmt.Println(all_jobs)

	//AMD64_jobs := selectJobByArch(all_jobs, "X86_64")
	//AMD64_jobs_idle := selectJobByStatus(AMD64_jobs, 1)

	worker_claimed := 0
	cpu_claimed := 0
	total_workers := 0
	total_cpus := 0
	for _, v := range worker_map {
		//logger.Printf("State: %s, Arch: %s, Hostname: %s, TotalCpus: %d\n", v[0].State, v[0].Arch, v[0].Hostname, v[0].TotalCpus)

		if len(v) > 1 {
			worker_claimed += 1
			cpu_claimed += len(v) - 1
		}
		total_workers += 1
		total_cpus += v[0].TotalCpus
	}
	//logger.Printf("worker_claimed: %d, cpu_claimed: %d, total_workers: %d, total_cpus: %d, jobs_idle: %d\n", worker_claimed, cpu_claimed, total_workers, total_cpus, len(AMD64_jobs_idle))

	// fmt.Println("worker_claimed: ", worker_claimed)
	fmt.Println("cpu_claimed: ", cpu_claimed)
	//fmt.Println("total_workers: ", total_workers)
	fmt.Println("total_cpus: ", total_cpus)
	fmt.Println("all_jobs: ", len(all_jobs))
	avg_node_cpus := float64(total_cpus) / float64(total_workers)
	metric := math.Floor(float64(len(all_jobs)) / float64(total_cpus))
	fmt.Printf("Metric: ((all_jobs) %d)/(tot_cpus) %d)/(avg_cpus) %2.f = %2.f\n", len(all_jobs), total_cpus, avg_node_cpus, metric)

	prom_help := "# HELP cluster_utilization The amount of requests served by the server in total\n"
	prom_type := "# TYPE cluster_utilization counter\n"
	prom_metric := "amd64_cluster_utilization " + fmt.Sprintf("%.2f", metric)
	c.String(200, prom_help+prom_type+prom_metric)
}

func get_arm64_metrics(c *gin.Context) {

	file, err := os.OpenFile("workers.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Create a logger that writes to the file
	//logger := log.New(file, "", log.LstdFlags)
	_, worker_map := getWorkers(arm64)
	all_jobs := getJobs(arm64)

	//fmt.Println(all_jobs)

	//AMD64_jobs := selectJobByArch(all_jobs, "X86_64")
	//AMD64_jobs_idle := selectJobByStatus(AMD64_jobs, 1)

	worker_claimed := 0
	cpu_claimed := 0
	total_workers := 0
	total_cpus := 0
	for _, v := range worker_map {
		//logger.Printf("State: %s, Arch: %s, Hostname: %s, TotalCpus: %d\n", v[0].State, v[0].Arch, v[0].Hostname, v[0].TotalCpus)

		if len(v) > 1 {
			worker_claimed += 1
			cpu_claimed += len(v) - 1
		}
		total_workers += 1
		total_cpus += v[0].TotalCpus
	}
	//logger.Printf("worker_claimed: %d, cpu_claimed: %d, total_workers: %d, total_cpus: %d, jobs_idle: %d\n", worker_claimed, cpu_claimed, total_workers, total_cpus, len(AMD64_jobs_idle))

	// fmt.Println("worker_claimed: ", worker_claimed)
	fmt.Println("cpu_claimed: ", cpu_claimed)
	//fmt.Println("total_workers: ", total_workers)
	fmt.Println("total_cpus: ", total_cpus)
	fmt.Println("all_jobs: ", len(all_jobs))
	avg_node_cpus := float64(total_cpus) / float64(total_workers)
	metric := math.Floor(float64(len(all_jobs)) / float64(total_cpus))
	fmt.Printf("Metric: ((all_jobs) %d)/(tot_cpus) %d)/(avg_cpus) %2.f = %2.f\n", len(all_jobs), total_cpus, avg_node_cpus, metric)

	prom_help := "# HELP cluster_utilization The amount of requests served by the server in total\n"
	prom_type := "# TYPE cluster_utilization counter\n"
	prom_metric := "arm64_cluster_utilization " + fmt.Sprintf("%.2f", metric)
	c.String(200, prom_help+prom_type+prom_metric)
}

func main() {
	router := gin.Default()

	router.GET("/metrics/amd64", get_amd64_metrics)
	router.GET("/metrics/arm64", get_arm64_metrics)
	router.GET("/jobs/amd64", getJobsAMD64)
	router.GET("/jobs/arm64", getJobsARM64)
	fmt.Println(string(amd64))
	fmt.Println(string(arm64))
	router.Run(":8080")
}
