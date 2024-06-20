package cloudcompute

import (
	"errors"
	"fmt"
	"strings"
)

type DockerProvider struct {
	Concurrency       int
	RegisteredPlugins []*Plugin
	Jobs              []*Job
	JobSummaries      map[string]JobSummary
	Logs              map[string]string
}

/*
func NewDockerProvider(maxConcurrency int) (*DockerProvider, error) {
	dcm := NewDockerComputeManager(DockerComputeManagerConfig{
		Concurrency: 10,
	})
	dcm.StartMonitor()
	return &DockerComputeProvider{dcm}

}
*/

func (dp *DockerProvider) registeredPlugin(name string) (*Plugin, error) {
	for _, v := range dp.RegisteredPlugins {
		if v.Name == name {
			return v, nil
		}
	}
	return nil, errors.New("Plugin is not registered.")
}

/*

func (dp *DockerProvider) start() error {
	//go function to continuously evaluate and run jobs
	var runtimeLimit int = 3
	semaphore := make(chan struct{}, runtimeLimit)
	for i := 0; i < 1000; i++ {
		semaphore <- struct{}{}
		go func(event int) {
			defer func() {
				<-semaphore
			}()
			//runHelloWorld()
			fmt.Printf("Running:%d\n", event)
		}(i)
	}

	// Wait for goroutines to finish by acquiring all slots.
	for i := 0; i < cap(semaphore); i++ {
		semaphore <- struct{}{}
	}
	return nil
}
*/

func (dp *DockerProvider) runJob(job *Job) error {
	//rate limited running of jobs
	plugin, err := dp.registeredPlugin(job.JobDefinition)
	if err != nil {
		return err
	}
	runner, err := NewRunner(job, plugin)
	if err != nil {
		panic(err)
	}
	defer runner.Close()
	runner.Start()

	return nil
}

func (dp *DockerProvider) RegisterPlugin(plugin *Plugin) (PluginRegistrationOutput, error) {
	dp.RegisteredPlugins = append(dp.RegisteredPlugins, plugin)
	return PluginRegistrationOutput{
		Name:         plugin.Name + ":0",
		ResourceName: "cc:local-docker",
	}, nil
}

func (dp *DockerProvider) UnregisterPlugin(nameAndRevision string) error {
	for i, v := range dp.RegisteredPlugins {
		if v.Name == nameAndRevision {
			plugins := dp.RegisteredPlugins
			dp.RegisteredPlugins = append(plugins[:i], plugins[i+1:]...)
		}
	}
	return nil
}

func (dp *DockerProvider) JobLog(submittedJobId string) ([]string, error) {
	if log, ok := dp.Logs[submittedJobId]; ok {
		return []string{log}, nil
	}
	return nil, errors.New(fmt.Sprintf("Log not found for job: %s", submittedJobId))
}

func (dp *DockerProvider) Status(jobQueue string, query JobsSummaryQuery) error {
	queryString := StatusQueryString(query)
	jobSummaries := []JobSummary{}
	processingBatchSize := 100
	for i, job := range dp.Jobs {
		if strings.HasPrefix(job.JobName, queryString) {
			if i%processingBatchSize == 0 {
				query.JobSummaryFunction(jobSummaries)
				jobSummaries = []JobSummary{}
			} else {
				jobSummaries = append(jobSummaries, dp.JobSummaries[job.JobName])
			}
		}
	}
	//process any remaining summaries in the queue
	if len(jobSummaries) > 0 {
		query.JobSummaryFunction(jobSummaries)
	}
	return nil
}

func (dp *DockerProvider) SubmitJob(job *Job) error {
	if job == nil {
		return errors.New("Job cannot be nil.")
	}
	dp.Jobs = append(dp.Jobs, job)
	return nil
}
