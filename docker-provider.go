package cloudcompute

import (
	"errors"
	"fmt"
	"strings"
)

type DockerProvider struct {
	Concurrency       int
	RegisteredPlugins []*Plugin
	Jobs              []Job
	JobSummaries      map[string]JobSummary
	Logs              map[string]string
}

/*
func NewDockerProvider(maxConcurrency int) (*DockerProvider, error) {

}

func (dp *DockerProvider) start() error {
	//go function to continuously evaluate and run jobs
}
*/

//type DockerPlugin struct{}

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
	return nil
}
