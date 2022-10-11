package watcompute

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/aws"
)

var awsComputeProfile string = "wat_compute" //@TODO: get this from environment
var awsLogGroup string = "/aws/batch/job"
var ctx context.Context = context.Background()

type AwsBatchProvider struct {
	client *batch.Client
	logs   *cloudwatchlogs.Client
}

func NewAwsBatchProvider() (*AwsBatchProvider, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithSharedConfigProfile(awsComputeProfile))
	if err != nil {
		log.Println("Failed to load an AWS Config")
		return nil, err
	}

	svc := batch.NewFromConfig(cfg)
	logs := cloudwatchlogs.NewFromConfig(cfg)

	return &AwsBatchProvider{svc, logs}, nil
}

func (abp *AwsBatchProvider) SubmitJob(job *Job) error {
	var retryStrategy *types.RetryStrategy
	var timeout *types.JobTimeout

	if job.RetryAttemts > 0 {
		retryStrategy = &types.RetryStrategy{Attempts: &job.RetryAttemts}
	}

	if job.JobTimeout > 0 {
		timeout = &types.JobTimeout{AttemptDurationSeconds: &job.JobTimeout}
	}

	input := &batch.SubmitJobInput{
		JobDefinition:      &job.JobDefinition,
		JobName:            &job.JobName,
		JobQueue:           &job.JobQueue,
		DependsOn:          toBatchDependency(job.DependsOn),
		ContainerOverrides: toBatchContainerOverrides(job.ContainerOverrides),
		Parameters:         job.Parameters,
		Tags:               job.Tags,
		RetryStrategy:      retryStrategy,
		Timeout:            timeout,
	}

	submitResult, err := abp.client.SubmitJob(ctx, input)
	if err != nil {
		log.Printf("Failed to submit batch job: %s using definition %s on queue %s.\n", job.JobName, job.JobDefinition, job.JobQueue)
		return err
	}

	job.SubmittedJob = &SubmitJobResult{
		JobId:        submitResult.JobId,
		ResourceName: submitResult.JobArn,
	}

	return nil
}

func (abp *AwsBatchProvider) Status(jobQueue string, query JobsSummaryQuery) ([]JobSummary, error) {

	queryString := fmt.Sprintf("WAT_C_%s*", query.QueryValue)

	eventFilter := types.KeyValuesPair{
		Name:   aws.String("JOB_NAME"),
		Values: []string{queryString},
	}

	input := batch.ListJobsInput{
		JobQueue: &jobQueue,
		Filters:  []types.KeyValuesPair{eventFilter},
	}

	output, err := abp.client.ListJobs(ctx, &input)
	if err != nil {
		return nil, err
	}
	return listOutput2JobSummary(output), nil
}

//@TODO this assumes the logs are rather short.
//Need to update for logs that require pagenation in the AWS SDK
func (abp *AwsBatchProvider) JobLog(submittedJobId string) ([]string, error) {
	jobDesc, err := abp.describeBatchJobs([]string{submittedJobId})
	if err != nil {
		return nil, err
	}
	cfg := cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  &awsLogGroup,
		LogStreamName: jobDesc.Jobs[0].Container.LogStreamName,
	}
	logevents, err := abp.logs.GetLogEvents(ctx, &cfg)
	out := make([]string, len(logevents.Events))
	for i, v := range logevents.Events {
		t := time.Unix(*v.Timestamp, 0)
		out[i] = fmt.Sprintf("%v: %s", t, *v.Message)
	}
	return out, nil
}

func (abp *AwsBatchProvider) listBatchJob(job *Job) (*batch.ListJobsOutput, error) {
	input := batch.ListJobsInput{
		JobQueue:  &job.JobQueue,
		JobStatus: types.JobStatusSucceeded,
	}

	return abp.client.ListJobs(ctx, &input)
}

func (abp *AwsBatchProvider) describeBatchJobs(submittedJobIds []string) (*batch.DescribeJobsOutput, error) {
	input := batch.DescribeJobsInput{
		Jobs: submittedJobIds,
	}
	return abp.client.DescribeJobs(ctx, &input)
}

/*
func jobIds(jobs []Job) []string {
	jobIds := make([]string, len(jobs))
	for i, v := range jobs {
		jobIds[i] = *v.SubmittedJob.JobId
	}
	return jobIds
}
*/

func toBatchContainerOverrides(co ContainerOverrides) *types.ContainerOverrides {

	awskvp := make([]types.KeyValuePair, len(co.Environment))

	for i, kvp := range co.Environment {
		kvpl := kvp //make local copy of kvp to avoid aws pointing to a single kvp reference
		awskvp[i] = types.KeyValuePair{
			Name:  &kvpl.Name,
			Value: &kvpl.Value,
		}
	}

	return &types.ContainerOverrides{
		Command:     co.Command,
		Environment: awskvp,
	}
}

func toBatchDependency(jobDependency []JobDependency) []types.JobDependency {
	batchDeps := make([]types.JobDependency, len(jobDependency))
	for i, d := range jobDependency {
		batchDeps[i] = types.JobDependency{
			JobId: &d.JobId,
			Type:  types.ArrayJobDependencySequential,
		}
	}
	return batchDeps
}

func listOutput2JobSummary(output *batch.ListJobsOutput) []JobSummary {
	js := make([]JobSummary, len(output.JobSummaryList))
	for i, s := range output.JobSummaryList {
		js[i] = JobSummary{
			JobId:        *s.JobId,
			JobName:      *s.JobName,
			CreatedAt:    s.CreatedAt,
			StartedAt:    s.StartedAt,
			Status:       string(s.Status),
			StatusDetail: s.StatusReason,
			StoppedAt:    s.StoppedAt,
			ResourceName: *s.JobArn,
		}
	}
	return js
}
