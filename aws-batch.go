package cloudcompute

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

	. "github.com/usace/cc-go-sdk"
)

var awsLogGroup string = "/aws/batch/job"
var ctx context.Context = context.Background()

type AwsBatchProviderInput struct {
	ExecutionRole string
	BatchRegion   string
	ConfigProfile string
}

// AWS Batch Compute Provider implementation
type AwsBatchProvider struct {
	client        *batch.Client
	logs          *cloudwatchlogs.Client
	executionRole string
}

func NewAwsBatchProvider(input AwsBatchProviderInput) (*AwsBatchProvider, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(input.BatchRegion),
		config.WithSharedConfigProfile(input.ConfigProfile))
	if err != nil {
		log.Println("Failed to load an AWS Config")
		return nil, err
	}

	svc := batch.NewFromConfig(cfg)
	logs := cloudwatchlogs.NewFromConfig(cfg)

	return &AwsBatchProvider{svc, logs, input.ExecutionRole}, nil
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

func (abp *AwsBatchProvider) RegisterPlugin(plugin *Plugin) (PluginRegistrationOutput, error) {
	var timout *types.JobTimeout
	if plugin.ExecutionTimeout != nil {
		timout = &types.JobTimeout{AttemptDurationSeconds: plugin.ExecutionTimeout}
	}
	input := &batch.RegisterJobDefinitionInput{
		JobDefinitionName: &plugin.Name,
		Type:              types.JobDefinitionTypeContainer,
		ContainerProperties: &types.ContainerProperties{
			Command:          plugin.Command,
			Environment:      kvpToBatchKvp(plugin.DefaultEnvironment),
			ExecutionRoleArn: &abp.executionRole,
			Image:            &plugin.ImageAndTag,
			//MountPoints:      volumesToBatch(plugin.Volumes),
			//Volumes
			ResourceRequirements: []types.ResourceRequirement{
				{
					Type:  types.ResourceTypeMemory,
					Value: &plugin.ComputeEnvironment.Memory,
				},
				{
					Type:  types.ResourceTypeVcpu,
					Value: &plugin.ComputeEnvironment.VCPU,
				},
			},
			Secrets: credsToBatchSecrets(plugin.Credentials),
		},
		Timeout: timout,
	}
	output, err := abp.client.RegisterJobDefinition(ctx, input)
	pro := PluginRegistrationOutput{}
	if err == nil {
		pro = PluginRegistrationOutput{
			Name:         *output.JobDefinitionName,
			ResourceName: *output.JobDefinitionArn,
			Revision:     *output.Revision,
		}
	}
	return pro, err
}

func (abp *AwsBatchProvider) UnregisterPlugin(nameAndRevision string) error {
	dji := batch.DeregisterJobDefinitionInput{
		JobDefinition: &nameAndRevision,
	}
	_, err := abp.client.DeregisterJobDefinition(ctx, &dji)
	log.Printf("Unable to deregister AWS Batch Job: %s.  Error: %s\n", nameAndRevision, err)
	return err
}

func (abp *AwsBatchProvider) Status(jobQueue string, query JobsSummaryQuery) ([]JobSummary, error) {

	queryString := fmt.Sprintf("%s_C_%s*", CcProfile, query.QueryValue)

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

// @TODO this assumes the logs are rather short.
// Need to update for logs that require pagenation in the AWS SDK
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
	if logevents == nil {
		return []string{"No logs"}, nil
	}
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

func toBatchContainerOverrides(co ContainerOverrides) *types.ContainerOverrides {

	awskvp := make([]types.KeyValuePair, len(co.Environment))
	awsrr := make([]types.ResourceRequirement, len(co.ResourceRequirements))

	for i, kvp := range co.Environment {
		kvpl := kvp //make local copy of kvp to avoid aws pointing to a single kvp reference
		awskvp[i] = types.KeyValuePair{
			Name:  &kvpl.Name,
			Value: &kvpl.Value,
		}
	}

	for i, rr := range co.ResourceRequirements {
		rrl := rr
		awsrr[i] = types.ResourceRequirement{
			Type:  types.ResourceType(rrl.Type),
			Value: &rrl.Value,
		}
	}

	return &types.ContainerOverrides{
		Command:              co.Command,
		Environment:          awskvp,
		ResourceRequirements: awsrr,
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

func kvpToBatchKvp(kvps []KeyValuePair) []types.KeyValuePair {
	bkvps := make([]types.KeyValuePair, len(kvps))
	for i, kvp := range kvps {
		name, value := kvp.Name, kvp.Value
		bkvps[i] = types.KeyValuePair{
			Name:  &name,
			Value: &value,
		}
	}
	return bkvps
}

func credsToBatchSecrets(creds []KeyValuePair) []types.Secret {
	secrets := make([]types.Secret, len(creds))
	for i, s := range creds {
		nlocal, vlocal := s.Name, s.Value
		secrets[i] = types.Secret{
			Name:      &nlocal,
			ValueFrom: &vlocal,
		}
	}
	return secrets
}

func paramsMapToKvp(params map[string]string) []types.KeyValuePair {
	pout := make([]types.KeyValuePair, len(params))
	i := 0
	for k, v := range params {
		klocal, vlocal := k, v
		pout[i] = types.KeyValuePair{
			Name:  &klocal,
			Value: &vlocal,
		}
		i++
	}
	return pout
}

/*
func volumesToBatch(volumes []PluginComputeVolumes) ([]types.MountPoint, []types.Volume) {
	mps := make([]types.MountPoint, len(volumes))
	bvs:=make([]types.Volume,len(volumes))
	for i, v := range volumes {
		mps[i] = types.MountPoint{
			ContainerPath: &v.MountPoint,
			ReadOnly:      &v.ReadOnly,
			SourceVolume:  &v.ResourceName,
		}
		bvs[i]=types.Volume{
			Name: &v.Name,
			EfsVolumeConfiguration: ,
		}
	}
	return mps
}
*/
