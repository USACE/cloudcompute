package cloudcompute

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

var awsLogGroup string = "/aws/batch/job"
var ctx context.Context = context.Background()

//options are any set of valid AWS Batch config options.
//for example, to set max retries to unlimited:
/*
	input.Options=[]func(o *config.LoadOptions) error{
		config.WithRetryer(func() aws.Retryer {
			return retry.AddWithMaxAttempts(retry.NewStandard(), 0)
		}))
	}
*/
type AwsBatchProviderInput struct {
	ExecutionRole string
	BatchRegion   string
	ConfigProfile string
	Options       []func(o *config.LoadOptions) error
}

// AWS Batch Compute Provider implementation
type AwsBatchProvider struct {
	client        *batch.Client
	logs          *cloudwatchlogs.Client
	executionRole string
}

func NewAwsBatchProvider(input AwsBatchProviderInput) (*AwsBatchProvider, error) {

	options := []func(o *config.LoadOptions) error{
		config.WithRegion(input.BatchRegion),
	}

	if input.ConfigProfile != "" {
		options = append(options, config.WithSharedConfigProfile(input.ConfigProfile))
	}

	if len(input.Options) > 0 {
		options = append(options, input.Options...)
	}
	cfg, err := config.LoadDefaultConfig(context.TODO(), options...)

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

	/////////////
	//var isPriviledged bool = false
	//var hostPath string = "/dev/fuse"

	//////////////

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
			Secrets:         credsToBatchSecrets(plugin.Credentials),
			Privileged:      &plugin.Priviledged,
			LinuxParameters: LinuxParamsToBatchParams(plugin.LinuxParameters),
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

// Terminates jobs submitted to AWS Batch job queues
func (abp *AwsBatchProvider) TerminateJobs(input TermminateJobInput) error {
	jobs := input.VendorJobs
	if jobs == nil {
		input.Query.JobSummaryFunction = func(summaries []JobSummary) {
			for _, job := range summaries {
				output := abp.terminateJob(job.JobName, job.JobId, input.Reason)
				if input.TerminateJobFunction != nil {
					input.TerminateJobFunction(output)
				}
			}
		}
		statuserr := abp.Status(input.JobQueue, input.Query)
		if statuserr != nil {
			return statuserr
		}
	} else {
		for _, job := range jobs {
			output := abp.terminateJob(job.Name(), job.ID(), input.Reason)
			if input.TerminateJobFunction != nil {
				input.TerminateJobFunction(output)
			}
		}
		return nil
	}
	return nil
}

// Terminates everything running in a queue
func (abp *AwsBatchProvider) TerminateQueue(input TermminateJobInput) error {

	input.Query.JobSummaryFunction = func(summaries []JobSummary) {
		for _, job := range summaries {
			output := abp.terminateJob(job.JobName, job.JobId, input.Reason)
			if input.TerminateJobFunction != nil {
				input.TerminateJobFunction(output)
			}
		}
	}
	statuserr := abp.QueueSummary(input.JobQueue, input.Query)
	if statuserr != nil {
		return statuserr
	}

	return nil
}

func (abp *AwsBatchProvider) terminateJob(name string, id string, reason string) TerminateJobOutput {
	tji := batch.TerminateJobInput{
		JobId:  &id,
		Reason: &reason,
	}
	_, err := abp.client.TerminateJob(context.TODO(), &tji)

	return TerminateJobOutput{
		JobName: name,
		JobId:   id,
		Err:     err,
	}
}

func (abp *AwsBatchProvider) QueueSummary(jobQueue string, query JobsSummaryQuery) error {
	if query.JobSummaryFunction == nil {
		return errors.New("Missing JubSummaryFunction.  You have no way to process the result.")
	}

	var nextToken *string

	statusList := []types.JobStatus{
		types.JobStatusSubmitted,
		types.JobStatusPending,
		types.JobStatusRunnable,
		types.JobStatusStarting,
		types.JobStatusRunning,
	}
	for _, status := range statusList {
		for {
			input := batch.ListJobsInput{
				JobQueue:  &jobQueue,
				JobStatus: status,
				NextToken: nextToken,
			}

			output, err := abp.client.ListJobs(ctx, &input)
			if err != nil {
				log.Printf("Failed retrieving Jobs List for jobs %s on queue %s\n", status, jobQueue)
			}
			query.JobSummaryFunction(listOutput2JobSummary(output))
			nextToken = output.NextToken
			if nextToken == nil {
				break
			}
		}
	}
	return nil
}

func (abp *AwsBatchProvider) Status(jobQueue string, query JobsSummaryQuery) error {
	if query.JobSummaryFunction == nil {
		return errors.New("Missing JubSummaryFunction.  You have no way to process the result.")
	}

	/*
		var queryString string
		switch query.QueryLevel {
		case SUMMARY_COMPUTE:
			queryString = fmt.Sprintf("%s_C_%s*", CcProfile, query.QueryValue.Compute)
		case SUMMARY_EVENT:
			queryString = fmt.Sprintf("%s_C_%s_E_%s*", CcProfile, query.QueryValue.Compute, query.QueryValue.Event)
		case SUMMARY_MANIFEST:
			queryString = fmt.Sprintf("%s_C_%s_E_%s_M_%s", CcProfile, query.QueryValue, query.QueryValue.Compute, query.QueryValue.Event)
		}
	*/
	queryString := StatusQueryString(query)

	eventFilter := types.KeyValuesPair{
		Name:   aws.String("JOB_NAME"),
		Values: []string{queryString},
	}

	var nextToken *string

	for {
		input := batch.ListJobsInput{
			JobQueue:  &jobQueue,
			Filters:   []types.KeyValuesPair{eventFilter},
			NextToken: nextToken,
		}

		output, err := abp.client.ListJobs(ctx, &input)
		if err != nil {
			return err
		}
		query.JobSummaryFunction(listOutput2JobSummary(output))
		nextToken = output.NextToken
		if nextToken == nil {
			break
		}
	}
	return nil
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
	if err != nil {
		return nil, err
	}
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
		depCopy := d
		batchDeps[i] = types.JobDependency{
			JobId: &depCopy.JobId,
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

func LinuxParamsToBatchParams(ccLinuxParams *PluginLinuxParameters) *types.LinuxParameters {
	if ccLinuxParams == nil {
		return nil
	}
	var batchDevices []types.Device
	if len(ccLinuxParams.Devices) > 0 {
		batchDevices = make([]types.Device, len(ccLinuxParams.Devices))
		for i, v := range ccLinuxParams.Devices {
			device := v
			batchDevices[i] = types.Device{
				HostPath:      &device.HostPath,
				ContainerPath: &device.ContainerPath,
			}
		}
	}
	batchParams := types.LinuxParameters{
		Devices: batchDevices,
	}
	return &batchParams
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
