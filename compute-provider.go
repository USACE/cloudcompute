package cloudcompute

import (
	"errors"
	"regexp"

	"github.com/google/uuid"
)

type ResourceType string

const (
	ResourceTypeGpu             ResourceType = "GPU"
	ResourceTypeVcpu            ResourceType = "VCPU"
	ResourceTypeMemory          ResourceType = "MEMORY"
	ResourceTypeAttachedStorage ResourceType = "ATTACHEDSTORAGE"

	SUMMARY_COMPUTE  string = "COMPUTE"
	SUMMARY_EVENT    string = "EVENT"
	SUMMARY_MANIFEST string = "MANIFEST"
)

// Input for terminating jobs submitted to a queue.
// the list of jobs to terminate is determined by either
// a StatusQuery with each job in the status query being terminated
// or a list of VendorJobs for termination.
type TermminateJobInput struct {
	//Users reason for terminating the job
	Reason string

	//The Vendor Job Queue the job was submitted to
	JobQueue string

	//Optional. A jobs summary query that will generate a list of jobs to terminate
	//if this value is provided the JobSummaryQuery JobSummaryFunction is ignored
	//and should be left empty
	Query JobsSummaryQuery

	//Optional. A list of VendorJobs to terminate
	VendorJobs []VendorJob

	//Optional.  A function to process the results of each terminated job
	TerminateJobFunction TerminateJobFunction
}

type TerminateJobOutput struct {

	//CloudCompute Job Name
	JobName string

	//Error if returned from the terminate operation
	Err error

	//Vendor Job ID
	JobId string
}

// function to process the results of each job termination
type TerminateJobFunction func(output TerminateJobOutput)

// Interface for a compute provider.  Curretnly there is a single implementation for AwsBatch
type ComputeProvider interface {
	SubmitJob(job *Job) error
	TerminateJobs(input TermminateJobInput) error
	Status(jobQueue string, query JobsSummaryQuery) error
	JobLog(submittedJobId string) ([]string, error)
	RegisterPlugin(plugin *Plugin) (PluginRegistrationOutput, error)
	UnregisterPlugin(nameAndRevision string) error
}

// Overrides the container command or environment from the base values
// provided in the job description
type ContainerOverrides struct {
	Command              []string
	Environment          []KeyValuePair
	ResourceRequirements []ResourceRequirement
}

type ResourceRequirement struct {
	Type  ResourceType
	Value string
}

// This is a single "job" or unit of compute for a ComputeProvider
// Essentually it is a mapping of a single Manifest
type Job struct {
	EventID            uuid.UUID
	ManifestID         uuid.UUID
	JobName            string
	JobQueue           string
	JobDefinition      string
	ContainerOverrides ContainerOverrides
	DependsOn          []JobDependency
	Parameters         map[string]string
	Tags               map[string]string
	RetryAttemts       int32
	JobTimeout         int32            //duration in seconds
	SubmittedJob       *SubmitJobResult //reference to the job information from the compute environment
}

// JobDependency is a graph dependency relationship.
// When created for a manifest, the JobId value should be the manifestId. When a Compute
// is run, Compute will map manifestIds to submitted JobIds as they are submitted and
// handle the dependency mapping for the compute environment
type JobDependency struct {
	//Cloud Compute Job Identifier
	//should be ManifestID when being added as a dependency in a Manifest
	JobId string
}

type VendorJob interface {
	ID() string
	Name() string
}

type SubmitJobResult struct {

	//Vendor ID
	JobId *string

	//Vendor Resource Name
	ResourceName *string //ARN in AWS
}

// function to process the results of a JobSummary request
// summaries are processed in batches of JobSummaries
// but will continue until all jobs are reported.  In AWS
// this processes the slice of summaries for the initial
// request and all subsequenct continutation tokens
type JobSummaryFunction func(summaries []JobSummary)

type JobSummary struct {
	//identifier for the compute environment being used.  e.g. AWS Batch Job ID
	JobId string

	//cloud compute job name
	JobName string

	//unix timestamp in milliseconds for when the job was created
	CreatedAt *int64

	//unix timestamp in milliseconds for when the job was started
	StartedAt *int64

	//status string value
	Status string

	//human readable string of the status
	StatusDetail *string

	//unix timestamp in milliseconds for when the job was stopped
	StoppedAt *int64

	//Compute Vendor resource name for the job.  e.g. the Job ARN for AWS
	ResourceName string
}

func (js JobSummary) ID() string {
	return js.JobId
}

func (js JobSummary) Name() string {
	return js.JobName
}

type JobsSummaryQuery struct {
	//The Level to request.  Must be one of three values
	//COMPUTE/EVENT/MANIFEST as represented by the SUMMARY_{level} constants
	QueryLevel string

	//the GUIDs representing the referenced levels
	//The value must include preceding levels, so COMPUTE level must have at least the compute GUID
	//EVENT level must have the compuyte and event levels and MANIFEST level has all three guids
	QueryValue JobNameParts

	//a required function to process each job returned in the query
	JobSummaryFunction JobSummaryFunction
}

type JobNameParts struct {
	Compute  string
	Event    string
	Manifest string
}

func (jnp *JobNameParts) Parse(jobname string) error {
	re := regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	guids := re.FindAllString(jobname, -1)
	if len(guids) != 3 {
		return errors.New("Invalid Job Name")
	}
	jnp.Compute = guids[0]
	jnp.Event = guids[1]
	jnp.Manifest = guids[2]
	return nil
}

type KeyValuePair struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type KeyValuePairs []KeyValuePair

func (kvps KeyValuePairs) HasKey(key string) bool {
	for _, kvp := range kvps {
		if kvp.Name == key {
			return true
		}
	}
	return false
}
