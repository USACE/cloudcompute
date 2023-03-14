package cloudcompute

import "github.com/google/uuid"

type ResourceType string

const (
	ResourceTypeGpu             ResourceType = "GPU"
	ResourceTypeVcpu            ResourceType = "VCPU"
	ResourceTypeMemory          ResourceType = "MEMORY"
	ResourceTypeAttachedStorage ResourceType = "ATTACHEDSTORAGE"
)

type ComputeProvider interface {
	SubmitJob(job *Job) error
	Status(jobQueue string, query JobsSummaryQuery) ([]JobSummary, error)
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
	Type  string
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
	JobTimeout         int32 //duration in seconds
	SubmittedJob       *SubmitJobResult
}

// JobDependency is a graph dependency relationship.
// When created for a manifest, the JobId value should be the manifestId. When a Compute
// is run, Compute will map manifestIds to submitted JobIds as they are submitted and
// handle the dependency mapping for the compute environment
type JobDependency struct {
	JobId string //should be ManifestID when being added as a dependency in a Manifest
}

type SubmitJobResult struct {
	JobId        *string
	ResourceName *string //ARN in AWS
}

type JobSummary struct {
	JobId        string
	JobName      string
	CreatedAt    *int64
	StartedAt    *int64
	Status       string
	StatusDetail *string
	StoppedAt    *int64
	ResourceName string
}

const (
	SUMMARY_COMPUTE  string = "COMPUTE"
	SUMMARY_EVENT           = "EVENT"
	SUMMARY_MANIFEST        = "MANIFEST"
)

type JobsSummaryQuery struct {
	QueryLevel string //COMPUTE/EVENT/MANIFEST
	QueryValue string
}

type KeyValuePair struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}
