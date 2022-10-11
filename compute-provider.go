package watcompute

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
	//ListJobs(jobQueue string, status string) ([]Job, error)
	//EventSummary(eventID uuid.UUID) ([]JobSummary, error)
	Status(jobQueue string, query JobsSummaryQuery) ([]JobSummary, error)
	JobLog(submittedJobId string) ([]string, error)

	//CreateJobDef
	//AddCredential
}

type ResourceRequirement struct {
	Type  ResourceType
	Value string
}

type ContainerOverrides struct {
	Command     []string
	Environment []KeyValuePair
}

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

type JobDependency struct {
	JobId string
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
	Name  string
	Value string
}
