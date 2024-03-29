package cloudcompute

import (
	"errors"
	"fmt"
	"log"

	. "github.com/usace/cc-go-sdk"

	"github.com/google/uuid"
)

// CloudCompute is a compute submission for a single dag for a set of events
// The compute environment Job Queue and Job Definitions must exist before a CloudCompute
// can be initiated.
type CloudCompute struct {
	//Compute Identifier
	ID uuid.UUID `json:"id"`

	//User friendly Name for the compute
	Name string `json:"name"`

	//JobQueue to push the events to
	JobQueue string `json:"jobQueue"`

	//Event generator
	Events EventGenerator `json:"events"`

	//compute provider for the compute (typically AwsBatchProvider)
	ComputeProvider ComputeProvider `json:"computeProvider"`

	//map of cloud compute job identifier (manifest id) to submitted job identifier (VendorID) in the compute provider
	submissionIdMap map[string]string
}

// Runs a Compute on the ComputeProvider
func (cc *CloudCompute) Run() error {
	cc.submissionIdMap = make(map[string]string)
	for cc.Events.HasNextEvent() {
		event := cc.Events.NextEvent()
		for _, manifest := range event.Manifests {
			if len(manifest.Inputs.PayloadAttributes) > 0 || len(manifest.Inputs.DataSources) > 0 {
				computeStore, err := NewCcStore(manifest.ManifestID)
				if err != nil {
					return err
				}
				p := Payload{
					Attributes: manifest.Inputs.PayloadAttributes,
					Stores:     manifest.Stores,
					Inputs:     manifest.Inputs.DataSources,
					Outputs:    manifest.Outputs,
					Actions:    manifest.Actions,
				}
				err = computeStore.SetPayload(p)
				if err != nil {
					log.Fatalf("Unable to set payload: %s", err)
				}
			}
			env := append(manifest.Inputs.Environment, KeyValuePair{CcManifestId, manifest.ManifestID})
			env = append(env, KeyValuePair{CcEventID, event.ID.String()})
			if !env.HasKey(CcEventNumber) {
				env = append(env, KeyValuePair{CcEventNumber, fmt.Sprint(event.EventNumber)})
			}
			env = append(env, KeyValuePair{CcPluginDefinition, manifest.PluginDefinition})
			job := Job{
				JobName:       fmt.Sprintf("%s_C_%s_E_%s_M_%s", CcProfile, cc.ID.String(), event.ID.String(), manifest.ManifestID),
				JobQueue:      cc.JobQueue,
				JobDefinition: manifest.PluginDefinition,
				DependsOn:     cc.mapDependencies(&manifest),
				Parameters:    manifest.Inputs.Parameters,
				Tags:          manifest.Tags,
				RetryAttemts:  manifest.RetryAttemts,
				JobTimeout:    manifest.JobTimeout,
				ContainerOverrides: ContainerOverrides{
					Environment:          env,
					Command:              manifest.Command,
					ResourceRequirements: manifest.ResourceRequirements,
				},
			}
			err := cc.ComputeProvider.SubmitJob(&job)
			if err != nil {
				return err //@TODO what happens if a set submit ok then one fails?  How do we cancel? See notes below
			}
			cc.submissionIdMap[manifest.ManifestID] = *job.SubmittedJob.JobId
		}
	}
	return nil
}

/*
//@Will
Note: if a manifest submission in an event fails, then what should plan be:
   - a) fail fast and cancel all just submitted to the compute provider
   - b) skip the event, log that it failed to submit, then move on to the next one?
*/

// Requests the status of a given compute at the COMPUTE, EVENT, or JOB level
// A JobSummaryFunction is necessary to process the status
func (cc *CloudCompute) Status(query JobsSummaryQuery) error {
	return cc.ComputeProvider.Status(cc.JobQueue, query)
}

// Requests the run log for a manifest
func (cc *CloudCompute) Log(manifestId string) ([]string, error) {
	if submittedJobId, ok := cc.submissionIdMap[manifestId]; ok {
		return cc.ComputeProvider.JobLog(submittedJobId)
	}
	return nil, errors.New(fmt.Sprintf("Invalid Manifest ID: %v", manifestId))
}

// Cancels jobs submitted to compute environment
func (cc *CloudCompute) Cancel(reason string) error {
	input := TermminateJobInput{
		Reason:   reason,
		JobQueue: cc.JobQueue,
		Query: JobsSummaryQuery{
			QueryLevel: SUMMARY_COMPUTE,
			QueryValue: JobNameParts{Compute: cc.ID.String()},
		},
	}
	return cc.ComputeProvider.TerminateJobs(input)
}

// Maps the Dependency identifiers to the compute environment identifiers received from submitted jobs.
func (cc *CloudCompute) mapDependencies(manifest *ComputeManifest) []JobDependency {
	sdeps := make([]JobDependency, len(manifest.Dependencies))
	for i, d := range manifest.Dependencies {
		if sdep, ok := cc.submissionIdMap[d.JobId]; ok {
			sdeps[i] = JobDependency{sdep}
		}
	}
	return sdeps
}

/////////////////////////////
//////// MANIFEST ///////////

// ComputeManifest is the information necessary to execute a single job in an event
// @TODO Dependencies could be an array of string but for now is a struct so that we could add additional dependency information should the need arise.
type ComputeManifest struct {
	ManifestName         string          `yaml:"manifest_name" json:"manifest_name"`
	ManifestID           string          `yaml:"manifest_id,omitempty" json:"manifest_id"`
	Command              []string        `yaml:"command" json:"command" `
	Dependencies         []JobDependency `yaml:"dependencies" json:"dependencies"`
	Stores               []DataStore     `yaml:"stores" json:"stores"`
	Inputs               PluginInputs    `yaml:"inputs" json:"inputs"`
	Outputs              []DataSource    `yaml:"outputs" json:"outputs"`
	Actions              []Action
	PluginDefinition     string                `yaml:"plugin_definition" json:"plugin_definition"` //plugin resource name. "name:version"
	Tags                 map[string]string     `yaml:"tags" json:"tags"`
	RetryAttemts         int32                 `yaml:"retry_attempts" json:"retry_attempts"`
	JobTimeout           int32                 `yaml:"job_timeout" json:"job_timeout"`
	ResourceRequirements []ResourceRequirement `yaml:"resource_requirements" json:"resource_requirements"`
}

//JobDefinition string            `yaml:"job_definition"`

// Job level inputs that can be injected into a container
type PluginInputs struct {
	Environment       KeyValuePairs     `json:"environment"`
	Parameters        map[string]string `json:"parameters"`
	DataSources       []DataSource      `json:"dataSources"`
	PayloadAttributes PayloadAttributes `json:"payloadAttributes"`
}

/////////////////////////////
///////// EVENT /////////////

// EVENT is a single run through the DAG
type Event struct {
	ID          uuid.UUID         `json:"id"`
	EventNumber int64             `json:"event_number"`
	Manifests   []ComputeManifest `json:"manifests"`
}

// Adds a manifest to the Event
func (e *Event) AddManifest(m ComputeManifest) {
	e.Manifests = append(e.Manifests, m)
}

// Adds a manifest at a specific ordinal position in the event.
func (e *Event) AddManifestAt(m ComputeManifest, i int) {
	e.Manifests = append(e.Manifests[:i+1], e.Manifests[i:]...)
	e.Manifests[i] = m
}

/////////////////////////////
///////// PLUGIN ////////////

// Plugin struct is used to interact with the compute environment and create a Job Definition
// this is likely going to be moved to the CCAPI.
// When entering credentials, use the format of the compute provider.
// For example when using AWS Batch: "AWS_ACCESS_KEY_ID", "arn:aws:secretsmanager:us-east-1:01010101010:secret:mysecret:AWS_ACCESS_KEY_ID::
type Plugin struct {
	//ID                 uuid.UUID
	Name string `json:"name" yaml:"name"`
	//Revision           string                   `json:"revision" yaml:"revision"`
	ImageAndTag        string                   `json:"image_and_tag" yaml:"image_and_tag"`
	Description        string                   `json:"description" yaml:"description"`
	Command            []string                 `json:"command" yaml:"command"`
	ComputeEnvironment PluginComputeEnvironment `json:"compute_environment" yaml:"compute_environment"`
	DefaultEnvironment []KeyValuePair           `json:"environment" yaml:"environment"` //default values for the container environment
	Volumes            []PluginComputeVolumes   `json:"volumes" yaml:"volumes"`
	Credentials        []KeyValuePair           `json:"credentials" yaml:"credentials"`
	Parameters         map[string]string        `json:"parameters" yaml:"parameters"`
	RetryAttemts       int32                    `json:"retry_attempts" yaml:"retry_attempts"`
	ExecutionTimeout   *int32                   `json:"execution_timeout" yaml:"execution_timeout"`
}

type PluginComputeEnvironment struct {
	VCPU   string `json:"vcpu" yaml:"vcpu"`
	Memory string `json:"memory" yaml:"memory"`
}

type PluginComputeVolumes struct {
	Name         string `json:"name" yaml:"name"`
	ResourceName string `json:"resource_name" yaml:"resource_name"`
	ReadOnly     bool   `json:"read_only" yaml:"read_only"`
	MountPoint   string `json:"mount_point" yaml:"mount_point"` //default is "/data"
}

type PluginRegistrationOutput struct {
	Name         string
	ResourceName string
	Revision     int32
}

type PluginManifest struct {
}
