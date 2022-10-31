package watcompute

import (
	"errors"
	"fmt"

	. "github.com/usace/wat-go"

	"github.com/google/uuid"
)

//WatCompute is a compute submission for a single dag for a set of events
//The compute environment Job Queue and Job Definitions must exist before a WatCompute
//can be initiated.
type WatCompute struct {
	ID              uuid.UUID         `json:"id"`
	Name            string            `json:"name"`
	JobQueue        string            `json:"jobQueue"`
	Events          EventGenerator    `json:"events"`
	ComputeProvider ComputeProvider   `json:"computeProvider"`
	submissionIdMap map[string]string //maps manifest id to submitted job identifier in the compute provider
}

//Runs a WatCompute on the ComputeProvider
func (wc *WatCompute) Run() error {
	wc.submissionIdMap = make(map[string]string)
	for wc.Events.HasNextEvent() {
		event := wc.Events.NextEvent()
		for _, manifest := range event.Manifests {
			env := append(manifest.Inputs.Environment, KeyValuePair{"WAT_MANIFEST_ID", manifest.ManifestID})
			job := Job{
				JobName:       fmt.Sprintf("WAT_C_%s_E_%s_M_%s", wc.ID.String(), event.ID.String(), manifest.ManifestID),
				JobQueue:      wc.JobQueue,
				JobDefinition: manifest.PluginDefinition,
				DependsOn:     wc.mapDependencies(&manifest),
				Parameters:    manifest.Inputs.Parameters,
				Tags:          manifest.Tags,
				RetryAttemts:  manifest.RetryAttemts,
				JobTimeout:    manifest.JobTimeout,
				ContainerOverrides: ContainerOverrides{
					Environment: env,
					Command:     manifest.Command,
				},
			}
			err := wc.ComputeProvider.SubmitJob(&job)
			if err != nil {
				return err //@TODO what happens if a set submit ok then one fails?  How do we cancel? See notes below
			}
			wc.submissionIdMap[manifest.ManifestID] = *job.SubmittedJob.JobId
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

//Requests the status of a given compute at the COMPUTE, EVENT, or JOB level
func (wc *WatCompute) Status(query JobsSummaryQuery) ([]JobSummary, error) {
	return wc.ComputeProvider.Status(wc.JobQueue, query)
}

//Requests the run log for a manifest
func (wc *WatCompute) Log(manifestId string) ([]string, error) {
	if submittedJobId, ok := wc.submissionIdMap[manifestId]; ok {
		return wc.ComputeProvider.JobLog(submittedJobId)
	}
	return nil, errors.New(fmt.Sprintf("Invalid Manifest ID: %v", manifestId))
}

//Cancels the entire wat compute includening jobs submitted to compute environment and
//events in the WatCompute which have not been submitted to the compute provider
func (wc *WatCompute) Cancel() error {
	return errors.New("Not implemented")
}

//Maps the WAT Dependency identifiers to the compute environment identifiers received from submitted jobs.
func (wc *WatCompute) mapDependencies(manifest *Manifest) []JobDependency {
	sdeps := make([]JobDependency, len(manifest.Dependencies))
	for i, d := range manifest.Dependencies {
		if sdep, ok := wc.submissionIdMap[d.JobId]; ok {
			sdeps[i] = JobDependency{sdep}
		}
	}
	return sdeps
}

/////////////////////////////
//////// MANIFEST ///////////

//Manifest is the information necessary to execute a single job in an event
//@TODO Dependencies could be an array of string but for now is a struct so that we could add additional dependency information should the need arise.
type Manifest struct {
	ManifestName     string            `yaml:"manifest_name" json:"manifest_name"`
	ManifestID       string            `yaml:"manifest_id,omitempty" json:"manifest_id"`
	Command          []string          `yaml:"command" json:"command" `
	Dependencies     []JobDependency   `yaml:"dependencies" json:"dependencies"`
	Inputs           PluginInputs      `yaml:"inputs" json:"inputs"`
	Outputs          []DataSource      `yaml:"outputs" json:"outputs"`
	PluginDefinition string            `yaml:"plugin_definition" json:"plugin_definition"` //plugin resource name. "name:version"
	Tags             map[string]string `yaml:"tags" json:"tags"`
	RetryAttemts     int32             `yaml:"retry_attempts" json:"retry_attempts"`
	JobTimeout       int32             `yaml:"job_timeout" json:"job_timeout"`
}

//JobDefinition string            `yaml:"job_definition"`

//Job level inputs that can be injected into a container
type PluginInputs struct {
	Environment       []KeyValuePair         `json:"environment"`
	Parameters        map[string]string      `json:"parameters"`
	DataSources       []DataSource           `json:"dataSources"`
	PayloadAttributes map[string]interface{} `json:"payloadAttributes"`
}

/////////////////////////////
///////// EVENT /////////////

//EVENT is a single run through the DAG
type Event struct {
	ID          uuid.UUID  `json:"id"`
	EventNumber int64      `json:"event_number"` //optional
	Manifests   []Manifest `json:"manifests"`
}

//Adds a manifest to the Event
func (e *Event) AddManifest(m Manifest) {
	e.Manifests = append(e.Manifests, m)
}

//Adds a manifest at a specific ordinal position in the event.
func (e *Event) AddManifestAt(m Manifest, i int) {
	e.Manifests = append(e.Manifests[:i+1], e.Manifests[i:]...)
	e.Manifests[i] = m
}

/////////////////////////////
///////// PLUGIN ////////////

//Plugin struct is used to interact with the compute environment and create a Job Definition
//this is likely going to be moved to the WATAPI.
//When entering credentials, use the format of the compute provider.
//For example when using AWS Batch: "AWS_ACCESS_KEY_ID", "arn:aws:secretsmanager:us-east-1:01010101010:secret:mysecret:AWS_ACCESS_KEY_ID::
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
