package cloudcompute

import (
	"errors"
	"fmt"

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

/*
	Changes
	 - added a payload identifier uuid.  CC store switch to that UUID rather than manifest UUID
	 - Payload identifier gets added to compute job as a tag so we can always look up the payload from a job in a compute env
	 - Responsibility to write the payload is shifted to the computemanifest
	 - rules for writing are:
	   - if there are no attributes or datasources then the compute RUN method will skip writing a manifest.  Probably should add actions here or just skip this check
	   - else, call manifest.WritePayload
	   - manifest.writepayload will:
	     - check to see if a payload was previously written (i.e. does the manifest have a payload id?)
		   - if so: return
		   - if not, write a new payload
*/

// Runs a Compute on the ComputeProvider
func (cc *CloudCompute) Run() error {
	cc.submissionIdMap = make(map[string]string)
	for cc.Events.HasNextEvent() {
		event := cc.Events.NextEvent()

		//go func(event Event) {
		for _, manifest := range event.Manifests {
			if len(manifest.Inputs.PayloadAttributes) > 0 || len(manifest.Inputs.DataSources) > 0 {
				err := manifest.WritePayload() //guarantees the payload id written to the manifest
				if err != nil {
					return err
				}
			}
			env := append(manifest.Inputs.Environment,
				KeyValuePair{CcPayloadId, manifest.payloadID.String()},
				KeyValuePair{CcEventID, event.ID.String()})

			if !env.HasKey(CcEventNumber) {
				env = append(env, KeyValuePair{CcEventNumber, fmt.Sprint(event.EventNumber)})
			}

			//the manifest substitution is will be removed in future versions.
			//it is only supported now to ease the transition to payloadId vs manifestId
			if !env.HasKey(CcManifestId) {
				env = append(env, KeyValuePair{CcManifestId, manifest.ManifestID})
			}

			env = append(env, KeyValuePair{CcPluginDefinition, manifest.PluginDefinition}) //@TODO do we need this?
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
		//}(event)
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
	payloadID            uuid.UUID             `yaml:"-" json:"-"`
}

// This is a transitional method that will be removed in a future version
// It is intended to facilitate running manifests written prior to
// payloadId
// @Depricated
func (cm *ComputeManifest) GetPayload() uuid.UUID {
	return cm.payloadID
}

func (cm *ComputeManifest) WritePayload() error {
	if cm.payloadID == uuid.Nil {
		payloadId := uuid.New()
		computeStore, err := NewCcStore(payloadId.String())
		if err != nil {
			return err
		}
		p := Payload{
			Attributes: cm.Inputs.PayloadAttributes,
			Stores:     cm.Stores,
			Inputs:     cm.Inputs.DataSources,
			Outputs:    cm.Outputs,
			Actions:    cm.Actions,
		}
		err = computeStore.SetPayload(p)
		if err != nil {
			return err
		}
		if cm.Tags == nil {
			cm.Tags = make(map[string]string)
		}
		cm.Tags["payload"] = payloadId.String()
		cm.payloadID = payloadId
	}
	return nil
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
