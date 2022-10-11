package watcompute

import (
	"errors"
	"fmt"

	. "github.com/usace/wat-go"

	"github.com/google/uuid"
)

/*
type WatCache interface{}
*/

//WatCompute is a compute submission for a single dag for a set of events
//The compute environment Job Queue and Job Definitions must exist before a WatCompute
//can be initiated.
type WatCompute struct {
	ID              uuid.UUID
	Name            string
	JobQueue        string
	Events          EventGenerator
	ComputeProvider ComputeProvider
	submissionIdMap map[string]string //maps manifest id to submitted job identifier in the compute provider
}

//Runs a WatCompute on the ComputeProvider
func (wc *WatCompute) Run() error {
	wc.submissionIdMap = make(map[string]string)
	for wc.Events.HasNextEvent() {
		event := wc.Events.NextEvent()
		for _, manifest := range event.Manifests {
			job := Job{
				JobName:       fmt.Sprintf("WAT_C_%s_E_%s_M_%s", wc.ID.String(), event.ID.String(), manifest.ManifestID),
				JobQueue:      wc.JobQueue,
				JobDefinition: manifest.JobDefinition,
				DependsOn:     wc.mapDependencies(&manifest),
				Parameters:    manifest.Inputs.Parameters,
				Tags:          manifest.Tags,
				RetryAttemts:  manifest.RetryAttemts,
				JobTimeout:    manifest.JobTimeout,
				ContainerOverrides: ContainerOverrides{
					Environment: manifest.Inputs.Environment,
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
type Manifest struct {
	ManifestName  string            `yaml:"manifest_name"`
	ManifestID    string            `yaml:"manifest_id,omitempty"`
	Command       []string          `yaml:"command"`
	Dependencies  []JobDependency   `yaml:"dependencies"`
	Inputs        PluginInputs      `yaml:"inputs"`
	Outputs       []DataSource      `yaml:"outputs"`
	JobDefinition string            `yaml:"job_definition"`
	Tags          map[string]string `yaml:"tags"`
	RetryAttemts  int32             `yaml:"retry_attempts"`
	JobTimeout    int32             `yaml:"job_timeout"`
}

//Job level inputs that can be injected into a container
type PluginInputs struct {
	Environment       []KeyValuePair
	Parameters        map[string]string
	DataSources       []DataSource
	PayloadAttributes map[string]interface{}
}

//Generalized data sources including FILE, DB, etc
//The credential attribute is the credential prefix
//used to identify credentials in the environment.
//For example "MODEL_LIBRARY" would match "MODEL_LIBRARY_AWS_ACCESS_KEY_ID"
//or an empty string to ignore a prefix match.
//@TODO Depricated.  Is now in the SDK
/*
type DataSource struct {
	Name      string
	ID        uuid.UUID //optional.  used primarily for topological sort based on input/output dependencies
	DataType  string    //file,db,
	StoreType string    //S3
	EnvPrefix string
	//Credentials string //the credential prefix used to identify credenti
	Paths      []string          //testing to support options like shapefiles which a single source consists of multiple files
	Parameters map[string]string //testing this approach to work with internal path types
}
*/

/////////////////////////////
///////// EVENT /////////////

//EVENT is a single run through the DAG
type Event struct {
	ID          uuid.UUID
	EventNumber int64 //optional
	Manifests   []Manifest
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

//@Will Plugin concepts gets moved to the API that creates job definitions.  WATCOMPUTE
//is a library for running job descriptions that already exist

//Plugin struct is used to interact with the compute environment and create a Job Description
//this is likely going to be moved to the WATAPI.
//When entering credentials, use the format of the compute provider.
//For example when using AWS Batch: "AWS_ACCESS_KEY_ID", "arn:aws:secretsmanager:us-east-1:01010101010:secret:mysecret:AWS_ACCESS_KEY_ID::
type Plugin struct {
	Name               string `json:"name" yaml:"name"`
	Version            string
	ImageAndTag        string `json:"image_and_tag" yaml:"image_and_tag"`
	Description        string
	Command            []string `json:"command" yaml:"command"`
	ComputeEnvironment PluginComputeEnvironment
	DefaultEnvironment []KeyValuePair //default values for the container environment
	Volumes            []PluginComputeVolumes
	Credentials        []KeyValuePair
	Parameters         map[string]string
	RetryAttemts       int32
}

type PluginComputeEnvironment struct {
	VCPU   int
	Memory int
}

type PluginComputeVolumes struct {
	Name       string
	MountPoint string //default is "/data"
}
