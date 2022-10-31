package watcompute

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/aws/aws-sdk-go/aws"
)

func TestRegisterPlugin(t *testing.T) {
	plugin := Plugin{
		Name: "WAT_GO_UNIT_TEST",
		//Revision:    "1",
		ImageAndTag: "wat-test:0.07",
		Description: "Unit test for WAT Plugin Registration",
		Command:     []string{"/app/wattest"},
		ComputeEnvironment: PluginComputeEnvironment{
			VCPU:   "1",
			Memory: "1024",
		},
		DefaultEnvironment: []KeyValuePair{
			{
				Name:  "WAT_AWS_DEFAULT_REGION",
				Value: "us-east-1",
			},
			{
				Name:  "WAT_AWS_S3_BUCKET",
				Value: "mmc-storage-6",
			},
		},
		Credentials: []KeyValuePair{
			{
				Name:  "WAT_AWS_ACCESS_KEY_ID",
				Value: "arn:aws:secretsmanager:us-east-1:03",
			},
			{
				Name:  "WAT_AWS_SECRET_ACCESS_KEY",
				Value: "arn:aws:secretsmanager:us-east-1:03",
			},
		},
	}

	/*
		d, err := yaml.Marshal(&plugin)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Print(string(d))
	*/

	compute, err := NewAwsBatchProvider()
	if err != nil {
		t.Log(err)
	}
	output, err := compute.RegisterPlugin(&plugin)
	if err != nil {
		t.Log(err)
	}

	t.Log(output)
}

func TestUnRegisterPlugin(t *testing.T) {
	nameAndRevision := "WAT_GO_UNIT_TEST:1"
	compute, err := NewAwsBatchProvider()
	if err != nil {
		t.Log(err)
	}
	err = compute.UnregisterPlugin(nameAndRevision)
	if err != nil {
		t.Log(err)
	}
}

/*
func TestSampleBatchJob(t *testing.T) {
	job := BatchJob{
		JobName:       "WAT-BATCH-RUN3", "WAT C:0ed5647b-62d1-4f10-9f8d-f63821953889 E:0ed5647b-62d1-4f10-9f8d-f63821953889 M:0ed5647b-62d1-4f10-9f8d-f63821953889"
		JobQueue:      "WAT-QUEUE3",
		JobDefinition: "wat-ras-unsteady2:1",
	}
	err := RunJob(&job)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(job)
}
*/

func TestListBatchJob(t *testing.T) {
	compute, err := NewAwsBatchProvider()
	if err != nil {
		t.Log(err)
	}
	job := Job{
		JobQueue: "WAT-QUEUE3",
	}
	list, err := compute.listBatchJob(&job)
	if err != nil {
		t.Log(err)
	}
	t.Log(list)
}

/*
func TestDescribeBatchJobs(t *testing.T) {
	jobs := []Job{
		{SubmittedJob: &SubmitJobResult{JobId: aws.String("0ed5647b-62d1-4f10-9f8d-f63821953889")}},
		{SubmittedJob: &SubmitJobResult{JobId: aws.String("132f9720-bb85-4c23-b0af-ec4fe388fb13")}},
	}
	compute, err := NewAwsBatchProvider()
	if err != nil {
		t.Log(err)
	}

	djs, err := compute.describeBatchJobs(jobs)
	if err != nil {
		t.Log(err)
	}
	t.Log(djs)
}

func TestGetBatchJobLog(t *testing.T) {
	job := Job{
		SubmittedJob: &SubmitJobResult{JobId: aws.String("0ed5647b-62d1-4f10-9f8d-f63821953889")},
	}
	compute, err := NewAwsBatchProvider()
	if err != nil {
		t.Log(err)
	}

	log, err := compute.JobLog(job)
	if err != nil {
		t.Log(err)
	}
	t.Log(log)
}
*/

func TestKvpToBatchKvp(t *testing.T) {
	kvp := []KeyValuePair{
		{
			Name:  "Name1",
			Value: "Value1",
		},
		{
			Name:  "Name2",
			Value: "Value2",
		},
	}
	bkvp := kvpToBatchKvp(kvp)

	validresult := []types.KeyValuePair{
		{
			Name:  aws.String("Name1"),
			Value: aws.String("Value1"),
		},
		{
			Name:  aws.String("Name2"),
			Value: aws.String("Value2"),
		},
	}

	if !reflect.DeepEqual(bkvp, validresult) {
		t.Errorf("Expected %v got %v\n", printKvps(validresult), printKvps(bkvp))
	}
}

func TestCredsToBatchSecrets(t *testing.T) {
	creds := []KeyValuePair{
		{
			Name:  "Cred1",
			Value: "Val1",
		},
		{
			Name:  "Cred2",
			Value: "Val2",
		},
	}

	secrets := credsToBatchSecrets(creds)

	validresult := []types.Secret{
		{
			Name:      aws.String("Cred1"),
			ValueFrom: aws.String("Val1"),
		},
		{
			Name:      aws.String("Cred2"),
			ValueFrom: aws.String("Val2"),
		},
	}
	if !reflect.DeepEqual(secrets, validresult) {
		t.Errorf("Expected %v got %v\n", printSecrets(validresult), printSecrets(secrets))
	}

}

func TestParamsMapToKvp(t *testing.T) {
	params := map[string]string{
		"Key1": "Val1",
		"Key2": "Val2",
	}

	pkvp := paramsMapToKvp(params)

	validresult := []types.KeyValuePair{
		{
			Name:  aws.String("Key1"),
			Value: aws.String("Val1"),
		},
		{
			Name:  aws.String("Key2"),
			Value: aws.String("Val2"),
		},
	}

	if !reflect.DeepEqual(pkvp, validresult) {
		t.Errorf("Expected %s got %s\n", printKvps(validresult), printKvps(pkvp))
	}
}

func printKvps(kvps []types.KeyValuePair) string {
	s := "{"
	for _, kvp := range kvps {
		s += fmt.Sprintf("{Key: %s, Value %s}", *kvp.Name, *kvp.Value)
	}
	s += ("}\n")
	return s
}

func printSecrets(secrets []types.Secret) string {
	s := "{"
	for _, secret := range secrets {
		s += fmt.Sprintf("{Key: %s, Value %s}", *secret.Name, *secret.ValueFrom)
	}
	s += ("}\n")
	return s
}
