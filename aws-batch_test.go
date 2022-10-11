package watcompute

import (
	"testing"
)

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
