package cloudcompute

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSingleEvent(t *testing.T) {
	computeProvider, err := NewAwsBatchProvider()
	if err != nil {
		t.Log(err)
	}
	m1id := uuid.New().String()
	m2id := uuid.New().String()
	m3id := uuid.New().String()
	m4id := uuid.New().String()
	m5id := uuid.New().String()

	events := []Event{
		{
			ID: uuid.New(),
			Manifests: []Manifest{
				{
					ManifestName:     "MANIFEST3",
					ManifestID:       m3id,
					Dependencies:     []JobDependency{{m2id}},
					PluginDefinition: "wat-ras-unsteady2:1",
				},
				{
					ManifestName:     "MANIFEST4",
					ManifestID:       m4id,
					Dependencies:     []JobDependency{{m1id}, {m3id}},
					PluginDefinition: "wat-ras-unsteady2:1",
				},
				{
					ManifestName:     "MANIFEST5",
					ManifestID:       m5id,
					Dependencies:     []JobDependency{{m2id}, {m3id}},
					PluginDefinition: "wat-ras-unsteady2:1",
				},
				{
					ManifestName:     "MANIFEST1",
					ManifestID:       m1id,
					Dependencies:     []JobDependency{},
					PluginDefinition: "wat-ras-unsteady2:1",
				},
				{
					ManifestName:     "MANIFEST2",
					ManifestID:       m2id,
					Dependencies:     []JobDependency{{m1id}},
					PluginDefinition: "wat-ras-unsteady2:1",
				},
			},
		},
	}

	computeID := uuid.New()
	compute := CloudCompute{
		Name:            "WAT_COMPUTE1",
		ID:              computeID,
		JobQueue:        "WAT-QUEUE3",
		Events:          NewEventList(events),
		ComputeProvider: computeProvider,
	}

	err = compute.Run()
	if err != nil {
		t.Fatal(err)
	}

	waitAndPrintStatus(&compute, t)
	printLogs(&compute, []string{m1id, m2id}, t)
}

func TestSimpleEvents(t *testing.T) {
	computeProvider, err := NewAwsBatchProvider()
	if err != nil {
		t.Log(err)
	}

	events := []Event{
		{
			ID: uuid.New(),
			Manifests: []Manifest{
				{
					ManifestName:     "MMC_TIMING_PLUGIN",
					ManifestID:       uuid.New().String(),
					PluginDefinition: "wat-ras-unsteady2:1",
				},
			},
		},
		{
			ID: uuid.New(),
			Manifests: []Manifest{
				{
					ManifestName:     "MMC_TIMING_PLUGIN",
					ManifestID:       uuid.New().String(),
					PluginDefinition: "wat-ras-unsteady2:1",
				},
			},
		},
	}

	computeID := uuid.New()
	compute := CloudCompute{
		Name:            "WAT_COMPUTE1",
		ID:              computeID,
		JobQueue:        "WAT-QUEUE3",
		Events:          NewEventList(events),
		ComputeProvider: computeProvider,
	}

	err = compute.Run()
	if err != nil {
		t.Fatal(err)
	}

	waitAndPrintStatus(&compute, t)
}

func TestSingleJobEnvAndCommand(t *testing.T) {
	computeProvider, err := NewAwsBatchProvider()
	if err != nil {
		t.Log(err)
	}
	m1id := uuid.New().String()

	events := []Event{
		{
			ID: uuid.New(),
			Manifests: []Manifest{
				{
					ManifestName:     "MANIFEST1",
					ManifestID:       m1id,
					PluginDefinition: "WAT-ECHO-TEST2:2",
					Inputs: PluginInputs{
						Environment: []KeyValuePair{
							{
								Name:  "WAT_JOB",
								Value: "AFDSG-OUYESD-123456",
							},
							{
								Name:  "WAT_ENV_TEST",
								Value: "This is a test!",
							},
						},
					},
					Command: []string{"sh", "-c", "echo $WAT_ENV_TEST"},
				},
			},
		},
	}

	computeID := uuid.New()
	compute := CloudCompute{
		Name:            "WAT_COMPUTE1",
		ID:              computeID,
		JobQueue:        "WAT-QUEUE3",
		Events:          NewEventList(events),
		ComputeProvider: computeProvider,
	}

	err = compute.Run()
	if err != nil {
		t.Fatal(err)
	}

	waitAndPrintStatus(&compute, t)
	printLogs(&compute, []string{m1id}, t)

}

func TestMmcTimingViaEnv(t *testing.T) {
	computeProvider, err := NewAwsBatchProvider()
	if err != nil {
		t.Log(err)
	}
	m1id := uuid.New().String()

	events := []Event{
		{
			ID: uuid.New(),
			Manifests: []Manifest{
				{
					ManifestName: "MMC-TIMING-TEST",
					ManifestID:   m1id,

					PluginDefinition: "WAT-MMC-TIMING:2",
					Inputs: PluginInputs{
						Environment: []KeyValuePair{
							{
								Name:  "MMC_FAIL_PLAN",
								Value: "BirchLakeDam.p10.hdf",
							},
							{
								Name:  "MMC_NO_FAIL_PLAN",
								Value: "BirchLakeDam.p09.hdf",
							},
							{
								Name:  "MMC_BREACH_TIME",
								Value: "05FEB2099 01:25:00",
							},
							{
								Name:  "MMC_SCENARIO",
								Value: "TW1",
							},
							{
								Name:  "MMC_DELTA",
								Value: "2.0",
							},
							{
								Name:  "MMC_S3_ROOT",
								Value: "/adrian_christopher_test/Birch_Lake_Dam",
							},
							{
								Name:  "RASLIBCMD",
								Value: "/app/raslib",
							},
							{
								Name:  "AWS_REGION",
								Value: "us-east-1",
							},
							{
								Name:  "AWS_S3_BUCKET",
								Value: "mmc-storage-6",
							},
						},
					},
				},
			},
		},
	}

	computeID := uuid.New()
	compute := CloudCompute{
		Name:            "WAT_COMPUTE1",
		ID:              computeID,
		JobQueue:        "WAT-QUEUE3",
		Events:          NewEventList(events),
		ComputeProvider: computeProvider,
	}

	err = compute.Run()
	if err != nil {
		t.Fatal(err)
	}

	waitAndPrintStatus(&compute, t)
	printLogs(&compute, []string{m1id}, t)

}

func TestMmcTimingViaEnvOptima(t *testing.T) {
	computeProvider, err := NewAwsBatchProvider()
	if err != nil {
		t.Log(err)
	}
	m1id := uuid.New().String()

	events := []Event{
		{
			ID: uuid.New(),
			Manifests: []Manifest{
				{
					ManifestName: "MMC-TIMING-TEST",
					ManifestID:   m1id,

					PluginDefinition: "WAT-MMC-TIMING:2",
					Inputs: PluginInputs{
						Environment: []KeyValuePair{
							{
								Name:  "MMC_FAIL_PLAN",
								Value: "Optima_MMC.p08.hdf",
							},
							{
								Name:  "MMC_NO_FAIL_PLAN",
								Value: "Optima_MMC.p11.hdf",
							},
							{
								Name:  "MMC_BREACH_TIME",
								Value: "08FEB2099 13:50:00",
							},
							{
								Name:  "MMC_SCENARIO",
								Value: "ILDP",
							},
							{
								Name:  "MMC_DELTA",
								Value: "2.0",
							},
							{
								Name:  "MMC_S3_ROOT",
								Value: "/adrian_christopher_test/Optima_Dam_OK20510",
							},
							{
								Name:  "RASLIBCMD",
								Value: "/app/raslib",
							},
							{
								Name:  "AWS_REGION",
								Value: "us-east-1",
							},
							{
								Name:  "AWS_BUCKET",
								Value: "mmc-storage-6",
							},
						},
					},
				},
			},
		},
	}

	computeID := uuid.New()
	compute := CloudCompute{
		Name:            "WAT_COMPUTE1",
		ID:              computeID,
		JobQueue:        "WAT-QUEUE3",
		Events:          NewEventList(events),
		ComputeProvider: computeProvider,
	}

	err = compute.Run()
	if err != nil {
		t.Fatal(err)
	}

	waitAndPrintStatus(&compute, t)
	printLogs(&compute, []string{m1id}, t)

}

func TestSingleJobParameters(t *testing.T) {
	computeProvider, err := NewAwsBatchProvider()
	if err != nil {
		t.Log(err)
	}
	m1id := uuid.New().String()

	events := []Event{
		{
			ID: uuid.New(),
			Manifests: []Manifest{
				{
					ManifestName: "MANIFEST1",
					ManifestID:   m1id,

					PluginDefinition: "WAT-ECHO-TEST2:2",
					Inputs: PluginInputs{
						Environment: []KeyValuePair{
							{
								Name:  "WAT_TEST",
								Value: "This was a test of the WAT compute system....",
							},
						},
						Parameters: map[string]string{
							"myparam": "echo $WAT_TEST",
						},
					},
					Command: []string{"sh", "-c", "Ref::myparam"},
				},
			},
		},
	}

	computeID := uuid.New()
	compute := CloudCompute{
		Name:            "WAT_COMPUTE1",
		ID:              computeID,
		JobQueue:        "WAT-QUEUE3",
		Events:          NewEventList(events),
		ComputeProvider: computeProvider,
	}

	err = compute.Run()
	if err != nil {
		t.Fatal(err)
	}

	waitAndPrintStatus(&compute, t)
	printLogs(&compute, []string{m1id}, t)

}

func TestMmcTimingJson(t *testing.T) {
	computeProvider, err := NewAwsBatchProvider()
	if err != nil {
		t.Log(err)
	}
	m1id := uuid.New().String()

	events := []Event{
		{
			ID: uuid.New(),
			Manifests: []Manifest{
				{
					ManifestName: "MMC-TIMING-TEST",
					ManifestID:   m1id,

					PluginDefinition: "WAT-MMC-TIMING:2",
					Inputs: PluginInputs{
						Environment: []KeyValuePair{
							{
								Name:  "MMC_FAIL_PLAN",
								Value: "BirchLakeDam.p10.hdf",
							},
							{
								Name:  "MMC_NO_FAIL_PLAN",
								Value: "BirchLakeDam.p09.hdf",
							},
							{
								Name:  "MMC_BREACH_TIME",
								Value: "05FEB2099 01:25:00",
							},
							{
								Name:  "MMC_SCENARIO",
								Value: "TW1",
							},
							{
								Name:  "MMC_DELTA",
								Value: "2.0",
							},
							{
								Name:  "MMC_S3_ROOT",
								Value: "/adrian_christopher_test/Birch_Lake_Dam",
							},
							{
								Name:  "RASLIBCMD",
								Value: "/app/raslib",
							},
							{
								Name:  "AWS_REGION",
								Value: "us-east-1",
							},
							{
								Name:  "AWS_BUCKET",
								Value: "mmc-storage-6",
							},
						},
					},
				},
			},
		},
	}
	computeID := uuid.New()
	compute := CloudCompute{
		Name:            "WAT_COMPUTE1",
		ID:              computeID,
		JobQueue:        "WAT-QUEUE3",
		Events:          NewEventList(events),
		ComputeProvider: computeProvider,
	}

	json, err := json.Marshal(compute)
	fmt.Println(string(json))
}

func TestSingleJobTags(t *testing.T) {
	computeProvider, err := NewAwsBatchProvider()
	if err != nil {
		t.Log(err)
	}
	m1id := uuid.New().String()

	events := []Event{
		{
			ID: uuid.New(),
			Manifests: []Manifest{
				{
					ManifestName:     "MANIFEST1",
					ManifestID:       m1id,
					PluginDefinition: "WAT-ECHO-TEST3:1",
					Tags: map[string]string{
						"TAG1": "This is TAG1",
						"TAG2": "This is TAG2",
					},
				},
			},
		},
	}

	computeID := uuid.New()
	compute := CloudCompute{
		Name:            "WAT_COMPUTE1",
		ID:              computeID,
		JobQueue:        "WAT-QUEUE3",
		Events:          NewEventList(events),
		ComputeProvider: computeProvider,
	}

	err = compute.Run()
	if err != nil {
		t.Fatal(err)
	}

	waitAndPrintStatus(&compute, t)
	printLogs(&compute, []string{m1id}, t)

}

func TestSingleJobWatSDK(t *testing.T) {
	computeProvider, err := NewAwsBatchProvider()
	if err != nil {
		t.Log(err)
	}
	m1id := uuid.New().String()

	events := []Event{
		{
			ID: uuid.New(),
			Manifests: []Manifest{
				{
					ManifestName:     "MANIFEST1",
					ManifestID:       m1id,
					PluginDefinition: "WAG-GO-TEST3:7",
				},
			},
		},
	}

	computeID := uuid.New()
	compute := CloudCompute{
		Name:            "WAT_COMPUTE1",
		ID:              computeID,
		JobQueue:        "WAT-QUEUE3",
		Events:          NewEventList(events),
		ComputeProvider: computeProvider,
	}

	err = compute.Run()
	if err != nil {
		t.Fatal(err)
	}

	waitAndPrintStatus(&compute, t)
	printLogs(&compute, []string{m1id}, t)

}

func printLogs(compute *CloudCompute, manifestIds []string, t *testing.T) {
	for _, m := range manifestIds {
		fmt.Printf("-------------- %s --------------\n", m)
		logs, err := compute.Log(m)
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range logs {
			fmt.Println(l)
		}
	}
}

func waitAndPrintStatus(compute *CloudCompute, t *testing.T) {
	query := JobsSummaryQuery{SUMMARY_COMPUTE, compute.ID.String()}
	for {
		statuses, err := compute.Status(query)
		if err != nil {
			t.Log(err)
			break
		}
		for _, s := range statuses {
			fmt.Println(s)
		}
		fmt.Println("--------------------------------------------------------")
		if allDone(statuses) {
			break
		}
		time.Sleep(time.Second * 5)
	}
}

func allDone(statuses []JobSummary) bool {
	for _, s := range statuses {
		if !(s.Status == "SUCCEEDED" || s.Status == "FAILED") {
			return false
		}
	}
	return true
}
