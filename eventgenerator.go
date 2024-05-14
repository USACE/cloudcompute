package cloudcompute

import (
	"errors"
	"fmt"
	"log"
)

// EventGenerators provide an iterator type interface to work with sets of events for a Compute.
type EventGenerator interface {
	HasNextEvent() bool
	NextEvent() Event
}

type ArrayEventGenerator struct {
	event    Event
	start    int64
	end      int64
	position int64
}

func NewArrayEventGenerator(event Event, start int64, end int64) (*ArrayEventGenerator, error) {
	manifestCount := len(event.Manifests)
	for i := 0; i < manifestCount; i++ {
		err := event.Manifests[i].WritePayload()
		if err != nil {
			return nil, fmt.Errorf("Failed to write payload for manifest %s: %s\n", event.Manifests[i].ManifestID, err)
		}
	}
	return &ArrayEventGenerator{
		event:    event,
		position: start,
		end:      end,
	}, nil
}

func (aeg *ArrayEventGenerator) HasNextEvent() bool {
	return aeg.position <= aeg.end
}

// @TODO this is quick and dirty...needs two changes
//  1. move sorting to a new function shared by the event generators
//  2. only sort once.  Can add a sorted flag to the ArrayEventGenerator...or something like that
func (aeg *ArrayEventGenerator) NextEvent() Event {
	event := aeg.event
	event.EventNumber = aeg.position
	aeg.position++
	if len(event.Manifests) > 1 {
		orderedIds, err := event.TopoSort()
		if err != nil {
			log.Printf("Unable to order event %s: %s\n", event.ID, err)
			return event
		}
		orderedManifests := make([]ComputeManifest, len(event.Manifests))
		for i, oid := range orderedIds {
			orderedManifests[i], err = getManifest(event.Manifests, oid)
			if err != nil {
				log.Printf("Unable to order event %s: %s\n", event.ID, err)
				return event
			}
		}
		event.Manifests = orderedManifests
	}
	return event
}

// EventList is an EventGenerator composed of a slice of events.
// Events are enumerated in the order they were placed in the slice.
type EventList struct {
	currentEvent int
	events       []Event
}

// Instantiates a new EventList
func NewEventList(events []Event) *EventList {
	el := EventList{
		currentEvent: -1,
		events:       events,
	}
	return &el
}

// Determines if all of the events have been enumerated
func (el *EventList) HasNextEvent() bool {
	el.currentEvent++
	return el.currentEvent < len(el.events)
}

//@TODO: Could optimize the sorting an instead of returning a list of ordered IDs, return a sorted list of manifests.....

// Retrieves the next event.  Attempts to perform a topological sort on the manifest slice before returning.
// If sort fails it will log the issue and return the unsorted manifest slice
func (el *EventList) NextEvent() Event {
	event := el.events[el.currentEvent]
	if len(event.Manifests) > 1 {
		orderedIds, err := event.TopoSort()
		if err != nil {
			log.Printf("Unable to order event %s: %s\n", event.ID, err)
			return event
		}
		orderedManifests := make([]ComputeManifest, len(event.Manifests))
		for i, oid := range orderedIds {
			orderedManifests[i], err = getManifest(event.Manifests, oid)
			if err != nil {
				log.Printf("Unable to order event %s: %s\n", event.ID, err)
				return event
			}
		}
		event.Manifests = orderedManifests
	}
	return event
}

func getManifest(manifests []ComputeManifest, id string) (ComputeManifest, error) {
	for _, m := range manifests {
		if m.ManifestID == id {
			return m, nil
		}
	}
	return ComputeManifest{}, errors.New("Unable to find Manifest in list")
}

// StochasticEvents is an EventGenerator that generates sets of stochastic events
// based on a manifest tempate and start and end indices.
// This type is not currently implemented
type StochasticEvents struct {
	eventStartIndex  int
	eventEndIndex    int
	manifestTemplate ComputeManifest
}
