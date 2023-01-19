package cloudcompute

import (
	"errors"
	"log"
)

//EventGenerators provide an iterator type interface to work with sets of events for a WatCompute.
type EventGenerator interface {
	HasNextEvent() bool
	NextEvent() Event
}

//EventList is an EventGenerator composed of a slice of events.
//Events are enumerated in the order they were placed in the slice.
type EventList struct {
	currentEvent int
	events       []Event
}

//Instantiates a new EventList
func NewEventList(events []Event) *EventList {
	el := EventList{
		currentEvent: -1,
		events:       events,
	}
	return &el
}

//Determines if all of the events have been enumerated
func (el *EventList) HasNextEvent() bool {
	el.currentEvent++
	return el.currentEvent < len(el.events)
}

//@TODO: Could optimize the sorting an instead of returning a list of ordered IDs, return a sorted list of manifests.....

//Retrieves the next event.  Attempts to perform a topological sort on the manifest slice before returning.
//If sort fails it will log the issue and return the unsorted manifest slice
func (el *EventList) NextEvent() Event {
	event := el.events[el.currentEvent]
	if len(event.Manifests) > 1 {
		orderedIds, err := event.TopoSort()
		if err != nil {
			log.Printf("Unable to order event %s: %s\n", event.ID, err)
			return event
		}
		orderedManifests := make([]Manifest, len(event.Manifests))
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

func getManifest(manifests []Manifest, id string) (Manifest, error) {
	for _, m := range manifests {
		if m.ManifestID == id {
			return m, nil
		}
	}
	return Manifest{}, errors.New("Unable to find Manifest in list")
}

//StochasticEvents is an EventGenerator that generates sets of stochastic events
//based on a manifest tempate and start and end indices.
//This type is not currently implemented
type StochasticEvents struct {
	eventStartIndex  int
	eventEndIndex    int
	manifestTemplate Manifest
}
