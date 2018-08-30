/*
Copyright 2018 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/

package appender

import "sort"

// struct/list storing uncommitted samples, with time sorting support
type pendingData struct {
	t int64
	v interface{}
}

type pendingList []pendingData

func (l pendingList) Len() int           { return len(l) }
func (l pendingList) Less(i, j int) bool { return l[i].t < l[j].t }
func (l pendingList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

type samplesList struct {
	unordered  bool
	maxtime    int64
	sentPtr    int
	historyPtr int
	data       pendingList
}

func (l *samplesList) append(t int64, v interface{}) bool {

	if t <= l.maxtime {
		// check for duplicates
		i := sort.Search(l.data.Len(), func(i int) bool { return l.data[i].t >= t })
		if i == l.data.Len() || l.data[i].t == t {
			// the sample is a duplicate (got the same timestamp)
			return true
		}
	}

	if t > l.maxtime {
		l.maxtime = t
	} else {
		l.unordered = true
	}

	l.data = append(l.data, pendingData{t: t, v: v})
	return false
}

func (l *samplesList) sort() {
	if l.unordered {
		// only sort unsent data
		sort.Sort(l.data[l.historyPtr:])
		l.unordered = false
	}
}

// get list of data that need to be sent
func (l *samplesList) getPending() pendingList {
	return l.data[l.historyPtr:]
}

// Set the sent pointer based on how many samples were processed
func (l *samplesList) updateSentIndex(index int) {
	l.sentPtr = l.historyPtr + index
}

// Advance the list: crop history part, sent part become history
func (l *samplesList) sendConfirmed() {
	l.data = l.data[l.historyPtr:]
	l.historyPtr = l.sentPtr - l.historyPtr
	l.sentPtr = l.historyPtr
}
