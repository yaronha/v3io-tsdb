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

package utils

import (
	"github.com/v3io/v3io-tsdb/config"
)

func TimeToDHM(tmilli int64) (int, int) {
	t := int(tmilli / 1000)
	//m := t/60 - ((t/3600) * 60)
	h := t/3600 - ((t / 3600 / 24) * 24)
	d := t / 3600 / 24
	return d, h
}

func TimeToChunkId(cfg *config.TsdbConfig, tmilli int64) int {
	d, h := TimeToDHM(tmilli)
	dpo := cfg.DaysPerObj

	if dpo == 1 {
		return h
	}

	dayIndex := d - ((d / dpo) * dpo)
	// dividing the total number of hours by int(dpo/2) will guarantee up to 72 attributes per object
	return (dayIndex*24 + h) / (dpo / 2)
}
