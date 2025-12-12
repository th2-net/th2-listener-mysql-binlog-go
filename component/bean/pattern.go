/*
 Copyright 2025 Exactpro (Exactpro Systems Limited)

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package bean

import "regexp"

var (
	queryMatchData []matchData
)

type matchData struct {
	regex     *regexp.Regexp
	operation Operation
}

func init() {
	queryMatchData = []matchData{
		{
			regex:     regexp.MustCompile(`(?i)^\s*TRUNCATE\s+TABLE\s+(` + "`" + `?[\w]+` + "`" + `?\.)?` + "`" + `?[\w]+` + "`" + `?\s*;?$`),
			operation: truncateOperation,
		},
		{
			regex:     regexp.MustCompile(`(?i)^\s*CREATE\s+(TEMPORARY\s+)?TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(` + "`" + `?[\w]+` + "`" + `?\.)?` + "`" + `?[\w]+` + "`" + `?\s*\((?s).*\)\s*;?$`),
			operation: createTableOperation,
		},
		{
			regex:     regexp.MustCompile(`(?i)^\s*DROP\s+TABLE\s+(IF\s+EXISTS\s+)?(` + "`" + `?[\w]+` + "`" + `?\.)?` + "`" + `?[\w]+` + "`" + `?\s*;?$`),
			operation: dropTableOperation,
		},
		{
			regex:     regexp.MustCompile(`(?i)^\s*ALTER\s+TABLE\s+(` + "`" + `?[\w]+` + "`" + `?\.)?` + "`" + `?[\w]+` + "`" + `?\s+(?s).+;?$`),
			operation: alterTableOperation,
		},
	}
}

func ExtractOperation(query string) Operation {
	for _, matchData := range queryMatchData {
		if matchData.regex.MatchString(query) {
			return matchData.operation
		}
	}
	return unknownOperation
}
