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

const (
	schemaGroup = "schema"
	tableGroup  = "table"
)

var (
	queryMatchData []matchData
)

type matchData struct {
	regex       *regexp.Regexp
	subexpNames []string
	operation   Operation
}

func init() {
	queryMatchData = []matchData{
		newMatchData(regexp.MustCompile(`(?i)^\s*TRUNCATE\s+TABLE\s+(`+"`"+`?(?P<`+schemaGroup+`>[\w]+)`+"`"+`?\.)?`+"`"+`?(?P<`+tableGroup+`>[\w]+)`+"`"+`?\s*;?$`), truncateOperation),
		newMatchData(regexp.MustCompile(`(?i)^\s*TRUNCATE\s+TABLE\s+(`+"`"+`?(?P<`+schemaGroup+`>[\w]+)`+"`"+`?\.)?`+"`"+`?(?P<`+tableGroup+`>[\w]+)`+"`"+`?\s*;?$`), truncateOperation),
		newMatchData(regexp.MustCompile(`(?i)^\s*CREATE\s+(TEMPORARY\s+)?TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(`+"`"+`?(?P<`+schemaGroup+`>[\w]+)`+"`"+`?\.)?`+"`"+`?(?P<`+tableGroup+`>[\w]+)`+"`"+`?\s*\((?s).*\).*;?$`), createTableOperation),
		newMatchData(regexp.MustCompile(`(?i)^\s*DROP\s+TABLE\s+(IF\s+EXISTS\s+)?(`+"`"+`?(?P<`+schemaGroup+`>[\w]+)`+"`"+`?\.)?`+"`"+`?(?P<`+tableGroup+`>[\w]+)`+"`"+`?\s*;?$`), dropTableOperation),
		newMatchData(regexp.MustCompile(`(?i)^\s*ALTER\s+TABLE\s+(`+"`"+`?(?P<`+schemaGroup+`>[\w]+)`+"`"+`?\.)?`+"`"+`?(?P<`+tableGroup+`>[\w]+)`+"`"+`?\s+(?s).+;?$`), alterTableOperation),
	}
}

func ExtractOperation(query string) (string, string, Operation) {
	for _, matchData := range queryMatchData {
		matches := matchData.regex.FindStringSubmatch(query)
		if matches == nil {
			continue
		}
		var schema, table string
		for i, match := range matches {
			switch matchData.subexpNames[i] {
			case schemaGroup:
				schema = match
			case tableGroup:
				table = match
			}
		}
		return schema, table, matchData.operation
	}
	return "", "", UnknownOperation
}

func newMatchData(regex *regexp.Regexp, operation Operation) matchData {
	return matchData{
		regex:       regex,
		subexpNames: regex.SubexpNames(),
		operation:   operation,
	}
}
