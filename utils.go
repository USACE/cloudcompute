package cloudcompute

import (
	"fmt"

	. "github.com/usace/cc-go-sdk"
)

func StatusQueryString(query JobsSummaryQuery) string {
	var queryString string

	switch query.QueryLevel {
	case SUMMARY_COMPUTE:
		queryString = fmt.Sprintf("%s_C_%s*", CcProfile, query.QueryValue.Compute)
	case SUMMARY_EVENT:
		queryString = fmt.Sprintf("%s_C_%s_E_%s*", CcProfile, query.QueryValue.Compute, query.QueryValue.Event)
	case SUMMARY_MANIFEST:
		queryString = fmt.Sprintf("%s_C_%s_E_%s_M_%s", CcProfile, query.QueryValue, query.QueryValue.Compute, query.QueryValue.Event)
	}

	return queryString
}
