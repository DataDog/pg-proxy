// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package query

import "github.com/jackc/pgx/v5/pgproto3"

// MessageToText takes in a postgres message and returns the type of message as a string
func MessageToText(msg pgproto3.Message) string {
	switch msg.(type) {
	case *pgproto3.AuthenticationCleartextPassword:
		return "authentication_cleartext_password"
	case *pgproto3.AuthenticationGSS:
		return "authentication_gss"
	case *pgproto3.AuthenticationGSSContinue:
		return "authentication_gss_continue"
	case *pgproto3.AuthenticationMD5Password:
		return "authentication_md5_password"
	case *pgproto3.AuthenticationOk:
		return "authentication_ok"
	case *pgproto3.AuthenticationSASL:
		return "authentication_sasl"
	case *pgproto3.AuthenticationSASLContinue:
		return "authentication_sasl_continue"
	case *pgproto3.AuthenticationSASLFinal:
		return "authentication_sasl_final"
	case *pgproto3.BackendKeyData:
		return "backend_key"
	case *pgproto3.Bind:
		return "bind"
	case *pgproto3.BindComplete:
		return "bind_complete"
	case *pgproto3.CancelRequest:
		return "cancel_request"
	case *pgproto3.Close:
		return "close"
	case *pgproto3.CloseComplete:
		return "close_complete"
	case *pgproto3.CommandComplete:
		return "command_complete"
	case *pgproto3.CopyBothResponse:
		return "copy_both_response"
	case *pgproto3.CopyData:
		return "copy_data"
	case *pgproto3.CopyDone:
		return "copy_done"
	case *pgproto3.CopyFail:
		return "copy_fail"
	case *pgproto3.CopyInResponse:
		return "copy_in_response"
	case *pgproto3.CopyOutResponse:
		return "copy_out_response"
	case *pgproto3.DataRow:
		return "data_row"
	case *pgproto3.Describe:
		return "describe"
	case *pgproto3.EmptyQueryResponse:
		return "empty_query_response"
	case *pgproto3.ErrorResponse:
		return "error_response"
	case *pgproto3.Execute:
		return "execute"
	case *pgproto3.Flush:
		return "flush"
	case *pgproto3.FunctionCall:
		return "function_call"
	case *pgproto3.FunctionCallResponse:
		return "function_call_response"
	case *pgproto3.GSSEncRequest:
		return "gss_enc_request"
	case *pgproto3.NoData:
		return "no_data"
	case *pgproto3.NoticeResponse:
		return "notice_response"
	case *pgproto3.NotificationResponse:
		return "notification_response"
	case *pgproto3.ParameterDescription:
		return "parameter_description"
	case *pgproto3.ParameterStatus:
		return "parameter_status"
	case *pgproto3.Parse:
		return "parse"
	case *pgproto3.ParseComplete:
		return "parse_complete"
	case *pgproto3.PortalSuspended:
		return "portal_suspended"
	case *pgproto3.Query:
		return "query"
	case *pgproto3.ReadyForQuery:
		return "ready_for_query"
	case *pgproto3.RowDescription:
		return "row_description"
	case *pgproto3.SSLRequest:
		return "ssl_request"
	case *pgproto3.StartupMessage:
		return "startup_message"
	case *pgproto3.Sync:
		return "sync"
	case *pgproto3.Terminate:
		return "terminate"
	default:
		return "unknown"
	}
}
