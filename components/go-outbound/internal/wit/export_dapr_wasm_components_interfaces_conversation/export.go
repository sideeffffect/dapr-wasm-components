// Conversation (alpha2), served by the embedded runtime's API —
// ConverseAlpha2. Mirrors wasi-grpc-outbound's conversation.rs: content-part
// messages with role wrappers, tool definitions and tool calls,
// structured-output schema, prompt-cache retention, per-token usage.
package export_dapr_wasm_components_interfaces_conversation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/anyjson"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	cv "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_conversation"
)

func conversationError(f *call.Failure) cv.ConversationError {
	if f.IsPermission() {
		return cv.MakeConversationErrorPermissionDenied(f.Message)
	}
	return cv.MakeConversationErrorComponentNotFound(f.Message)
}

func converseError(f *call.Failure) cv.ConverseError {
	switch {
	case f.Status == 404:
		return cv.MakeConverseErrorContextNotFound(f.Message)
	case strings.Contains(f.ErrorCode, "CONTENT") || strings.Contains(f.ErrorCode, "FILTER"):
		return cv.MakeConverseErrorContentFiltered(f.Message)
	case f.Status == 400:
		return cv.MakeConverseErrorInvalidRequest(f.Message)
	}
	return cv.MakeConverseErrorConversation(conversationError(f))
}

// jsonToStruct parses a JSON object; malformed JSON the app supplied is a
// programming error, so it traps.
func jsonToStruct(text string) *structpb.Struct {
	s, err := anyjson.JSONToStruct(text)
	if err != nil {
		panic(err.Error())
	}
	return s
}

func contentPb(content []cv.ContentPart) []*runtimev1pb.ConversationMessageContent {
	out := make([]*runtimev1pb.ConversationMessageContent, len(content))
	for i, part := range content {
		out[i] = &runtimev1pb.ConversationMessageContent{Text: part.Text}
	}
	return out
}

func toolCallsPb(calls []cv.ToolCall) []*runtimev1pb.ConversationToolCalls {
	out := make([]*runtimev1pb.ConversationToolCalls, len(calls))
	for i, c := range calls {
		out[i] = &runtimev1pb.ConversationToolCalls{
			Id: call.OptPtr(c.Id),
			ToolTypes: &runtimev1pb.ConversationToolCalls_Function{Function: &runtimev1pb.ConversationToolCallsOfFunction{
				Name: c.Function.Name, Arguments: c.Function.Arguments,
			}},
		}
	}
	return out
}

func messagePb(m cv.Message) *runtimev1pb.ConversationMessage {
	out := &runtimev1pb.ConversationMessage{}
	switch m.Tag() {
	case cv.MessageDeveloper:
		p := m.Developer()
		out.MessageTypes = &runtimev1pb.ConversationMessage_OfDeveloper{OfDeveloper: &runtimev1pb.ConversationMessageOfDeveloper{
			Name: call.OptPtr(p.Name), Content: contentPb(p.Content),
		}}
	case cv.MessageSystem:
		p := m.System()
		out.MessageTypes = &runtimev1pb.ConversationMessage_OfSystem{OfSystem: &runtimev1pb.ConversationMessageOfSystem{
			Name: call.OptPtr(p.Name), Content: contentPb(p.Content),
		}}
	case cv.MessageUser:
		p := m.User()
		out.MessageTypes = &runtimev1pb.ConversationMessage_OfUser{OfUser: &runtimev1pb.ConversationMessageOfUser{
			Name: call.OptPtr(p.Name), Content: contentPb(p.Content),
		}}
	case cv.MessageAssistant:
		a := m.Assistant()
		out.MessageTypes = &runtimev1pb.ConversationMessage_OfAssistant{OfAssistant: &runtimev1pb.ConversationMessageOfAssistant{
			Name: call.OptPtr(a.Name), Content: contentPb(a.Content), ToolCalls: toolCallsPb(a.ToolCalls),
		}}
	case cv.MessageTool:
		tm := m.Tool()
		out.MessageTypes = &runtimev1pb.ConversationMessage_OfTool{OfTool: &runtimev1pb.ConversationMessageOfTool{
			ToolId: call.OptPtr(tm.ToolId), Name: tm.Name, Content: contentPb(tm.Content),
		}}
	default:
		panic(fmt.Sprintf("unknown message tag %d", m.Tag()))
	}
	return out
}

func inputPb(in cv.ConversationInput) *runtimev1pb.ConversationInputAlpha2 {
	messages := make([]*runtimev1pb.ConversationMessage, len(in.Messages))
	for i, m := range in.Messages {
		messages[i] = messagePb(m)
	}
	return &runtimev1pb.ConversationInputAlpha2{Messages: messages, ScrubPii: call.OptPtr(in.ScrubPii)}
}

func toolsPb(tools []cv.Tool) []*runtimev1pb.ConversationTools {
	out := make([]*runtimev1pb.ConversationTools, len(tools))
	for i, tool := range tools {
		var parameters *structpb.Struct
		if tool.Function.Parameters.IsSome() {
			parameters = jsonToStruct(tool.Function.Parameters.Some())
		}
		out[i] = &runtimev1pb.ConversationTools{ToolTypes: &runtimev1pb.ConversationTools_Function{
			Function: &runtimev1pb.ConversationToolsFunction{
				Name: tool.Function.Name, Description: call.OptPtr(tool.Function.Description), Parameters: parameters,
			},
		}}
	}
	return out
}

// durationPb: prompt-cache-retention is a Go duration string ("24h") in
// the WIT contract, so time.ParseDuration is the exact grammar (conversation.rs
// approximates it with humantime).
func durationPb(text string) *durationpb.Duration {
	d, err := time.ParseDuration(text)
	if err != nil {
		panic(fmt.Sprintf("invalid prompt-cache-retention %q: %v", text, err))
	}
	return durationpb.New(d)
}

// parametersPb: the WIT contract carries options.parameters as one JSON
// object; the proto wants map<string, Any>. Values in the protojson wrapper
// form the Dapr docs use ({"@type": ".../google.protobuf.Int64Value",
// "value": "100"}) are packed as that wrapper — like daprd decodes them
// from the HTTP API; anything else as Any(google.protobuf.Value).
func parametersPb(text string) map[string]*anypb.Any {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var parsed any
	if err := dec.Decode(&parsed); err != nil {
		panic(fmt.Sprintf("parameters is not valid JSON: %v", err))
	}
	entries, ok := parsed.(map[string]any)
	if !ok {
		panic("parameters must be a JSON object")
	}
	out := make(map[string]*anypb.Any, len(entries))
	for key, value := range entries {
		if object, isObject := value.(map[string]any); isObject {
			a, wrapped, err := anyjson.PackProtojsonWrapper(object)
			if err != nil {
				panic(err.Error())
			}
			if wrapped {
				out[key] = a
				continue
			}
		}
		raw, err := json.Marshal(value)
		if err != nil {
			panic(err.Error())
		}
		a, err := anyjson.PackJSON(string(raw))
		if err != nil {
			panic(err.Error())
		}
		out[key] = a
	}
	return out
}

func toolCallWit(c *runtimev1pb.ConversationToolCalls) cv.ToolCall {
	var id witTypes.Option[string]
	if c.Id != nil {
		id = witTypes.Some(c.GetId())
	} else {
		id = witTypes.None[string]()
	}
	// A missing function (unknown tool type) maps to empty name/arguments.
	f := c.GetFunction()
	return cv.ToolCall{Id: id, Function: cv.ToolCallFunction{Name: f.GetName(), Arguments: f.GetArguments()}}
}

func toolCallsWit(calls []*runtimev1pb.ConversationToolCalls) []cv.ToolCall {
	out := make([]cv.ToolCall, len(calls))
	for i, c := range calls {
		out[i] = toolCallWit(c)
	}
	return out
}

func usageWit(u *runtimev1pb.ConversationResultAlpha2CompletionUsage) cv.Usage {
	completion := witTypes.None[cv.CompletionTokensDetails]()
	if d := u.GetCompletionTokensDetails(); d != nil {
		completion = witTypes.Some(cv.CompletionTokensDetails{
			AcceptedPredictionTokens: d.GetAcceptedPredictionTokens(),
			AudioTokens:              d.GetAudioTokens(),
			ReasoningTokens:          d.GetReasoningTokens(),
			RejectedPredictionTokens: d.GetRejectedPredictionTokens(),
		})
	}
	prompt := witTypes.None[cv.PromptTokensDetails]()
	if d := u.GetPromptTokensDetails(); d != nil {
		prompt = witTypes.Some(cv.PromptTokensDetails{AudioTokens: d.GetAudioTokens(), CachedTokens: d.GetCachedTokens()})
	}
	return cv.Usage{
		CompletionTokens:        u.GetCompletionTokens(),
		PromptTokens:            u.GetPromptTokens(),
		TotalTokens:             u.GetTotalTokens(),
		CompletionTokensDetails: completion,
		PromptTokensDetails:     prompt,
	}
}

func outputWit(o *runtimev1pb.ConversationResultAlpha2) cv.ConversationOutput {
	choices := make([]cv.Choice, len(o.GetChoices()))
	for i, c := range o.GetChoices() {
		choices[i] = cv.Choice{
			FinishReason: c.GetFinishReason(),
			Index:        c.GetIndex(),
			Message: cv.ResultMessage{
				Content:   c.GetMessage().GetContent(),
				ToolCalls: toolCallsWit(c.GetMessage().GetToolCalls()),
			},
		}
	}
	model := witTypes.None[string]()
	if o.Model != nil {
		model = witTypes.Some(o.GetModel())
	}
	usage := witTypes.None[cv.Usage]()
	if o.GetUsage() != nil {
		usage = witTypes.Some(usageWit(o.GetUsage()))
	}
	return cv.ConversationOutput{Choices: choices, Model: model, Usage: usage}
}

func Converse(componentName string, inputs []cv.ConversationInput, options witTypes.Option[cv.ConversationOptions]) witTypes.Result[cv.ConversationResponse, cv.ConverseError] {
	inputsPb := make([]*runtimev1pb.ConversationInputAlpha2, len(inputs))
	for i, in := range inputs {
		inputsPb[i] = inputPb(in)
	}
	// An absent options record is the same as all fields unset.
	opts := options.SomeOr(cv.ConversationOptions{})
	req := &runtimev1pb.ConversationRequestAlpha2{
		Name:        componentName,
		ContextId:   call.OptPtr(opts.ContextId),
		Inputs:      inputsPb,
		Metadata:    call.MetadataMap(opts.Metadata),
		ScrubPii:    call.OptPtr(opts.ScrubPii),
		Temperature: call.OptPtr(opts.Temperature),
		Tools:       toolsPb(opts.Tools),
		ToolChoice:  call.OptPtr(opts.ToolChoice),
	}
	if opts.Parameters.IsSome() {
		req.Parameters = parametersPb(opts.Parameters.Some())
	}
	if opts.ResponseFormat.IsSome() {
		req.ResponseFormat = jsonToStruct(opts.ResponseFormat.Some())
	}
	if opts.PromptCacheRetention.IsSome() {
		req.PromptCacheRetention = durationPb(opts.PromptCacheRetention.Some())
	}

	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.ConversationResponseAlpha2, error) {
		return api.ConverseAlpha2(ctx, req)
	})
	if f != nil {
		return witTypes.Err[cv.ConversationResponse](converseError(f))
	}
	outputs := make([]cv.ConversationOutput, len(r.GetOutputs()))
	for i, o := range r.GetOutputs() {
		outputs[i] = outputWit(o)
	}
	contextID := witTypes.None[string]()
	if r.ContextId != nil {
		contextID = witTypes.Some(r.GetContextId())
	}
	return witTypes.Ok[cv.ConversationResponse, cv.ConverseError](cv.ConversationResponse{Outputs: outputs, ContextId: contextID})
}
