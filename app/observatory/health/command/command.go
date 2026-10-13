package command

import (
	"context"
	"encoding/json"
	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
	"google.golang.org/grpc"
	"time"
)

type service struct {
	UnimplementedHealthServiceServer
	observer *health.Observer
}

func result(o *health.Observer, data any, err error) *Result {
	r := health.NewResult(o, data, err)
	out := &Result{Success: r.Success, Run: r.Run}
	if r.Error != nil {
		out.Error = &CommandError{Code: r.Error.Code, Message: r.Error.Message}
	}
	if r.Success && data != nil {
		raw, _ := json.Marshal(data)
		if string(raw) != "null" {
			out.Data = string(raw)
		}
	}
	return out
}
func (s *service) Status(context.Context, *StatusRequest) (*Result, error) {
	if s.observer == nil {
		return result(nil, nil, nil), nil
	}
	return result(s.observer, json.RawMessage(s.observer.Status()), nil), nil
}
func (s *service) DrainEvents(context.Context, *DrainEventsRequest) (*Result, error) {
	o := s.observer
	if err := o.FeatureError("events"); err != nil {
		return result(o, nil, err), nil
	}
	ev, dropped := o.DrainEvents()
	return result(o, health.EventBatch{Events: ev, Dropped: dropped}, nil), nil
}
func (s *service) NetworkChanged(_ context.Context, r *NetworkChangedRequest) (*Result, error) {
	o := s.observer
	if o == nil {
		return result(nil, nil, nil), nil
	}
	o.NetworkChanged(r.Network)
	return result(o, nil, nil), nil
}
func (s *service) NetworkChangedWithLabels(_ context.Context, r *NetworkChangedWithLabelsRequest) (*Result, error) {
	o := s.observer
	if err := o.FeatureError("host_labels"); err != nil {
		return result(o, nil, err), nil
	}
	// The measurement-context implementation provides the atomic labels call.
	return result(o, nil, health.NewCommandError("feature_disabled", "network labels are unavailable")), nil
}
func (s *service) NetworkLost(context.Context, *NetworkLostRequest) (*Result, error) {
	o := s.observer
	if o == nil {
		return result(nil, nil, nil), nil
	}
	o.NetworkLost()
	return result(o, nil, nil), nil
}
func (s *service) UserPresent(context.Context, *UserPresentRequest) (*Result, error) {
	o := s.observer
	if err := o.FeatureError("user_present"); err != nil {
		return result(o, nil, err), nil
	}
	o.UserPresent()
	return result(o, nil, nil), nil
}
func (s *service) SetMemoryLow(_ context.Context, r *SetMemoryLowRequest) (*Result, error) {
	o := s.observer
	if err := o.FeatureError("memory_pressure"); err != nil {
		return result(o, nil, err), nil
	}
	o.SetMemoryLow(r.IsLow)
	return result(o, nil, nil), nil
}
func (s *service) Pin(_ context.Context, r *PinRequest) (*Result, error) {
	o := s.observer
	if err := o.FeatureError("pinning"); err != nil {
		return result(o, nil, err), nil
	}
	if r.Outbound == "" {
		return result(o, nil, health.NewCommandError("invalid_argument", "pin requires an outbound")), nil
	}
	if !o.Pin(r.Balancer, r.Outbound, r.Run) {
		return result(o, nil, health.NewCommandError("invalid_argument", "pin was not accepted")), nil
	}
	return result(o, nil, nil), nil
}
func (s *service) Release(_ context.Context, r *ReleaseRequest) (*Result, error) {
	o := s.observer
	if err := o.FeatureError("pinning"); err != nil {
		return result(o, nil, err), nil
	}
	if !o.Release(r.Balancer, r.Run) {
		return result(o, nil, health.NewCommandError("invalid_argument", "release was not accepted")), nil
	}
	return result(o, nil, nil), nil
}
func (s *service) RequestProviderRefresh(_ context.Context, r *RequestProviderRefreshRequest) (*Result, error) {
	o := s.observer
	if o == nil {
		return result(nil, nil, nil), nil
	}
	data, err := o.ProviderRefreshInfo(r.Provider)
	return result(o, data, err), nil
}
func (s *service) PeekReport(context.Context, *PeekReportRequest) (*Result, error) {
	o := s.observer
	if err := o.FeatureError("report"); err != nil {
		return result(o, nil, err), nil
	}
	return result(o, json.RawMessage(o.PeekReport()), nil), nil
}
func (s *service) TakeReport(_ context.Context, r *TakeReportRequest) (*Result, error) {
	o := s.observer
	if err := o.FeatureError("report"); err != nil {
		return result(o, nil, err), nil
	}
	return result(o, json.RawMessage(o.TakeReport(r.IsFinal)), nil), nil
}

const (
	probePoll   = 250 * time.Millisecond
	probeMargin = 300 * time.Millisecond
)

func (s *service) RequestProbe(ctx context.Context, r *RequestProbeRequest) (*Result, error) {
	o := s.observer
	if o == nil {
		return result(nil, nil, nil), nil
	}
	ticket, err := o.RequestProbe(health.ProbeRequest{Balancer: r.Balancer, Outbounds: r.Outbounds, IsUDP: r.IsUdp})
	if err != nil {
		return result(o, nil, err), nil
	}
	if r.WaitMs <= 0 {
		return result(o, ticket, nil), nil
	}
	until := time.Now().Add(time.Duration(r.WaitMs) * time.Millisecond)
	if deadline, ok := ctx.Deadline(); ok && deadline.Add(-probeMargin).Before(until) {
		until = deadline.Add(-probeMargin)
	}
	for {
		raw, done, err := o.ProbeResult(ticket.Run, ticket.ID)
		if err != nil {
			return result(o, nil, err), nil
		}
		wait := time.Until(until)
		if done || wait <= 0 {
			return result(o, struct {
				health.ProbeTicket
				Result json.RawMessage `json:"result,omitempty"`
			}{ticket, raw}, nil), nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(min(wait, probePoll)):
		}
	}
}
func (s *service) ProbeResult(_ context.Context, r *ProbeResultRequest) (*Result, error) {
	o := s.observer
	if o == nil {
		return result(nil, nil, nil), nil
	}
	raw, _, err := o.ProbeResult(r.Run, r.Id)
	return result(o, json.RawMessage(raw), err), nil
}
func (s *service) SetProviderHeaders(_ context.Context, r *SetProviderHeadersRequest) (*Result, error) {
	o := s.observer
	if o == nil {
		return result(nil, nil, nil), nil
	}
	return result(o, nil, o.SetProviderHeaders(r.Provider, r.Headers)), nil
}
func (s *service) ExportProviderSet(_ context.Context, r *ExportProviderSetRequest) (*Result, error) {
	o := s.observer
	if o == nil {
		return result(nil, nil, nil), nil
	}
	raw, seq, err := o.ExportProviderSet(r.Provider, r.After)
	if err != nil || raw == nil {
		return result(o, nil, err), nil
	}
	return result(o, health.ProviderSetData{Seq: seq, Set: raw}, nil), nil
}
func (s *service) Register(server *grpc.Server) { RegisterHealthServiceServer(server, s) }
func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, cfg interface{}) (interface{}, error) {
		v := core.MustFromContext(ctx)
		sv := &service{}
		err := v.RequireFeatures(func(o extension.Observatory) { sv.observer, _ = o.(*health.Observer) }, true)
		if err != nil {
			return nil, err
		}
		return sv, nil
	}))
}
