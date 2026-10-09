package command

import (
	"context"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
	"google.golang.org/grpc"
)

type service struct {
	UnimplementedHealthServiceServer
	observer *health.Observer
}

func (s *service) ready() error {
	if s.observer == nil {
		return errors.New("healthObservatory is not configured")
	}
	return nil
}

func (s *service) GetStatus(context.Context, *GetStatusRequest) (*GetStatusResponse, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	return &GetStatusResponse{Json: string(s.observer.Status())}, nil
}

func (s *service) DrainEvents(context.Context, *DrainEventsRequest) (*DrainEventsResponse, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	events, dropped := s.observer.DrainEvents()
	return &DrainEventsResponse{Json: string(events), Dropped: dropped}, nil
}

func (s *service) NetworkChanged(_ context.Context, r *NetworkChangedRequest) (*NetworkChangedResponse, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if r.Lost {
		s.observer.NetworkLost()
	} else {
		s.observer.NetworkChanged(r.Network)
	}
	return &NetworkChangedResponse{}, nil
}

func (s *service) Pin(_ context.Context, r *PinRequest) (*PinResponse, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if r.Outbound == "" {
		return &PinResponse{Ok: s.observer.Release(r.Balancer, r.Run)}, nil
	}
	return &PinResponse{Ok: s.observer.Pin(r.Balancer, r.Outbound, r.Run)}, nil
}

func (s *service) Refresh(_ context.Context, r *RefreshRequest) (*RefreshResponse, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	return &RefreshResponse{Ok: s.observer.RequestRefresh(r.Provider)}, nil
}

func (s *service) GetReport(_ context.Context, r *GetReportRequest) (*GetReportResponse, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	var report []byte
	if r.Take || r.Final {
		report = s.observer.TakeReport(r.Final)
	} else {
		report = s.observer.Report()
	}
	return &GetReportResponse{Json: string(report)}, nil
}

// Probe polls the result instead of waiting on the observer, which keeps no
// channels for waiting callers. It answers this much before the deadline of
// the call with what is known by then.
const (
	probePoll   = 250 * time.Millisecond
	probeMargin = 300 * time.Millisecond
)

func (s *service) Probe(ctx context.Context, r *ProbeRequest) (*ProbeResponse, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	ticket, err := s.observer.RequestProbe(health.ProbeRequest{Balancer: r.Balancer, Outbounds: r.Outbounds})
	if err != nil {
		return nil, err
	}
	until := time.Now().Add(time.Duration(r.WaitMs) * time.Millisecond)
	if deadline, ok := ctx.Deadline(); ok && deadline.Add(-probeMargin).Before(until) {
		until = deadline.Add(-probeMargin)
	}
	for {
		result, done := s.observer.ProbeResult(ticket.ID)
		wait := time.Until(until)
		if done || result == nil || wait <= 0 {
			return &ProbeResponse{Id: ticket.ID, Run: ticket.Run, Json: string(result)}, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(min(wait, probePoll)):
		}
	}
}

// SetProviderHeaders fails like the observer does: the error names the
// header, never its value.
func (s *service) SetProviderHeaders(_ context.Context, r *SetProviderHeadersRequest) (*SetProviderHeadersResponse, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if err := s.observer.SetProviderHeaders(r.Provider, r.Headers); err != nil {
		return nil, err
	}
	return &SetProviderHeadersResponse{}, nil
}

// GetProviderSet hands over the token and the keys of the outbounds of the
// provider, as HandlerService hands over outbounds: the API listens where
// only the host reaches it.
func (s *service) GetProviderSet(_ context.Context, r *GetProviderSetRequest) (*GetProviderSetResponse, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	set, seq, err := s.observer.ProviderSet(r.Provider, r.After)
	if err != nil {
		return nil, err
	}
	return &GetProviderSetResponse{Json: string(set), Seq: seq}, nil
}

func (s *service) Register(server *grpc.Server) {
	RegisterHealthServiceServer(server, s)
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, cfg interface{}) (interface{}, error) {
		v := core.MustFromContext(ctx)
		sv := &service{}
		err := v.RequireFeatures(func(o extension.Observatory) {
			sv.observer, _ = o.(*health.Observer)
		}, true)
		if err != nil {
			return nil, err
		}
		return sv, nil
	}))
}
