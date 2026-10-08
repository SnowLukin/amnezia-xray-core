package command

import (
	"context"

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
