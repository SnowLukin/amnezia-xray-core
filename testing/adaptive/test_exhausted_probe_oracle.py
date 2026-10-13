import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock
from pathlib import Path

import scenarios as s
import bench


class Context:
    def __init__(self, value):self.value=value
    def __enter__(self):return self.value
    def __exit__(self,*args):pass


def event(at, typ, **fields):return {'ts':int(at*1000),'type':typ,'fields':fields}


class ExhaustedOracleTest(unittest.TestCase):
    def run_oracle(self,extra_after=False):
        events=[event(16,'probe_start',outbound='a1',reason='confirm'),
                event(17,'probe_result',outbound='a1',is_ok=False),event(25,'exhausted'),
                event(36,'probe_start',outbound='a1',reason='recheck'),
                event(37,'probe_result',outbound='a1',is_ok=False),
                event(40,'probe_start',outbound='a2',reason='recheck'),
                event(41,'probe_result',outbound='a2',is_ok=False),
                event(44,'probe_start',outbound='b1',reason='recheck'),
                event(45,'probe_result',outbound='b1',is_ok=False),
                event(92,'switch',reason='recovered',to='a2')]
        client=SimpleNamespace(events=events,direct_bytes=(0,0),status=lambda:{
            'paths':[{'outbound':tag} for tag in ['a1','a2','b1']],
            'balancers':[{'is_exhausted':True,'active':''}]})
        accepted=[(s.A0,15.8),(s.A0,15.9),(s.A0,16),(s.A0,36),(s.A1,40),(s.B0,44)]
        if extra_after:accepted.append((s.A0,36.1))
        sampler=SimpleNamespace(between=lambda port,start,end:sum(p==port and start<=at<=end for p,at in accepted))
        clock=[0]
        def timeline(traffic,actions,duration):
            for at,call in actions:clock[0]=at;call()
        with tempfile.TemporaryDirectory() as work:
            target=Path(work)/'target.log';target.write_text('')
            traffic=SimpleNamespace(records=[(81,True,0.1,1,'t1.test')])
            with mock.patch.object(bench,'Client',return_value=Context(client)), \
                 mock.patch.object(bench,'ConnSampler',return_value=Context(sampler)), \
                 mock.patch.object(bench,'Traffic',return_value=traffic), \
                 mock.patch.object(bench,'chaos'),mock.patch.object(s,'run_timeline',side_effect=timeline), \
                 mock.patch.object(s.time,'time',side_effect=lambda:clock[0]):
                rows=s.exhausted(SimpleNamespace(target_log=str(target)),'unused',work,'exhausted')
        return next(row for row in rows if row['check']=='probe connections at the proxy vs probe_start events')

    def test_user_connections_before_exhaustion_are_not_called_probes(self):
        row=self.run_oracle()
        self.assertTrue(row['ok'],row)
        self.assertTrue(row['value'].startswith('3/3'),row)

    def test_extra_connection_during_quarantine_probe_is_rejected(self):
        row=self.run_oracle(extra_after=True)
        self.assertFalse(row['ok'],row)
        self.assertTrue(row['value'].startswith('4/3'),row)

class SleepPacketOracleTest(unittest.TestCase):
    def run_oracle(self, up=0, down=110, sockets=0):
        events=[event(115,'idle_closed',outbound='w1',connections=1),
                event(125,'outbound_sleep',outbound='w1',reason='idle')]
        c=SimpleNamespace(events=events,direct_bytes=(0,0))
        cs=SimpleNamespace(packets=lambda port,start,end:(up,down) if start==125 else (3,1),
                           between=lambda *args:sockets)
        tr=SimpleNamespace(timeout=5,records=[(145,True,0.1,1,'t1.test')])
        quiet=SimpleNamespace(last=100,ended=115.1)
        marks={'start':100,'release':100,'away':100,'end':150,'status':{'is_asleep':True}}
        with mock.patch.object(s,'idle_close_common',return_value=[]):
            return s.idle_close_on_checks(c,cs,tr,quiet,marks,True)

    def test_server_residue_does_not_show_that_client_is_awake(self):
        rows=self.run_oracle()
        self.assertTrue(next(row[2] for row in rows if row[0].startswith('datagrams at the proxy')))
        self.assertTrue(next(row[2] for row in rows if row[0]=='new sockets of w1 after the sleep'))

    def test_client_datagram_still_fails(self):
        rows=self.run_oracle(up=1)
        self.assertFalse(next(row[2] for row in rows if row[0].startswith('datagrams at the proxy')))

    def test_new_socket_still_fails(self):
        rows=self.run_oracle(sockets=1)
        self.assertFalse(next(row[2] for row in rows if row[0]=='new sockets of w1 after the sleep'))

    def test_missing_counter_is_not_accepted(self):
        rows=self.run_oracle(up=None,down=None)
        self.assertFalse(next(row[2] for row in rows if row[0].startswith('datagrams at the proxy')))

if __name__=='__main__':unittest.main()
