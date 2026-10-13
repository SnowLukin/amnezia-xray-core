"""Test-tag-only panic isolation stand. A large neighbor download stays alive."""
import concurrent.futures,http.server,json,os,socket,struct,subprocess,threading,time,tempfile,sys
PAYLOAD=b'x'*(8*1024*1024);progress=threading.Event()
class Target(http.server.BaseHTTPRequestHandler):
 def do_GET(self):
  self.send_response(200);self.send_header('Content-Length',str(len(PAYLOAD)));self.end_headers()
  for i in range(0,len(PAYLOAD),16384):self.wfile.write(PAYLOAD[i:i+16384]);self.wfile.flush();progress.set();time.sleep(.002)
 def log_message(self,*args):pass

def port():
 with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
def exact(s,n):
 b=b''
 while len(b)<n:
  chunk=s.recv(n-len(b))
  if not chunk:raise EOFError('socks or target closed')
  b+=chunk
 return b

def socks(p,target):
 s=socket.create_connection(('127.0.0.1',p),timeout=10);s.sendall(b'\x05\x01\x00');assert exact(s,2)==b'\x05\x00';s.sendall(b'\x05\x01\x00\x01\x7f\x00\x00\x01'+struct.pack('!H',target));reply=exact(s,4)
 if reply[1]:s.close();raise IOError('SOCKS error '+str(reply[1]))
 exact(s,6 if reply[3]==1 else 18);return s

def run(binary,mode):
 server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Target);threading.Thread(target=server.serve_forever,daemon=True).start();listen=port();bad=port();progress.clear()
 config={'log':{'loglevel':'warning'},'inbounds':[{'listen':'127.0.0.1','port':listen,'protocol':'socks','settings':{'auth':'noauth'}}],'outbounds':[{'tag':'a','protocol':'freedom','settings':{},'selection':{'balancer':'auto','cost':1,'probe':'local'}}],'routing':{'balancers':[{'tag':'auto','fallbackTag':'a','strategy':{'type':'leastCost'}}],'rules':[{'type':'field','network':'tcp','balancerTag':'auto'}]},'healthObservatory':{'probes':{'profiles':[{'name':'local','steps':[{'name':'reply','destinations':['http://127.0.0.1:'+str(server.server_port)+'/'],'minBytes':1}]}]},'telemetry':{'events':{},'report':{}}}}
 env=os.environ.copy();env['XRAY_RESILIENCE_PANIC']='connection:127.0.0.1:'+str(bad) if mode=='connection' else 'health:loop';tmp=tempfile.mkdtemp(prefix='resilience-panic-');cfg=os.path.join(tmp,'config.json');json.dump(config,open(cfg,'w'));logpath=os.path.join(tmp,'xray.log');log=open(logpath,'w');proc=subprocess.Popen([binary,'run','-c',cfg],env=env,stdout=log,stderr=log)
 try:
  for _ in range(100):
   if proc.poll() is not None:raise RuntimeError(open(logpath).read())
   try:
    with socket.create_connection(('127.0.0.1',listen),timeout=.1):break
   except OSError:time.sleep(.05)
  def download():
   with socks(listen,server.server_port) as s:
    s.sendall(b'GET / HTTP/1.1\r\nHost: stand.example\r\nConnection: close\r\n\r\n');data=b''
    while True:
     b=s.recv(65536)
     if not b:break
     data+=b
    body=data.split(b'\r\n\r\n',1)[1];assert body==PAYLOAD,(len(body),len(PAYLOAD));return len(body)
  with concurrent.futures.ThreadPoolExecutor() as executor:
   future=executor.submit(download);assert progress.wait(5)
   if mode=='connection':
    try:
     with socks(listen,bad) as s:s.recv(1)
    except (EOFError,OSError):pass
   count=future.result(timeout=30)
  assert proc.poll() is None
  log.flush();logs=open(logpath).read();assert 'panic_recovered' in logs,logs;assert 'scope='+('connection' if mode=='connection' else 'subsystem') in logs
  print(json.dumps({'mode':mode,'download_bytes':count,'process_alive':True,'recovered':True,'artifacts':tmp}))
 finally:
  proc.terminate();proc.wait(timeout=10);server.shutdown();server.server_close();log.close()
if __name__=='__main__':
 for mode in ['connection','health']:run(os.path.abspath(sys.argv[1]),mode)
