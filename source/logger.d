module logger;

import std.datetime, std.conv, std.stdio, std.algorithm, std.concurrency, std.exception, std.file, std.string, std.range, std.socket, std.json;
import core.thread, core.memory, core.sys.linux.sys.inotify, core.sys.posix.unistd, core.sys.posix.poll;

extern(C){
    size_t strnlen(const(char)* s, size_t maxlen);
    enum NAME_MAX = 255;
}

public struct Watch {
	int wd;
}

public struct Event {
	Watch wd;
	uint mask, cookie;
	string name;
}

const(char)[] name(ref inotify_event e)
{
    auto ptr = cast(const(char)*)(&e+1); 
    auto len = strnlen(ptr, e.len);
    return ptr[0..len];
}
auto maxEvent(){ return inotify_event.sizeof + NAME_MAX + 1; }

struct STOP_THREAD {
	string thread_name;
}

auto size(ref inotify_event e) { return e.sizeof + e.len; }

enum LOGGING_THREAD_EVENTS {LOG_THREAD_STOPPED = 0};
enum WRITER_THREAD_EVENTS {WRITER_INITIATED = 0, WRITER_INIT_FAILED, WRITER_FAILED};
enum READER_THREAD_EVENTS {STOP_ALL_THREADS = 0, READ};
enum SV_THREAD_EVENTS {ALL_THREADS_STOPPED};
enum LOG_TYPE {INFO = "[INFO]", ERROR="[ERROR]"};

void log(LOG_TYPE type, string id, string msg){
	writeln(type ~ "[" ~ Clock.currTime.to!string ~ "][" ~ id ~ "] " ~ msg);
}

class INotify{ 
	private int fd= -1; // file descriptor that inotify_init return  
	private string path;
	private ubyte[] buffer;
	private Watch self;
	private string[Watch] logFiles;
	private string[Watch] newContainers;
	
	this(string path) { 
		fd = inotify_init();		
		enforce(fd >= 0, "Failed to init inotify");
		this.path = path;
		self = Watch(inotify_add_watch(fd, path.toStringz, IN_DELETE | IN_CREATE | IN_DELETE_SELF));
		log(LOG_TYPE.INFO, "reader", "Add watcher: " ~ path);
		enforce(self.wd >= 0, "Can't add watcher to " ~ path);
		buffer = new ubyte[20 * maxEvent];
	}

	~this() {
		if(fd >= 0){
			close(fd);
		}
	}

	void print() {

		foreach(k,v;logFiles){
			writeln(k.wd.to!string ~ ' ' ~ v);
		
		}	writeln("-------------");
	}

	public void addWatcher(string logFilePath) {
		auto wd = inotify_add_watch(fd, logFilePath.toStringz, IN_MODIFY);
		enforce(wd >= 0, "Can't add watcher to "~logFilePath);
		logFiles[Watch(wd)] = logFilePath;
		log(LOG_TYPE.INFO, "reader", "Add watcher: " ~ logFilePath);
	}

	public void read(LoggingThreadPool pool, Tid writer, Tid reader) {
		pollfd fds = {fd, POLLIN};
		int timeout_msecs = 1000;	
		if(poll(&fds, 1, timeout_msecs) != 0) {	
			long len = .read(fd, buffer.ptr, buffer.length);
			auto head = buffer.ptr;

			while(len > 0) {
				auto eptr = cast(inotify_event *)head;
				auto sz = size(*eptr);
				head += sz;
				len -= sz;
				string name = .name(*eptr).dup;
				
				if(eptr.wd == self.wd)	
					final switch(eptr.mask & (IN_DELETE | IN_CREATE)) {
						case IN_DELETE :
							string logFile = path ~ name ~ '/' ~ name ~ "-json.log";
							pool.delThread(logFile, reader);
							log(LOG_TYPE.INFO, "reader", "Container was deleted! " ~ name);
							break; 
						case IN_CREATE :
							auto wd = inotify_add_watch(fd, (path ~ name).toStringz, IN_CREATE);
							enforce(wd >= 0, "Can't bind watcher to " ~ name);
							if(.exists(path ~ name ~ '/' ~ name ~ "-json.log"))
								inotify_rm_watch(fd, wd);
							else
								newContainers[Watch(wd)] = name;
							break;
					}
				else if (Watch(eptr.wd) in newContainers) 
					final switch(eptr.mask & (IN_CREATE | IN_IGNORED)) {
						case IN_CREATE : 
							if(startsWith(name, newContainers[Watch(eptr.wd)])) {
							   pool.addThread(newContainers[Watch(eptr.wd)], this);
							   inotify_rm_watch(fd, eptr.wd);
							}
							break;
						case IN_IGNORED: 
							log(LOG_TYPE.INFO, "reader", "Watch for " ~ newContainers[Watch(eptr.wd)] ~ " was removed explicitly  or automatically (file was deleted, or filesystem was unmounted)");
							newContainers.remove(Watch(eptr.wd));
							break;
					}
				else
					final switch(eptr.mask & (IN_MODIFY | IN_IGNORED)) {
						case IN_MODIFY :
							string p = logFiles[Watch(eptr.wd)];
							pool.getTid(p).send(writer);
							break; 
						case IN_IGNORED : 
							log(LOG_TYPE.INFO, "reader", "Watch for " ~ logFiles[Watch(eptr.wd)] ~ " was removed explicitly  or automatically (file was deleted, or filesystem was unmounted)");
							break;
					} 
			}	
		}
	}
}

class LoggingThreadPool {
	private Tid[string] pool;
	string dir;
	Tid writer;
	this(string path, INotify notifier, Tid writer) {
		dir = path;
		this.writer = writer;
		foreach(container; dirEntries(path, SpanMode.shallow)) {
			auto name = container.name;
			addThread(name.split('/').back, notifier);
		}
	}

	Tid getTid(string path) {
		return pool[path];
	}

	void addThread(string container, INotify notifier) {
		string path = dir ~ container;
		string logFilePath = path ~ '/' ~ container ~"-json.log";
		string confPath = path ~ "/." ~ container ~ "-json.conf";
		File config;
		ulong pos = 0;
		auto logFile = File(logFilePath, "r");
		auto logSize = logFile.size;	// get logfile size in bytes
		logFile.detach();
		if(exists(confPath) && pos <= logSize)
		{
			config = File(confPath, "r");
			string s_pos = config.readln();
			pos = parse!ulong(s_pos);
		}
		else
		{
			config = File(confPath, "w");
			pos = 0;
			config.writef("%d",pos);
		}
		config.detach();
		auto hostname = File(path ~ "/hostname", "r");
		string service_name = hostname.readln(); 	//get service_name
		hostname.detach();
		if(logFilePath !in pool) {
			pool[logFilePath] = spawn(&readLog, pos, logFilePath, confPath, service_name.chop);
			notifier.addWatcher(logFilePath);
			pool[logFilePath].send(writer);
		}
	}

	bool delNameFromPool(string logFilePath) {
		pool.remove(logFilePath);
		return (pool.length == 0);  
	}

	void delThread(string logFilePath, Tid waiter) {
		STOP_THREAD stop_msg = {thread_name : logFilePath};
		pool[logFilePath].prioritySend(stop_msg, waiter);
	}	

	void stopAllThreads(Tid waiter){
		foreach(name, tid; pool) 
			delThread(name, waiter);
	}	
}


class SocketPool {
	TcpSocket socket;
	this(string hostname, ushort port) {
		auto addr = new InternetAddress(hostname, port);
		socket = new TcpSocket(addr);
		socket.setKeepAlive(10,10);
	}

	bool send(immutable(string)[] buffer, ulong pos, Tid thread_tid){
		foreach(item; buffer)
		{
				long size = socket.send(item ~ '\n');
				if(size == Socket.ERROR)
					return false;
				else 
					thread_tid.send(size, pos);				
		}
		return true;
	}
}

void reader(Tid sv, Tid writer_tid, string folder) {
	auto notifier = new INotify(folder);
	auto pool = new LoggingThreadPool(folder, notifier, writer_tid);

	for(bool running = true; running;)
		receive(
			(LOGGING_THREAD_EVENTS e, string thread_name) {
				running = !pool.delNameFromPool(thread_name);
				if(!running) {
					log(LOG_TYPE.INFO, "reader", "All logging threads were stopped!");
					sv.prioritySend(SV_THREAD_EVENTS.ALL_THREADS_STOPPED);
				}
			},
			(READER_THREAD_EVENTS e) {
				final switch(e) {
					case READER_THREAD_EVENTS.STOP_ALL_THREADS:
						pool.stopAllThreads(thisTid);
						break;
					case READER_THREAD_EVENTS.READ:
						notifier.read(pool, writer_tid, thisTid);
						thisTid.send(READER_THREAD_EVENTS.READ);
				}
			}
		);
	log(LOG_TYPE.INFO, "reader", "Thread was normally terminated!");
}

void writer(Tid sv,string hostname, ushort port){
	try{
		auto pool = new SocketPool(hostname, port);
		sv.send(WRITER_THREAD_EVENTS.WRITER_INITIATED);
		for(bool running = true; running;)
			receive(
				(Tid thread_tid, ulong pos, immutable(char[])[] buffer) {
					running = pool.send(cast(immutable)buffer, pos, thread_tid);
					if(!running)
					{
						sv.prioritySend(WRITER_THREAD_EVENTS.WRITER_FAILED);
						log(LOG_TYPE.ERROR, "writer", "Error occured while sending to logstash!");
					}
				},
				(OwnerTerminated msg) {
					log(LOG_TYPE.ERROR, "writer", "Owner terminated!");
					running = false;
				}
				);
	}
	catch(SocketException e){
		sv.prioritySend(WRITER_THREAD_EVENTS.WRITER_INIT_FAILED);
		log(LOG_TYPE.ERROR, "writer", e.msg);
	}
	log(LOG_TYPE.INFO, "writer", "Thread was normally terminated!");
}

void readLog(ulong seek, string pathToLog, string pathToConf, string service_name) {
	auto logFile = File(pathToLog, "r");
	auto config = File(pathToConf, "w");
	config.writef("%d", seek);
	config.flush();

	for(bool running = true; running;)
	receive (
		(Tid writer){
			logFile.seek(seek, SEEK_SET);
			string[] buffer;
			foreach(line; logFile.byLine())
			{	
				auto json = parseJSON(line, 1);
				json.object["service_name"] = service_name;
				buffer ~= json.toString;
			}
			seek = logFile.tell();
			writer.send(thisTid, seek, cast(immutable)buffer);
		},
		(long size, ulong pos) {
			enforce(size > 0, "Failed to send buffer to logstash!");
			config.seek(0, SEEK_SET);
			config.writef("%d", pos);
			config.flush();				
		},
		(STOP_THREAD msg, Tid waiter) {
			log(LOG_TYPE.INFO, "logging thread", "Thread, that watched " ~ msg.thread_name ~ ", was stopped!");
			logFile.detach();
			config.detach();
			running = false;
			waiter.prioritySend(LOGGING_THREAD_EVENTS.LOG_THREAD_STOPPED, msg.thread_name);
		}
		);
}

int main(string[] args) {
	if(args.length != 4)
		writeln("Usage : stasher path_to_docker_containers logstash_host port\nExample: stasher /var/lib/docker/containers localhost 12345");
	else {
		enforce(exists(args[1]), "Folder doesn't exist!");
		auto folder = args[1].back == '/' ? args[1] : args[1] ~ '/';
		auto writer_tid = spawn(&writer, thisTid, args[2], to!ushort(args[3]));
		Tid reader_tid; 

		for(bool running = true; running;)
			receive(
					(WRITER_THREAD_EVENTS e){
						final switch(e) {
							case WRITER_THREAD_EVENTS.WRITER_INITIATED:
								reader_tid = spawn(&reader, thisTid, writer_tid, folder);
								reader_tid.send(READER_THREAD_EVENTS.READ);
								break;
							case WRITER_THREAD_EVENTS.WRITER_INIT_FAILED:
								running = false;
								break;
							case WRITER_THREAD_EVENTS.WRITER_FAILED:
								reader_tid.prioritySend(READER_THREAD_EVENTS.STOP_ALL_THREADS);
								break;
						}
					},
					(SV_THREAD_EVENTS e) {
						running = false;
					}
				);
		log(LOG_TYPE.INFO, "supervisor", "Shutdown logger!");
	}	
	return 0;
}
