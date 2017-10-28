const rpc = require('pm2-axon-rpc');
const axon = require('pm2-axon');
const { EventEmitter } = require('events');
const Router = require('./router');
const _ = require('lodash');
const puid = new (require('puid'))(false);
const Redis = require('ioredis');
const debug = require('debug')('spinal:broker');

class Broker extends EventEmitter {
	constructor(options) {
		super();

		this.id = puid.generate();
		this.rep = new axon.RepSocket();
		this.server = new rpc.Server(this.rep);
		this.options = options || {};
		this.redis = null;
		this.redis_prefix = this.options.redis_prefix || 'spinal:';
		this.queue = null;

		if (this.options.redis) {
			debug('[redis] initialize');
			this.redis = new Redis(this.options.redis);
			const Queue = require('./queue');
			this.queue = new Queue(this, { redis: this.options.redis });
		}

		this.router = new Router(this, {
			redis: this.redis,
			redis_prefix: this.redis_prefix || 'spinal:',
		});

		this.metrics = (require('./metrics'))(this, this.options);

		if (this.options.restapi) {
			const RestAPI = require('./restapi');
			this.restapi = new RestAPI(this, this.options);
		}

		this.server.expose('handshake', this.handshake.bind(this));
		this.server.expose('heartbeat', this.heartbeat.bind(this));
		this.server.expose('ping', this.ping.bind(this));
		this.server.expose('bye', this.bye.bind(this));
		this.server.expose('rpc', this.rpc.bind(this));
		this.server.expose('jobAdd', this.jobAdd.bind(this));
	}

	start(port, done) {
		if (typeof port === 'function') {
			done = port;
			port = 7557;
		}

		this.rep.port = parseInt(port, 10);
		this.rep.bind(this.req.port);
		this.server.sock.server.on('listening', () => {
			if (done) {
				done.apply(this.rep, null);
			}
		});
	}

	stop(done) {
		debug(`------- stop ${this.id} -------`);
		for (const id in this.router.nodes) {
			if (id) {
				this.router.removeNode(id, 'shutdown');
			}
		}

		try {
			this.rep.close(() => done && done.apply(this.rep, null));
		} catch (e) {
			// console.error(e)
		}

		if (this.restapi) {
			this.restapi.onstop();
		}

		if (this.redis) {
			this.redis.quit();
			this.queue.onstop();
		}
	}

	rpc(method, arg, options, reply) {
		this.metrics.meter('broker.method.calls').mark();
		this.router.call(method, arg, options, reply);
	}

	jobAdd(name, data, options, reply) {
		const handleJobEvent = (eventName, jobId, args) => {
			const node = this.router.nodes[data._caller_id];
			if (!node && !node.client) { return; }

			const callArgs = ['job event', eventName, jobId].concat(args);
			callArgs.push(_.noop); // handle reply

			node.client.call.apply(node.client, callArgs);
		};

		if (this.queue) {
			const job = this.queue.addJob(name, data, options, err => reply(err, job.id));

			if (data._caller_id) {
				const supportedEvents = ['complete', 'failed'];
				supportedEvents.forEach(eventName =>
					job.on(eventName, () => handleJobEvent(eventName, job.id, [].slice.call(arguments))));
			}
		} else {
			reply(new Error('Service queue unavailable on this broker'));
		}
	}

	handshake(data, reply, sock) {
		debug(`[_handshake] ${data.namespace} (${data.id})`);
		data.hostname = data.hostname || sock.remoteAddress;
		this.router.addNode(data);
		this.router.heartbeating(data);

		// create worker queue
		for (const item of data.methods) {
			if (item.indexOf(':worker') > -1) {
				const method = item.split(':')[0];
				if (this.queue) {
					this.queue.addWorker(method);
				}
			}
		}

		reply(null, this.router.listMethods());
		this.emit('handshake', data);
	}

	heartbeat(data, reply) {
		debug(`_heartbeat ${data.id}`);
		this.router.heartbeating(data);
		reply(null, this.router.listMethods());
	}

	bye(id, reply) {
		debug(`_bye ${id}`);
		this.router.removeNode(id, 'bye');
		reply(null, 'bye');
		this.emit('bye', id);
	}

	ping(data, reply) {
		debug(`_ping ${data.id}`);
		reply(null, 'pong');
	}
}

module.exports = Broker;
