const Kue = require('kue')
const URLparse = require('url').parse
const debug = require('debug')('spinal:queue')
const _ = require('lodash')
const async = require('async')
const _Kues = []


class Queue {
  constructor(broker, options) {
    options = options || {};
    this.broker = broker;
    this.job_names = [];
    this.queues = {};
    this.redis_config = {};
    this.ttl_buffer = 1000;
    this.q = Kue.createQueue({
      prefix: 'q',
      redis: options.redis
    });
    this.q.client.once('connect', () => {
      debug('Queue ready! ... connected with redis')
    });
    _Kues.push(this.q);
  }

  addWorker(name) {
    if (this.job_names.indexOf(name) > -1) { return false; }
    debug('Add worker `' + name + '`');
    this.job_names.push(name);
    this.q.process(name, this.jobFn(name));

    // restore previous concurrent num
    this.broker.redis.get("concurrent:" + name).then(concurrent => {
      if (concurrent) {
        this.setConcurrent(name, concurrent);
        debug('Set ' + name + ' concurrent to ' + concurrent + ' from cache');
      }
    });
  }

  jobFn(name) {
    return (job, done) => {
      debug('Process ' + name + '(' + job.id + ')');
      this.broker.router.call(name + ':worker', job.data, { timeout: job._ttl - this.ttl_buffer },
        (err, result, options, job_options) => {
          /* istanbul ignore if */
          if (err) {
            debug('Job Failed ' + name + '(' + job.id + ')');
            debug(err);
            return done(err);
          }
          if (job_options.logs) {
            for (let i = 0; i < job_options.logs.length; i++) {
              job.log(job_options.logs[i]);
            }
          }
          done(err, result);
          debug('Finished ' + name + '(' + job.id + ')');
        });
    }
  }

  addJob(name, data, options, fn) {
    // if (!this.queues[name]) return false
    this.broker.metrics.meter('broker.queue.add').mark();
    let job = this.q.create(name, data)
      .priority(options.priority || 'normal')
      .removeOnComplete(options.removeOnComplete || false)
      .attempts(options.attempts)
      .ttl((options.ttl ? options.ttl + this.ttl_buffer : 10000 + this.ttl_buffer)) // 10s
      .backoff(options.backoff)
      .delay(options.delay)
      .save(function (err) {
        debug('Add job ' + job.type + '(' + job.id + ') ' + JSON.stringify(data));
        fn && fn(err);
      });
    return job;
  }

  setConcurrent(name, concurrency) {
    let group = _.groupBy(this.q.workers, function (n) { return n.type });
    if (group[name]) {
      let count = group[name].length;
      // curcurrent increase/decrase step
      if (typeof concurrency == 'string') {
        if (concurrency[0] == '+' || concurrency[0] == '-') {
          concurrency = count + parseInt(concurrency);
        } else
          concurrency = parseInt(concurrency);
      }
      if (count < concurrency) {
        debug('Increase concurrent worker `' + name + '` ' + count + '->' + concurrency);
        for (let i = count; i < concurrency; i++) {
          debug('New worker ' + name + '(' + (i + 1) + ')');
          this.q.process(name, this.jobFn(name));
        }
      } else if (count == concurrency) {
        debug('Does not adjust concurrent worker `' + name + '` = ' + count);
      } else {
        debug('Decrease concurrent worker `' + name + '` ' + count + '->' + concurrency);
        for (let i = count - 1; i >= concurrency; i--) {
          var work = group[name][i];
          for (let j = 0; j < this.q.workers.length; j++) {
            if (this.q.workers[j].id === work.id) {
              this.q.workers.splice(j, 1);
              work.shutdown(() => {
                debug('Drop worker ' + name + '(' + (i + 1) + ')');
              });
              break;
            }
          }
        }
      }
      // save worker num to redis
      debug("Save concurrent:" + name + " to redis = " + concurrency);
      this.broker.redis.set("concurrent:" + name, concurrency);
      return true;
      // not found worker
    } else {
      if (this.broker.router.routing[name + ':worker']) {
        concurrency = parseInt(concurrency);
        if (concurrency <= 0) { return false; }
        debug('New worker ' + name + ' for ' + concurrency);
        this.q.process(name, concurrency, this.jobFn(name));
        return true;
      }
      return false;
    }
  }

  removeNamespace(ns) {
    debug('Removed all `' + ns + '` workers');
    this.setConcurrent(ns, 0);
    _.remove(this.job_names, (item) => { return item.indexOf(ns) == 0 });
  }

  workerCount(name) {
    let group = _.groupBy(this.q.workers, (n) => { return n.type });
    let result = {};
    for (let type in group) {
      result[type] = group[type].length;
    }
    return result[name] || result;
  }

  jobCount(callback) {
    let groupByTypes = {};
    Kue.singleton.types((err, types) => {
      /* istanbul ignore next */
      function getCountByType(type, done) {
        async.parallel({
          inactive: (cb) => { Kue.singleton.card(type + ':inactive', cb) },
          active: (cb) => { Kue.singleton.card(type + ':active', cb) },
          complete: (cb) => { Kue.singleton.card(type + ':complete', cb) },
          failed: (cb) => { Kue.singleton.card(type + ':failed', cb) },
          delayed: (cb) => { Kue.singleton.card(type + ':delayed', cb) },
        }, (err, results) => {
          groupByTypes[type] = results;
          done(err);
        });
      }
      async.each(types, getCountByType, (err) => {
        callback(err, groupByTypes);
      });
    });
  }

  onstopdone() {
    this.q.shutdown(() => {
      this.q.workers = []
      this.q = null;
      done && done();
    });
  }
}



module.exports = Queue;
