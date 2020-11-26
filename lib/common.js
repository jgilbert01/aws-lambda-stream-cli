const aws = require('aws-sdk');

const _ = require('highland');
const BbPromise = require('bluebird');
const zlib = require('zlib');
const lodash = require('lodash');

const debug = require('debug')('common');

aws.config.setPromisesDependency(BbPromise);

function Common(opts) {
    this.options = opts;
    this.s3 = new S3(opts);
    this.lambda = new Lambda(opts);
}

module.exports = (opts) => {
    return new Common(opts);
}

Common.prototype.print = data => console.log(JSON.stringify(data, null, 2));
Common.prototype.isKinesis = () => this.options.format === 'kinsis';
Common.prototype.isEventBridge = () => this.options.format === 'eventbridge';
Common.prototype.isRaw = () => this.options.format === 'raw';

function S3(opts) {
    this.options = opts;

    this.s3 = new aws.S3({
        httpOptions: {
            timeout: 2000
        },
        logger: process.stdout,
        credentials: new aws.SharedIniFileCredentials({ profile: this.options.source }),
        region: this.options.region
    });

    this.gunzip = BbPromise.promisify(zlib.gunzip);
}

S3.prototype.paginate = function () {

    const self = this;

    let marker = undefined;

    return _((push, next) => {

        const params = {
            Bucket: self.options.bucket,
            // Delimiter: '/',
            Prefix: self.options.stream ?
                self.options.stream + '/' + self.options.prefix :
                self.options.prefix,
            Marker: marker
        };

        debug('params: %j', params);

        self.s3.listObjects(params).promise()
            .then(data => {
                debug('listObjects data: %j', lodash.omit(data, ['Contents']));
                if (data.IsTruncated) {
                    marker = lodash.last(data.Contents)['Key'];
                } else {
                    marker = undefined;
                }

                data.Contents.forEach(obj => {
                    push(null, obj);
                })
            })
            .catch(err => {
                push(err, null);
            })
            .finally(() => {
                if (marker) {
                    next();
                } else {
                    push(null, _.nil);
                }
            })
    });
}

S3.prototype.get = function (obj) {
    debug('obj: %j', obj);

    const self = this;

    const params = {
        Bucket: this.options.b,
        Key: obj.Key
    };

    return _(
        self.s3.getObject(params).promise()
            .then(data => {
                // if (obj.Key.endsWith('546.gz')) throw new Error('for testing');

                // debug('decompress file: %j', data);
                // let gunzip = _.wrapCallback(zlib.gunzip);
                if (self.isKinesis()) {
                    return this.gunzip(Buffer.from(data.Body));
                } else {
                    return Buffer.from(data.Body).toString();
                }
            })
            .then(data => {
                // debug('parse file: %j', data.toString());
                return {
                    key: obj.Key,
                    obj: self.isKinesis() ? JSON.parse(data) : data
                };
            })
            .catch(err => {
                err.obj = obj;
                return BbPromise.reject(err);
            })
    )
        ;
}

S3.prototype.getEvents = function (obj) {
    return this.s3.get(obj)
        .flatMap((data) => {
            const key = data.key;
            const records = this.isKinesis() ? data.obj.Records : lodash.castArray(data.obj.split('\n'));

            return _((push, next) => {

                let record = records.shift();

                if (record) {
                    try {
                        let event = this.isKinesis() ?
                            Buffer.from(record.kinesis.data, 'base64').toString('utf8') :
                            record;
                        event = JSON.parse(event);

                        push(null, {
                            key: key,
                            record: this.isKinesis() ? record : {},
                            event: event
                        });
                    } catch (err) {
                        console.log(err);
                        push(err);
                    }

                    next();
                } else {
                    push(null, _.nil);
                }
            })
        })
        ;
}

function Lambda(opts) {
    this.options = opts;

    this.lambda = new aws.Lambda({
        httpOptions: {
            timeout: 3000
        },
        logger: process.stdout,
        credentials: new aws.SharedIniFileCredentials({ profile: this.options.target }),
        region: this.options.region
    });

    this.count = 0;
}

Lambda.prototype.invoke = function (batch) {
    debug('invoke: %s', batch.records.length);

    const self = this;

    const payload = new Buffer(JSON.stringify({
        Records: batch.records
        // Records: batch.map(row => {
        //     return {
        //         kinesis: {
        //             sequenceNumber: row.record.kinesis.sequenceNumber,
        //             data: new Buffer(JSON.stringify(row.event)).toString('base64')
        //         }
        //     };
        // })
    }));

    const params = {
        FunctionName: batch.functionName || this.options.function,
        // InvocationType: 'DryRun',
        InvocationType: this.options.dry ? 'DryRun' : payload.length <= 100000 ? 'Event' : 'RequestResponse',
        Payload: payload,
        Qualifier: this.options.qualifier
    };

    debug('params: %j', params); // JSON.stringify(params, null, 2));

    return _(
        self.lambda.invoke(params).promise()
            .then(resp => {
                this.count++;
                // debug('response: %s', JSON.stringify(resp, null, 2));
                resp.batch = batch.records.length;
                return resp;
            })
        // .catch(err => {
        // 	console.error(err);
        // 	return err;
        // })
    );
}