const aws = require('aws-sdk');

const _ = require('highland');
const BbPromise = require('bluebird');
const zlib = require('zlib');
const lodash = require('lodash');

const debug = require('debug')('cli');

const { pageObjectsFromS3, getObjectFromS3, split } = require('aws-lambda-stream');

aws.config.setPromisesDependency(BbPromise);

module.exports.now = require('moment')().utc();
module.exports.debug = data => debug(JSON.stringify(data, null, 2));
module.exports.print = data => console.log(JSON.stringify(data, null, 2));

module.exports.digest = (uow) => ({
    digest: {
        id: uow.event.id,
        type: (uow.event.uow && uow.event.uow.event && uow.event.uow.event.type) || (uow.event.uow && uow.event.uow.batch[0].event.type) || uow.event.type,
        functionname: uow.event.tags.functionname,
        pipeline: uow.event.tags.pipeline,
        err: uow.event.err ? {
            name: uow.event.err.name,
            message: uow.event.err.message,
        } : undefined,
    },
    ...uow,
});

module.exports.count = (counters, uow) => {
    const type = uow.event.type;
    const functionname = uow.event.tags.functionname;
    const pipeline = `${functionname}|${uow.event.tags.pipeline}`;

    counters.total = (counters.total ? counters.total : 0) + 1;
    
    if (!counters.types) counters.types = {};
    const types = counters.types;
    types[type] = (types[type] ? types[type] : 0) + 1;

    if (!counters.functions) counters.functions = {};
    const functions = counters.functions;
    functions[pipeline] = (functions[pipeline] ? functions[pipeline] : 0) + 1;

    return counters;
};

module.exports.list = (argv) => {
    const uows = [{
        listRequest: {
            Bucket: argv.bucket,
            Prefix: argv.region ?
                argv.region + '/' + argv.prefix :
                argv.prefix,
        },
    }];

    return _(uows)
        .through(pageObjectsFromS3({ parallel: argv.parallel }))
        .tap(debug);
};

module.exports.head = (argv) => {
    const uows = [{
        listRequest: {
            Bucket: argv.bucket,
            // Delimiter: '/',
            Prefix: argv.region ?
                argv.region + '/' + argv.prefix :
                argv.prefix,
        },
    }];

    return _(uows)
        .through(pageObjectsFromS3({ parallel: argv.parallel }))
        .map((uow) => ({
            ...uow,
            getRequest: {
                Bucket: argv.bucket,
                Key: uow.listResponse.Content.Key,
            },
        }))
        .through(getObjectFromS3({ parallel: argv.parallel }))
        .flatMap(split())
        .map((uow) => {
            const { detail, ...eb } = JSON.parse(uow.getResponse.line);
            return ({
                ...uow,
                record: {
                    ...uow.record,
                    eb,
                },
                event: detail,
            });
        })
        .tap(debug);
};


// function Lambda(opts) {
//     this.options = opts;

//     this.lambda = new aws.Lambda({
//         httpOptions: {
//             timeout: 3000
//         },
//         logger: process.stdout,
//         credentials: new aws.SharedIniFileCredentials({ profile: this.options.target }),
//         region: this.options.region
//     });

//     this.count = 0;
// }

// Lambda.prototype.invoke = function (batch) {
//     debug('invoke: %s', batch.records.length);

//     const self = this;

//     const payload = new Buffer(JSON.stringify({
//         Records: batch.records
//         // Records: batch.map(row => {
//         //     return {
//         //         kinesis: {
//         //             sequenceNumber: row.record.kinesis.sequenceNumber,
//         //             data: new Buffer(JSON.stringify(row.event)).toString('base64')
//         //         }
//         //     };
//         // })
//     }));

//     const params = {
//         FunctionName: batch.functionName || this.options.function,
//         // InvocationType: 'DryRun',
//         InvocationType: this.options.dry ? 'DryRun' : payload.length <= 100000 ? 'Event' : 'RequestResponse',
//         Payload: payload,
//         Qualifier: this.options.qualifier
//     };

//     debug('params: %j', params); // JSON.stringify(params, null, 2));

//     return _(
//         self.lambda.invoke(params).promise()
//             .then(resp => {
//                 this.count++;
//                 // debug('response: %s', JSON.stringify(resp, null, 2));
//                 resp.batch = batch.records.length;
//                 return resp;
//             })
//         // .catch(err => {
//         // 	console.error(err);
//         // 	return err;
//         // })
//     );
// }