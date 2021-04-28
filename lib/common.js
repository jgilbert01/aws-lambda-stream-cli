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

module.exports.filterByType = argv => uow => argv.type === '*' || argv.type === uow.event.type;

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
    if (uow.event && uow.event.type) {
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
    }

    if (uow.recordCount) {
        counters.recordCount = (counters.recordCount ? counters.recordCount : 0) + uow.recordCount;
    }

    if (uow.invokeRequest) {
        if (!counters.invoked) counters.invoked = { total: 0, statuses: {} };
        counters.invoked.total = counters.invoked.total + 1;

        if (uow.invokeResponse) {
            const status = uow.invokeResponse.StatusCode;
            const statuses = counters.invoked.statuses;
            statuses[status] = (statuses[status] ? statuses[status] : 0) + 1;    
        }
    }

    if (uow.err) {
        counters.errors = (counters.errors ? counters.errors : 0) + 1;
        if (!counters.errored) counters.errored = [];
        counters.errored.push(uow);
    }

    return counters;
};

module.exports.preBatchCount = (counters) => (uow) => module.exports.count(counters, uow);

module.exports.list = (argv) => {
    aws.config.region = argv.region;

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
    aws.config.region = argv.region;

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

module.exports.batchWithSize = (max = 100000) => {
    let batched = [];

    return (err, x, push, next) => {
        if (err) {
            push(err);
            next();
        }
        else if (x === nil) {
            if (batched.length > 0) {
                push(null, batched);
            }

            push(null, nil);
        }
        else {
            const buf = Buffer.from(JSON.stringify({
                Records: batched.concat(x)
            }));

            if (buf.length <= max) {
                batched.push(x);
            } else {
                push(null, batched);
                batched = [x];
            }

            next();
        }
    };
};

module.exports.errors = (error, push) => {
    if (error.uow) {
        // catch so we can count and log at the end
        const { uow, ...err } = error;
        push(null, { ...uow, err });
    } else {
        push(err);
    }
}
