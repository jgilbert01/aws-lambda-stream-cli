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
