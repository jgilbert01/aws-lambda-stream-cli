exports.command = 'replay [bucket] [prefix]'
exports.describe = 'Replay the events in [bucket] for [prefix]'

const _ = require('highland');
const lodash = require('lodash');
const now = require('moment')().utc();

exports.builder = {
    bucket: {
        alias: 'b',
        describe: 'bucket containing the events'
    },
    stream: {
        alias: 's',
        describe: 'stream that delivered the events - root prefix'
    },
    prefix: {
        alias: 'p',
        default: `${now.format('YYYY')}/${now.format('MM')}/${now.format('DD')}/`
    },
    type: {
        alias: 't',
        default: '*'
    },
    function: {
        alias: 'f',
        describe: 'function to replay the events to'
    },
    qualifier: {
        default: '$LATEST'
    },
    dry: {
        default: true,
        type: 'boolean'
    },
    batch: {
        default: 25
    },
    parallel: {
        default: 16
    },
    rate: {
        default: 2
    },
    window: {
        default: 500
    },
    region: {
        alias: 'r',
        default: process.env.AWS_REGION || 'us-east-1'
    },
    source: {
        alias: 'sp',
        description: 'source account profile',
        default: process.env.AWS_PROFILE || 'default'
    },
    target: {
        alias: 'tp',
        description: 'target account profile',
        default: process.env.AWS_PROFILE || 'default'
    },
    // TODO target format
    format: {
        alias: 'f',
        choices: [
            'kinesis', // events wrapped in kinesis records 
            'eventbridge', // event wrapped in cloudwatch event format, delimited by EOL
            'raw' // raw event format, delimited by EOL
        ],
        default: 'kinesis' // original
    },
}

exports.handler = (argv) => {
    const common = require('../../lib/common')(argv);

    common.print(argv);

    const filterByType = obj => argv.type === '*' || argv.type === obj.event.type;

    let batched = [];

    const batchWithSize = function(err, x, push, next) {
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

            // TODO format x as a Record (kinesis or dynamo) or cw event
 
            let buf = Buffer.from(JSON.stringify({
                Records: batched.concat(x)
            }));

            if (buf.length <= 100000) {
                batched.push(x);
            } else {
                push(null, batched);
                batched = [x];
            }

            next();
        }
    }

    const replay = function(s) {
        return s
            .map(obj => common.s3.getEvents(obj))
            .parallel(argv.parallel)

            .filter(filterByType)

            .map(obj => {
                typeCount.total = (typeCount.total ? typeCount.total : 0) + 1;
                typeCount[obj.event.type] = (typeCount[obj.event.type] ? typeCount[obj.event.type] : 0) + 1;

                return obj.record;
            })

            .consume(batchWithSize)
            .tap(batch => console.log('Batch Size: ', batch.length))

            .map(batch => {
                return {
                    records: batch
                };
            })
            // .tap(common.print)

            .ratelimit(argv.rate, argv.window)

            .map(batch => common.lambda.invoke(batch))
            .parallel(argv.parallel)
            // .tap(common.print)

            .reduce(0, (counter, resp) => {
                console.log('Running Event Count: ', counter + resp.batch);
                console.log('Running Error Count: ', errs.length);
                console.log('Running Invoke Count: ', common.lambda.count);
                return counter + resp.batch;
            })
            .tap(counter => eventCount += counter)
            ;
    }        

    const errors = function(err, push) {
        console.error(err);
        console.error(err.stack);

        if (err.obj) {
            errs.push(err);
        } else {
            push(err);
        }
    }

    let typeCount = {};
    let eventCount = 0;
    let errs = [];

    const retryErrors = function(s) {
        const errorStream = () => {
            if (errs.length) console.log('Retrying: %s Error(s)', errs.length);
            let errors = errs;
            errs = [];
            batched = [];

            return _((push, next) => {
                let err = errors.shift();
                if (err) {
                    console.log('Retrying: %j', err);
                    push(null, err.obj);
                    next();
                } else {
                    push(null, _.nil);
                }
            })
                .through(replay)
                .errors(err => {
                    errs.push(err);
                    console.error(err);
                    console.error(err.stack);
                })
                ;
        };

        return s
            .consume(function(err, x, push, next) {
                if (err) {
                    push(err);
                    next();
                }
                else if (x === _.nil) {
                    next(errorStream());
                }
                else {
                    push(null, x);
                    next();
                }
            })
            ;
    }

    common.s3.paginate()
        .through(replay)
        .errors(errors)
        .through(retryErrors)
        .done(() => {
            console.log('Event Count: %s', eventCount);
            console.log('Error Count: %s', errs.length);
            console.log('Invoke Count: %s', common.lambda.count);
            console.log('Types: %s', JSON.stringify(typeCount, null, 2))
        })
        ;        
}
