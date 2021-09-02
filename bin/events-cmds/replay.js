const { invokeLambda, toKinesisRecords, toBatchUow } = require('aws-lambda-stream');
const { now, head, print, debug, preBatchCount, count, filterByType, errors, batchWithSize } = require('../../lib/common');

exports.command = 'replay [bucket] [prefix]'
exports.describe = 'Replay the events in [bucket] for [prefix]'

exports.builder = {
    bucket: {
        alias: 'b',
        describe: 'bucket containing the events'
    },
    region: {
        alias: 'r',
        default: process.env.AWS_REGION || 'us-east-1',
        describe: 'root prefix'
    },
    prefix: {
        alias: 'p',
        default: `${now.format('YYYY')}/${now.format('MM')}/${now.format('DD')}/`,
        describe: 'folder of events to replay'
    },
    type: {
        alias: 't',
        default: '*'
    },
    functionname: {
        alias: 'f',
        describe: 'function to replay the events to'
    },
    qualifier: {
        default: '$LATEST'
    },
    dry: {
        alias: 'd',
        default: true,
        type: 'boolean'
    },
    continuationToken: {
        alias: 'c',
        describe: 'pick up where we left off'
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
}

const counters = {};
const MAX = 100000;

exports.handler = (argv) => {
    print(argv);

    head(argv)
        .filter(filterByType(argv))

        .tap(preBatchCount(counters))

        .consume(batchWithSize(MAX))
        .map(toBatchUow)
        // .tap(print)

        .map((uow => {
            const payload = Buffer.from(JSON.stringify(toKinesisRecords(
                uow.batch ?
                    uow.batch.map(b => b.event) :
                    [uow.event]
            )));

            return {
                ...uow,
                recordCount: uow.batch ? uow.batch.length : 1,
                invokeRequest: {
                    FunctionName: argv.functionname,
                    Qualifier: argv.qualifier,
                    InvocationType:
                        argv.dry ? 'DryRun' :
                            argv.async && payload.length <= MAX ?
                                'Event' :
                                'RequestResponse',
                    Payload: payload,
                },
            };
        }))

        .ratelimit(argv.rate, argv.window)
        .through(invokeLambda({ parallel: argv.parallel }))
        .tap(debug)

        .errors(errors)

        .reduce(counters, count)
        .tap(() => console.log('Counters:'))
        .tap(print)

        .errors(console.log)
        .done(() => { });
}
