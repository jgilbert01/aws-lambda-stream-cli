const { invokeLambda } = require('aws-lambda-stream');

const { now, head, print, debug } = require('../../lib/common');

exports.command = 'resubmit [bucket] [prefix]'
exports.describe = 'Resubmit the faults in [bucket] for [prefix]'

exports.builder = {
    bucket: {
        alias: 'b',
        describe: 'bucket containing the faults'
    },
    region: {
        alias: 'r',
        default: process.env.AWS_REGION || 'us-east-1',
        describe: 'root prefix'
    },
    prefix: {
        alias: 'p',
        default: `${now.format('YYYY')}/${now.format('MM')}/${now.format('DD')}/`,
        describe: 'folder of faults to retrieve'
    },
    dry: {
        default: true,
        type: 'boolean'
    },
    async: {
        default: false,
        type: 'boolean'
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

exports.handler = (argv) => {
    print(argv);
    head(argv)

        .filter(
            uow => (uow.event && uow.event.uow &&
                uow.event.uow.record && uow.event.uow.record.kinesis) ||
                (uow.event && uow.event.uow &&
                    uow.event.uow.batch && uow.event.uow.batch[0] &&
                    uow.event.uow.batch[0].record.kinesis)
        )

        .map((uow => {
            let payload = {
                Records: uow.event.uow.batch ?
                    uow.event.uow.batch.map(b => b.record) :
                    [uow.event.uow.record]
            };

            payload = Buffer.from(JSON.stringify(payload));

            return {
                ...uow,
                recordCount: uow.event.uow.batch ? uow.event.uow.batch.length : 1,
                invokeRequest: {
                    FunctionName: uow.event.tags.functionname,
                    Qualifier: argv.qualifier,
                    InvocationType:
                        argv.dry ? 'DryRun' :
                            argv.async && payload.length <= 100000 ?
                                'Event' :
                                'RequestResponse',
                    Payload: payload,
                },
            };
        }))

        .ratelimit(argv.rate, argv.window)
        .through(invokeLambda({ parallel: argv.parallel }))
        .tap(uow => print({
            functionName: uow.invokeRequest.FunctionName,
            invokeResponse: uow.invokeResponse,
        }))
        .tap(debug)

        .reduce({}, (counters, uow) => {
            const status = uow.invokeResponse.StatusCode;
            const functionname = uow.event.tags.functionname;
            const pipeline = `${functionname}|${uow.event.tags.pipeline}`;

            counters.total = (counters.total ? counters.total : 0) + 1;
            counters.recordCount = (counters.recordCount ? counters.recordCount : 0) + uow.recordCount;

            if (!counters.statuses) counters.statuses = {};
            const statuses = counters.statuses;
            statuses[status] = (statuses[status] ? statuses[status] : 0) + 1;

            if (!counters.functions) counters.functions = {};
            const functions = counters.functions;
            functions[pipeline] = (functions[pipeline] ? functions[pipeline] : 0) + 1;

            return counters;
        })
        .tap(() => console.log('Counters:'))
        .tap(print)

        .errors(console.log)
        .done(() => { });
}
