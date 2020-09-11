exports.command = 'resubmit [bucket] [prefix]'
exports.describe = 'Resubmit the faults in [bucket] for [prefix]'

const _ = require('highland');
const now = require('moment')().utc();

exports.builder = {
    bucket: {
        alias: 'b',
        describe: 'bucket containing the faults'
    },
    stream: {
        alias: 's',
        describe: 'stream that delivered the faults - root prefix'
    },
    prefix: {
        alias: 'p',
        default: `${now.format('YYYY')}/${now.format('MM')}/${now.format('DD')}/`
    },
    function: {
        alias: 'f',
        describe: 'function to resubmit the events to'
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
}

exports.handler = (argv) => {
    const common = require('../../lib/common')(argv);

    common.print(argv);

    common.s3.paginate()
        .flatMap(obj => common.s3.get(obj))
        .map(obj => {
            const fault = obj.obj;

            const newKinesisEvent = {
                key: obj.key,
                functionName: fault.tags.functionName,
                records: []
            };

            const uows = Array.isArray(fault.uow) ? fault.uow : [fault.uow];

            uows.forEach(uow => {
                if (uow.batch) {
                    newKinesisEvent.records = newKinesisEvent.records.concat(fault.uow.batch.map(b => b.record));
                }

                if (uow.record) {
                    newKinesisEvent.records.push(fault.uow.record);
                }
            });

            if (newKinesisEvent.records.length === 0) {
                const e = new Error('No records in: ' + obj.key);
                e.fault = fault;
                throw e;
            }

            return newKinesisEvent;
        })
        .tap(common.print)

        .ratelimit(argv.rate, argv.window)

        .map(batch => common.lambda.invoke(batch))
        .parallel(argv.parallel)
        .tap(common.print)

        .collect()
        .tap(d => console.log('Count: %s', d.length))

        .errors(common.print)
        .done(() => { })
        ;
}
