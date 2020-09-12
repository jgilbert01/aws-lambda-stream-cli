exports.command = 'display [bucket] [prefix]'
exports.describe = 'Display the faults in [bucket] for [prefix]'

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
    digest: {
        default: false,
        type: 'boolean',
        describe: 'output is more concise'
    },
    parallel: {
        default: 16
    },
    region: {
        alias: 'r',
        default: process.env.AWS_REGION || 'us-east-1'
    },
    source: {
        alias: 'sp',
        description: 'source account profile',
        default: process.env.AWS_PROFILE || 'default'
    }
}

exports.handler = (argv) => {
    const common = require('../../lib/common')(argv);

    common.print(argv);

    common.s3.paginate()
        .map(obj => common.s3.get(obj))
        .parallel(argv.parallel)

        .map((uow) => ({
            digest: {
                id: uow.obj.id,
                functionname: uow.obj.tags.functionname,
                pipeline: uow.obj.tags.pipeline,
                type: (uow.obj.uow.event && uow.obj.uow.event.type) || uow.obj.uow.batch[0].event.type,
                err: {
                    name: uow.obj.err.name,
                    message: uow.obj.err.message,
                }
            },
            ...uow,
        }))

        .tap(uow => argv.digest ? common.print(uow.digest) : common.print(uow))

        .reduce({}, (counters, cur) => {
            counters.total = (counters.total ? counters.total : 0) + 1;
            counters[cur.digest.type] = (counters[cur.digest.type] ? counters[cur.digest.type] : 0) + 1;
            counters[cur.digest.functionname] = (counters[cur.digest.functionname] ? counters[cur.digest.functionname] : 0) + 1;
            counters[`${cur.digest.functionname}|${cur.digest.pipeline}`] = (counters[`${cur.digest.functionname}|${cur.digest.pipeline}`] ? counters[`${cur.digest.functionname}|${cur.digest.pipeline}`] : 0) + 1;
            return counters;
        })
        .tap(counters => console.log("Counters: %s", JSON.stringify(counters, null, 2)))

        .errors(common.print)
        .done(() => { })
        ;
}