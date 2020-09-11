exports.command = 'ls [bucket] [prefix]'
exports.describe = 'List the fault files in [bucket] for [prefix]'

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
        .tap(common.print)

        .collect()
        .tap(d => console.log('Count: %s', d.length))

        .errors(common.print)
        .done(() => {})
        ;
}