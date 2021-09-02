const { now, head, print, debug, digest, count } = require('../../lib/common');

exports.command = 'display [bucket] [prefix]'
exports.describe = 'Display the faults in [bucket] for [prefix]'

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
    digest: {
        alias: 'd',
        default: false,
        type: 'boolean',
        describe: 'output is more concise'
    },
    continuationToken: {
        alias: 'c',
        describe: 'pick up where we left off'
    },
    parallel: {
        default: 16
    },
}

exports.handler = (argv) => {
    print(argv);

    head(argv)

        .map(digest)
        .tap(debug)

        .tap(uow => argv.digest ? print(uow.digest) : print(uow.event))

        .reduce({}, count)
        .tap(() => console.log('Counters:'))
        .tap(print)

        .errors(console.log)
        .done(() => { });
}