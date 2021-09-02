const { now, head, print, debug, digest, count, filterByType } = require('../../lib/common');

exports.command = 'display [bucket] [prefix]'
exports.describe = 'Display the events in [bucket] for [prefix]'

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
        describe: 'folder of events to retrieve'
    },
    type: {
        alias: 't',
        default: '*'
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

        .filter(filterByType(argv))
        .map(digest)
        .tap(debug)
        .tap(uow => argv.digest ? print(uow.digest) : print(uow.event))

        .reduce({}, count)
        .tap(() => console.log('Counters:'))
        .tap(print)

        .errors(console.log)
        .done(() => { });
}