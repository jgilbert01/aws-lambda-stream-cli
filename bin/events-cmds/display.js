const { now, head, print, debug, digest, count } = require('../../lib/common');

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
    parallel: {
        default: 16
    },
}

exports.handler = (argv) => {
    print(argv);

    const filterByType = uow => argv.type === '*' || argv.type === uow.event.type;

    head(argv)

        .filter(filterByType)
        .map(digest)
        .tap(debug)
        .tap(uow => argv.digest ? print(uow.digest) : print(uow.event))

        .reduce({}, count)
        .tap(() => console.log('Counters:'))
        .tap(print)

        .errors(console.log)
        .done(() => { });
}