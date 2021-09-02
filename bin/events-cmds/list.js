const { now, list, print } = require('../../lib/common');

exports.command = 'ls [bucket] [prefix]'
exports.describe = 'List the event files in [bucket] for [prefix]'

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
        describe: 'folder of objects to retrieve'
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
    list(argv)

        .tap(uow => print(uow.listResponse.Content))

        .collect()
        .tap(collected => console.log('Count: %s', collected.length))

        .errors(console.log)
        .done(() => { });
}
