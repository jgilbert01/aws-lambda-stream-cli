const { now, head, print, count } = require('../../lib/common');

exports.command = 'count [bucket] [prefix]'
exports.describe = 'Count the events in [bucket] for [prefix]'

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
    parallel: {
        default: 16
    },
}

exports.handler = (argv) => {
    print(argv);
    head(argv)

        .reduce({}, count)
        .tap(() => console.log('Counters:'))
        .tap(print)

        .errors(console.log)
        .done(() => { });
}