const { now, head, print } = require('../../lib/common');

exports.command = 'count [bucket] [prefix]'
exports.describe = 'Count the faults in [bucket] for [prefix]'

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
    parallel: {
        default: 16
    },
}

exports.handler = (argv) => {
    print(argv);
    head(argv)

        .collect()
        .tap(collected => console.log('Count: %s', collected.length))

        .errors(console.log)
        .done(() => { });
}