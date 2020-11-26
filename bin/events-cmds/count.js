exports.command = 'count [bucket] [prefix]'
exports.describe = 'Count the events in [bucket] for [prefix]'

const _ = require('highland');
const lodash = require('lodash');
const now = require('moment')().utc();

exports.builder = {
    bucket: {
        alias: 'b',
        describe: 'bucket containing the events'
    },
    stream: {
        alias: 's',
        describe: 'stream that delivered the events - root prefix'
    },
    prefix: {
        alias: 'p',
        default: `${now.format('YYYY')}/${now.format('MM')}/${now.format('DD')}/`
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
    },
    format: {
        alias: 'f',
        choices: [
            'kinesis', // events wrapped in kinesis records 
            'eventbridge', // event wrapped in cloudwatch event format, delimited by EOL
            'raw' // raw event format, delimited by EOL
        ],
        default: 'kinesis' // original
    }
}

exports.handler = (argv) => {
    const common = require('../../lib/common')(argv);

    common.print(argv);

    common.s3.paginate()
        .map(obj => common.s3.getEvents(obj))
        .parallel(argv.parallel)

        .reduce({}, (counters, cur) => {
            counters.total = (counters.total ? counters.total : 0) + 1;
            counters[cur.event.type] = (counters[cur.event.type] ? counters[cur.event.type] : 0) + 1;
            return counters;
        })
        .tap(counters => console.log("Counters: %s", JSON.stringify(counters, null, 2)))

        .errors(common.print)
        .done(() => {})
        ;
}