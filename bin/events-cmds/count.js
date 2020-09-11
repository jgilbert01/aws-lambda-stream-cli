exports.command = 'count [bucket] [prefix]'
exports.describe = 'Count the events in [bucket] for [prefix]'

const _ = require('highland');
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
    }
}

exports.handler = (argv) => {
    const common = require('../../lib/common')(argv);

    common.print(argv);

    const get = (obj) => {
        return common.s3.get(obj)
            .flatMap((data) => {
                const key = data.key;
                const records = data.obj.Records;

                return _((push, next) => {

                    let record = records.shift();

                    if (record) {
                        try {
                            let event = new Buffer(record.kinesis.data, 'base64').toString('utf8');
                            event = JSON.parse(event);

                            push(null, {
                                key: key,
                                record: record,
                                event: event
                            });
                        } catch (err) {
                            console.log(err);
                            push(err);
                        }

                        next();
                    } else {
                        push(null, _.nil);
                    }
                })
            })
            ;
    }

    common.s3.paginate()
        .map(obj => get(obj))
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