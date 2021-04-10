const { invokeLambda } = require('aws-lambda-stream');

const { now, head, print, debug, digest, count } = require('../../lib/common');

exports.command = 'resubmit [bucket] [prefix]'
exports.describe = 'Resubmit the faults in [bucket] for [prefix]'

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
    // // function: {
    // //     alias: 'f',
    // //     describe: 'function to resubmit the events to'
    // // },
    // qualifier: {
    //     default: '$LATEST'
    // },
    dry: {
        default: true,
        type: 'boolean'
    },
    batch: {
        default: 25
    },
    parallel: {
        default: 16
    },
    rate: {
        default: 2
    },
    window: {
        default: 500
    },
    // target: {
    //     alias: 'tp',
    //     description: 'target account profile',
    //     default: process.env.AWS_PROFILE || 'default'
    // },
}

exports.handler = (argv) => {
    print(argv);
    head(argv)

        // .map(obj => {
        //     const fault = obj.obj;

        //     const newKinesisEvent = {
        //         key: obj.key,
        //         functionName: fault.tags.functionName,

        //         // TODO pipeline ???

        //         records: []
        //     };

        //     const uows = Array.isArray(fault.uow) ? fault.uow : [fault.uow];

        //     uows.forEach(uow => {
        //         if (uow.batch) {
        //             newKinesisEvent.records = newKinesisEvent.records.concat(fault.uow.batch.map(b => b.record));
        //         }

        //         if (uow.record) {
        //             newKinesisEvent.records.push(fault.uow.record);
        //         }
        //     });

        //     if (newKinesisEvent.records.length === 0) {
        //         const e = new Error('No records in: ' + obj.key);
        //         e.fault = fault;
        //         throw e;
        //     }

        //     return newKinesisEvent;
        // })
        // .tap(debug)

        // .map((uow => {

        //     return {
        //         ...uow,
        //         invokeRequest: {
        //             FunctionName: 'helloworld',
        //         },
        //     };
        // }))

        // .ratelimit(argv.rate, argv.window)


        // TODO params
        // .through(invokeLambda({ parallel: argv.parallel }))

        // .map(batch => common.lambda.invoke(batch))
        // .parallel(argv.parallel)
        .tap(debug)

        .collect()
        .tap(collected => console.log('Count: %s', collected.length))

        .errors(console.log)
        .done(() => { });
}
