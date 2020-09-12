# aws-lambda-stream-cli
Command line utilities for viewing events in your event lake, replaying events, resubmitting faults and more

These utilities support sister project [aws-lambda-stream](https://github.com/jgilbert01/aws-lambda-stream).

## Installation

* `npm i aws-lambda-stream-cli -g`

## Commands
Invoke help commands below for details on the commands and parameters, such as:

```
$ events help
Commands:
  display [bucket] [prefix]  Display the events in [bucket] for [prefix]
  ls [bucket] [prefix]       List the event files in [bucket] for [prefix]
  replay [bucket] [prefix]   Replay the events in [bucket] for [prefix]

$ events ls help
events ls [bucket] [prefix]

Options:
  --help          Show help                                            [boolean]
  --bucket, -b    bucket containing the events
  --stream, -s    stream that delivered the events - root prefix
  --source, --sp  source account profile                    [default: "default"]
  --prefix, -p                                          [default: "2018/12/11/"]
  --region, -r                                            [default: "us-east-1"]
```

* events help
* events ls help
* events count help
* events display help
* events replay help
* faults help
* faults ls help
* faults count help
* faults display help
* faults resubmit help

## Configuration Files
Default parameter configurations can be setup in .eventsrc and .faultsrc json files.

* Example .eventsrc file:
```
{
    "bucket": "my-event-lake-s3-stg-bucket-tjv122u7812s",
    "stream": "stg-my-event-streams-s1"
}
```

### Additional configuration details:
* https://github.com/yargs/yargs/blob/8789bf4f56940248316c58997b107b029dbdb297/docs/advanced.md#rc-files
* https://github.com/yargs/yargs/blob/8789bf4f56940248316c58997b107b029dbdb297/docs/api.md#configobject

