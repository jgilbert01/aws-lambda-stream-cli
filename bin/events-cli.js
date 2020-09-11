#!/usr/bin/env node

const findUp = require('find-up')
const fs = require('fs')

const configPath = findUp.sync(['.eventsrc', '.eventsrc.json'])
const config = configPath ? JSON.parse(fs.readFileSync(configPath)) : {}

require('yargs')
  .commandDir('events-cmds')
  .demandCommand()
  .config(config)
  .help()
  .argv