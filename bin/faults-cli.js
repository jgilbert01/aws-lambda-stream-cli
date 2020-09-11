#!/usr/bin/env node

const findUp = require('find-up')
const fs = require('fs')

const configPath = findUp.sync(['.faultsrc', '.faultsrc.json'])
const config = configPath ? JSON.parse(fs.readFileSync(configPath)) : {}

require('yargs')
.commandDir('faults-cmds')
  .demandCommand()
  .help()
  .config(config)
  .argv