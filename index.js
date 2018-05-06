const {unlink, createWriteStream, createReadStream} = require('fs')
const {tmpdir} = require('os')
const {join} = require('path')
const {PassThrough, Duplex} = require('stream')
const createRawShpWriteStream = require('shp-write-stream')
const {ZipFile} = require('yazl')
const detectSchema = require('./lib/detect-schema')

function convert(options = {}) {
  const tmpDir = options.tmpDir || process.env.TMP_DIR || tmpdir()
  const id = randomizer()
  const tmpFilesPrefix = join(tmpDir, id)
  const context = {}

  const zipFile = new ZipFile()
  const zipStream = zipFile.outputStream

  let _schema = options.schema
  let _internalShpWriteStream

  function getInternalShpWriteStream() {
    if (!_internalShpWriteStream) {
      _internalShpWriteStream = createInternalShpWriteStream()
    }
    return _internalShpWriteStream
  }

  function createInternalShpWriteStream() {
    return createRawShpWriteStream(
      // Schema
      {
        point: _schema,
        multipoint: _schema,
        line: _schema,
        polygon: _schema
      },
      // MakeStream
      (shpType, extension) => {
        const tmpFilePath = `${tmpFilesPrefix}-${shpType}.${extension}`
        context[`${shpType}-${extension}`] = {shpType, extension, tmpFilePath}
        return createWriteStream(tmpFilePath)
      },
      // End of processing
      (err, headers) => {
        if (err) {
          console.error('Conversion failed')
          console.error(err)
          cleanTmpFiles(context)
        } else {
          addHeadersToContext(headers, context)

          // Add files to archive
          Object.values(context).forEach(({shpType, extension, tmpFilePath, header}) => {
            const stream = new PassThrough()
            zipFile.addReadStream(stream, `${shpType}.${extension}`)
            stream.write(header)
            const content = createReadStream(tmpFilePath)
            content.pipe(stream)
            content.on('close', () => cleanTmpFile(tmpFilePath)) // TODO Improve error management
          })

          zipFile.end()
        }
      }
    )
  }

  const duplex = new Duplex({
    writableObjectMode: true,
    write(feature, enc, cb) {
      if (!_schema) {
        _schema = detectSchema(feature.properties)
      }
      getInternalShpWriteStream().write(feature, cb)
    },
    final(cb) {
      getInternalShpWriteStream().end()
      cb()
    },
    read() {
      return zipStream.read()
    }
  })

  zipStream.on('readable', () => {
    let hasMoreData = true
    while (hasMoreData) {
      const chunk = zipStream.read()
      if (chunk) {
        duplex.push(chunk)
      } else {
        hasMoreData = false
      }
    }
  })

  return duplex
}

function randomizer() {
  return Math.random().toString(36).substring(2, 15)
}

function addHeadersToContext(headers, context) {
  Object.keys(headers).forEach(shpType => {
    Object.keys(headers[shpType]).forEach(extension => {
      context[`${shpType}-${extension}`].header = headers[shpType][extension]
    })
  })
}

function cleanTmpFile(path) {
  unlink(path, err => {
    if (err) {
      console.error(`Unable to delete temporary file: ${path}`)
      console.error(err)
    }
  })
}

function cleanTmpFiles(context) {
  Object.values(context).forEach(({tmpFilePath}) => cleanTmpFile(tmpFilePath))
}

module.exports = {convert}