const path = require('path')
const {unlink, createWriteStream, createReadStream} = require('fs')
const {tmpdir} = require('os')
const {PassThrough, Duplex, Readable} = require('stream')
const {ZipFile} = require('yazl')
const {reproject} = require('reproject')
const createRawShpWriteStream = require('./shp-write-stream')
const detectSchema = require('./detect-schema')

function readProjectionNumber(userParameter, defaultValue) {
  if (!userParameter) {
    return defaultValue
  }

  if (!Number.isInteger(userParameter)) {
    throw new TypeError('ProjectionNumber must be an integer')
  }

  return userParameter
}

function createConvertStream(options = {}) {
  const temporaryDir = options.tmpDir || process.env.TMP_DIR || tmpdir()
  const id = randomizer()
  const temporaryFilesPrefix = path.join(temporaryDir, id)
  const context = {}
  const layerName = options.layer || 'features'
  // fieldLengths contains field name - field length pairs
  // when not provided field lengths will be determined from the first feature provided
  const fieldLengths = options.fieldLengths || {}
  const padValues = options.padValues ?? true;

  let zipFile
  let zipStream
  if (!options.returnAsFileStreams) {
    zipFile = new ZipFile()
    zipStream = zipFile.outputStream           
  }


  let _reproject
  let _schema = options.schema
  let internalShpWriteStream

  let fileStreamStream = new PassThrough({
    objectMode: true
  })

  const sourceCrs = readProjectionNumber(options.sourceCrs, 4326)
  const targetCrs = readProjectionNumber(options.targetCrs, sourceCrs)

  if (sourceCrs !== targetCrs) {
    const from = require(`epsg-index/s/${sourceCrs}.json`).proj4
    const to = require(`epsg-index/s/${targetCrs}.json`).proj4
    _reproject = f => reproject(f, from, to)
  }

  const prjFileContent = Buffer.from(require(`epsg-index/s/${targetCrs}.json`).wkt)
  const opts = { padValues };

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
        const temporaryFilePath = `${temporaryFilesPrefix}-${shpType}.${extension}`
        context[`${shpType}-${extension}`] = {shpType, extension, tmpFilePath: temporaryFilePath}
        return createWriteStream(temporaryFilePath)
      },
      opts,
      // End of processing
      (error, headers) => {
        if (error) {
          console.error('Conversion failed')
          console.error(error)
          cleanTemporaryFiles(context)
        } else {
          addHeadersToContext(headers, context)
          const shpTypes = Object.keys(headers)
          const writeShpType = shpTypes.length > 1

          // Add files to archive
          Object.values(context).forEach(({shpType, extension, tmpFilePath, header}) => {
            if (extension === 'shp') {
              const prjFilename = getFileName(layerName, shpType, 'prj', writeShpType)
              if (!options.returnAsFileStreams) {
                zipFile.addBuffer(prjFileContent, prjFilename)
              } else {
                fileStreamStream.write({
                  stream: Readable.from(prjFileContent),
                  filename: prjFilename
                })
              }
            }
            const filename = getFileName(layerName, shpType, extension, writeShpType)
            const stream = new PassThrough()
            stream.write(header)
            const content = createReadStream(tmpFilePath)
            content.pipe(stream)
            if (!options.returnAsFileStreams) {
              zipFile.addReadStream(stream, filename)
            } else {
              fileStreamStream.write({
                stream,
                filename
              })
            }
            content.on('close', () => cleanTemporaryFile(tmpFilePath)) // TODO Improve error management
          })
          if (!options.returnAsFileStreams) {
            zipFile.end()
          } else {
            fileStreamStream.end()
          }
        }
      }
    )
  }

  const duplex = new Duplex({
    writableObjectMode: true,
    readableObjectMode: !!options.returnAsFileStreams,
    write(feature, enc, cb) {
      if (!_schema) {
        _schema = detectSchema(feature.properties, fieldLengths || {})
      }

      if (!internalShpWriteStream) {
        internalShpWriteStream = createInternalShpWriteStream()
      }
      internalShpWriteStream.write(_reproject ? _reproject(feature) : feature, cb)
    },
    final(cb) {
      if (internalShpWriteStream) {
        internalShpWriteStream.end(cb)
      }
      else {
        if (!options.returnAsFileStreams) {
          zipFile.end()
          cb()
        }
      }
    },
    read() {
      if (!options.returnAsFileStreams) {
        return zipStream.read()
      } else {
        return fileStreamStream.read()
      }
    }
  })

  let stream = options.returnAsFileStreams ? fileStreamStream : zipStream

  stream.on('readable', () => {
    let hasMoreData = true
    while (hasMoreData) {
      const chunk = stream.read()
      if (chunk) {
        duplex.push(chunk)
      } else {
        hasMoreData = false
      }
    }
  })

  stream.on('end', () => {
    duplex.push(null)
  })

  return duplex
}

function getFileName(layerName, shpType, extension, writeShpType = true) {
  if (writeShpType) {
    return `${layerName}.${shpType}.${extension}`
  }

  return `${layerName}.${extension}`
}

function randomizer() {
  return Math.random().toString(36).slice(2, 15)
}

function addHeadersToContext(headers, context) {
  for (const shpType of Object.keys(headers)) {
    for (const extension of Object.keys(headers[shpType])) {
      context[`${shpType}-${extension}`].header = headers[shpType][extension]
    }
  }
}

function cleanTemporaryFile(path) {
  unlink(path, error => {
    if (error) {
      console.error(`Unable to delete temporary file: ${path}`)
      console.error(error)
    }
  })
}

function cleanTemporaryFiles(context) {
  Object.values(context).forEach(({tmpFilePath}) => cleanTemporaryFile(tmpFilePath))
}

module.exports = {createConvertStream}
