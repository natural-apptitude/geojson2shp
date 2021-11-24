const DATE_REGEX = /^(\d{4})-(\d\d)-(\d\d)/
// as specified by shapefile spec
const MAX_STRING_FIELD_LENGTH = 254

function detectSchema(properties, fieldLengths = {}) {
  const schema = []
  for (const key of Object.keys(properties)) {
    const value = properties[key]
    if (typeof value === 'string') {
      if (DATE_REGEX.test(value)) {
        schema.push({name: key, type: 'date'})
      } else {
        const length = Math.max(80, Math.min((fieldLengths[key] || value.length), MAX_STRING_FIELD_LENGTH));
        schema.push({name: key, type: 'character', length})
      }
    } else if (typeof value === 'number') {
      schema.push({name: key, type: 'number'})
    } else if (typeof value === 'boolean') {
      schema.push({name: key, type: 'boolean'})
    }
  }

  return schema
}

module.exports = detectSchema
