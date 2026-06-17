const DATE_REGEX = /^(\d{4})-(\d\d)-(\d\d)/
// as specified by shapefile spec
const MAX_STRING_FIELD_LENGTH = 254
const MAX_NUMERIC_PRECISION = 15

const getDecimalPlaces = (value) => {
  if (!Number.isFinite(value) || Number.isInteger(value)) {
    return 0
  }

  const asString = value.toString()
  if (asString.includes('e') || asString.includes('E')) {
    const fixed = value.toFixed(MAX_NUMERIC_PRECISION)
    const fraction = fixed.split('.')[1] || ''
    return fraction.replace(/0+$/, '').length
  }

  const fraction = asString.split('.')[1] || ''
  return fraction.length
}

const inferNumericPrecisions = (features) => {
  const precisions = {}

  for (const feature of features) {
    for (const [key, value] of Object.entries(feature.properties || {})) {
      if (typeof value !== 'number' || !Number.isFinite(value)) {
        continue
      }
      precisions[key] = Math.max(precisions[key] ?? 0, getDecimalPlaces(value))
    }
  }

  return precisions
}

const getNumberField = (key, value, numericPrecisions) => {
  const field = {name: key, type: 'number'}
  const inferredPrecision = numericPrecisions[key]

  if (Number.isInteger(inferredPrecision)) {
    field.precision = inferredPrecision
  } else if (Number.isInteger(value)) {
    field.precision = 0
  }

  return field
}

function detectSchema(properties, fieldLengths = {}, numericPrecisions = {}) {
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
      schema.push(getNumberField(key, value, numericPrecisions))
    } else if (typeof value === 'boolean') {
      schema.push({name: key, type: 'boolean'})
    }
  }

  return schema
}

const detectSchemaFromFeatures = (features, fieldLengths = {}) => {
  const firstProperties = features.find((feature) => feature.properties)?.properties
  if (!firstProperties) {
    return []
  }

  const numericPrecisions = inferNumericPrecisions(features)
  return detectSchema(firstProperties, fieldLengths, numericPrecisions)
}

module.exports = detectSchema
module.exports.detectSchemaFromFeatures = detectSchemaFromFeatures
module.exports.inferNumericPrecisions = inferNumericPrecisions
