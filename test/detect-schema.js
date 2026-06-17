const test = require('ava')
const detectSchema = require('../lib/detect-schema')
const {detectSchemaFromFeatures} = require('../lib/detect-schema')

test('detect simple schema', t => {
  const properties = {
    id: '31555801AB0409',
    commune: '31555',
    prefixe: '801',
    section: 'AB',
    numero: '409',
    contenance: 18669,
    created: '2015-06-11',
    updated: '2015-10-06'
  }
  t.deepEqual(detectSchema(properties), [
    {name: 'id', type: 'character', length: 80},
    {name: 'commune', type: 'character', length: 80},
    {name: 'prefixe', type: 'character', length: 80},
    {name: 'section', type: 'character', length: 80},
    {name: 'numero', type: 'character', length: 80},
    {name: 'contenance', type: 'number', precision: 0},
    {name: 'created', type: 'date'},
    {name: 'updated', type: 'date'}
  ])
})

test('detect simple schema w 100 char string', t => {
  const properties = {
    id: '31555801AB0409',
    commune: '31555',
    prefixe: '801',
    section: 'nCu0veuUw7X4FT5JfOnkjkSjHNsrDXi10tssNy2dZNUrKomXlVcvQtjPwDrNFENv8FTMWi6e8jd9ojV0D5s5N77gY1BKn7CVN5DM',
    numero: '409',
    contenance: 18669,
    created: '2015-06-11',
    updated: '2015-10-06'
  }
  t.deepEqual(detectSchema(properties), [
    {name: 'id', type: 'character', length: 80},
    {name: 'commune', type: 'character', length: 80},
    {name: 'prefixe', type: 'character', length: 80},
    {name: 'section', type: 'character', length: 100},
    {name: 'numero', type: 'character', length: 80},
    {name: 'contenance', type: 'number', precision: 0},
    {name: 'created', type: 'date'},
    {name: 'updated', type: 'date'}
  ])
})

test('detect simple schema w 300 char string, capping the field length at the max value (254)', t => {
  const properties = {
    id: '31555801AB0409',
    commune: '31555',
    prefixe: '801',
    section: 'sV2yO66lqPuJ1xboIYgNRw64XYot32dvmQPiZn93kDAMEw3TBsThYA0NQWKCT8YdnUIeWoPiVjGfqRt7USKGfUkBbscmlJ0LIFcndlpTxRxAXuBe9xMGfQn7hWCH8Dhi0DrMy8YeO0jcELakgYXJrVRaFI6yX43NzyHRd12WVbSXfMxYfZ9kzmtB59Uv2TXQLJ3ApxMWfeNlczyaXeyH5D6mhX4PahSmfXtfVrQSUDuqNlpoz7DfyHfL7Vxpt4U70MpSeOFwb9vcnjvy6dQS9mFkJh6OpTvC8duUUIy5qmRT',
    numero: '409',
    contenance: 18669,
    created: '2015-06-11',
    updated: '2015-10-06'
  }
  t.deepEqual(detectSchema(properties), [
    {name: 'id', type: 'character', length: 80},
    {name: 'commune', type: 'character', length: 80},
    {name: 'prefixe', type: 'character', length: 80},
    {name: 'section', type: 'character', length: 254},
    {name: 'numero', type: 'character', length: 80},
    {name: 'contenance', type: 'number', precision: 0},
    {name: 'created', type: 'date'},
    {name: 'updated', type: 'date'}
  ])
})

test('detectSchemaFromFeatures uses precision 0 for all-integer numeric fields', t => {
  const features = [
    {type: 'Feature', properties: {conScore: 1, area: 2}},
    {type: 'Feature', properties: {conScore: 3, area: 4}}
  ]

  t.deepEqual(detectSchemaFromFeatures(features), [
    {name: 'conScore', type: 'number', precision: 0},
    {name: 'area', type: 'number', precision: 0}
  ])
})

test('detectSchemaFromFeatures infers decimal precision across features', t => {
  const features = [
    {type: 'Feature', properties: {metBUnits: 1.2}},
    {type: 'Feature', properties: {metBUnits: 3.45}}
  ]

  t.deepEqual(detectSchemaFromFeatures(features), [
    {name: 'metBUnits', type: 'number', precision: 2}
  ])
})

test('detectSchemaFromFeatures uses higher precision when a field mixes integers and decimals', t => {
  const features = [
    {type: 'Feature', properties: {value: 1}},
    {type: 'Feature', properties: {value: 2.5}}
  ]

  t.deepEqual(detectSchemaFromFeatures(features), [
    {name: 'value', type: 'number', precision: 1}
  ])
})
