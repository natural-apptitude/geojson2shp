const test = require('ava')
const detectSchema = require('../lib/detect-schema')

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
    {name: 'contenance', type: 'number'},
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
    {name: 'contenance', type: 'number'},
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
    {name: 'contenance', type: 'number'},
    {name: 'created', type: 'date'},
    {name: 'updated', type: 'date'}
  ])
})
