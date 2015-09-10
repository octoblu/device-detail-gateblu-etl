_             = require 'lodash'
url           = require 'url'
async         = require 'async'
moment        = require 'moment'
request       = require 'request'
ElasticSearch = require 'elasticsearch'

QUERY = require './query.json'

class Command
  constructor : ->
    sourceElasticsearchUrl       = process.env.SOURCE_ELASTICSEARCH_URL ? 'localhost:9200'
    @destinationElasticsearchUrl = process.env.DESTINATION_ELASTICSEARCH_URL ? 'localhost:9200'
    @captureRangeInMinutes       = process.env.CAPTURE_RANGE_IN_MINUTES

    @sourceElasticsearch = new ElasticSearch.Client host: sourceElasticsearchUrl

  run: =>
    @search @query(), (error, result) =>
      throw error if error?

      connectors = @normalize result
      async.each connectors, @update, (error) =>
        throw error if error?
        console.log "it's done...maybe?"
        process.exit 0

  query: =>
    return QUERY unless @captureRangeInMinutes?

    captureSince = moment().subtract parseInt(@captureRangeInMinutes), 'minutes'

    query = _.cloneDeep QUERY
    query.aggs.addGatebluDevice.filter.and.push({
      range:
        _timestamp:
          gte: captureSince
    })

    return query

  update: (connector, callback) =>
    uri = url.format
      protocol: 'http'
      host: @destinationElasticsearchUrl
      pathname: "/gateblu_device_add_history/event/#{connector.connector}"

    console.log "updating with connector...", connector

    request.put uri, json: connector, (error, response, body) =>
      return callback error if error?
      return callback new Error(JSON.stringify body) if response.statusCode >= 300
      callback null

  search: (body, callback=->) =>
    @sourceElasticsearch.search({
      index: 'device_status_gateblu'
      type:  'event'
      search_type: 'count'
      body:  body
    }, callback)

  normalize: (result) =>
    buckets = result.aggregations.addGatebluDevice.group_by_connector.buckets
    _.map buckets, (bucket) =>
      connector: bucket.key
      workflow: 'device-add-to-gateblu'
      total: bucket.beginRecord.doc_count
      successes: bucket.endRecord.doc_count
      failures: bucket.beginRecord.doc_count - bucket.endRecord.doc_count

command = new Command()
command.run()
