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

      connectors = @process @normalize result
      async.each connectors, @update, (error) =>
        throw error if error?
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
      {
        connector: bucket.key
        beginTime: bucket.beginRecord.beginTime.value
        endTime:   bucket.endRecord.endTime.value
        workflow: 'device-add-to-gateblu'
      }

  process: (connectors) =>
    _.map connectors, (connector) =>
      {workflow, connector, beginTime, endTime} = connector

      formattedBeginTime = null
      formattedBeginTime = moment(beginTime).toISOString() if beginTime?
      formattedEndTime = null
      formattedEndTime = moment(endTime).toISOString() if endTime?

      elapsedTime = null
      elapsedTime = endTime - beginTime if beginTime? && endTime?

      {
        connector: connector
        workflow: workflow
        beginTime: formattedBeginTime
        endTime: formattedEndTime
        elapsedTime: elapsedTime
        success: endTime?
      }

command = new Command()
command.run()
