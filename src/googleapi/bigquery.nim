import json, os, strformat, streams, asyncdispatch
import connection, print

const bqRoot = "https://www.googleapis.com/bigquery/v2"

type
  DatasetReference* = ref object
    datasetId*: string
    projectId*: string

  Dataset* = ref object
    kind*: string
    id*: string
    datasetReference*: DatasetReference
    location*: string

  TableReference* = ref object
    datasetId*: string
    projectId*: string
    tableId*: string

  Table* = ref object
    kind*: string
    id*: string
    tableReference*: TableReference
    `type`*: string
    creationTime*: string


proc getDatasets*(conn: Connection, projectId: string): Future[seq[Dataset]] {.async.} =
  let dataJson = await conn.get(&"{bqRoot}/projects/{projectId}/datasets")
  return to(dataJson["datasets"], seq[Dataset])


proc getTables*(conn: Connection, projectId: string, datasetId: string): Future[seq[Table]] {.async.} =
  let dataJson = await conn.get(&"{bqRoot}/projects/{projectId}/datasets/{datasetId}/tables?maxResults=10000")
  if "tables" notin dataJson:
    return
  return to(dataJson["tables"], seq[Table])


proc getTable*(conn: Connection, projectId, datasetId, tableId: string): Future[JsonNode] {.async.} =
  return await conn.get(&"{bqRoot}/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/")


proc insertQueryJob*(
    conn: Connection,
    projectId: string,
    sqlQuery: string,
    cache: bool = true,
    maxResults: int = 1000000
  ): Future[string] {.async.} =
  ## starts a bigquery query job
  let body = %*{
    "configuration": {
      "query": {
        "query": sqlQuery,
        "useQueryCache": cache,
        "maxResults": maxResults
      }
    }
  }
  var url = &"{bqRoot}/projects/" & projectId & "/jobs"
  var jsonData = await conn.post(url, body)
  if "error" in jsonData:
    raise newException(Exception, $jsonData["error"])
  return jsonData["jobReference"]["jobId"].str


proc insertQueryJobIntoTable*(
    conn: Connection,
    projectId: string,
    sqlQuery: string,
    toProjectId: string,
    toDatasetId: string,
    toTableId: string,
    overwrite = false
    ):
  Future[string] {.async.} =
  ## starts a bigquery query job to put results into a table
  let body = %*{
    "configuration": {
      "query": {
        "query": sqlQuery,
        "createDisposition": "CREATE_IF_NEEDED",
        "writeDisposition": "WRITE_TRUNCATE",
        "destinationTable": {
          "projectId": toProjectId,
          "datasetId": toDatasetId,
          "tableId": toTableId
        }
      }
    }
  }
  var url = &"{bqRoot}/projects/" & projectId & "/jobs"
  var jsonData = await conn.post(url, body)
  if "error" in jsonData:
    raise newException(Exception, $jsonData["error"])
  return jsonData["jobReference"]["jobId"].str


proc pollQueryJob*(conn: Connection, projectId: string, jobId: string, maxResults: int = 10000): Future[JsonNode] {.async.} =
  ## ask google results, if they are not done you will get jobComplete = false
  var url = &"{bqRoot}/projects/" & projectId & "/queries/" & jobId & "?maxResults=" & $maxResults
  return await conn.get(url)


proc cancelQueryJob*(conn: Connection, projectId: string, jobId: string): Future[JsonNode] {.async.} =
  ## ask google results, if they are not done you will get jobComplete = false
  var url = &"{bqRoot}/projects/" & projectId & "/jobs/" & jobId & "/cancel"
  return await conn.post(url, %*{})


proc tableInsertAll*(conn: Connection, projectId, datasetId, tableId: string, rows: seq[JsonNode]): Future[JsonNode] {.async.} =
  ## insert data into bigquery table
  assert rows.len > 0
  var newRows: seq[JsonNode]
  for row in rows:
    newRows.add %*{
      "json": row
    }
  let body = %*{
    "kind": "bigquery#tableDataInsertAllRequest",
    "skipInvalidRows": true,
    "ignoreUnknownValues": false,
    "rows": newRows
  }
  var url = &"{bqRoot}/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/insertAll"
  var jsonResp = await conn.post(url, body)
  return jsonResp


proc tableInsert*(
    conn: Connection,
    projectId, datasetId: string,
    table: JsonNode
  ): Future[JsonNode] {.async.} =
  ## Creates a new bigquery table
  var url = &"{bqRoot}/projects/{projectId}/datasets/{datasetId}/tables"
  var jsonResp = await conn.post(url, table)
  return jsonResp

proc tablePatch*(
    conn: Connection,
    projectId, datasetId, tableId: string,
    table: JsonNode
  ): Future[JsonNode] {.async.} =
  ## Alters table keepting the data, if table can't be updated safely returns error.
  var url = &"{bqRoot}/projects/{projectId}/datasets/{datasetId}/tables/{tableId}"
  var jsonResp = await conn.patch(url, table)
  return jsonResp

proc tableUpdate*(
    conn: Connection,
    projectId, datasetId, tableId: string,
    table: JsonNode
  ): Future[JsonNode] {.async.} =
  ## Overwrites a table potentially deleting all data.
  var url = &"{bqRoot}/projects/{projectId}/datasets/{datasetId}/tables/{tableId}"
  var jsonResp = await conn.put(url, table)
  return jsonResp



when isMainModule:

  proc main() {.async.} =
    var conn = await newConnection("your_service_account.json")

    block:
      for ds in await conn.getDatasets("your-project"):
          print ds.datasetReference.datasetId, ds.location
          for tb in await conn.getTables("your-project", ds.datasetReference.datasetId):
            print "    ", tb.tableReference.tableId

    block:
      let jobId = await conn.insertQueryJob("your-project", "select 1")
      print jobId
      while true:
        let resultsJson = await conn.pollQueryJob("your-project", jobId)
        echo pretty resultsJson
        if resultsJson["jobComplete"].getBool == true:
          break

    block:
      let jobId = await conn.insertQueryJob("your-project", "select 1")
      print jobId
      let resultsJson = await conn.cancelQueryJob("your-project", jobId)
      echo pretty resultsJson

  waitFor main()