import json, os, strformat, streams, asyncdispatch
import connection

const dsRoot = "https://datastore.googleapis.com/v1"

proc runQuery*(conn: Connection, projectId, queryString: string): Future[JsonNode] {.async.} =
  let data = %* {
    "gqlQuery": {
      "allowLiterals": true,
      "queryString": queryString
    }
  }
  return await conn.post(&"{dsRoot}/projects/{projectId}:runQuery", data)


when isMainModule:

  proc main() {.async.} =
    var conn = await newConnection("your_service_account.json")
    var res = await conn.runQuery(
      "your project",
      "select 1"
    )
    var page = res["batch"]["entityResults"][0]
    echo page["entity"]["properties"]["name"]["stringValue"].getStr()
    echo page["entity"]["properties"]["security_level"]["integerValue"].getStr()
    echo page["entity"]["properties"]["data_json"]["blobValue"].getStr()

  waitFor main()
