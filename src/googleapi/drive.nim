
import asyncdispatch, connection, json, os, streams, strformat, strutils, uri

const driveRoot = "https://www.googleapis.com/drive/v3"

proc list*(conn: Connection, q: string): Future[JsonNode] {.async.} =
  return await conn.get(&"{driveRoot}/files?q={encodeUrl(q)}")

proc list*(
  conn: Connection, q: string,
  fields: seq[string]
): Future[JsonNode] {.async.} =
  let fieldsStr = encodeUrl(fields.join(","))
  return await conn.get(&"{driveRoot}/files?q={encodeUrl(q)}&fields={fieldsStr}")

when isMainModule:

  proc main() {.async.} =
    var conn = await newConnection("your_service_account.json")
    var res = await conn.list("")
    echo pretty res

  waitFor main()
