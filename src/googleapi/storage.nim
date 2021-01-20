import json, os, strformat, streams, asyncdispatch, uri, httpclient, mimetypes, ospaths
import connection

const storageRoot = "https://www.googleapis.com/storage/v1"
const uploadRoot = "https://www.googleapis.com/upload/storage/v1"


var m = newMimetypes()
proc extractFileExt(filePath: string): string =
  var (_, _, ext) = splitFile(filePath)
  return ext


proc upload*(
    conn: Connection,
    bucketId: string,
    objectId: string,
    data: string):
    Future[JsonNode] {.async.} =

  let url = &"{uploadRoot}/b/{bucketId}/o?uploadType=media&name={encodeUrl(objectId)}"

  var client = newAsyncHttpClient()
  client.headers = newHttpHeaders({
    "Authorization": "Bearer " & await conn.getAuthToken(),
    "Content-Length": $data.len,
    "Content-Type": m.getMimetype(objectId.extractFileExt())
  })
  let resp = await client.post(url, data)
  let resultStr = await resp.bodyStream.readAll()
  result = parseJson(resultStr)
  client.close()


proc download*(
    conn: Connection,
    bucketId: string,
    objectId: string):
    Future[string] {.async.} =

  let url = &"{storageRoot}/b/{bucketId}/o/{objectId}?alt=media"
  var client = newAsyncHttpClient()
  client.headers = newHttpHeaders({
    "Authorization": "Bearer " & await conn.getAuthToken(),
  })
  let resp = await client.get(url)
  result = await resp.bodyStream.readAll()


proc getMeta*(
    conn: Connection,
    bucketId: string,
    objectId: string):
    Future[JsonNode] {.async.} =

  return await conn.get(&"{storageRoot}/b/{bucketId}/o/{objectId}")

proc list*(
    conn: Connection,
    bucketId: string,
    prefix: string):
    Future[JsonNode] {.async.} =
  return await conn.get(&"{storageRoot}/b/{bucketId}/o?prefix={encodeUrl(prefix)}")


when isMainModule:

  proc main() {.async.} =

    var conn = waitFor newConnection("your_service_account.json")

    let data = "this is contents that can be binary too!"
    var err = conn.upload("your_bucket", "path/key/to/object.txt", data)
    var data2 = await conn.download("your_bucket", "path/key/to/object.txt")
    var meta = await conn.getMeta("your_bucket", "path/key/to/object.txt")


  waitFor main()
