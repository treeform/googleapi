import asyncdispatch, connection, httpclient, json, mimetypes, os, ospaths,
    streams, strformat, uri

const storageRoot = "https://www.googleapis.com/storage/v1"
const uploadRoot = "https://www.googleapis.com/upload/storage/v1"

type 
  CacheKind = enum
    maxAge
    maxStale
    minfresh
    noCache = "no-cache"
    noStore = "no-store"
    noTransform = "no-transform"
    onlyIfCached = "only-if-cached"
  CacheControl = object
    case kind: CacheKind
    of maxAge, maxStale, minfresh:
      seconds: int
    else: discard

proc `$`(cache: CacheControl): string = 
  case cache.kind:
    of maxAge:
      "max-age=" & $cache.seconds
    of maxStale:
      "max-stale=" & $cache.seconds
    of minfresh:
      "min-fresh=" & $cache.seconds
    else: $cache.kind

proc `$`(cacheControls: openArray[CacheControl]): string = 
  for i, cache in cacheControls:
    result.add $cache
    if i != cacheControls.high:
      result.add ","

proc initMaxAge*(s: int): CacheControl = CacheControl(kind: maxAge, seconds: s)
proc initMaxStale*(s: int): CacheControl = CacheControl(kind: maxStale, seconds: s)
proc initMinFresh*(s: int): CacheControl = CacheControl(kind: minfresh, seconds: s)

const 
  NoCache* = CacheControl(kind: noCache)
  NoStore* = CacheControl(kind: noStore)
  NoTransform* = CacheControl(kind: noTransform)
  OnlyIfCached* = CacheControl(kind: onlyIfCached)

var m = newMimetypes()
proc extractFileExt(filePath: string): string =
  var (_, _, ext) = splitFile(filePath)
  return ext

proc upload*(
    conn: Connection,
    bucketId: string,
    objectId: string,
    data: string,
    cacheControl: varargs[CacheControl] = initMaxAge(3600)):
    Future[JsonNode] {.async.} =

  let url = &"{uploadRoot}/b/{bucketId}/o?uploadType=media&name={encodeUrl(objectId)}"

  var client = newAsyncHttpClient()
  client.headers = newHttpHeaders({
    "Authorization": "Bearer " & await conn.getAuthToken(),
    "Content-Length": $data.len,
    "Content-Type": m.getMimetype(objectId.extractFileExt()),
    "Cache-Control": $cacheControl
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
