import jwt, json, times, httpclient, asyncdispatch, cgi, json, os, strformat,
  streams
import print

const bqRoot = "https://www.googleapis.com/bigquery/v2"

type
  Connection* = ref object
    authToken: string
    authTokenExpireTime: float64
    email: string
    privateKey: string
    scope: string

  GoogleExcpetion* = object of Exception


proc loadServiceAccount(
  conn: Connection,
  serviceAccountPath: string) =
  let serviceAccount = parseJson(readFile(serviceAccountPath))
  conn.email = serviceAccount["client_email"].getStr()
  conn.privateKey = serviceAccount["private_key"].getStr()
  # Define needed scopes
  conn.scope = "https://www.googleapis.com/auth/cloud-platform " &
    "https://www.googleapis.com/auth/logging.write " &
    "https://www.googleapis.com/auth/drive"


proc newConnection*(serviceAccountPath: string): Future[Connection] {.async.} =
  var conn = Connection()
  conn.loadServiceAccount(serviceAccountPath)
  return conn


proc getAuthToken*(conn: Connection): Future[string] {.async.} =
  if conn.authTokenExpireTime > epochTime():
    return conn.authToken

  var tok = JWT(
    header: JOSEHeader(alg: RS256, typ: "JWT"),
    claims: toClaims(%*{
    "iss": conn.email,
    "scope": conn.scope,
    "aud": "https://www.googleapis.com/oauth2/v4/token",
    "exp": int(epochTime() + 60 * 60),
    "iat": int(epochTime())
  }))

  tok.sign(conn.privateKey)

  let postdata = "grant_type=" & encodeUrl(
    "urn:ietf:params:oauth:grant-type:jwt-bearer") & "&assertion=" & $tok

  proc request(url: string, body: string): string =
    var client = newHttpClient()
    client.headers = newHttpHeaders({
      "Content-Length": $body.len,
      "Content-Type": "application/x-www-form-urlencoded"
    })
    result = client.postContent(url, body)
    client.close()

  let dataJson = request(
    "https://www.googleapis.com/oauth2/v4/token", postdata).parseJson()

  if "access_token" notin dataJson:
    raise newException(GoogleExcpetion, "Could not get google AuthToken")

  conn.authToken = dataJson["access_token"].str
  conn.authTokenExpireTime = float64(epochTime() + 60 * 60)

  return conn.authToken


proc get*(conn: Connection, url: string):
    Future[JsonNode] {.async.} =
  ## Generic get request
  var client = newAsyncHttpClient()
  client.headers = newHttpHeaders({
    "Authorization": "Bearer " & await conn.getAuthToken(),
    "Content-Type": "application/json"
  })
  let resp = await client.get(url)
  let resultStr = await resp.bodyStream.readAll()
  result = parseJson(resultStr)
  client.close()


proc post*(conn: Connection, url: string, body: JsonNode):
    Future[JsonNode] {.async.} =
  ## Generic post request
  var client = newAsyncHttpClient()
  client.headers = newHttpHeaders({
    "Authorization": "Bearer " & await conn.getAuthToken(),
    "Content-Type": "application/json"
  })
  let resp = await client.post(url, $body)
  let resultStr = await resp.bodyStream.readAll()
  result = parseJson(resultStr)
  client.close()


proc patch*(conn: Connection, url: string, body: JsonNode):
    Future[JsonNode] {.async.} =
  ## Generic patch request
  var client = newAsyncHttpClient()
  client.headers = newHttpHeaders({
    "Authorization": "Bearer " & await conn.getAuthToken(),
    "Content-Type": "application/json"
  })
  let resp = await client.request(url, httpMethod = HttpPatch, $body)
  let resultStr = await resp.bodyStream.readAll()
  result = parseJson(resultStr)
  client.close()


proc put*(conn: Connection, url: string, body: JsonNode):
  Future[JsonNode] {.async.} =
  ## Generic patch request
  var client = newAsyncHttpClient()
  client.headers = newHttpHeaders({
    "Authorization": "Bearer " & await conn.getAuthToken(),
    "Content-Type": "application/json"
  })
  let resp = await client.request(url, httpMethod = HttpPut, $body)
  let resultStr = await resp.bodyStream.readAll()
  result = parseJson(resultStr)
  client.close()