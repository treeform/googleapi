import asyncdispatch, cgi, httpclient, json, jwt, os, streams, strformat, times

const bqRoot = "https://www.googleapis.com/bigquery/v2"

type
  Connection* = ref object
    authToken: string
    authTokenExpireTime: float64
    email*: string
    privateKey: string
    scope: string

  GoogleException* = object of Exception

proc loadServiceAccount(
  conn: Connection,
  clientEmail: string,
  privateKey: string,
) =
  conn.email = clientEmail
  conn.privateKey = privateKey
  # Define needed scopes
  conn.scope = "https://www.googleapis.com/auth/cloud-platform " &
    "https://www.googleapis.com/auth/logging.write " &
    "https://www.googleapis.com/auth/drive " &
    "https://www.googleapis.com/auth/datastore"

proc loadServiceAccount(
  conn: Connection,
  serviceAccountPath: string
) =
  let serviceAccount = parseJson(readFile(serviceAccountPath))
  conn.loadServiceAccount(
    serviceAccount["client_email"].getStr(),
    serviceAccount["private_key"].getStr()
  )

proc newConnection*(serviceAccountPath: string): Future[Connection] {.async.} =
  var conn = Connection()
  conn.loadServiceAccount(serviceAccountPath)
  return conn

proc newConnection*(clientEmail, privateKey: string): Future[Connection] {.async.} =
  var conn = Connection()
  conn.loadServiceAccount(clientEmail, privateKey)
  return conn

proc getAuthToken*(conn: Connection): Future[string] {.async.} =
  if conn.authTokenExpireTime > epochTime():
    return conn.authToken

  let now = epochTime()

  # var token = quickjwt.sign(
  #   header = %*{
  #     "alg": "RS256",
  #     "typ": "JWT"
  #   },
  #   claim = %*{
  #     "iss": conn.email,
  #     "scope": conn.scope,
  #     "aud": "https://www.googleapis.com/oauth2/v4/token",
  #     "exp": int(epochTime() + 60 * 60),
  #     "iat": int(epochTime())
  #   },
  #   secret = conn.privateKey
  # )

  let header = %*{
    "alg": "RS256",
    "typ": "JWT"
  }
  let claims = %*{
    "iss": conn.email,
    "scope": conn.scope,
    "aud": "https://www.googleapis.com/oauth2/v4/token",
    "exp": int(epochTime() + 60 * 60),
    "iat": int(epochTime())
  }
  var jwtObj = initJWT(header.toHeader, claims.toClaims)
  jwtObj.sign(conn.privateKey)
  var token = $jwtObj

  # var token = signJwt(
  #   header = %*{
  #     "alg": "RS256",
  #     "typ": "JWT"
  #   },
  #   claims = %*{
  #     "iss": conn.email,
  #     "scope": conn.scope,
  #     "aud": "https://www.googleapis.com/oauth2/v4/token",
  #     "exp": int(now + 60 * 60),
  #     "iat": int(now)
  #   },
  #   secret = conn.privateKey
  # )
  # echo "key took: ", epochTime() - now

  let postdata = "grant_type=" & encodeUrl(
    "urn:ietf:params:oauth:grant-type:jwt-bearer") & "&assertion=" & token

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
    raise newException(GoogleException, "Could not get google AuthToken")

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

proc delete*(conn: Connection, url: string):
  Future[JsonNode] {.async.} =
  ## Generic patch request
  var client = newAsyncHttpClient()
  client.headers = newHttpHeaders({
    "Authorization": "Bearer " & await conn.getAuthToken(),
    "Content-Type": "application/json"
  })
  let resp = await client.request(url, httpMethod = HttpDelete)
  let resultStr = await resp.bodyStream.readAll()
  result = parseJson(resultStr)
  client.close()
