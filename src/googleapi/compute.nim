import json, os, strformat, streams, asyncdispatch
import connection

const cmRoot = "https://www.googleapis.com/compute/v1"

proc instancesList*(conn: Connection, projectId, zone: string): Future[JsonNode] {.async.} =
  return await conn.get(&"{cmRoot}/projects/{projectId}/zones/{zone}/instances")


when isMainModule:

  proc main() {.async.} =
    var conn = await newConnection("your_service_account.json")
    var res = await conn.instancesList("your-project", "us-central1-c")
    echo pretty res
    for instnace in res["items"]:
      echo instnace["name"]
      echo instnace["networkInterfaces"][0]["networkIP"]

  waitFor main()
