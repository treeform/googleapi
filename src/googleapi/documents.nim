import json, os, strformat, streams, asyncdispatch
import connection

const docsRoot = "https://docs.googleapis.com/v1"


proc getDocument*(
    conn: Connection,
    documentId: string
  ): Future[JsonNode] {.async.} =

  return await conn.get(
    &"{docsRoot}/documents/{documentId}"
  )


when isMainModule:

  proc main() {.async.} =

    var conn = waitFor newConnection("your_service_account.json")

    let documentId = "... get this value from the sheet url ..."
    var documentJson = await conn.getDocument(documentId)
    echo documentJson

  waitFor main()
