import json, os, strformat, streams, asyncdispatch
import connection

const sheetsRoot = "https://sheets.googleapis.com/v4"


proc getSpreadsheet*(
    conn: Connection,
    spreadsheetId: string
  ): Future[JsonNode] {.async.} =

    return await conn.get(
    &"{sheetsRoot}/spreadsheets/{spreadsheetId}"
    )

proc getValues*(
    conn: Connection,
    spreadsheetId,
    valueRange: string
  ): Future[JsonNode] {.async.} =

    return await conn.get(
    &"{sheetsRoot}/spreadsheets/{spreadsheetId}/values/{valueRange}"
    )

proc setValues*(
    conn: Connection,
    spreadsheetId,
    valueRange: string,
    data: JsonNode
  ): Future[JsonNode] {.async.} =

    return await conn.put(
    &"{sheetsRoot}/spreadsheets/{spreadsheetId}/values/{valueRange}?valueInputOption=USER_ENTERED",
    data
    )

when isMainModule:

    proc main() {.async.} =

        var conn = waitFor newConnection("your_service_account.json")

        let spreadsheetId = "... get this value from the sheet url ..."
        var spreadsheetJson = await conn.getSpreadsheet(spreadsheetId)
        echo spreadsheetJson

    waitFor main()
