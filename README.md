
![Google Cloud Logo](https://cloud.google.com/_static/images/cloud/icons/favicons/onecloud/super_cloud.png)

## Google API - for Nim.

This is a gowing collection of google APIs for nim.

So far it has:

* Bigquery
* Compute
* Datastore

Feel free to add new services as they are wrapped. But it might just be easier to use REST Api.


## REST API

But you can always access Google API through REST. For example find the REST documniation you want like:

https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery

There you see a url and the json to send:

```POST https://datastore.googleapis.com/v1/projects/{projectId}:runQuery```

Just construct your own JSON and use `con.get`, `con.post`, `con.put` or `con.delete`:

```nim
const dsRoot = "https://datastore.googleapis.com/v1"
let data = %* {
  "gqlQuery": {
    "allowLiterals": true,
    "queryString": queryString
  }
}
return await conn.post(&"{dsRoot}/projects/{projectId}:runQuery", data)
```

No now you can use any google library with this trick!

## Service accounts

There are many ways to access google APIs but the best way is service accounts. In the end you'll end up using service accounts so might as well start now.

You can create a service account here: https://cloud.google.com/iam/docs/creating-managing-service-accounts

Then you download the JSON for your service account and load that to create a connection.

```nim
var conn = await newConnection("your_service_account.json")
```

Service account is like an email + public key (with with some extra JSON) that you use to access any google service.

You might run into permission issues with your service account, simply share any google project/service/dataset/page/document/sheet/table ... with the email of the service account, like as if it was a real person.