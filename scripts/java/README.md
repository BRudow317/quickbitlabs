# MyUtilPackage

A bunch of stuff that helps me out, the list is probably outdated:

```text   
MyFileUtil:
    readFile(path)                           - Read file to string
    readFileLines(path)                      - Read file as lines
    writeFile(path, content, append)         - Write string to file
    copyFile(src, dest, overwrite)           - Copy file
    moveFile(src, dest)                      - Move/rename file
    deleteFile(path)                         - Delete file/directory
    listFiles(dir, ext, recursive)           - List files
    getFileInfo(path)                        - Get file metadata
    zipFiles(zipPath, files)                 - Create ZIP archive
    watchDirectory(dir, handler)             - Watch for changes


SERVLET REQUEST:
    logRequest(request, logger)              - Log request details
    getRequestBody(request)                  - Read body as string
    getRequestBodyAs(request, class)         - Parse body to object
    getRequestParams(request)                - Get params as Map
    getRequestHeaders(request)               - Get headers as Map
    getClientIp(request)                     - Get real client IP
    getBearerToken(request)                  - Extract Bearer token

SERVLET RESPONSE:
    sendJson(response, data, status)         - Send JSON response
    sendError(response, status, message)     - Send error response
    sendSuccess(response, message, data)     - Send success response
    sendFile(response, file, filename)       - Send file download
    sendHtml(response, html, status)         - Send HTML response
    sendRedirect(req, resp, url, flash)      - Redirect with flash
    setCorsHeaders(response, origin, methods) - Set CORS headers

JSON/JACKSON:
    toJson(object)                           - Object to JSON
    toJsonPretty(object)                     - Pretty printed JSON
    fromJson(json, class)                    - JSON to object
    fromJsonList(json, class)                - JSON to List
    fromJsonMap(json)                        - JSON to Map
    jsonNode(json)                           - Parse to JsonNode
    createJsonObject()                       - Create ObjectNode
    createJsonArray()                        - Create ArrayNode
    mergeJson(base, overlay)                 - Merge JSON objects



FORMAT CONVERSION:
    jsonToXml(json, rootName)                - JSON → XML
    xmlToJson(xml)                           - XML → JSON
    jsonToYaml(json)                         - JSON → YAML
    yamlToJson(yaml)                         - YAML → JSON
    tomlToJson(toml)                         - TOML → JSON
    jsonToToml(json)                         - JSON → TOML
    convertFormat(content, from, to)         - Universal converter

DATABASE (PostgreSQL):
    DatabaseConfig(host, port, db, user, pw) - Config holder
    getConnection(config)                    - Get connection
    withConnection(config, consumer)         - Managed connection
    withTransaction(config, consumer)        - Transaction wrapper
    queryForList(conn, sql, params)          - Query to List<Map>
    queryForObject(conn, sql, class, params) - Query to object
    execute(conn, sql, params)               - Execute update
    insert(conn, sql, params)                - Insert & get ID
    batchInsert(conn, sql, rows)             - Batch insert
    tableExists(conn, tableName)             - Check table exists
    getTableColumns(conn, tableName)         - Get column metadata
```