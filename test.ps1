=Invoke-WebRequest Uri 'http://127.0.0.1:42002/items/' -Headers {'Authorization'='Bearer my-secret-api-key'} -UseBasicParsing;.StatusCode; .Content
