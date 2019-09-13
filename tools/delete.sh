curl http://localhost:32768/solr/FmsGraph/update -H "Content-type: text/xml" --data-binary '<delete><query>*:*</query></delete>'
curl http://localhost:32768/solr/FmsGraph/update -H "Content-type: text/xml" --data-binary '<commit />'
