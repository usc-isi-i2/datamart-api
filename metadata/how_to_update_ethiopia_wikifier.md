## Ethiopia Wikifier
### Dependency
The ethioipia wikifier depend on Elastic Search service (currently running on version 5.6). To run the service, you need to upload the proper index file `region-ethiopia-exploded-edges.tsv` in this folder.

To update the index (add more ethiopia related nodes for zones / regions / woredas), please follow this example:

|node1   | label       |node2
|-----   | -----       |-----
|Q4850656| P31       |   Q13221722
|Q4850656|label      |  Bale Gasegar  
|Q4850656|P2006190001|Q202107  
|Q4850656|P2006190002|Q646859  
|Q4850656|P2006190003|Q4850656

- `node1` column indicates the new node value, usually it should starts from `Q` with some numbers.
- First label `P31` is `instance of`, depending on the node level, it will be different
- - if it is a woreda, the node2 values should be `Q13221722` (third-level administrative country subdivision)
- - if it is a zone, the node2 values should be `Q13220204` (second-level administrative country subdivision)
- -  if it is a area / region, the node2 values should be `Q10864048` (first-level administrative country subdivision)

- Second `label` is the name of this place, which will be indexed in Elastic Search. Multiple label is allowed if this place has multiple names.
- 3rd / 4th / 5th lines `P2006190001/P2006190002/P2006190003` is used to indicate the details location information. `P2006190001` indicates the first-level administrative location node, `P2006190002` indicates the second-level administrative location node, `P2006190003` indicates the third-level administrative location node. Depending on the level of current node, it may only have `P2006190001` for area / region nodes, and only have `P2006190001` / `P2006190002` for zone nodes.

Once those information finished, you can append the new lines to the bottom of the file. Then, run following codes in python to upload the index file:
```
from api.variable.ethiopia_wikifier import EthiopiaWikifier
# please update es_server and es_index to proper value if needed
test = EthiopiaWikifier(es_server="", es_index="")
test.upload_to_es("region-ethiopia-exploded-edges.tsv")
'''
# then the program will print something like this to indicate upload finish:
Totally skipped 0 nodes in black list
Done!
Index: ethiopia_wikifier_index already exists...
create response: 200
Finished loading the elasticsearch index
'''
```