
## Remove images from the test data to get better stats

Clone the repo

`oxen clone http://0.0.0.0:3000/repositories/SmallCatDog`

Pull all the data

`oxen pull`

Checkout branch so we can safely remove data

`oxen checkout -b remove-test-data`

Remove an image file

`rm test/10.jpg`

Remove that reference from the test_annotations

`head -n 9 annotations/test_annotations.txt > annotations/test_annotations_modified.txt`

`mv annotations/test_annotations_modified.txt annotations/test_annotations.txt`

See that they have been removed in status

`oxen status`

Add the changes

`oxen add annotations/test_annotations.txt`

`oxen add test/10.jpg`

See that they show up as removed and modified in the status

`oxen status`

Commit the removals

`oxen commit -m "remove 10.jpg from test"`

Revert back to main to show that we can revert

`oxen checkout main`
