Took me a bit to grock what that article was really saying with Apache Arrow and Parquet and all these columner formats, and I have some working demo code now...

After playing with it I see the benefits as:

- Use parquet for a smaller footprint on disk
- Load parquet into datafusion
    - I did not see how to do this 
- Use Datafusion for quick operations without having to load all of the data into memory
    - We can load certain columns at a time, and do operations on them, like count, sum, max, etc
- Use datafusion to paginate without loading all into memory
- Use datafusion to diff tabular data