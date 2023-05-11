import oxen

repo = oxen.RemoteRepo("ox/CatsVsDogs", host="0.0.0.0:3001")

train_file = "annotations/train.csv"
dataset = oxen.Dataset(
    repo,
    paths=[train_file],
)
df = dataset.df(train_file)
print(df)
