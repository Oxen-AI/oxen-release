import oxen

repo = oxen.RemoteRepo("ox/CIFAR-10", host="0.0.0.0:3001")
dataset = oxen.Dataset(repo)
df = dataset.df("train.csv")
print(df)
