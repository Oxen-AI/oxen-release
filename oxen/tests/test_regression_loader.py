from oxen.loaders import RegressionLoader
import os


def test_chat_loader(house_prices_local_repo_no_commits):
    repo = house_prices_local_repo_no_commits
    data_file = os.path.join(repo.path, "prices.csv")

    loader = RegressionLoader(data_file, "price", ["sqft", "num_bed", "num_bath"])
    result = loader.run()
    assert len(result) == 2

    # is a series
    assert result[0].len() == 5

    # is a dataframe
    assert result[1].height == 5
    assert result[1].width == 3
