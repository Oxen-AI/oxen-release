from oxen.loaders import RegressionLoader
import os


def test_regression_loader(house_prices_local_repo_no_commits):
    repo = house_prices_local_repo_no_commits
    data_file = os.path.join(repo.path, "prices.csv")

    loader = RegressionLoader(data_file, "price", ["sqft", "num_bed", "num_bath"])
    result = loader.run()
    assert len(result) == 2

    # is a dataframe
    assert result[0].height == 5
    assert result[0].width == 3

    # is a series
    assert result[1].len() == 5

    print(result[0])
    print(result[1])
