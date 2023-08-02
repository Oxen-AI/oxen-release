# Oxen CLI testing with Aruba

UNDER ACTIVE CONSTRUCTION

## Setup 

1. From the `cli-test` folder, run `bundle install` to install the required gems. 

2. Create a `.env` file in the `cli-test` folder containing the credentials for the test user (@test): 
    
    ```
    OXEN_API_KEY=<test-user-API-key>
    ```

## Running all tests
```bash
$ bundle exec rspec
```

## Running specific tests 
```bash
$ bundle exec rspec spec/spec_remote_hub/remote_hub_remove_image_spec.rb
```

## Current limitations 
- Currently only running tests against remote hub - TODO add local and local remote 
- Long setup times - will be improved substantially w/ Oxen 0.7.0
- Redundant setup / teardown steps - need to further investigate ways to create better fixtures in aruba
- Performance metrics currently only printed to console - TODO create oxen-native report and clean up console output

## Setup for running local tests 

(To avoid re-cloning on every test run for local tests)

Switch to fixtures directory: 
```bash 
cd spec/fixtures
```

Run seed script locally to set up local fixture repos 
```bash
source create_fixtures.sh
```
