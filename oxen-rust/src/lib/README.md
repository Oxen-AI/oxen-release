# liboxen 🐂

A library with all of the core oxen functionality. To be exposed via CLI, server, and other language integrations.

## Documentation

Watch the projects source and rebuild docs on change

`cargo install cargo-watch https`

Enable auto refresh on save of the file

`npm install -g browser-sync`

Run docs

```bash
cargo watch -s 'cargo doc && browser-sync start --port 8000 --ss target/doc -s target/doc --directory --no-open'
```

Navigate to http://localhost:8000/liboxen/