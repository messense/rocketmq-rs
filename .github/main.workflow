workflow "Build" {
  on = "push"
  resolves = ["Build"]
}

action "Build" {
  uses = "icepuma/rust-github-action@master"
  args = "cargo build && cargo test"
}
