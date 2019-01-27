workflow "Build" {
  on = "push"
  resolves = ["cargo"]
}

action "cargo" {
  uses = "icepuma/rust-github-action@master"
  args = "cargo build && cargo test"
}
